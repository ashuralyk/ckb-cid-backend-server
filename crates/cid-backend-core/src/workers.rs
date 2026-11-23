//! One OS thread per configured Process. Batch submit work runs on that thread.

use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::process::{ProcessEntry, ProcessResolver};
use crate::process_batch::{
    run_process_batch, BatchMode, BatchOutcome, BatchReportDelta, ProbeBudget, ProcessBatchParams,
    RefundWarnings,
};
use crate::rpc::{LiveCell, Rpc};
use crate::scan::ScannedIntention;
use crate::ssri::SsriHost;
use crate::submit::SubmitState;
use crate::worker_commands::WorkerCommands;
use ckb_types::packed::{Byte32, CellDep, OutPoint};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

fn process_thread_name(hash: &[u8; 32]) -> String {
    format!(
        "cid-proc-{:02x}{:02x}{:02x}{:02x}",
        hash[0], hash[1], hash[2], hash[3]
    )
}

fn process_snapshot(process: &ResolvedProcess) -> (OutPoint, Option<OutPoint>) {
    (
        process.live_cell.out_point.clone(),
        process
            .code_cell
            .as_ref()
            .map(|cell| cell.out_point.clone()),
    )
}

#[derive(Default, PartialEq, Eq)]
pub(crate) enum RefundPriority {
    #[default]
    None,
    Ready,
    AwaitingStatus(Byte32),
}

impl RefundPriority {
    pub(crate) fn grant(&mut self) {
        if !matches!(self, Self::AwaitingStatus(_)) {
            *self = Self::Ready;
        }
    }

    pub(crate) fn is_awaiting(&self, tx_hash: &Byte32) -> bool {
        matches!(self, Self::AwaitingStatus(pending) if pending == tx_hash)
    }

    pub(crate) fn confirm_admission(&mut self, tx_hash: &Byte32) {
        if self.is_awaiting(tx_hash) {
            *self = Self::None;
        }
    }
}

/// Per-Process mutable state (digest thread and worker thread via lock).
pub struct ProcessWorkerState {
    pub entry: ProcessEntry,
    pub resolver: ProcessResolver,
    pub submit: SubmitState,
    /// State dependencies of submitted filter refunds, keyed by transaction.
    pub(crate) refund_state_deps: HashMap<Vec<u8>, OutPoint>,
    /// Give refunds the first opportunity after a digest releases Process state.
    pub(crate) refund_priority: RefundPriority,
    pub(crate) refund_warnings: RefundWarnings,
    unfunded_process: Option<(OutPoint, Option<OutPoint>)>,
    /// Shared across assembly jobs within a tick, cleared after all jobs finish.
    pub(crate) probe_budget: Option<ProbeBudget>,
    pub generation: u64,
    /// Exit 41 quarantines a state/code pair for the lifetime of the worker.
    quarantines: HashSet<(OutPoint, Option<OutPoint>)>,
    pub(crate) filter_missing_logged: bool,
    last_process_code: Option<OutPoint>,
}

impl ProcessWorkerState {
    pub fn new(entry: ProcessEntry) -> Self {
        Self::with_generation(entry, 1)
    }

    pub fn with_generation(entry: ProcessEntry, generation: u64) -> Self {
        let resolver = ProcessResolver::for_entry(entry.clone());
        Self {
            entry,
            resolver,
            submit: SubmitState::default(),
            refund_state_deps: HashMap::new(),
            refund_priority: RefundPriority::None,
            refund_warnings: RefundWarnings::default(),
            unfunded_process: None,
            probe_budget: None,
            generation,
            quarantines: HashSet::new(),
            filter_missing_logged: false,
            last_process_code: None,
        }
    }

    pub(crate) fn refresh_process_code(&mut self, cell: Option<&LiveCell>) {
        if let Some(cell) = cell {
            if self.last_process_code.as_ref() != Some(&cell.out_point) {
                self.last_process_code = Some(cell.out_point.clone());
                self.filter_missing_logged = false;
                self.unfunded_process = None;
                self.clear_execution_rejections();
            }
        }
        self.resolver.set_tick_process_code(cell);
    }

    pub(crate) fn quarantine_process(&mut self, process: &ResolvedProcess) {
        self.quarantines.insert(process_snapshot(process));
    }

    pub(crate) fn is_process_quarantined(&self, process: &ResolvedProcess) -> bool {
        self.quarantines.contains(&process_snapshot(process))
    }

    pub(crate) fn mark_process_unfunded(&mut self, process: &ResolvedProcess) {
        self.unfunded_process = Some(process_snapshot(process));
    }

    pub(crate) fn is_process_unfunded(&self, process: &ResolvedProcess) -> bool {
        self.unfunded_process.as_ref() == Some(&process_snapshot(process))
    }

    pub(crate) fn clear_execution_rejections(&mut self) {
        self.submit.rejected_combos.clear();
    }

    pub(crate) fn has_pending_filter_refund(&self, state: &OutPoint) -> bool {
        self.submit.pending.iter().any(|(hash, tx)| {
            !tx.currently_committed && self.refund_state_deps.get(hash) == Some(state)
        })
    }

    pub fn process_type_hash(&self) -> [u8; 32] {
        self.entry.identity_type_hash
    }
}

#[allow(clippy::large_enum_variant)]
enum ProcessWorkerJob {
    Ping(Sender<()>),
    Shutdown,
    RunBatch {
        /// Epoch when the job was enqueued; worker ignores late replies after cancel.
        job_epoch: u64,
        process: ResolvedProcess,
        batch: Vec<ScannedIntention>,
        rest: Arc<[ScannedIntention]>,
        execution_code_key: Vec<u8>,
        tick_cell_deps: Vec<CellDep>,
        cid_locks: Arc<[CidLockIdentity]>,
        refund_policy: RefundPolicy,
        max_assembled_tx_bytes: usize,
        min: u32,
        mode: BatchMode,
        commands: Option<Arc<WorkerCommands>>,
        reply: SyncSender<(BatchOutcome, BatchReportDelta)>,
    },
}

struct PendingBatch {
    reply: Receiver<(BatchOutcome, BatchReportDelta)>,
    rest: Arc<[ScannedIntention]>,
    mode: BatchMode,
}

/// Completed RunBatch poll result for the digest loop.
pub struct PolledBatch {
    pub outcome: BatchOutcome,
    pub delta: BatchReportDelta,
    pub rest: Arc<[ScannedIntention]>,
    pub(crate) mode: BatchMode,
}

/// Handle to a Process worker thread.
pub struct ProcessWorker {
    state: Arc<Mutex<ProcessWorkerState>>,
    job_tx: Sender<ProcessWorkerJob>,
    join: Option<JoinHandle<()>>,
    thread_name: String,
    /// Shared cancel/epoch: bump to invalidate in-flight RunBatch before submit.
    epoch: Arc<AtomicU64>,
    /// True while a RunBatch is outstanding (sent, reply not yet consumed).
    batch_outstanding: Arc<AtomicBool>,
    /// Non-blocking reply slot — digest loop polls within the tick (P1).
    pending: Mutex<Option<PendingBatch>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StartBatchError {
    AlreadyOutstanding,
    WorkerDead,
}

impl ProcessWorker {
    pub fn with_state<R>(&self, f: impl FnOnce(&ProcessWorkerState) -> R) -> R {
        let guard = self.state.lock().expect("process worker state");
        f(&guard)
    }

    pub fn with_state_mut<R>(&self, f: impl FnOnce(&mut ProcessWorkerState) -> R) -> R {
        let mut guard = self.state.lock().expect("process worker state");
        f(&mut guard)
    }

    pub fn thread_name(&self) -> &str {
        &self.thread_name
    }

    pub fn batch_outstanding(&self) -> bool {
        self.batch_outstanding.load(Ordering::SeqCst)
    }

    pub fn ping(&self) -> bool {
        let (tx, rx) = mpsc::channel();
        if self.job_tx.send(ProcessWorkerJob::Ping(tx)).is_err() {
            return false;
        }
        rx.recv_timeout(Duration::from_secs(5)).is_ok()
    }

    /// Non-blocking poll for a completed RunBatch. Never invents outcomes.
    pub fn try_poll_batch(&self) -> Option<PolledBatch> {
        let mut slot = self.pending.lock().expect("pending batch");
        let pending = slot.as_ref()?;
        match pending.reply.try_recv() {
            Ok(result) => {
                let pending = slot.take().expect("pending");
                self.batch_outstanding.store(false, Ordering::SeqCst);
                Some(PolledBatch {
                    outcome: result.0,
                    delta: result.1,
                    rest: pending.rest,
                    mode: pending.mode,
                })
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                let pending = slot.take().expect("pending");
                self.batch_outstanding.store(false, Ordering::SeqCst);
                Some(PolledBatch {
                    outcome: BatchOutcome::Continue,
                    delta: BatchReportDelta {
                        last_error: Some("worker dead: RunBatch reply channel closed".into()),
                        ..BatchReportDelta::default()
                    },
                    rest: pending.rest,
                    mode: pending.mode,
                })
            }
        }
    }

    /// Dispatch RunBatch without blocking the digest loop (P1).
    ///
    /// One in-flight batch per Process (`batch_outstanding`). Outcomes are
    /// collected via [`try_poll_batch`] within the same tick.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_batch(
        &self,
        process: ResolvedProcess,
        batch: Vec<ScannedIntention>,
        execution_code_key: Vec<u8>,
        tick_cell_deps: Vec<CellDep>,
        cid_locks: Arc<[CidLockIdentity]>,
        refund_policy: RefundPolicy,
        max_assembled_tx_bytes: usize,
        rest: Vec<ScannedIntention>,
        min: u32,
        mode: BatchMode,
        commands: Option<Arc<WorkerCommands>>,
    ) -> Result<(), StartBatchError> {
        if self
            .batch_outstanding
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(StartBatchError::AlreadyOutstanding);
        }
        let job_epoch = self.epoch.load(Ordering::SeqCst);
        let rest: Arc<[_]> = rest.into();
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if self
            .job_tx
            .send(ProcessWorkerJob::RunBatch {
                job_epoch,
                process,
                batch,
                rest: rest.clone(),
                execution_code_key,
                tick_cell_deps,
                cid_locks,
                refund_policy,
                max_assembled_tx_bytes,
                min,
                mode,
                commands,
                reply: reply_tx,
            })
            .is_err()
        {
            self.batch_outstanding.store(false, Ordering::SeqCst);
            return Err(StartBatchError::WorkerDead);
        }
        let mut slot = self.pending.lock().expect("pending batch");
        *slot = Some(PendingBatch {
            reply: reply_rx,
            rest,
            mode,
        });
        Ok(())
    }

    /// Bump epoch so an in-flight job fails closed before submit.
    fn cancel_inflight(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn shutdown_inner(&mut self) {
        self.cancel_inflight();
        let _ = self.job_tx.send(ProcessWorkerJob::Shutdown);
        if let Some(handle) = self.join.take() {
            // Keep JoinHandle until join completes — never detach/abandon (P0-2).
            let _ = handle.join();
        }
    }

    fn shutdown(mut self) {
        self.shutdown_inner();
    }
}

impl Drop for ProcessWorker {
    fn drop(&mut self) {
        // P3: cancel + join so we never abandon a mid-submit thread on drop.
        if self.join.is_some() {
            self.shutdown_inner();
        }
    }
}

fn worker_thread_main<R, S>(
    job_rx: Receiver<ProcessWorkerJob>,
    state: Arc<Mutex<ProcessWorkerState>>,
    epoch: Arc<AtomicU64>,
    rpc: Arc<R>,
    ssri: Arc<S>,
) where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    S: SsriHost + Send + Sync + 'static,
{
    for job in job_rx {
        match job {
            ProcessWorkerJob::Ping(reply) => {
                let _ = reply.send(());
            }
            ProcessWorkerJob::Shutdown => break,
            ProcessWorkerJob::RunBatch {
                job_epoch,
                process,
                batch,
                rest,
                execution_code_key,
                tick_cell_deps,
                cid_locks,
                refund_policy,
                max_assembled_tx_bytes,
                min,
                mode,
                commands,
                reply,
            } => {
                if epoch.load(Ordering::SeqCst) != job_epoch {
                    // Cancelled before start — fail closed; do not synthesize success.
                    let skipped = batch.len();
                    let _ = reply.send((
                        BatchOutcome::Continue,
                        BatchReportDelta {
                            skipped,
                            last_error: Some("worker: RunBatch cancelled (stale epoch)".into()),
                            ..BatchReportDelta::default()
                        },
                    ));
                    continue;
                }
                let hash = {
                    let g = state.lock().expect("state");
                    g.process_type_hash()
                };
                let result = run_process_batch(
                    rpc.clone(),
                    ssri.as_ref(),
                    ProcessBatchParams {
                        hash,
                        state: state.clone(),
                        process: &process,
                        batch,
                        rest: &rest,
                        execution_code_key: &execution_code_key,
                        tick_cell_deps: &tick_cell_deps,
                        cid_locks: &cid_locks,
                        refund_policy: &refund_policy,
                        max_assembled_tx_bytes,
                        job_epoch,
                        epoch: epoch.clone(),
                        min,
                        mode,
                        commands,
                    },
                );
                // Always forward the real outcome — especially Submitted* after
                // a successful submit. Epoch cancel is checked inside
                // run_process_batch before submit; do not rewrite results (P3-6).
                let _ = reply.send(result);
            }
        }
    }
}

fn spawn_process_worker<R, S>(
    hash: [u8; 32],
    state: Arc<Mutex<ProcessWorkerState>>,
    rpc: Arc<R>,
    ssri: Arc<S>,
) -> ProcessWorker
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    S: SsriHost + Send + Sync + 'static,
{
    let (job_tx, job_rx) = mpsc::channel();
    let name = process_thread_name(&hash);
    let thread_name = name.clone();
    let state_thread = state.clone();
    let epoch = Arc::new(AtomicU64::new(0));
    let epoch_thread = epoch.clone();
    let batch_outstanding = Arc::new(AtomicBool::new(false));
    let rpc_c = rpc.clone();
    let ssri_c = ssri.clone();
    let join = thread::Builder::new()
        .name(name)
        .spawn(move || worker_thread_main(job_rx, state_thread, epoch_thread, rpc_c, ssri_c))
        .expect("spawn process worker thread");
    ProcessWorker {
        state,
        job_tx,
        join: Some(join),
        thread_name,
        epoch,
        batch_outstanding,
        pending: Mutex::new(None),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerRemoveError {
    InFlight,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerRegisterError {
    /// A RunBatch is outstanding on this Process thread.
    BatchInFlight,
    /// Code identity change refused while submit.pending is non-empty.
    PendingCodeChange,
}

pub struct WorkerPool<R, S> {
    rpc: Arc<R>,
    ssri: Arc<S>,
    workers: HashMap<[u8; 32], ProcessWorker>,
}

impl<R, S> WorkerPool<R, S>
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    S: SsriHost + Send + Sync + 'static,
{
    pub fn new(rpc: Arc<R>, ssri: Arc<S>) -> Self {
        Self {
            rpc,
            ssri,
            workers: HashMap::new(),
        }
    }

    pub fn rpc(&self) -> &Arc<R> {
        &self.rpc
    }

    pub fn ssri(&self) -> &Arc<S> {
        &self.ssri
    }

    pub fn from_entries(entries: Vec<ProcessEntry>, rpc: Arc<R>, ssri: Arc<S>) -> Self {
        let mut pool = Self::new(rpc, ssri);
        for entry in entries {
            let key = entry.identity_type_hash;
            if pool.workers.contains_key(&key) {
                log::warn!(
                    "duplicate ProcessEntry for type hash {:02x?}; keeping first",
                    key
                );
                continue;
            }
            let _ = pool.register(entry);
        }
        pool
    }

    pub fn get(&self, process_type_hash: &[u8; 32]) -> Option<&ProcessWorker> {
        self.workers.get(process_type_hash)
    }

    pub fn get_mut(&mut self, process_type_hash: &[u8; 32]) -> Option<&mut ProcessWorker> {
        self.workers.get_mut(process_type_hash)
    }

    pub fn for_each_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&ProcessWorker),
    {
        for w in self.workers.values() {
            f(w);
        }
    }

    /// Register or update a Process worker.
    ///
    /// Refuses while a RunBatch is outstanding. Refuses a code-identity change
    /// while `submit.pending` is non-empty (P0-2 / P1-5).
    pub fn register(&mut self, entry: ProcessEntry) -> Result<bool, WorkerRegisterError> {
        let key = entry.identity_type_hash;
        match self.workers.get(&key) {
            None => {
                let inner = Arc::new(Mutex::new(ProcessWorkerState::with_generation(entry, 1)));
                let worker = spawn_process_worker(key, inner, self.rpc.clone(), self.ssri.clone());
                self.workers.insert(key, worker);
                Ok(true)
            }
            Some(worker) => {
                if worker.batch_outstanding() {
                    return Err(WorkerRegisterError::BatchInFlight);
                }
                let code_changed = worker.with_state(|s| s.entry.code != entry.code);
                let pending = worker.with_state(|s| !s.submit.pending.is_empty());
                if code_changed && pending {
                    return Err(WorkerRegisterError::PendingCodeChange);
                }
                worker.with_state_mut(|state| {
                    state.generation = state.generation.saturating_add(1);
                    state.entry = entry.clone();
                    state.resolver = ProcessResolver::for_entry(entry);
                    if code_changed {
                        state.last_process_code = None;
                        state.filter_missing_logged = false;
                        state.unfunded_process = None;
                        state.clear_execution_rejections();
                    }
                });
                Ok(false)
            }
        }
    }

    pub fn generation(&self, hash: &[u8; 32]) -> Option<u64> {
        self.workers
            .get(hash)
            .map(|w| w.with_state(|s| s.generation))
    }

    pub fn is_pending(&self, hash: &[u8; 32]) -> bool {
        self.workers
            .get(hash)
            .is_some_and(|w| w.with_state(|s| !s.submit.pending.is_empty()))
    }

    pub fn pending_hashes(&self) -> Vec<[u8; 32]> {
        self.workers
            .iter()
            .filter(|(_, w)| w.with_state(|s| !s.submit.pending.is_empty()))
            .map(|(h, _)| *h)
            .collect()
    }

    pub fn try_remove(&mut self, hash: &[u8; 32]) -> Result<bool, WorkerRemoveError> {
        let Some(worker) = self.workers.get(hash) else {
            return Ok(false);
        };
        if worker.batch_outstanding() || worker.with_state(|s| !s.submit.pending.is_empty()) {
            return Err(WorkerRemoveError::InFlight);
        }
        let worker = self.workers.remove(hash).expect("exists");
        worker.shutdown();
        Ok(true)
    }

    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.workers.contains_key(hash)
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    pub fn hashes(&self) -> Vec<[u8; 32]> {
        let mut keys: Vec<_> = self.workers.keys().copied().collect();
        keys.sort();
        keys
    }
}

// Back-compat alias: tests/docs referred to ProcessWorker as the worker record.
pub use ProcessWorkerState as ProcessWorkerInner;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{LiveCell, RpcError, TxStatus};
    use crate::ssri::SsriError;
    use crate::type_id::{type_id_script_hash, TypeIdIdentity};
    use cid_process_executor::{AssembledTx, MockHost};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::Byte32;
    use std::sync::Arc;

    struct EmptyRpc;
    crate::impl_host_query_rpc_stubs!(EmptyRpc);
    impl Rpc for EmptyRpc {
        fn list_live_intentions(
            &self,
            _: &[u8; 32],
            _: ScriptHashType,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(vec![])
        }
        fn get_live_process_by_type_hash(
            &self,
            _: &[u8; 32],
        ) -> Result<Option<LiveCell>, RpcError> {
            Ok(None)
        }
        fn find_live_cells_by_type_script(
            &self,
            _: &ckb_types::packed::Script,
            _: usize,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(vec![])
        }
        fn dry_run_transaction(&self, _: &TransactionView) -> Result<(), RpcError> {
            Ok(())
        }
        fn submit_transaction(&self, _: &TransactionView) -> Result<Byte32, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
        fn get_transaction_status(&self, _: &Byte32) -> Result<TxStatus, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
        fn tip_block_number(&self) -> Result<u64, RpcError> {
            Ok(0)
        }
    }

    struct NopSsri;
    impl SsriHost for NopSsri {
        fn get_batch_bounds(&self, _: &[u8]) -> Result<(u32, u32), SsriError> {
            Ok((1, 1))
        }
        fn assemble(&self, _: &[u8], _: &[u8], _: MockHost) -> Result<AssembledTx, SsriError> {
            Err(SsriError::Unusable("nop".into()))
        }
    }

    fn entry(state_args: [u8; 32], code_args: [u8; 32]) -> ProcessEntry {
        ProcessEntry {
            identity_type_hash: type_id_script_hash(&state_args),
            identity: TypeIdIdentity::new(state_args),
            code: TypeIdIdentity::new(code_args),
            ssri_elf_path: None,
        }
    }

    fn pool() -> WorkerPool<EmptyRpc, NopSsri> {
        WorkerPool::new(Arc::new(EmptyRpc), Arc::new(NopSsri))
    }

    #[test]
    fn register_spawns_named_thread_and_ping() {
        let mut pool = pool();
        let e = entry([0x01; 32], [0x02; 32]);
        let hash = e.identity_type_hash;
        assert!(pool.register(e).unwrap());
        let w = pool.get(&hash).unwrap();
        assert!(w.thread_name().starts_with("cid-proc-"));
        assert!(w.ping(), "worker thread must respond to ping");
    }

    #[test]
    fn register_new_and_update_preserves_submit_when_code_same() {
        let mut pool = pool();
        let e1 = entry([0x01; 32], [0x02; 32]);
        let hash = e1.identity_type_hash;
        assert!(pool.register(e1.clone()).unwrap());
        pool.get(&hash).unwrap().with_state_mut(|w| {
            w.submit.in_flight.insert(vec![0xaa]);
        });

        let mut e1b = e1.clone();
        e1b.ssri_elf_path = None;
        assert!(!pool.register(e1b).unwrap());
        pool.get(&hash).unwrap().with_state(|w| {
            assert!(w.submit.in_flight.contains(&vec![0xaa]));
        });
    }

    #[test]
    fn register_code_change_clears_rejected_combos() {
        let mut pool = WorkerPool::from_entries(
            vec![entry([0x10; 32], [0x20; 32])],
            Arc::new(EmptyRpc),
            Arc::new(NopSsri),
        );
        let hash = type_id_script_hash(&[0x10; 32]);
        pool.get(&hash).unwrap().with_state_mut(|w| {
            w.submit.rejected_combos.insert(vec![0x01]);
        });
        assert!(!pool.register(entry([0x10; 32], [0x21; 32])).unwrap());
        pool.get(&hash).unwrap().with_state(|w| {
            assert!(w.submit.rejected_combos.is_empty());
        });
    }

    #[test]
    fn register_refuses_code_change_when_pending_nonempty() {
        let mut pool = WorkerPool::from_entries(
            vec![entry([0x10; 32], [0x20; 32])],
            Arc::new(EmptyRpc),
            Arc::new(NopSsri),
        );
        let hash = type_id_script_hash(&[0x10; 32]);
        pool.get(&hash).unwrap().with_state_mut(|w| {
            w.submit.pending.insert(
                vec![0x01],
                crate::submit::PendingTx {
                    consumed: vec![],
                    created: vec![],
                    seen_committed: false,
                    currently_committed: false,
                    admission_deadline: None,
                },
            );
        });
        assert!(matches!(
            pool.register(entry([0x10; 32], [0x21; 32])),
            Err(WorkerRegisterError::PendingCodeChange)
        ));
        assert!(pool.contains(&hash));
    }

    #[test]
    fn try_remove_in_flight_when_pending_nonempty() {
        let mut pool = WorkerPool::from_entries(
            vec![entry([0x30; 32], [0x31; 32])],
            Arc::new(EmptyRpc),
            Arc::new(NopSsri),
        );
        let hash = type_id_script_hash(&[0x30; 32]);
        pool.get(&hash).unwrap().with_state_mut(|w| {
            w.submit.pending.insert(
                vec![0x99],
                crate::submit::PendingTx {
                    consumed: vec![],
                    created: vec![],
                    seen_committed: false,
                    currently_committed: false,
                    admission_deadline: None,
                },
            );
        });
        assert!(matches!(
            pool.try_remove(&hash),
            Err(WorkerRemoveError::InFlight)
        ));
        assert!(pool.contains(&hash));
    }

    #[test]
    fn try_remove_ok_stops_thread() {
        let mut pool = WorkerPool::from_entries(
            vec![entry([0x40; 32], [0x41; 32])],
            Arc::new(EmptyRpc),
            Arc::new(NopSsri),
        );
        let hash = type_id_script_hash(&[0x40; 32]);
        assert!(pool.get(&hash).unwrap().ping());
        assert!(pool.try_remove(&hash).unwrap());
        assert!(!pool.contains(&hash));
        assert!(!pool.try_remove(&hash).unwrap());
    }

    #[test]
    fn len_hashes_is_empty() {
        let pool = WorkerPool::from_entries(
            vec![entry([0x50; 32], [0x51; 32]), entry([0x52; 32], [0x53; 32])],
            Arc::new(EmptyRpc),
            Arc::new(NopSsri),
        );
        assert_eq!(pool.len(), 2);
        assert!(!pool.is_empty());
        assert_eq!(pool.hashes().len(), 2);
    }

    /// P0-2: while RunBatch is outstanding, refuse remove and re-register so a
    /// second worker thread cannot be spawned against the same Process.
    #[test]
    fn refuse_remove_and_register_while_run_batch_outstanding() {
        let mut pool = pool();
        let e = entry([0x60; 32], [0x61; 32]);
        let hash = e.identity_type_hash;
        assert!(pool.register(e.clone()).unwrap());

        let worker = pool.get(&hash).unwrap();
        assert!(!worker.batch_outstanding());
        worker.batch_outstanding.store(true, Ordering::SeqCst);

        assert!(matches!(
            pool.try_remove(&hash),
            Err(WorkerRemoveError::InFlight)
        ));
        assert!(matches!(
            pool.register(entry([0x60; 32], [0x62; 32])),
            Err(WorkerRegisterError::BatchInFlight)
        ));
        assert!(matches!(
            pool.register(e.clone()),
            Err(WorkerRegisterError::BatchInFlight)
        ));
        assert!(pool.contains(&hash));
        assert_eq!(pool.len(), 1);

        pool.get(&hash)
            .unwrap()
            .batch_outstanding
            .store(false, Ordering::SeqCst);
        // JoinHandle still live after refused remove — thread responds to ping.
        assert!(pool.get(&hash).unwrap().ping());
    }
}
