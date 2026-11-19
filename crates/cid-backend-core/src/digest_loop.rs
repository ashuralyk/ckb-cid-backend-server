use crate::code_deps::{resolve_execution_cell_deps, resolve_type_id_code_cell, CodeDepError};
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::{ProcessEntry, ProcessResolver, ResolvedProcess};
use crate::process_batch::{self, BatchOutcome, BatchReportDelta};
use crate::process_busy::ProcessBusyMap;
use crate::rpc::{out_point_key, Rpc, RpcError, TxStatus};
use crate::scan::{bucket_by_process_type_hash, scan_intentions_multi, ScannedIntention};
use crate::ssri::SsriHost;
use crate::submit::{combo_fingerprint, execution_code_key, SubmitState};
use crate::type_id::TypeIdCodeDep;
use crate::workers::{WorkerPool, WorkerRemoveError};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellDep};
use ckb_types::prelude::*;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const LOG_TARGET: &str = "cid_backend";

/// Default max molecule size for guest-assembled execution txs (512 KiB).
pub const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;

fn hex_pref(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

pub struct LoopConfig {
    /// Primary CID lock scan `code_hash`. From daemon Type-ID config this is
    /// the derived TYPE_ID type-script hash paired with [`ScriptHashType::Type`].
    pub cid_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
    /// Static CellDeps (extras / local fixtures). Always merged after Type-ID
    /// resolution. Not used for Type-ID code discovery.
    pub extra_cell_deps: Vec<CellDep>,
    /// Type-ID identities for CID lock / Process code (and optional Process
    /// lock) cells. Resolved every tick; empty keeps legacy static-only deps.
    pub type_id_code_deps: Vec<TypeIdCodeDep>,
    /// Blocks that must exist on top of a committed execution tx before
    /// occupancy is released. `1` settles on first sight of Committed at the
    /// tip. Daemon default is [`DEFAULT_CONFIRMATION_DEPTH`].
    pub confirmation_depth: u64,
    pub extra_scan_locks: Vec<CidLockIdentity>,
    pub refund_policy: RefundPolicy,
    /// Max molecule bytes for `Process.assemble` passthrough txs.
    pub max_assembled_tx_bytes: usize,
}

impl LoopConfig {
    /// Scan the primary lock and explicitly configured legacy identities.
    pub fn scan_lock_identities(&self) -> Vec<CidLockIdentity> {
        let mut locks = vec![CidLockIdentity::new(self.cid_code_hash, self.hash_type)];
        for lock in &self.extra_scan_locks {
            if !locks.contains(lock) {
                locks.push(*lock);
            }
        }
        locks
    }
}

/// Conservative default: 24 CKB blocks (~3 minutes at ~8s). Overridable via
/// `CID_CONFIRMATION_DEPTH`. Tests typically use `1`.
pub const DEFAULT_CONFIRMATION_DEPTH: u64 = 24;

/// Slim loop snapshot for observability / tests.
#[derive(Clone, Debug, Default)]
pub struct LoopState {
    pub last_tip: Option<u64>,
    pub tick_cell_deps: Vec<CellDep>,
    pub execution_code_key: Vec<u8>,
    /// When exactly one Process worker is configured, exposes its submit state.
    pub submit: Option<SubmitState>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: usize,
    pub skipped: usize,
    pub submitted: usize,
    pub conflicts: usize,
    /// Indexer live-set fetch succeeded (zero cells still counts as success).
    pub scan_ok: bool,
    /// Last scan/submit error on this tick, if any.
    pub last_error: Option<String>,
    /// Isolate windows considered this tick (including cached splits).
    pub isolate_windows: usize,
}

pub trait Clock {
    fn sleep(&self, duration: Duration);
}

pub struct NopClock;

impl Clock for NopClock {
    fn sleep(&self, _duration: Duration) {}
}

pub struct RealClock;

impl Clock for RealClock {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Deferred register/delete from the HTTP registry API (applied at tick start).
#[derive(Clone, Debug)]
pub enum WorkerCommand {
    Register(ProcessEntry),
    /// Disable Process. `generation` stamps the worker epoch when delete was
    /// first deferred (`None` = fresh API delete). Re-register bumps generation
    /// so a stale deferred delete is a no-op (P0-1).
    Delete {
        hash: [u8; 32],
        generation: Option<u64>,
    },
}

impl WorkerCommand {
    fn identity(&self) -> [u8; 32] {
        match self {
            Self::Register(entry) => entry.identity_type_hash,
            Self::Delete { hash, .. } => *hash,
        }
    }

    pub fn delete(hash: [u8; 32]) -> Self {
        Self::Delete {
            hash,
            generation: None,
        }
    }
}

pub struct DigestLoop<R, C, S>
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    C: Clock,
    S: SsriHost + Send + Sync + 'static,
{
    rpc: Arc<R>,
    clock: C,
    ssri: Arc<S>,
    config: LoopConfig,
    workers: WorkerPool<R, S>,
    /// Optional shared command queue for live registry API updates.
    worker_cmds: Option<Arc<Mutex<Vec<WorkerCommand>>>>,
    /// Optional shared pending map for registry API code-rebind checks (P1-6).
    busy: Option<ProcessBusyMap>,
    last_tip: Option<u64>,
    tick_cell_deps: Vec<CellDep>,
    execution_code_key: Vec<u8>,
}

impl<R, C, S> DigestLoop<R, C, S>
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    C: Clock,
    S: SsriHost + Send + Sync + 'static,
{
    pub fn new(rpc: R, clock: C, ssri: S, config: LoopConfig) -> Self {
        let rpc = Arc::new(rpc);
        let ssri = Arc::new(ssri);
        let workers = WorkerPool::new(rpc.clone(), ssri.clone());
        Self::with_workers(rpc, clock, ssri, config, workers, None)
    }

    pub fn with_workers(
        rpc: Arc<R>,
        clock: C,
        ssri: Arc<S>,
        config: LoopConfig,
        workers: WorkerPool<R, S>,
        worker_cmds: Option<Arc<Mutex<Vec<WorkerCommand>>>>,
    ) -> Self {
        Self {
            rpc,
            clock,
            ssri,
            config,
            workers,
            worker_cmds,
            busy: None,
            last_tip: None,
            tick_cell_deps: Vec::new(),
            execution_code_key: Vec::new(),
        }
    }

    pub fn with_workers_owned(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        workers: WorkerPool<R, S>,
        worker_cmds: Option<Arc<Mutex<Vec<WorkerCommand>>>>,
    ) -> Self {
        let rpc = Arc::new(rpc);
        let ssri = Arc::new(ssri);
        Self::with_workers(rpc, clock, ssri, config, workers, worker_cmds)
    }

    pub fn with_busy_map(mut self, busy: ProcessBusyMap) -> Self {
        self.busy = Some(busy);
        self
    }

    /// Seed one worker from `resolver` (tests / local fixtures).
    pub fn new_with_resolver(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        resolver: ProcessResolver,
    ) -> Self {
        let entry = resolver.entry().clone();
        let hash = entry.identity_type_hash;
        let rpc = Arc::new(rpc);
        let ssri = Arc::new(ssri);
        let pool = WorkerPool::from_entries(vec![entry], rpc.clone(), ssri.clone());
        if let Some(w) = pool.get(&hash) {
            w.with_state_mut(|state| state.resolver = resolver);
        }
        Self::with_workers(rpc, clock, ssri, config, pool, None)
    }

    pub fn rpc(&self) -> &R {
        self.rpc.as_ref()
    }

    pub fn ssri(&self) -> &S {
        self.ssri.as_ref()
    }

    pub fn workers(&self) -> &WorkerPool<R, S> {
        &self.workers
    }

    pub fn workers_mut(&mut self) -> &mut WorkerPool<R, S> {
        &mut self.workers
    }

    pub fn worker_cmds(&self) -> Option<Arc<Mutex<Vec<WorkerCommand>>>> {
        self.worker_cmds.clone()
    }

    /// Register (or update) a Process worker immediately.
    pub fn register_process(&mut self, entry: ProcessEntry) -> bool {
        match self.workers.register(entry) {
            Ok(created) => {
                tracing::info!(
                    target: LOG_TARGET,
                    event = if created {
                        "process.registered"
                    } else {
                        "process.updated"
                    },
                );
                created
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "process.register_refused",
                    error = ?e,
                );
                false
            }
        }
    }

    /// Remove a Process worker when idle.
    pub fn unregister_process(&mut self, hash: &[u8; 32]) -> Result<bool, WorkerRemoveError> {
        let removed = self.workers.try_remove(hash)?;
        if removed {
            tracing::info!(
                target: LOG_TARGET,
                event = "process.disabled",
                process_type_hash = %hex_pref(hash),
            );
        }
        Ok(removed)
    }

    fn drain_worker_commands(&mut self) {
        let Some(cmds) = &self.worker_cmds else {
            return;
        };
        // Fail closed on poisoned queue (P2-10).
        let mut batch = match cmds.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => {
                tracing::error!(target: LOG_TARGET, event = "worker_cmds.poisoned");
                return;
            }
        };
        // Only the latest registry request for an identity represents desired state.
        let mut seen = HashSet::new();
        batch.reverse();
        batch.retain(|cmd| seen.insert(cmd.identity()));
        let mut requeue = Vec::new();
        for cmd in batch.into_iter().rev() {
            match cmd {
                WorkerCommand::Register(entry) => {
                    let hash = entry.identity_type_hash;
                    match self.workers.register(entry.clone()) {
                        Ok(_) => {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "process.registered",
                            );
                            requeue.retain(|c| {
                                !matches!(c, WorkerCommand::Delete { hash: h, .. } if *h == hash)
                            });
                        }
                        Err(e) => {
                            tracing::warn!(
                                target: LOG_TARGET,
                                event = "process.register_deferred",
                                error = ?e,
                            );
                            requeue.push(WorkerCommand::Register(entry));
                        }
                    }
                }
                WorkerCommand::Delete { hash, generation } => {
                    if let Some(want) = generation {
                        if self.workers.generation(&hash) != Some(want) {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "process.delete_stale",
                                process_type_hash = %hex_pref(&hash),
                            );
                            continue;
                        }
                    }
                    match self.workers.try_remove(&hash) {
                        Ok(true) => {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "process.disabled",
                                process_type_hash = %hex_pref(&hash),
                            );
                        }
                        Ok(false) => {}
                        Err(WorkerRemoveError::InFlight) => {
                            let gen = generation.or_else(|| self.workers.generation(&hash));
                            requeue.push(WorkerCommand::Delete {
                                hash,
                                generation: gen,
                            });
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "process.delete_deferred",
                                process_type_hash = %hex_pref(&hash),
                            );
                        }
                    }
                }
            }
        }
        if !requeue.is_empty() {
            if let Some(q) = &self.worker_cmds {
                // Recover deferred Deletes even if the mutex was poisoned (P2-8).
                let mut g = q.lock().unwrap_or_else(|e| e.into_inner());
                requeue.retain(|old| !g.iter().any(|new| new.identity() == old.identity()));
                g.extend(requeue);
            }
        }
    }

    fn sync_busy_map(&self) {
        if let Some(busy) = &self.busy {
            busy.set_pending(self.workers.pending_hashes());
        }
    }

    pub fn state(&self) -> LoopState {
        let hashes = self.workers.hashes();
        let submit = if hashes.len() == 1 {
            self.workers
                .get(&hashes[0])
                .map(|w| w.with_state(|s| s.submit.clone()))
        } else {
            None
        };
        LoopState {
            last_tip: self.last_tip,
            tick_cell_deps: self.tick_cell_deps.clone(),
            execution_code_key: self.execution_code_key.clone(),
            submit,
        }
    }

    pub fn config(&self) -> &LoopConfig {
        &self.config
    }

    pub fn config_mut(&mut self) -> &mut LoopConfig {
        &mut self.config
    }

    pub fn run(&mut self) {
        loop {
            let _ = self.tick();
            self.clock.sleep(self.config.poll_interval);
        }
    }

    pub fn tick(&mut self) -> TickReport {
        tracing::info!(target: LOG_TARGET, event = "tick.start");
        let report = self.tick_inner();
        tracing::info!(
            target: LOG_TARGET,
            event = "tick.end",
            scanned = report.scanned,
            skipped = report.skipped,
            submitted = report.submitted,
            conflicts = report.conflicts,
            scan_ok = report.scan_ok,
            isolate_windows = report.isolate_windows,
        );
        report
    }

    fn tick_inner(&mut self) -> TickReport {
        self.drain_worker_commands();
        let mut report = TickReport::default();
        let tip = self.refresh_tip();
        self.reconcile_pending(tip);

        let resolved = match self.refresh_cell_deps() {
            Ok(deps) => deps,
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "code_deps.unavailable",
                    error = %e,
                );
                report.last_error = Some(format!("code deps: {e}"));
                self.clear_worker_tick_code();
                self.tick_cell_deps.clear();
                self.sync_busy_map();
                return report;
            }
        };
        let code_key = execution_code_key(&resolved.type_id_outpoints);
        if code_key != self.execution_code_key {
            if !self.execution_code_key.is_empty() || !code_key.is_empty() {
                self.workers.for_each_mut(|w| {
                    w.with_state_mut(|state| state.clear_execution_rejections());
                });
            }
            self.execution_code_key = code_key;
        }
        self.tick_cell_deps = resolved.cell_deps;

        self.refresh_worker_process_code();

        let scan_ids: Vec<([u8; 32], ScriptHashType)> = self
            .config
            .scan_lock_identities()
            .into_iter()
            .map(|id| (id.code_hash, id.hash_type))
            .collect();
        let scanned = match scan_intentions_multi(self.rpc.as_ref(), &scan_ids) {
            Ok(v) => v,
            Err(RpcError::Unavailable(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "scan.unavailable",
                    error = %m,
                );
                report.last_error = Some(format!("scan unavailable: {m}"));
                self.tick_cell_deps.clear();
                self.clear_worker_tick_code();
                self.sync_busy_map();
                return report;
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "scan.unavailable",
                    error = %e,
                );
                report.last_error = Some(format!("scan error: {e}"));
                self.tick_cell_deps.clear();
                self.clear_worker_tick_code();
                self.sync_busy_map();
                return report;
            }
        };
        report.scan_ok = true;
        report.scanned = scanned.len();

        let live_keys: HashSet<Vec<u8>> = scanned
            .iter()
            .map(|i| out_point_key(&i.out_point))
            .collect();

        self.workers.for_each_mut(|w| {
            w.with_state_mut(|state| {
                let pending_occupied = state.submit.pending_occupied_keys();
                state.submit.in_flight.retain(|k| {
                    (live_keys.contains(k) || pending_occupied.contains(k))
                        && !state.submit.rejected.contains(k)
                });
                // Keep marks while a refund is in-flight (pending occupied) so a
                // Drop/reorg can still take the below-min refund_only path.
                state
                    .pending_incompatible_refund
                    .retain(|k| live_keys.contains(k) || pending_occupied.contains(k));
            });
        });

        let queued_updates: HashSet<_> = self
            .worker_cmds
            .as_ref()
            .and_then(|cmds| cmds.lock().ok())
            .map(|cmds| cmds.iter().map(WorkerCommand::identity).collect())
            .unwrap_or_default();
        let mut eligible = Vec::new();
        for i in scanned {
            if queued_updates.contains(&i.args.process_type_hash) {
                report.skipped += 1;
                continue;
            }
            if self.workers.get(&i.args.process_type_hash).is_none() {
                report.skipped += 1;
                tracing::debug!(
                    target: LOG_TARGET,
                    event = "process.unregistered",
                    process_type_hash = %hex_pref(&i.args.process_type_hash),
                );
                continue;
            }
            let k = out_point_key(&i.out_point);
            let blocked = self
                .workers
                .get(&i.args.process_type_hash)
                .expect("checked")
                .with_state(|w| w.submit.in_flight.contains(&k) || w.submit.rejected.contains(&k));
            if blocked {
                continue;
            }
            eligible.push(i);
        }

        let buckets = bucket_by_process_type_hash(&eligible);
        // Multi-round only for reject-isolate expansions. Transient Continue /
        // Conflict wait for the next tick (same as pre-async semantics).
        for _round in 0..64 {
            let mut started = false;
            for (hash, bucket) in &buckets {
                let was_out = self
                    .workers
                    .get(hash)
                    .is_some_and(|w| w.batch_outstanding());
                if was_out {
                    continue;
                }
                self.process_bucket(*hash, bucket.clone(), &mut report);
                if self
                    .workers
                    .get(hash)
                    .is_some_and(|w| w.batch_outstanding())
                {
                    started = true;
                }
            }
            let more = self.wait_all_batches(&mut report);
            if !started || !more {
                break;
            }
        }

        self.tick_cell_deps.clear();
        self.clear_worker_tick_code();
        self.sync_busy_map();
        report
    }

    /// Sync Rejected/CachedRejected stack frames: drop_first(+rest) then shrink.
    /// Returns frames in push order (shrink is last / popped first) and skipped.
    fn reject_isolate_frames(
        window: &[ScannedIntention],
        rest: &[ScannedIntention],
        min: u32,
    ) -> (Vec<Vec<ScannedIntention>>, usize) {
        let mut drop_first = window[1..].to_vec();
        drop_first.extend_from_slice(rest);
        let mut frames = Vec::new();
        let mut skipped = 0usize;
        if window.len() > min as usize {
            if !drop_first.is_empty() {
                frames.push(drop_first);
            }
            frames.push(window[..window.len() - 1].to_vec());
        } else {
            skipped = 1;
            if !drop_first.is_empty() {
                frames.push(drop_first);
            }
        }
        (frames, skipped)
    }

    /// Remove fee-unaffordable / pending-incompatible cells from isolate frames
    /// so a sibling Rejected identity cannot shrink them away (P3).
    fn strip_pending_incompatible_refund(
        worker: &crate::workers::ProcessWorker,
        window: &[ScannedIntention],
        rest: &[ScannedIntention],
    ) -> (Vec<ScannedIntention>, Vec<ScannedIntention>) {
        worker.with_state(|s| {
            let keep = |i: &ScannedIntention| {
                !s.pending_incompatible_refund
                    .contains(&out_point_key(&i.out_point))
            };
            (
                window.iter().filter(|i| keep(i)).cloned().collect(),
                rest.iter().filter(|i| keep(i)).cloned().collect(),
            )
        })
    }

    fn apply_polled_batch(
        worker: &crate::workers::ProcessWorker,
        report: &mut TickReport,
        polled: crate::workers::PolledBatch,
    ) -> bool {
        Self::apply_batch_report(report, &polled.delta);
        let mut continue_round = false;
        match polled.outcome {
            BatchOutcome::Submitted | BatchOutcome::SubmittedProcess => {
                // Keep leftover isolate_pending frames saved on ProcessOccupied.
                let has_pending = worker.with_state(|s| !s.isolate_pending.is_empty());
                continue_round = has_pending;
                worker.with_state_mut(|state| {
                    if let Some(r) = polled.resume_on_continue {
                        state.isolate_resume = Some(r);
                    } else if !has_pending {
                        state.isolate_resume = None;
                        state.isolate_expanded.clear();
                    }
                });
            }
            BatchOutcome::Rejected | BatchOutcome::CachedRejected => {
                if polled.refund_only {
                    // Permanent refund_only failure is combo-cached in process_batch;
                    // never isolate-split below-min leftovers (P3).
                    continue_round = worker.with_state(|s| !s.isolate_pending.is_empty());
                } else {
                    // P0: apply sync drop_first + shrink — strip fee-unaffordable
                    // pending cells so they are not shrunk away (P3).
                    let (iso_window, iso_rest) = Self::strip_pending_incompatible_refund(
                        worker,
                        &polled.window,
                        &polled.rest,
                    );
                    if iso_window.is_empty() {
                        continue_round = worker.with_state(|s| !s.isolate_pending.is_empty());
                        if let Some(r) = polled.resume_on_continue {
                            worker.with_state_mut(|state| {
                                state.isolate_resume = Some(r);
                            });
                        }
                    } else {
                        let (frames, skipped) =
                            Self::reject_isolate_frames(&iso_window, &iso_rest, polled.min);
                        report.skipped += skipped;
                        continue_round = true;
                        worker.with_state_mut(|state| {
                            state.isolate_expanded.insert(polled.window_fp);
                            state.isolate_pending.extend(frames);
                        });
                    }
                }
            }
            BatchOutcome::RetryLater => {
                // Fee-unaffordable (etc.): no isolate split; keep draining any
                // leftover isolate_pending frames this tick (P3).
                continue_round = worker.with_state(|s| !s.isolate_pending.is_empty());
                if let Some(r) = polled.resume_on_continue {
                    worker.with_state_mut(|state| {
                        state.isolate_resume = Some(r);
                    });
                }
            }
            BatchOutcome::Continue => {
                // Unbuildable / soft skip: advance past window and keep isolating.
                // Conflict / submit failure set last_error or conflicts — next tick.
                if polled.delta.conflicts == 0
                    && polled.delta.last_error.is_none()
                    && polled.delta.skipped > 0
                {
                    continue_round = true;
                    worker.with_state_mut(|state| {
                        if let Some(r) = polled.resume_on_continue {
                            state.isolate_resume = Some(r);
                        } else {
                            state.isolate_resume = None;
                            state.isolate_expanded.clear();
                        }
                    });
                }
            }
            BatchOutcome::ProcessOccupied => {}
        }
        continue_round
    }

    /// Block until every outstanding RunBatch has an ack — never invent outcomes.
    /// Returns true when isolate should run another round this tick.
    fn wait_all_batches(&mut self, report: &mut TickReport) -> bool {
        let mut more = false;
        loop {
            let mut any_outstanding = false;
            for hash in self.workers.hashes() {
                let Some(worker) = self.workers.get(&hash) else {
                    continue;
                };
                if let Some(polled) = worker.try_poll_batch() {
                    if matches!(
                        polled.outcome,
                        BatchOutcome::Submitted | BatchOutcome::SubmittedProcess
                    ) {
                        self.sync_busy_map();
                    }
                    if Self::apply_polled_batch(worker, report, polled) {
                        more = true;
                    }
                } else if worker.batch_outstanding() {
                    any_outstanding = true;
                }
            }
            if !any_outstanding {
                return more;
            }
            thread::sleep(Duration::from_micros(100));
        }
    }

    fn refresh_worker_process_code(&mut self) {
        let snapshots: Vec<([u8; 32], crate::type_id::TypeIdIdentity)> = self
            .workers
            .hashes()
            .into_iter()
            .filter_map(|h| {
                self.workers
                    .get(&h)
                    .map(|w| (h, w.with_state(|s| s.entry.code.clone())))
            })
            .collect();
        for (identity_hash, code) in snapshots {
            let spec = TypeIdCodeDep {
                label: format!("worker_code_{}", hex_pref(&identity_hash)),
                identity: code,
            };
            let code_cell = match resolve_type_id_code_cell(self.rpc.as_ref(), &spec) {
                Ok(r) => Some(r.cell),
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "worker.code_resolve_miss",
                        process_type_hash = %hex_pref(&identity_hash),
                        error = %e,
                    );
                    None
                }
            };
            if let Some(w) = self.workers.get(&identity_hash) {
                w.with_state_mut(|state| state.refresh_process_code(code_cell.as_ref()));
            }
        }
    }

    fn clear_worker_tick_code(&mut self) {
        self.workers.for_each_mut(|w| {
            w.with_state_mut(|state| state.resolver.set_tick_process_code(None));
        });
    }

    fn refresh_cell_deps(&self) -> Result<crate::code_deps::ResolvedExecutionDeps, CodeDepError> {
        if self.config.type_id_code_deps.is_empty() {
            return Ok(crate::code_deps::ResolvedExecutionDeps {
                cell_deps: self.config.extra_cell_deps.clone(),
                process_code: None,
                type_id_outpoints: Vec::new(),
            });
        }
        resolve_execution_cell_deps(
            self.rpc.as_ref(),
            &self.config.type_id_code_deps,
            &self.config.extra_cell_deps,
        )
    }

    fn refresh_tip(&mut self) -> Option<u64> {
        self.refresh_tip_only()
    }

    fn reconcile_pending(&mut self, tip: Option<u64>) {
        let depth = self.config.confirmation_depth.max(1);
        let worker_hashes: Vec<[u8; 32]> = self.workers.hashes();
        for process_hash in worker_hashes {
            let hashes: Vec<Vec<u8>> = self
                .workers
                .get(&process_hash)
                .map(|w| w.with_state(|s| s.submit.pending.keys().cloned().collect()))
                .unwrap_or_default();
            for hash_bytes in hashes {
                let tx_hash = match Byte32::from_slice(&hash_bytes) {
                    Ok(h) => h,
                    Err(_) => {
                        if let Some(worker) = self.workers.get(&process_hash) {
                            worker.with_state_mut(|state| {
                                state.submit.pending.remove(&hash_bytes);
                            });
                        }
                        continue;
                    }
                };
                let tx_hex = hex_pref(tx_hash.as_slice());
                match self.rpc.get_transaction_status(&tx_hash) {
                    Ok(TxStatus::Dropped) => {
                        let Some(worker) = self.workers.get(&process_hash) else {
                            continue;
                        };
                        let seen_committed = worker.with_state(|state| {
                            state
                                .submit
                                .pending
                                .get(&hash_bytes)
                                .map(|p| p.seen_committed)
                                .unwrap_or(false)
                        });
                        if seen_committed {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "pending.reorg",
                                tx_hash = %tx_hex,
                            );
                        } else {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "pending.dropped",
                                tx_hash = %tx_hex,
                            );
                        }
                        worker.with_state_mut(|state| {
                            if let Some(p) = state.submit.pending.remove(&hash_bytes) {
                                for k in p.consumed {
                                    state.submit.in_flight.remove(&k);
                                }
                            }
                        });
                    }
                    Ok(TxStatus::Committed { block_number }) => {
                        if let Some(worker) = self.workers.get(&process_hash) {
                            worker.with_state_mut(|state| {
                                if let Some(p) = state.submit.pending.get_mut(&hash_bytes) {
                                    if !p.seen_committed {
                                        p.seen_committed = true;
                                        tracing::info!(
                                            target: LOG_TARGET,
                                            event = "pending.committed",
                                            tx_hash = %tx_hex,
                                            block_number,
                                        );
                                    }
                                }
                            });
                        }
                        let start_tip = tip;
                        let Some(fresh_tip) = self.refresh_tip_only() else {
                            continue;
                        };
                        let Some(worker) = self.workers.get(&process_hash) else {
                            continue;
                        };
                        if start_tip.map(|s| fresh_tip < s).unwrap_or(false) {
                            tracing::warn!(
                                target: LOG_TARGET,
                                event = "pending.tip_rewind",
                                tx_hash = %tx_hex,
                                from = start_tip.unwrap_or(fresh_tip),
                                to = fresh_tip,
                                block_number,
                            );
                            continue;
                        }
                        if fresh_tip < block_number {
                            tracing::debug!(
                                target: LOG_TARGET,
                                event = "pending.confirming",
                                tx_hash = %tx_hex,
                                confirmations = 0,
                                depth,
                                block_number,
                                tip = fresh_tip,
                            );
                            continue;
                        }
                        let confirmations = confirmation_count(fresh_tip, block_number);
                        if confirmations >= depth {
                            tracing::info!(
                                target: LOG_TARGET,
                                event = "pending.settled",
                                tx_hash = %tx_hex,
                                confirmations,
                                depth,
                            );
                            worker.with_state_mut(|state| {
                                state.submit.pending.remove(&hash_bytes);
                            });
                        } else {
                            tracing::debug!(
                                target: LOG_TARGET,
                                event = "pending.confirming",
                                tx_hash = %tx_hex,
                                confirmations,
                                depth,
                                block_number,
                                tip = fresh_tip,
                            );
                        }
                    }
                    Ok(TxStatus::Pending) => {}
                    Err(e) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "pending.status",
                            error = %e,
                            tx_hash = %tx_hex,
                        );
                    }
                }
            }
        }
    }

    fn refresh_tip_only(&mut self) -> Option<u64> {
        match self.rpc.tip_block_number() {
            Ok(tip) => {
                if let Some(prev) = self.last_tip {
                    if tip < prev {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "reorg.tip_rewind",
                            from = prev,
                            to = tip,
                        );
                    }
                }
                self.last_tip = Some(tip);
                Some(tip)
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tip.unavailable",
                    error = %e,
                );
                None
            }
        }
    }

    fn process_bucket(
        &mut self,
        hash: [u8; 32],
        bucket: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) {
        if bucket.is_empty() {
            return;
        }
        if self.workers.get(&hash).is_none() {
            report.skipped += bucket.len();
            return;
        }

        // Collect async RunBatch outcomes without blocking other Processes (P1).
        if let Some(worker) = self.workers.get(&hash) {
            if let Some(polled) = worker.try_poll_batch() {
                if matches!(
                    polled.outcome,
                    BatchOutcome::Submitted | BatchOutcome::SubmittedProcess
                ) {
                    self.sync_busy_map();
                }
                let outcome = polled.outcome;
                Self::apply_polled_batch(worker, report, polled);
                if matches!(outcome, BatchOutcome::SubmittedProcess) {
                    return;
                }
            }
            if worker.batch_outstanding() {
                return;
            }
        }

        let process = {
            let worker = self.workers.get(&hash).expect("exists");
            match worker.with_state_mut(|state| state.resolver.resolve(self.rpc.as_ref(), &hash)) {
                Ok(Some(p)) => p,
                Ok(None) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "process.resolve_miss",
                        process_type_hash = %hex_pref(&hash),
                    );
                    report.skipped += bucket.len();
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "process.resolve_miss",
                        process_type_hash = %hex_pref(&hash),
                        error = %e,
                    );
                    report.skipped += bucket.len();
                    return;
                }
            }
        };

        let (min, max) = match self.ssri.get_batch_bounds(&process.binary) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "batch_bounds",
                    error = %e,
                );
                report.skipped += bucket.len();
                return;
            }
        };
        if min == 0 || max < min {
            tracing::warn!(
                target: LOG_TARGET,
                event = "tick.skip",
                reason = "malformed_bounds",
                min,
                max,
            );
            report.skipped += bucket.len();
            return;
        }

        const MAX_ATTEMPTS: u32 = 64;
        let mut attempts = 0u32;
        if let Some(worker) = self.workers.get(&hash) {
            worker.with_state_mut(|state| {
                if state.isolate_resume.is_none() {
                    state.isolate_expanded.clear();
                }
            });
        }
        let resume_key = self
            .workers
            .get(&hash)
            .and_then(|w| w.with_state(|s| s.isolate_resume.clone()));
        let bucket = Self::rotate_bucket(bucket, resume_key.as_ref());
        let resume = self.isolate_window(
            hash,
            &process,
            bucket,
            min,
            max,
            report,
            &mut attempts,
            MAX_ATTEMPTS,
        );
        if let Some(worker) = self.workers.get(&hash) {
            worker.with_state_mut(|state| {
                if let Some(k) = resume {
                    state.isolate_resume = Some(k);
                } else {
                    state.isolate_resume = None;
                    state.isolate_expanded.clear();
                }
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn isolate_window(
        &mut self,
        hash: [u8; 32],
        process: &ResolvedProcess,
        items: Vec<ScannedIntention>,
        min: u32,
        max: u32,
        report: &mut TickReport,
        attempts: &mut u32,
        max_attempts: u32,
    ) -> Option<Vec<u8>> {
        const MAX_CACHED_SPLITS: u32 = 256;
        let mut cached_splits = 0u32;
        // Prefer async Rejected frames (drop_first + shrink) over re-walking the
        // bucket via `already` (P0).
        let mut stack = self
            .workers
            .get(&hash)
            .map(|w| {
                w.with_state_mut(|s| {
                    if s.isolate_pending.is_empty() {
                        Vec::new()
                    } else {
                        std::mem::take(&mut s.isolate_pending)
                    }
                })
            })
            .unwrap_or_default();
        if stack.is_empty() {
            stack.push(items);
        }
        while let Some(items) = stack.pop() {
            let items: Vec<ScannedIntention> = items
                .into_iter()
                .filter(|i| {
                    self.workers
                        .get(&hash)
                        .map(|w| {
                            w.with_state(|s| {
                                !s.submit.in_flight.contains(&out_point_key(&i.out_point))
                            })
                        })
                        .unwrap_or(true)
                })
                .collect();
            let (items, refund_only) = if items.len() < min as usize {
                let (pending_items, non_pending): (Vec<_>, Vec<_>) = self
                    .workers
                    .get(&hash)
                    .map(|w| {
                        w.with_state(|s| {
                            items.iter().cloned().partition(|i| {
                                s.pending_incompatible_refund
                                    .contains(&out_point_key(&i.out_point))
                            })
                        })
                    })
                    .unwrap_or_else(|| (Vec::new(), items));
                report.skipped += non_pending.len();
                if pending_items.is_empty() {
                    continue;
                }
                (pending_items, true)
            } else {
                (items, false)
            };
            report.isolate_windows += 1;
            let n = items.len().min(max as usize);
            let window = items[..n].to_vec();
            let rest = items[n..].to_vec();
            let fp = self.window_fingerprint(&window, process);
            let already = !refund_only
                && self
                    .workers
                    .get(&hash)
                    .map(|w| w.with_state(|s| s.isolate_expanded.contains(&fp)))
                    .unwrap_or(false);
            if already {
                // Already expanded once this sweep — do not re-push drop_first/
                // shrink (diamond explosion). Async Rejected queues frames via
                // isolate_pending at poll time instead of relying on this arm.
                continue;
            }
            let cached = self.window_combo_cached(&hash, &window, process);
            let outcome = if cached {
                tracing::debug!(
                    target: LOG_TARGET,
                    event = "submit.rejected_combo",
                    n = window.len(),
                );
                if refund_only {
                    // Combo already permanently rejected — soft skip, no isolate
                    // and no attempt budget burn (P3).
                    report.skipped += window.len();
                    if !rest.is_empty() {
                        stack.push(rest);
                    }
                    continue;
                }
                BatchOutcome::CachedRejected
            } else {
                if *attempts >= max_attempts {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "tick.skip",
                        reason = "reject_isolate_budget",
                        attempts,
                    );
                    report.skipped += items.len();
                    for leftover in stack.drain(..) {
                        report.skipped += leftover.len();
                    }
                    return items.first().map(|i| out_point_key(&i.out_point));
                }
                let resume_on_continue = rest.first().map(|i| out_point_key(&i.out_point));
                let outcome = self.dispatch_process_batch(
                    hash,
                    process,
                    window.clone(),
                    rest.clone(),
                    min,
                    resume_on_continue,
                    report,
                    refund_only,
                );
                if !matches!(outcome, BatchOutcome::CachedRejected) {
                    *attempts += 1;
                }
                outcome
            };
            if matches!(outcome, BatchOutcome::CachedRejected) {
                if cached_splits >= MAX_CACHED_SPLITS {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "tick.skip",
                        reason = "cached_split_budget",
                        cached_splits,
                    );
                    report.skipped += items.len();
                    for leftover in stack.drain(..) {
                        report.skipped += leftover.len();
                    }
                    return items.first().map(|i| out_point_key(&i.out_point));
                }
                cached_splits += 1;
            }
            match outcome {
                BatchOutcome::SubmittedProcess => {
                    let resume = rest
                        .first()
                        .or_else(|| stack.iter().find_map(|s| s.first()))
                        .map(|i| out_point_key(&i.out_point));
                    report.skipped += rest.len();
                    for leftover in stack.drain(..) {
                        report.skipped += leftover.len();
                    }
                    return resume;
                }
                BatchOutcome::ProcessOccupied => {
                    // Preserve unprocessed frames for the next round after poll.
                    if let Some(w) = self.workers.get(&hash) {
                        if !stack.is_empty() {
                            w.with_state_mut(|state| {
                                state.isolate_pending.append(&mut stack);
                            });
                        }
                    }
                    // Resume at this window after poll (do not skip to `rest`).
                    return items.first().map(|i| out_point_key(&i.out_point));
                }
                BatchOutcome::Submitted | BatchOutcome::Continue | BatchOutcome::RetryLater => {
                    if !rest.is_empty() {
                        stack.push(rest);
                    }
                }
                BatchOutcome::Rejected | BatchOutcome::CachedRejected => {
                    if refund_only {
                        // Should be rare (process_batch maps these to Continue);
                        // never isolate-split below-min refund leftovers.
                        if !rest.is_empty() {
                            stack.push(rest);
                        }
                    } else if let Some(w) = self.workers.get(&hash) {
                        let (iso_window, iso_rest) =
                            Self::strip_pending_incompatible_refund(w, &window, &rest);
                        if iso_window.is_empty() {
                            if !rest.is_empty() {
                                // rest may still hold pending cells — keep for scan.
                            }
                        } else {
                            w.with_state_mut(|state| state.isolate_expanded.insert(fp));
                            let (frames, skipped) =
                                Self::reject_isolate_frames(&iso_window, &iso_rest, min);
                            report.skipped += skipped;
                            stack.extend(frames);
                        }
                    }
                }
            }
        }
        None
    }

    fn batch_code_key(&self, process: &ResolvedProcess) -> Vec<u8> {
        let mut key = self.execution_code_key.clone();
        if let Some(cell) = &process.code_cell {
            key.extend_from_slice(cell.out_point.as_slice());
        }
        key
    }

    fn window_fingerprint(&self, batch: &[ScannedIntention], process: &ResolvedProcess) -> Vec<u8> {
        let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        combo_fingerprint(
            &ops,
            Some(&process.live_cell.out_point),
            &self.batch_code_key(process),
        )
    }

    fn rotate_bucket(
        bucket: Vec<ScannedIntention>,
        resume: Option<&Vec<u8>>,
    ) -> Vec<ScannedIntention> {
        let Some(key) = resume else {
            return bucket;
        };
        match bucket
            .iter()
            .position(|i| out_point_key(&i.out_point) == *key)
        {
            Some(i) if i > 0 => {
                let mut rotated = bucket[i..].to_vec();
                rotated.extend_from_slice(&bucket[..i]);
                rotated
            }
            _ => bucket,
        }
    }

    fn window_combo_cached(
        &self,
        hash: &[u8; 32],
        batch: &[ScannedIntention],
        process: &ResolvedProcess,
    ) -> bool {
        self.workers.get(hash).is_some_and(|w| {
            w.with_state(|state| {
                process_batch::window_combo_cached(
                    state,
                    batch,
                    process,
                    &self.batch_code_key(process),
                )
            })
        })
    }

    fn apply_batch_report(report: &mut TickReport, delta: &BatchReportDelta) {
        report.submitted += delta.submitted;
        report.skipped += delta.skipped;
        report.conflicts += delta.conflicts;
        if delta.last_error.is_some() {
            report.last_error = delta.last_error.clone();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_process_batch(
        &self,
        hash: [u8; 32],
        process: &ResolvedProcess,
        batch: Vec<ScannedIntention>,
        rest: Vec<ScannedIntention>,
        min: u32,
        resume_on_continue: Option<Vec<u8>>,
        report: &mut TickReport,
        refund_only: bool,
    ) -> BatchOutcome {
        let worker = match self.workers.get(&hash) {
            Some(w) => w,
            None => return BatchOutcome::Continue,
        };
        // Serialize dispatch with registry enqueue. A deferred delete or rebind
        // may remain queued while the previous transactions are reconciled.
        let _commands = if let Some(queue) = &self.worker_cmds {
            match queue.lock() {
                Ok(commands) if commands.iter().all(|cmd| cmd.identity() != hash) => Some(commands),
                _ => return BatchOutcome::Continue,
            }
        } else {
            None
        };
        let max_size = if self.config.max_assembled_tx_bytes == 0 {
            DEFAULT_MAX_ASSEMBLED_TX_BYTES
        } else {
            self.config.max_assembled_tx_bytes
        };
        let window_fp = self.window_fingerprint(&batch, process);
        match worker.start_batch(
            process.clone(),
            batch,
            self.batch_code_key(process),
            self.tick_cell_deps.clone(),
            self.config.refund_policy.clone(),
            max_size,
            window_fp,
            resume_on_continue,
            rest,
            min,
            refund_only,
        ) {
            Ok(()) => BatchOutcome::ProcessOccupied,
            Err(crate::workers::StartBatchError::AlreadyOutstanding) => {
                BatchOutcome::ProcessOccupied
            }
            Err(crate::workers::StartBatchError::WorkerDead) => {
                report.last_error = Some("worker dead: RunBatch send failed".into());
                BatchOutcome::Continue
            }
        }
    }
}

pub(crate) fn confirmation_count(tip: u64, commit_block: u64) -> u64 {
    if tip < commit_block {
        0
    } else {
        tip - commit_block + 1
    }
}

#[cfg(test)]
mod confirmation_math {
    use super::confirmation_count;

    #[test]
    fn count_is_zero_when_tip_rewinds_below_commit() {
        assert_eq!(confirmation_count(9, 10), 0);
        assert_eq!(confirmation_count(10, 10), 1);
        assert_eq!(confirmation_count(12, 10), 3);
    }
}
#[cfg(test)]
mod worker_cmd_tests {
    use super::*;
    use crate::process::ProcessEntry;
    use crate::rpc::{LiveCell, Rpc, RpcError, TxStatus};
    use crate::ssri::SsriError;
    use crate::ssri::SsriHost;
    use crate::type_id::{type_id_script_hash, TypeIdIdentity};
    use cid_process_executor::{AssembledTx, MockHost};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::Byte32;
    use std::time::Duration;

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

    fn entry(state: [u8; 32], code: [u8; 32]) -> ProcessEntry {
        ProcessEntry {
            identity_type_hash: type_id_script_hash(&state),
            identity: TypeIdIdentity::new(state),
            code: TypeIdIdentity::new(code),
            ssri_elf_path: None,
        }
    }

    fn cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: [0xC1; 32],
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![ckb_types::packed::CellDep::new_builder()
                .out_point(
                    ckb_types::packed::OutPoint::new_builder()
                        .tx_hash(ckb_types::packed::Byte32::from_slice(&[0xDEu8; 32]).unwrap())
                        .index(0u32.pack())
                        .build(),
                )
                .dep_type(ckb_types::core::DepType::Code.into())
                .build()],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            extra_scan_locks: vec![],
            refund_policy: Default::default(),
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    #[test]
    fn worker_command_register_and_delete_take_effect_on_tick() {
        let cmds = Arc::new(Mutex::new(Vec::new()));
        let rpc = Arc::new(EmptyRpc);
        let ssri = Arc::new(NopSsri);
        let mut loop_ = DigestLoop::with_workers(
            rpc.clone(),
            NopClock,
            ssri.clone(),
            cfg(),
            WorkerPool::new(rpc, ssri),
            Some(cmds.clone()),
        );
        assert!(loop_.workers().is_empty());

        let e = entry([0x11; 32], [0x22; 32]);
        let hash = e.identity_type_hash;
        cmds.lock().unwrap().push(WorkerCommand::Register(e));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "register must spawn worker"
        );

        cmds.lock().unwrap().push(WorkerCommand::delete(hash));
        let _ = loop_.tick();
        assert!(
            !loop_.workers().contains(&hash),
            "delete must stop idle worker"
        );
    }

    #[test]
    fn worker_command_delete_deferred_while_in_flight() {
        let cmds = Arc::new(Mutex::new(Vec::new()));
        let e = entry([0x33; 32], [0x44; 32]);
        let hash = e.identity_type_hash;
        let rpc = Arc::new(EmptyRpc);
        let ssri = Arc::new(NopSsri);
        let pool = WorkerPool::from_entries(vec![e], rpc.clone(), ssri.clone());
        pool.get(&hash).unwrap().with_state_mut(|w| {
            w.submit.pending.insert(
                vec![0x01],
                crate::submit::PendingTx {
                    consumed: vec![],
                    created: vec![],
                    seen_committed: false,
                },
            );
        });
        let mut loop_ =
            DigestLoop::with_workers(rpc, NopClock, ssri, cfg(), pool, Some(cmds.clone()));
        cmds.lock().unwrap().push(WorkerCommand::delete(hash));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "in-flight delete must stay deferred"
        );
        assert_eq!(cmds.lock().unwrap().len(), 1);
        assert!(matches!(
            cmds.lock().unwrap()[0],
            WorkerCommand::Delete { .. }
        ));

        // P0-1: re-register must invalidate the deferred delete.
        let e2 = entry([0x33; 32], [0x44; 32]);
        cmds.lock().unwrap().push(WorkerCommand::Register(e2));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "re-register must keep worker; stale delete must not apply"
        );
        assert!(
            cmds.lock()
                .unwrap()
                .iter()
                .all(|c| { !matches!(c, WorkerCommand::Delete { hash: h, .. } if *h == hash) }),
            "no stale Delete may remain after re-register"
        );
    }
}
