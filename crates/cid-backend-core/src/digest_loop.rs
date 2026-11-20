use crate::code_deps::{resolve_execution_cell_deps, resolve_type_id_code_cell, CodeDepError};
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::{ProcessEntry, ProcessResolver, ResolvedProcess};
use crate::process_batch::{self, BatchMode, BatchOutcome, BatchReportDelta};
use crate::process_busy::ProcessBusyMap;
use crate::rpc::{out_point_key, Rpc, RpcError, TxStatus};
use crate::scan::{bucket_by_process_type_hash, scan_intentions_multi, ScannedIntention};
use crate::ssri::{filter_intentions, SsriError, SsriHost};
use crate::submit::{execution_code_key, SubmitState};
use crate::type_id::TypeIdCodeDep;
pub use crate::worker_commands::{WorkerCommand, WorkerCommands};
use crate::workers::{WorkerPool, WorkerRemoveError};
use cid_intention_filter::{
    FILTER_ACCEPTED, FILTER_DEPOSIT_BELOW_MINIMUM, FILTER_MALFORMED_DIGEST,
    FILTER_UNSUPPORTED_ACTION, FILTER_UNSUPPORTED_ASSET,
};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellDep};
use ckb_types::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
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
    /// Assembly windows dispatched this tick.
    pub isolate_windows: usize,
}

struct FilteredBucket {
    process: ResolvedProcess,
    accepted: Vec<ScannedIntention>,
    refunds: Vec<ScannedIntention>,
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
    allow_missing_filter: bool,
    workers: WorkerPool<R, S>,
    /// Optional shared command queue for live registry API updates.
    worker_cmds: Option<Arc<WorkerCommands>>,
    /// Optional shared pending map for registry API code-rebind checks (P1-6).
    busy: Option<ProcessBusyMap>,
    last_tip: Option<u64>,
    /// Varies probe order for large candidate sets independently of search results.
    scan_number: u64,
    /// Indexer membership from the previous scan, used only to spot arrivals.
    previous_live_keys: HashSet<Vec<u8>>,
    tick_cell_deps: Vec<CellDep>,
    tick_cid_locks: Arc<[CidLockIdentity]>,
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
        worker_cmds: Option<Arc<WorkerCommands>>,
    ) -> Self {
        Self {
            rpc,
            clock,
            ssri,
            config,
            allow_missing_filter: false,
            workers,
            worker_cmds,
            busy: None,
            last_tip: None,
            scan_number: 0,
            previous_live_keys: HashSet::new(),
            tick_cell_deps: Vec::new(),
            tick_cid_locks: Arc::from([]),
            execution_code_key: Vec::new(),
        }
    }

    pub fn with_workers_owned(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        workers: WorkerPool<R, S>,
        worker_cmds: Option<Arc<WorkerCommands>>,
    ) -> Self {
        let rpc = Arc::new(rpc);
        let ssri = Arc::new(ssri);
        Self::with_workers(rpc, clock, ssri, config, workers, worker_cmds)
    }

    pub fn with_busy_map(mut self, busy: ProcessBusyMap) -> Self {
        self.busy = Some(busy);
        self
    }

    /// Opt in to assembly for guests that explicitly lack Process.filter.
    pub fn with_missing_filter_fallback(mut self, allow: bool) -> Self {
        self.allow_missing_filter = allow;
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

    pub fn worker_cmds(&self) -> Option<Arc<WorkerCommands>> {
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
        let Some(mut batch) = cmds.drain() else {
            tracing::error!(target: LOG_TARGET, event = "worker_cmds.poisoned");
            return;
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
            cmds.requeue(requeue);
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
        let released_digests = self.reconcile_pending(tip);

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

        let mut cid_locks = self.config.scan_lock_identities();
        let scan_ids: Vec<([u8; 32], ScriptHashType)> = cid_locks
            .iter()
            .map(|id| (id.code_hash, id.hash_type))
            .collect();
        if let Some(hash) = resolved.cid_lock_data_hash {
            cid_locks.extend(
                [
                    ScriptHashType::Data,
                    ScriptHashType::Data1,
                    ScriptHashType::Data2,
                ]
                .map(|hash_type| CidLockIdentity::new(hash, hash_type)),
            );
        }
        self.tick_cid_locks = cid_locks.into();
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

        let scan_number = self.scan_number;
        self.scan_number = self.scan_number.wrapping_add(1);
        let arrivals = Arc::new(if scan_number == 0 {
            HashSet::new()
        } else {
            live_keys
                .difference(&self.previous_live_keys)
                .cloned()
                .collect()
        });
        self.workers.for_each_mut(|w| {
            w.with_state_mut(|state| {
                let pending_occupied = state.submit.pending_occupied_keys();
                state.submit.in_flight.retain(|k| {
                    (live_keys.contains(k) || pending_occupied.contains(k))
                        && !state.submit.rejected.contains(k)
                });
                state
                    .refund_state_deps
                    .retain(|hash, _| state.submit.pending.contains_key(hash));
                state.probe_budget = Some(process_batch::ProbeBudget::for_scan(
                    scan_number,
                    arrivals.clone(),
                ));
            });
        });

        self.previous_live_keys = live_keys;
        let buckets = bucket_by_process_type_hash(scanned, &self.config.cid_code_hash);
        report.skipped +=
            report.scanned - buckets.iter().map(|(_, cells)| cells.len()).sum::<usize>();
        let mut buckets: Vec<_> = buckets
            .into_iter()
            .filter_map(|(hash, mut bucket)| {
                let Some(worker) = self.workers.get(&hash) else {
                    tracing::warn!(target: LOG_TARGET, event = "process.unregistered",
                        process_type_hash = %hex_pref(&hash), count = bucket.len());
                    report.skipped += bucket.len();
                    return None;
                };
                if self
                    .worker_cmds
                    .as_ref()
                    .is_some_and(|cmds| cmds.has_update(hash))
                {
                    report.skipped += bucket.len();
                    return None;
                }
                worker.with_state(|state| {
                    let pending = state.submit.pending_occupied_keys();
                    bucket.retain(|item| {
                        let key = out_point_key(&item.out_point);
                        !pending.contains(&key)
                            && !state.submit.in_flight.contains(&key)
                            && !state.submit.rejected.contains(&key)
                    });
                });
                if bucket.is_empty() {
                    return None;
                }
                self.prepare_bucket(hash, bucket, &mut report)
            })
            .collect();
        let mut searches: HashMap<_, _> = buckets
            .iter_mut()
            .filter(|bucket| !bucket.accepted.is_empty())
            .map(|bucket| {
                (
                    bucket.process.type_hash,
                    std::mem::take(&mut bucket.accepted),
                )
            })
            .collect();

        // Let an eligible refund use the state just released by a pending
        // digest before another digest can occupy it.
        for bucket in &mut buckets {
            if released_digests.contains(&bucket.process.type_hash) {
                self.dispatch_filter_refunds(bucket, &mut report);
            }
        }
        self.wait_all_batches(&mut report, &mut searches);

        // Give each Process one worker job per round, then handle its result.
        for _ in 0..64 {
            let mut started = false;
            for bucket in &buckets {
                let hash = &bucket.process.type_hash;
                let Some(worker) = self.workers.get(hash) else {
                    continue;
                };
                if let Some(items) = searches.remove(hash) {
                    self.process_bucket(&bucket.process, items, &mut report);
                    started |= worker.batch_outstanding();
                }
            }
            if !started {
                break;
            }
            self.wait_all_batches(&mut report, &mut searches);
        }

        for bucket in &mut buckets {
            self.dispatch_filter_refunds(bucket, &mut report);
        }
        self.wait_all_batches(&mut report, &mut searches);

        self.workers.for_each_mut(|worker| {
            worker.with_state_mut(|state| state.probe_budget = None);
        });
        self.tick_cell_deps.clear();
        self.clear_worker_tick_code();
        self.sync_busy_map();
        report
    }

    fn dispatch_filter_refunds(&self, bucket: &mut FilteredBucket, report: &mut TickReport) {
        if !bucket.refunds.is_empty() {
            self.dispatch_process_batch(
                bucket.process.type_hash,
                &bucket.process,
                std::mem::take(&mut bucket.refunds),
                Vec::new(),
                1,
                report,
                BatchMode::FilterRefund,
            );
        }
    }

    /// Block until every outstanding RunBatch has an ack.
    fn wait_all_batches(
        &mut self,
        report: &mut TickReport,
        searches: &mut HashMap<[u8; 32], Vec<ScannedIntention>>,
    ) {
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
                    Self::apply_batch_report(report, &polled.delta);
                    if polled.mode == BatchMode::Assemble
                        && !polled.rest.is_empty()
                        && (polled.outcome == BatchOutcome::Submitted
                            || (polled.outcome == BatchOutcome::Continue
                                && polled.delta.conflicts == 0
                                && polled.delta.last_error.is_none()
                                && polled.delta.skipped > 0))
                    {
                        searches.insert(hash, polled.rest.to_vec());
                    }
                } else if worker.batch_outstanding() {
                    any_outstanding = true;
                }
            }
            if !any_outstanding {
                return;
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
                cid_lock_data_hash: None,
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

    fn reconcile_pending(&mut self, tip: Option<u64>) -> HashSet<[u8; 32]> {
        let mut released_digests = HashSet::new();
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
                let status = self.rpc.get_transaction_status(&tx_hash);
                if let Some(worker) = self.workers.get(&process_hash) {
                    worker.with_state_mut(|state| {
                        if let Some(pending) = state.submit.pending.get_mut(&hash_bytes) {
                            pending.currently_committed = false;
                            if let Ok(TxStatus::Committed { block_number, .. }) = status {
                                if !pending.seen_committed {
                                    pending.seen_committed = true;
                                    tracing::info!(
                                        target: LOG_TARGET,
                                        event = "pending.committed",
                                        tx_hash = %tx_hex,
                                        block_number,
                                    );
                                }
                            }
                        }
                    });
                }
                match status {
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
                                if !state.refund_state_deps.contains_key(&hash_bytes) {
                                    released_digests.insert(process_hash);
                                }
                                for k in p.consumed {
                                    state.submit.in_flight.remove(&k);
                                }
                            }
                        });
                    }
                    Ok(TxStatus::Committed {
                        block_number,
                        block_hash,
                    }) => {
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
                        match self.rpc.get_block_hash(block_number) {
                            Ok(Some(canonical_hash)) if canonical_hash == block_hash => {}
                            Ok(_) => {
                                tracing::warn!(target: LOG_TARGET, event = "pending.noncanonical",
                                    tx_hash = %tx_hex, block_number);
                                continue;
                            }
                            Err(error) => {
                                tracing::warn!(target: LOG_TARGET, event = "pending.block_lookup",
                                    tx_hash = %tx_hex, block_number, %error);
                                continue;
                            }
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
                                if !state.refund_state_deps.contains_key(&hash_bytes) {
                                    released_digests.insert(process_hash);
                                }
                            });
                        } else {
                            worker.with_state_mut(|state| {
                                if let Some(pending) = state.submit.pending.get_mut(&hash_bytes) {
                                    pending.currently_committed = true;
                                }
                            });
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
        released_digests
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

    fn process_is_healthy(&self, process: &ResolvedProcess) -> bool {
        self.workers
            .get(&process.type_hash)
            .is_some_and(|worker| worker.with_state(|state| !state.is_process_quarantined(process)))
    }

    fn prepare_bucket(
        &self,
        hash: [u8; 32],
        mut bucket: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) -> Option<FilteredBucket> {
        let worker = self.workers.get(&hash)?;
        let cid = CidLockIdentity::new(self.config.cid_code_hash, self.config.hash_type);
        let process = match worker
            .with_state_mut(|state| state.resolver.resolve(self.rpc.as_ref(), &hash, cid))
        {
            Ok(Some(process)) => process,
            result => {
                tracing::warn!(target: LOG_TARGET, event = "process.resolve_miss",
                    process_type_hash = %hex_pref(&hash), error = ?result.err());
                report.skipped += bucket.len();
                return None;
            }
        };
        if !self.process_is_healthy(&process) {
            report.skipped += bucket.len();
            return None;
        }
        bucket.retain(|item| {
            if !item.has_supported_user_lock() {
                tracing::warn!(target: LOG_TARGET, event = "scan.unsupported_user_lock",
                    out_point = ?item.out_point, hash_type = item.args.entry.user_lock.hash_type);
                report.skipped += 1;
                return false;
            }
            match item.belongs_to_process(&cid.code_hash, &process.type_hash) {
                Ok(()) => true,
                Err(reason) => {
                    tracing::warn!(target: LOG_TARGET, event = "scan.unroutable",
                    out_point = ?item.out_point, ?reason);
                    report.skipped += 1;
                    false
                }
            }
        });

        let mut accepted = Vec::new();
        let mut refunds = Vec::new();
        match filter_intentions(self.ssri.as_ref(), &process.binary, &bucket) {
            Ok(reasons) => {
                for (item, reason) in bucket.into_iter().zip(reasons) {
                    match reason {
                        Some(FILTER_ACCEPTED) => accepted.push(item),
                        Some(
                            reason @ (FILTER_UNSUPPORTED_ASSET
                            | FILTER_DEPOSIT_BELOW_MINIMUM
                            | FILTER_MALFORMED_DIGEST
                            | FILTER_UNSUPPORTED_ACTION),
                        ) => {
                            tracing::info!(target: LOG_TARGET, event = "ssri.filter_refund",
                                process_type_hash = %hex_pref(&hash), out_point = ?item.out_point, reason);
                            refunds.push(item);
                        }
                        Some(reason) => {
                            // Reason 1 reports a failure to decode the packed cell output.
                            tracing::warn!(target: LOG_TARGET, event = "ssri.filter_protocol_error",
                                process_type_hash = %hex_pref(&hash), out_point = ?item.out_point,
                                reason);
                            report.skipped += 1;
                        }
                        None => {
                            tracing::warn!(target: LOG_TARGET, event = "ssri.filter_cell_too_large",
                                process_type_hash = %hex_pref(&hash), out_point = ?item.out_point);
                            report.skipped += 1;
                        }
                    }
                }
            }
            Err(SsriError::MethodNotFound) => {
                worker.with_state_mut(|state| {
                    if !state.filter_missing_logged {
                        tracing::warn!(target: LOG_TARGET, event = "ssri.filter_missing",
                            process_type_hash = %hex_pref(&hash),
                            fallback = self.allow_missing_filter);
                        state.filter_missing_logged = true;
                    }
                });
                if !self.allow_missing_filter {
                    report.skipped += bucket.len();
                    report.last_error = Some("Process.filter is missing".into());
                    return None;
                }
                accepted = bucket;
            }
            Err(error) => {
                tracing::warn!(target: LOG_TARGET, event = "ssri.filter_failed",
                    process_type_hash = %hex_pref(&hash), %error);
                report.skipped += bucket.len();
                report.last_error = Some(format!("Process.filter: {error}"));
                return None;
            }
        }

        Some(FilteredBucket {
            process,
            accepted,
            refunds,
        })
    }

    fn process_bucket(
        &self,
        process: &ResolvedProcess,
        mut items: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) {
        let hash = process.type_hash;
        let Some(worker) = self.workers.get(&hash) else {
            return;
        };
        let paused = worker.with_state(|state| {
            state.is_process_unfunded(process)
                || state.is_process_quarantined(process)
                || state
                    .probe_budget
                    .as_ref()
                    .expect("tick probe budget")
                    .remaining
                    == 0
        });
        if paused {
            report.skipped += items.len();
            return;
        }
        let (min, max) = match self.ssri.get_batch_bounds(&process.binary) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "batch_bounds",
                    error = %e,
                );
                report.skipped += items.len();
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
            report.skipped += items.len();
            return;
        }

        worker.with_state(|state| {
            items.retain(|item| {
                !state
                    .submit
                    .in_flight
                    .contains(&out_point_key(&item.out_point))
            });
        });
        let mut locks = HashSet::new();
        let mut window = Vec::new();
        let mut rest = Vec::new();
        for item in &items {
            if window.len() < max as usize && locks.insert(item.lock_group_key()) {
                window.push(item.clone());
            } else {
                rest.push(item.clone());
            }
        }
        if window.len() < min as usize {
            report.skipped += window.len() + rest.len();
            return;
        }

        report.isolate_windows += 1;
        self.dispatch_process_batch(
            hash,
            process,
            window,
            rest,
            min,
            report,
            BatchMode::Assemble,
        );
    }

    fn batch_code_key(&self, process: &ResolvedProcess) -> Vec<u8> {
        let mut key = self.execution_code_key.clone();
        if let Some(cell) = &process.code_cell {
            key.extend_from_slice(cell.out_point.as_slice());
        }
        key
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
        report: &mut TickReport,
        mode: BatchMode,
    ) -> BatchOutcome {
        let worker = match self.workers.get(&hash) {
            Some(w) => w,
            None => return BatchOutcome::Continue,
        };
        if self
            .worker_cmds
            .as_ref()
            .is_some_and(|cmds| cmds.has_update(hash))
        {
            return BatchOutcome::Continue;
        }
        let max_size = if self.config.max_assembled_tx_bytes == 0 {
            DEFAULT_MAX_ASSEMBLED_TX_BYTES
        } else {
            self.config.max_assembled_tx_bytes
        };
        match worker.start_batch(
            process.clone(),
            batch,
            self.batch_code_key(process),
            self.tick_cell_deps.clone(),
            self.tick_cid_locks.clone(),
            self.config.refund_policy.clone(),
            max_size,
            rest,
            min,
            mode,
            self.worker_cmds.clone(),
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
        let cmds = Arc::new(WorkerCommands::default());
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
        cmds.enqueue(WorkerCommand::Register(e));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "register must spawn worker"
        );

        cmds.enqueue(WorkerCommand::delete(hash));
        let _ = loop_.tick();
        assert!(
            !loop_.workers().contains(&hash),
            "delete must stop idle worker"
        );
    }

    #[test]
    fn worker_command_delete_deferred_while_in_flight() {
        let cmds = Arc::new(WorkerCommands::default());
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
                    currently_committed: false,
                },
            );
        });
        let mut loop_ =
            DigestLoop::with_workers(rpc, NopClock, ssri, cfg(), pool, Some(cmds.clone()));
        cmds.enqueue(WorkerCommand::delete(hash));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "in-flight delete must stay deferred"
        );
        assert_eq!(cmds.queued().len(), 1);
        assert!(matches!(cmds.queued()[0], WorkerCommand::Delete { .. }));

        // P0-1: re-register must invalidate the deferred delete.
        let e2 = entry([0x33; 32], [0x44; 32]);
        cmds.enqueue(WorkerCommand::Register(e2));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "re-register must keep worker; stale delete must not apply"
        );
        assert!(
            cmds.queued()
                .iter()
                .all(|c| { !matches!(c, WorkerCommand::Delete { hash: h, .. } if *h == hash) }),
            "no stale Delete may remain after re-register"
        );
    }
}
