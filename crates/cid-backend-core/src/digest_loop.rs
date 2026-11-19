use crate::assemble::{
    assemble_digest_incompatible, execution_from_assembled, validate_assembled_tx, AssembleError,
};
use crate::code_deps::{resolve_execution_cell_deps, resolve_type_id_code_cell, CodeDepError};
use crate::fee::CidLockIdentity;
use crate::host_query::{seed_host_for_batch, HostQueryError, HostQueryRpc};
use crate::process::{ProcessEntry, ProcessResolver, ResolvedProcess};
use crate::rpc::{out_point_key, Rpc, RpcError, TxStatus};
use crate::scan::{bucket_by_process_type_hash, scan_intentions_multi, ScannedIntention};
use crate::ssri::{build_batch_draft, draft_cells_for_batch, SsriError, SsriHost};
use crate::submit::{
    combo_fingerprint, dry_run_gate, execution_code_key, submit_execution, DryRunGate, SubmitState,
};
use crate::type_id::TypeIdCodeDep;
use crate::workers::{WorkerPool, WorkerRemoveError};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellDep};
use ckb_types::prelude::*;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
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
    /// Max molecule bytes for `Process.assemble` passthrough txs.
    pub max_assembled_tx_bytes: usize,
}

impl LoopConfig {
    /// Unique scan targets: primary CID lock only.
    pub fn scan_lock_identities(&self) -> Vec<CidLockIdentity> {
        vec![CidLockIdentity::new(self.cid_code_hash, self.hash_type)]
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

enum BatchOutcome {
    /// digest_ok submitted; Process input is occupied until settlement.
    SubmittedProcess,
    /// Submitted without spending Process (User Lock refund).
    Submitted,
    /// `SubmitRejected` for this exact Intention combo (caller isolates).
    Rejected,
    /// Combo already in `rejected_combos`; isolate without a new submit.
    CachedRejected,
    /// Conflict / Unavailable / assemble skip: remaining items may continue.
    Continue,
    /// Process recreate already occupied; stop digest_ok this tick.
    ProcessOccupied,
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
    Delete([u8; 32]),
}

pub struct DigestLoop<R: Rpc + HostQueryRpc, C: Clock, S: SsriHost> {
    rpc: R,
    clock: C,
    ssri: S,
    config: LoopConfig,
    workers: WorkerPool,
    /// Optional shared command queue for live registry API updates.
    worker_cmds: Option<Arc<Mutex<Vec<WorkerCommand>>>>,
    last_tip: Option<u64>,
    tick_cell_deps: Vec<CellDep>,
    execution_code_key: Vec<u8>,
}

impl<R: Rpc + HostQueryRpc, C: Clock, S: SsriHost> DigestLoop<R, C, S> {
    pub fn new(rpc: R, clock: C, ssri: S, config: LoopConfig) -> Self {
        Self::with_workers(
            rpc,
            clock,
            ssri,
            config,
            WorkerPool::from_entries(vec![]),
            None,
        )
    }

    pub fn with_workers(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        workers: WorkerPool,
        worker_cmds: Option<Arc<Mutex<Vec<WorkerCommand>>>>,
    ) -> Self {
        Self {
            rpc,
            clock,
            ssri,
            config,
            workers,
            worker_cmds,
            last_tip: None,
            tick_cell_deps: Vec::new(),
            execution_code_key: Vec::new(),
        }
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
        let mut pool = WorkerPool::from_entries(vec![entry]);
        if let Some(w) = pool.get_mut(&hash) {
            w.resolver = resolver;
        }
        Self::with_workers(rpc, clock, ssri, config, pool, None)
    }

    pub fn rpc(&self) -> &R {
        &self.rpc
    }

    pub fn ssri(&self) -> &S {
        &self.ssri
    }

    pub fn workers(&self) -> &WorkerPool {
        &self.workers
    }

    pub fn workers_mut(&mut self) -> &mut WorkerPool {
        &mut self.workers
    }

    pub fn worker_cmds(&self) -> Option<Arc<Mutex<Vec<WorkerCommand>>>> {
        self.worker_cmds.clone()
    }

    /// Register (or update) a Process worker immediately.
    pub fn register_process(&mut self, entry: ProcessEntry) -> bool {
        let created = self.workers.register(entry);
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

    /// Remove a Process worker when idle.
    pub fn unregister_process(
        &mut self,
        hash: &[u8; 32],
    ) -> Result<Option<crate::workers::ProcessWorker>, WorkerRemoveError> {
        let removed = self.workers.try_remove(hash)?;
        if removed.is_some() {
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
        let batch = std::mem::take(&mut *cmds.lock().unwrap_or_else(|e| e.into_inner()));
        let mut requeue = Vec::new();
        for cmd in batch {
            match cmd {
                WorkerCommand::Register(entry) => {
                    let _ = self.register_process(entry);
                }
                WorkerCommand::Delete(hash) => match self.workers.try_remove(&hash) {
                    Ok(Some(_)) => {
                        tracing::info!(
                            target: LOG_TARGET,
                            event = "process.disabled",
                            process_type_hash = %hex_pref(&hash),
                        );
                    }
                    Ok(None) => {}
                    Err(WorkerRemoveError::InFlight) => {
                        requeue.push(WorkerCommand::Delete(hash));
                        tracing::info!(
                            target: LOG_TARGET,
                            event = "process.delete_deferred",
                            process_type_hash = %hex_pref(&hash),
                        );
                    }
                },
            }
        }
        if !requeue.is_empty() {
            if let Some(q) = &self.worker_cmds {
                q.lock().unwrap_or_else(|e| e.into_inner()).extend(requeue);
            }
        }
    }

    pub fn state(&self) -> LoopState {
        let hashes = self.workers.hashes();
        let submit = if hashes.len() == 1 {
            self.workers.get(&hashes[0]).map(|w| w.submit.clone())
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
                return report;
            }
        };
        let code_key = execution_code_key(&resolved.type_id_outpoints);
        if code_key != self.execution_code_key {
            if !self.execution_code_key.is_empty() || !code_key.is_empty() {
                self.workers.for_each_mut(|w| {
                    w.submit.rejected_combos.clear();
                    w.isolate_expanded.clear();
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
        let scanned = match scan_intentions_multi(&self.rpc, &scan_ids) {
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
            let pending_occupied = w.submit.pending_occupied_keys();
            w.submit.in_flight.retain(|k| {
                (live_keys.contains(k) || pending_occupied.contains(k))
                    && !w.submit.rejected.contains(k)
            });
        });

        let mut eligible = Vec::new();
        for i in scanned {
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
            let w = self
                .workers
                .get(&i.args.process_type_hash)
                .expect("checked");
            if w.submit.in_flight.contains(&k) || w.submit.rejected.contains(&k) {
                continue;
            }
            eligible.push(i);
        }

        for (hash, bucket) in bucket_by_process_type_hash(&eligible) {
            self.process_bucket(hash, bucket, &mut report);
        }

        self.tick_cell_deps.clear();
        self.clear_worker_tick_code();
        report
    }

    fn refresh_worker_process_code(&mut self) {
        let snapshots: Vec<([u8; 32], crate::type_id::TypeIdIdentity)> = self
            .workers
            .hashes()
            .into_iter()
            .filter_map(|h| self.workers.get(&h).map(|w| (h, w.entry.code.clone())))
            .collect();
        for (identity_hash, code) in snapshots {
            let spec = TypeIdCodeDep {
                label: format!("worker_code_{}", hex_pref(&identity_hash)),
                identity: code,
            };
            let code_cell = match resolve_type_id_code_cell(&self.rpc, &spec) {
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
            if let Some(w) = self.workers.get_mut(&identity_hash) {
                w.resolver.set_tick_process_code(code_cell.as_ref());
            }
        }
    }

    fn clear_worker_tick_code(&mut self) {
        self.workers
            .for_each_mut(|w| w.resolver.set_tick_process_code(None));
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
            &self.rpc,
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
                .map(|w| w.submit.pending.keys().cloned().collect())
                .unwrap_or_default();
            for hash_bytes in hashes {
                let tx_hash = match Byte32::from_slice(&hash_bytes) {
                    Ok(h) => h,
                    Err(_) => {
                        if let Some(worker) = self.workers.get_mut(&process_hash) {
                            worker.submit.pending.remove(&hash_bytes);
                        }
                        continue;
                    }
                };
                let tx_hex = hex_pref(tx_hash.as_slice());
                match self.rpc.get_transaction_status(&tx_hash) {
                    Ok(TxStatus::Dropped) => {
                        let Some(worker) = self.workers.get_mut(&process_hash) else {
                            continue;
                        };
                        let seen_committed = worker
                            .submit
                            .pending
                            .get(&hash_bytes)
                            .map(|p| p.seen_committed)
                            .unwrap_or(false);
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
                        if let Some(p) = worker.submit.pending.remove(&hash_bytes) {
                            for k in p.consumed {
                                worker.submit.in_flight.remove(&k);
                            }
                        }
                    }
                    Ok(TxStatus::Committed { block_number }) => {
                        if let Some(worker) = self.workers.get_mut(&process_hash) {
                            if let Some(p) = worker.submit.pending.get_mut(&hash_bytes) {
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
                        }
                        let start_tip = tip;
                        let Some(fresh_tip) = self.refresh_tip_only() else {
                            continue;
                        };
                        let Some(worker) = self.workers.get_mut(&process_hash) else {
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
                            worker.submit.pending.remove(&hash_bytes);
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

        let process = {
            let worker = self.workers.get_mut(&hash).expect("exists");
            match worker.resolver.resolve(&self.rpc, &hash) {
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
        {
            let worker = self.workers.get_mut(&hash).expect("exists");
            if worker.isolate_resume.is_none() {
                worker.isolate_expanded.clear();
            }
        }
        let resume_key = self
            .workers
            .get(&hash)
            .and_then(|w| w.isolate_resume.clone());
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
        let worker = self.workers.get_mut(&hash).expect("exists");
        if let Some(k) = resume {
            worker.isolate_resume = Some(k);
        } else {
            worker.isolate_resume = None;
            worker.isolate_expanded.clear();
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
        let mut stack = vec![items];
        while let Some(items) = stack.pop() {
            let items: Vec<ScannedIntention> = items
                .into_iter()
                .filter(|i| {
                    self.workers
                        .get(&hash)
                        .map(|w| !w.submit.in_flight.contains(&out_point_key(&i.out_point)))
                        .unwrap_or(true)
                })
                .collect();
            if items.len() < min as usize {
                report.skipped += items.len();
                continue;
            }
            report.isolate_windows += 1;
            let n = items.len().min(max as usize);
            let window = items[..n].to_vec();
            let rest = items[n..].to_vec();
            let fp = self.window_fingerprint(&window, process);
            let already = self
                .workers
                .get(&hash)
                .map(|w| w.isolate_expanded.contains(&fp))
                .unwrap_or(false);
            if already {
                if !rest.is_empty() {
                    stack.push(rest);
                } else if window.len() > min as usize {
                    stack.push(window[1..].to_vec());
                }
                continue;
            }
            let cached = self.window_combo_cached(&hash, &window, process);
            let outcome = if cached {
                tracing::debug!(
                    target: LOG_TARGET,
                    event = "submit.rejected_combo",
                    n = window.len(),
                );
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
                let outcome = self.process_batch(hash, process, window.clone(), report);
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
                BatchOutcome::SubmittedProcess | BatchOutcome::ProcessOccupied => {
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
                BatchOutcome::Submitted | BatchOutcome::Continue => {
                    if !rest.is_empty() {
                        stack.push(rest);
                    }
                }
                BatchOutcome::Rejected | BatchOutcome::CachedRejected => {
                    if let Some(w) = self.workers.get_mut(&hash) {
                        w.isolate_expanded.insert(fp);
                    }
                    let mut drop_first = window[1..].to_vec();
                    drop_first.extend(rest);
                    if window.len() > min as usize {
                        if !drop_first.is_empty() {
                            stack.push(drop_first);
                        }
                        stack.push(window[..window.len() - 1].to_vec());
                    } else {
                        report.skipped += 1;
                        if !drop_first.is_empty() {
                            stack.push(drop_first);
                        }
                    }
                }
            }
        }
        None
    }

    fn window_fingerprint(&self, batch: &[ScannedIntention], process: &ResolvedProcess) -> Vec<u8> {
        let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        combo_fingerprint(
            &ops,
            Some(&process.live_cell.out_point),
            &self.execution_code_key,
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
        let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        let fp = combo_fingerprint(
            &ops,
            Some(&process.live_cell.out_point),
            &self.execution_code_key,
        );
        self.workers
            .get(hash)
            .map(|w| w.submit.rejected_combos.contains(&fp))
            .unwrap_or(false)
    }

    fn handle_incompatible_refunds(
        &mut self,
        hash: [u8; 32],
        batch: &[ScannedIntention],
        code: i8,
        report: &mut TickReport,
    ) -> BatchOutcome {
        let mut by_identity: Vec<(CidLockIdentity, Vec<ScannedIntention>)> = Vec::new();
        for item in batch {
            let id = item.lock_identity();
            if let Some((_, group)) = by_identity.iter_mut().find(|(existing, _)| *existing == id) {
                group.push(item.clone());
            } else {
                by_identity.push((id, vec![item.clone()]));
            }
        }

        if by_identity.len() > 1 {
            tracing::info!(
                target: LOG_TARGET,
                event = "refund.partition",
                code,
                groups = by_identity.len(),
            );
        }

        let mut submitted = false;
        for (_id, group) in by_identity {
            if group.is_empty() {
                continue;
            }
            match self.assemble_and_submit_refund(hash, &group, code, report) {
                BatchOutcome::Submitted => submitted = true,
                BatchOutcome::Rejected => return BatchOutcome::Rejected,
                BatchOutcome::CachedRejected => return BatchOutcome::CachedRejected,
                _ => {}
            }
        }

        if submitted {
            BatchOutcome::Submitted
        } else {
            BatchOutcome::Continue
        }
    }

    fn assemble_and_submit_refund(
        &mut self,
        hash: [u8; 32],
        group: &[ScannedIntention],
        code: i8,
        report: &mut TickReport,
    ) -> BatchOutcome {
        let worker = match self.workers.get_mut(&hash) {
            Some(w) => w,
            None => return BatchOutcome::Continue,
        };
        let skeleton = match assemble_digest_incompatible(group, &self.tick_cell_deps) {
            Ok(s) => {
                let out_sum: u64 =
                    s.tx.outputs()
                        .into_iter()
                        .map(|o| {
                            let c: u64 = o.capacity().unpack();
                            c
                        })
                        .sum();
                let in_sum: u64 = group.iter().map(|i| i.capacity).sum();
                let fee = in_sum.saturating_sub(out_sum);
                tracing::info!(
                    target: LOG_TARGET,
                    event = "refund.path",
                    code,
                    path = "user_lock",
                    fee,
                    n = group.len(),
                );
                s
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "refund.path",
                    code,
                    path = "unbuildable",
                    error = %e,
                );
                report.skipped += group.len();
                return BatchOutcome::Continue;
            }
        };

        let intent_ops: Vec<_> = group.iter().map(|i| i.out_point.clone()).collect();
        let fp = combo_fingerprint(&intent_ops, None, &self.execution_code_key);
        if worker.submit.rejected_combos.contains(&fp) {
            return BatchOutcome::CachedRejected;
        }
        let consumed: Vec<_> = skeleton
            .tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        match dry_run_gate(&self.rpc, &skeleton.tx) {
            DryRunGate::Rejected(m) => {
                worker.submit.rejected_combos.insert(fp);
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "dry_run.rejected",
                    error = %m,
                    n = group.len(),
                );
                report.last_error = Some(format!("dry-run rejected: {m}"));
                return BatchOutcome::Rejected;
            }
            DryRunGate::Skip(m) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "dry_run_unavailable",
                    error = %m,
                );
                report.last_error = Some(format!("dry-run: {m}"));
                report.skipped += group.len();
                return BatchOutcome::Continue;
            }
            DryRunGate::Proceed => {}
        }
        match submit_execution(&self.rpc, &skeleton, &mut worker.submit, &consumed) {
            Ok(hash) => {
                tracing::info!(
                    target: LOG_TARGET,
                    event = "submit.ok",
                    tx_hash = %hex_pref(hash.as_slice()),
                );
                report.submitted += 1;
                BatchOutcome::Submitted
            }
            Err(RpcError::Conflict(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.conflict",
                    error = %m,
                );
                report.conflicts += 1;
                report.last_error = Some(format!("submit conflict: {m}"));
                BatchOutcome::Continue
            }
            Err(RpcError::SubmitRejected(m)) => {
                worker.submit.rejected_combos.insert(fp);
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.rejected",
                    error = %m,
                    n = group.len(),
                );
                report.last_error = Some(format!("submit rejected: {m}"));
                BatchOutcome::Rejected
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "submit_failed",
                    error = %e,
                );
                report.last_error = Some(format!("submit failed: {e}"));
                report.skipped += group.len();
                BatchOutcome::Continue
            }
        }
    }

    fn process_batch(
        &mut self,
        hash: [u8; 32],
        process: &ResolvedProcess,
        batch: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) -> BatchOutcome {
        if self.window_combo_cached(&hash, &batch, process) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return BatchOutcome::CachedRejected;
        }

        if self
            .workers
            .get(&hash)
            .map(|w| w.submit.pending_occupies(&process.live_cell.out_point))
            .unwrap_or(false)
        {
            tracing::warn!(
                target: LOG_TARGET,
                event = "tick.skip",
                reason = "process_occupied",
            );
            report.skipped += batch.len();
            return BatchOutcome::ProcessOccupied;
        }

        let draft = build_batch_draft(&batch, process);
        let cells = draft_cells_for_batch(&batch, process);
        let tip = match self.rpc.tip_block_number() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "tip_block_number",
                    error = %e,
                );
                report.last_error = Some(format!("tip block: {e}"));
                report.skipped += batch.len();
                return BatchOutcome::Continue;
            }
        };
        let host = match seed_host_for_batch(&self.rpc, &cells, tip) {
            Ok(h) => h,
            Err(e) => {
                let reason = match &e {
                    HostQueryError::Rpc(_) => "host_query_rpc",
                    HostQueryError::MissingLiveCell(_) => "host_query_missing_cell",
                    HostQueryError::MissingBlockHash { .. } => "host_query_missing_tip_hash",
                };
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason,
                    error = %e,
                );
                report.last_error = Some(format!("host seed: {e}"));
                report.skipped += batch.len();
                return BatchOutcome::Continue;
            }
        };

        let max_size = if self.config.max_assembled_tx_bytes == 0 {
            DEFAULT_MAX_ASSEMBLED_TX_BYTES
        } else {
            self.config.max_assembled_tx_bytes
        };

        let assemble_outcome = self.ssri.assemble(&process.binary, &draft, host);
        let skeleton = match assemble_outcome {
            Ok(assembled) => {
                match validate_assembled_tx(&assembled, &batch, process, max_size) {
                    Ok(()) => {}
                    Err(AssembleError::SizeLimit { size, max }) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "assemble.invalid",
                            reason = "size_limit",
                            size,
                            max,
                        );
                        return self.record_invalid_assembled_reject(hash, &batch, process, report);
                    }
                    Err(AssembleError::InvalidAssembled(m)) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "assemble.invalid",
                            reason = "guest_tx",
                            error = %m,
                        );
                        return self.record_invalid_assembled_reject(hash, &batch, process, report);
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "tick.skip",
                            reason = "validate_assembled",
                            error = %e,
                        );
                        report.skipped += batch.len();
                        return BatchOutcome::Continue;
                    }
                }
                match execution_from_assembled(&assembled) {
                    Ok(s) => s,
                    Err(AssembleError::InvalidAssembled(m)) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "assemble.invalid",
                            reason = "execution_skeleton",
                            error = %m,
                        );
                        return self.record_invalid_assembled_reject(hash, &batch, process, report);
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "tick.skip",
                            reason = "execution_from_assembled",
                            error = %e,
                        );
                        report.skipped += batch.len();
                        return BatchOutcome::Continue;
                    }
                }
            }
            Err(SsriError::Incompatible(code)) => {
                tracing::info!(
                    target: LOG_TARGET,
                    event = "ssri.incompatible",
                    code,
                );
                return self.handle_incompatible_refunds(hash, &batch, code, report);
            }
            Err(SsriError::Unusable(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "ssri_unusable",
                    error = %m,
                );
                report.skipped += batch.len();
                return BatchOutcome::Continue;
            }
        };

        let consumed_process = skeleton
            .tx
            .inputs()
            .into_iter()
            .any(|i| i.previous_output() == process.live_cell.out_point);
        let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        let process_op = consumed_process.then(|| process.live_cell.out_point.clone());
        let fp = combo_fingerprint(&intent_ops, process_op.as_ref(), &self.execution_code_key);
        let worker = self.workers.get_mut(&hash).expect("exists");
        if worker.submit.rejected_combos.contains(&fp) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return BatchOutcome::CachedRejected;
        }
        let consumed: Vec<_> = skeleton
            .tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        match dry_run_gate(&self.rpc, &skeleton.tx) {
            DryRunGate::Rejected(m) => {
                worker.submit.rejected_combos.insert(fp);
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "dry_run.rejected",
                    error = %m,
                    n = batch.len(),
                );
                report.last_error = Some(format!("dry-run rejected: {m}"));
                return BatchOutcome::Rejected;
            }
            DryRunGate::Skip(m) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "dry_run_unavailable",
                    error = %m,
                );
                report.last_error = Some(format!("dry-run: {m}"));
                report.skipped += batch.len();
                return BatchOutcome::Continue;
            }
            DryRunGate::Proceed => {}
        }
        match submit_execution(&self.rpc, &skeleton, &mut worker.submit, &consumed) {
            Ok(hash) => {
                tracing::info!(
                    target: LOG_TARGET,
                    event = "submit.ok",
                    tx_hash = %hex_pref(hash.as_slice()),
                );
                report.submitted += 1;
                if consumed_process {
                    BatchOutcome::SubmittedProcess
                } else {
                    BatchOutcome::Submitted
                }
            }
            Err(RpcError::Conflict(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.conflict",
                    error = %m,
                );
                report.conflicts += 1;
                report.last_error = Some(format!("submit conflict: {m}"));
                BatchOutcome::Continue
            }
            Err(RpcError::SubmitRejected(m)) => {
                worker.submit.rejected_combos.insert(fp);
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.rejected",
                    error = %m,
                    n = batch.len(),
                );
                report.last_error = Some(format!("submit rejected: {m}"));
                BatchOutcome::Rejected
            }
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "tick.skip",
                    reason = "submit_failed",
                    error = %e,
                );
                report.last_error = Some(format!("submit failed: {e}"));
                report.skipped += batch.len();
                BatchOutcome::Continue
            }
        }
    }

    fn record_invalid_assembled_reject(
        &mut self,
        hash: [u8; 32],
        batch: &[ScannedIntention],
        process: &ResolvedProcess,
        report: &mut TickReport,
    ) -> BatchOutcome {
        let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        let fp = combo_fingerprint(
            &intent_ops,
            Some(&process.live_cell.out_point),
            &self.execution_code_key,
        );
        if let Some(w) = self.workers.get_mut(&hash) {
            w.submit.rejected_combos.insert(fp);
        }
        report.skipped += batch.len();
        BatchOutcome::Rejected
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
    use crate::ssri::{SsriError, SsriHost};
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
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    #[test]
    fn worker_command_register_and_delete_take_effect_on_tick() {
        let cmds = Arc::new(Mutex::new(Vec::new()));
        let mut loop_ = DigestLoop::with_workers(
            EmptyRpc,
            NopClock,
            NopSsri,
            cfg(),
            WorkerPool::from_entries(vec![]),
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

        cmds.lock().unwrap().push(WorkerCommand::Delete(hash));
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
        let mut pool = WorkerPool::from_entries(vec![e]);
        pool.get_mut(&hash).unwrap().submit.pending.insert(
            vec![0x01],
            crate::submit::PendingTx {
                consumed: vec![],
                created: vec![],
                seen_committed: false,
            },
        );
        let mut loop_ =
            DigestLoop::with_workers(EmptyRpc, NopClock, NopSsri, cfg(), pool, Some(cmds.clone()));
        cmds.lock().unwrap().push(WorkerCommand::Delete(hash));
        let _ = loop_.tick();
        assert!(
            loop_.workers().contains(&hash),
            "in-flight delete must stay deferred"
        );
        assert_eq!(cmds.lock().unwrap().len(), 1);
        assert!(matches!(cmds.lock().unwrap()[0], WorkerCommand::Delete(_)));
    }
}
