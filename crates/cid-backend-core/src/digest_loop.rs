use crate::assemble::{assemble_digest_incompatible, assemble_digest_ok, AssembleError};
use crate::code_deps::{resolve_execution_cell_deps, CodeDepError};
use crate::process::{ProcessResolver, ResolvedProcess};
use crate::rpc::{out_point_key, Rpc, RpcError, TxStatus};
use crate::scan::{bucket_by_process_type_hash, scan_intentions, ScannedIntention};
use crate::ssri::{to_intention_input, SsriError, SsriHost};
use crate::submit::{combo_fingerprint, submit_execution, SubmitState};
use crate::type_id::TypeIdCodeDep;
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellDep};
use ckb_types::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::time::Duration;

const LOG_TARGET: &str = "cid_backend";

fn hex_pref(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

pub struct LoopConfig {
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
}

/// Conservative default: 24 CKB blocks (~3 minutes at ~8s). Overridable via
/// `CID_CONFIRMATION_DEPTH`. Tests typically use `1`.
pub const DEFAULT_CONFIRMATION_DEPTH: u64 = 24;

pub struct LoopState {
    pub submit: SubmitState,
    pub resolver: ProcessResolver,
    /// Last observed tip. Memory-only; used to log tip rewinds.
    pub last_tip: Option<u64>,
    /// Memory-only per-Process-type-hash isolate resume (`out_point_key`).
    /// Next tick starts at this Intention so a skip/send budget cut does not
    /// restart at the same ASC head. Dropped on restart.
    pub isolate_resume: HashMap<[u8; 32], Vec<u8>>,
    /// Fingerprints already split this isolate sweep (memory-only). Prevents
    /// re-expanding the same cached window; cleared when a sweep completes.
    pub isolate_expanded: HashMap<[u8; 32], HashSet<Vec<u8>>>,
    /// CellDeps resolved for the current tick (Type-ID + static). Snapshot is
    /// fixed for every assemble in this tick. Cleared when the tick ends.
    pub tick_cell_deps: Vec<CellDep>,
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

pub struct DigestLoop<R: Rpc, C: Clock, S: SsriHost> {
    rpc: R,
    clock: C,
    ssri: S,
    config: LoopConfig,
    state: LoopState,
}

impl<R: Rpc, C: Clock, S: SsriHost> DigestLoop<R, C, S> {
    pub fn new(rpc: R, clock: C, ssri: S, config: LoopConfig, resolver: ProcessResolver) -> Self {
        Self {
            rpc,
            clock,
            ssri,
            config,
            state: LoopState {
                submit: SubmitState::default(),
                resolver,
                last_tip: None,
                isolate_resume: HashMap::new(),
                isolate_expanded: HashMap::new(),
                tick_cell_deps: Vec::new(),
            },
        }
    }

    pub fn rpc(&self) -> &R {
        &self.rpc
    }

    pub fn ssri(&self) -> &S {
        &self.ssri
    }

    pub fn state(&self) -> &LoopState {
        &self.state
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
        let mut report = TickReport::default();
        // Confirmation / reorg recovery must run even when Type-ID code deps
        // cannot be refreshed: already-submitted txs do not need new CellDeps.
        let tip = self.refresh_tip();
        self.reconcile_pending(tip);

        // Refresh Type-ID code CellDeps every tick so upgrades pick up the new
        // outpoint while script identity stays stable. Failures are transient:
        // skip NEW assembly/submit only; do not permanently reject Intentions.
        let resolved = match self.refresh_cell_deps() {
            Ok(deps) => deps,
            Err(e) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "code_deps.unavailable",
                    error = %e,
                );
                report.last_error = Some(format!("code deps: {e}"));
                self.state.resolver.set_tick_process_code(None);
                self.state.tick_cell_deps.clear();
                return report;
            }
        };
        self.state.tick_cell_deps = resolved.cell_deps;
        self.state
            .resolver
            .set_tick_process_code(resolved.process_code.as_ref().map(|c| &c.cell));

        let scanned =
            match scan_intentions(&self.rpc, &self.config.cid_code_hash, self.config.hash_type) {
                Ok(v) => v,
                Err(RpcError::Unavailable(m)) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "scan.unavailable",
                        error = %m,
                    );
                    report.last_error = Some(format!("scan unavailable: {m}"));
                    self.state.tick_cell_deps.clear();
                    self.state.resolver.set_tick_process_code(None);
                    return report;
                }
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "scan.unavailable",
                        error = %e,
                    );
                    report.last_error = Some(format!("scan error: {e}"));
                    self.state.tick_cell_deps.clear();
                    self.state.resolver.set_tick_process_code(None);
                    return report;
                }
            };
        report.scan_ok = true;
        report.scanned = scanned.len();

        let live_keys: HashSet<Vec<u8>> = scanned
            .iter()
            .map(|i| out_point_key(&i.out_point))
            .collect();
        let pending_occupied = self.state.submit.pending_occupied_keys();
        self.state.submit.in_flight.retain(|k| {
            (live_keys.contains(k) || pending_occupied.contains(k))
                && !self.state.submit.rejected.contains(k)
        });

        let eligible: Vec<ScannedIntention> = scanned
            .into_iter()
            .filter(|i| {
                let k = out_point_key(&i.out_point);
                !self.state.submit.in_flight.contains(&k)
                    && !self.state.submit.rejected.contains(&k)
            })
            .collect();

        for (_hash, bucket) in bucket_by_process_type_hash(&eligible) {
            self.process_bucket(bucket, &mut report);
        }
        self.state.tick_cell_deps.clear();
        self.state.resolver.set_tick_process_code(None);
        report
    }

    fn refresh_cell_deps(&self) -> Result<crate::code_deps::ResolvedExecutionDeps, CodeDepError> {
        if self.config.type_id_code_deps.is_empty() {
            return Ok(crate::code_deps::ResolvedExecutionDeps {
                cell_deps: self.config.extra_cell_deps.clone(),
                process_code: None,
            });
        }
        resolve_execution_cell_deps(
            &self.rpc,
            &self.config.type_id_code_deps,
            &self.config.extra_cell_deps,
        )
    }

    fn refresh_tip(&mut self) -> Option<u64> {
        match self.rpc.tip_block_number() {
            Ok(tip) => {
                if let Some(prev) = self.state.last_tip {
                    if tip < prev {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "reorg.tip_rewind",
                            from = prev,
                            to = tip,
                        );
                    }
                }
                self.state.last_tip = Some(tip);
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

    fn reconcile_pending(&mut self, tip: Option<u64>) {
        let depth = self.config.confirmation_depth.max(1);
        let hashes: Vec<Vec<u8>> = self.state.submit.pending.keys().cloned().collect();
        for hash_bytes in hashes {
            let tx_hash = match Byte32::from_slice(&hash_bytes) {
                Ok(h) => h,
                Err(_) => {
                    self.state.submit.pending.remove(&hash_bytes);
                    continue;
                }
            };
            let tx_hex = hex_pref(tx_hash.as_slice());
            match self.rpc.get_transaction_status(&tx_hash) {
                Ok(TxStatus::Dropped) => {
                    let seen_committed = self
                        .state
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
                    if let Some(p) = self.state.submit.pending.remove(&hash_bytes) {
                        for k in p.consumed {
                            self.state.submit.in_flight.remove(&k);
                        }
                    }
                }
                Ok(TxStatus::Committed { block_number }) => {
                    if let Some(p) = self.state.submit.pending.get_mut(&hash_bytes) {
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
                    // Re-read tip after status: a rewind during get_transaction_status
                    // (or a stale start-of-tick tip) must not settle against a height
                    // that is no longer canonical. Missing tip is Unavailable-style.
                    let start_tip = tip;
                    let Some(fresh_tip) = self.refresh_tip() else {
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
                        self.state.submit.pending.remove(&hash_bytes);
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

    fn process_bucket(&mut self, bucket: Vec<ScannedIntention>, report: &mut TickReport) {
        if bucket.is_empty() {
            return;
        }
        let hash = bucket[0].args.process_type_hash;
        let process = match self.state.resolver.resolve(&self.rpc, &hash) {
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
        let mut attempts = 0;
        if !self.state.isolate_resume.contains_key(&hash) {
            self.state.isolate_expanded.remove(&hash);
        }
        let bucket = Self::rotate_bucket(bucket, self.state.isolate_resume.get(&hash));
        let resume = self.isolate_window(
            &process,
            hash,
            bucket,
            min,
            max,
            report,
            &mut attempts,
            MAX_ATTEMPTS,
        );
        if let Some(k) = resume {
            self.state.isolate_resume.insert(hash, k);
        } else {
            self.state.isolate_resume.remove(&hash);
            self.state.isolate_expanded.remove(&hash);
        }
    }

    /// Split a `SubmitRejected` window into legal `[min, max]` sub-batches.
    /// Returns a resume `out_point_key` when leftover work should start there
    /// next tick (budget cut or Process occupied/consumed with remainder).
    ///
    /// Iterative so a long poison prefix cannot grow the stack. Cached combos
    /// are isolated without charging `attempts` (new `send_transaction` only).
    /// Each window fingerprint is expanded at most once per sweep; a separate
    /// cached-split budget stops diamond explosion. Progress persists in
    /// `isolate_expanded` until the sweep completes.
    #[allow(clippy::too_many_arguments)]
    fn isolate_window(
        &mut self,
        process: &ResolvedProcess,
        hash: [u8; 32],
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
                    !self
                        .state
                        .submit
                        .in_flight
                        .contains(&out_point_key(&i.out_point))
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
            let fp = Self::window_fingerprint(&window, process);
            let already = self
                .state
                .isolate_expanded
                .get(&hash)
                .map(|s| s.contains(&fp))
                .unwrap_or(false);
            if already {
                // Do not prefix-split again (that is the exponential diamond).
                // Linear-advance so a budget cut can resume remaining unique
                // suffixes without re-walking the expanded tree.
                if !rest.is_empty() {
                    stack.push(rest);
                } else if window.len() > min as usize {
                    stack.push(window[1..].to_vec());
                }
                continue;
            }
            let cached = self.window_combo_cached(&window, process);
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
                let outcome = self.process_batch(process, window.clone(), report);
                // Charge new send/assemble work only. CachedRejected is a
                // fingerprint hit (possibly after assemble chose refund).
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
                    self.state
                        .isolate_expanded
                        .entry(hash)
                        .or_default()
                        .insert(fp);
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

    fn window_fingerprint(batch: &[ScannedIntention], process: &ResolvedProcess) -> Vec<u8> {
        let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        combo_fingerprint(&ops, Some(&process.live_cell.out_point))
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

    /// True when this Intention set was `SubmitRejected` as digest_ok with
    /// this Process outpoint. Refund fingerprints omit Process and are
    /// checked after assemble, once the SSRI path is known.
    fn window_combo_cached(&self, batch: &[ScannedIntention], process: &ResolvedProcess) -> bool {
        let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
        self.state
            .submit
            .rejected_combos
            .contains(&combo_fingerprint(&ops, Some(&process.live_cell.out_point)))
    }

    fn process_batch(
        &mut self,
        process: &ResolvedProcess,
        batch: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) -> BatchOutcome {
        if self.window_combo_cached(&batch, process) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return BatchOutcome::CachedRejected;
        }
        let inputs: Vec<_> = batch.iter().map(to_intention_input).collect();
        let assembled = self.ssri.assemble(&process.binary, &inputs);
        let skeleton = match assembled {
            Ok(partial) => {
                if self
                    .state
                    .submit
                    .pending_occupies(&process.live_cell.out_point)
                {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "tick.skip",
                        reason = "process_occupied",
                    );
                    report.skipped += batch.len();
                    return BatchOutcome::ProcessOccupied;
                }
                match assemble_digest_ok(&batch, process, &partial, &self.state.tick_cell_deps) {
                    Ok(s) => s,
                    Err(AssembleError::CapacityShortfall { inputs, outputs }) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "tick.skip",
                            reason = "capacity_shortfall",
                            inputs,
                            outputs,
                        );
                        report.skipped += batch.len();
                        return BatchOutcome::Continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "tick.skip",
                            reason = "digest_ok",
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
                match assemble_digest_incompatible(&batch, &self.state.tick_cell_deps) {
                    Ok(s) => {
                        tracing::info!(
                            target: LOG_TARGET,
                            event = "refund.path",
                            code,
                            path = "user_lock",
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
                        report.skipped += batch.len();
                        return BatchOutcome::Continue;
                    }
                }
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
        let fp = combo_fingerprint(&intent_ops, process_op.as_ref());
        if self.state.submit.rejected_combos.contains(&fp) {
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
        match submit_execution(&self.rpc, &skeleton, &mut self.state.submit, &consumed) {
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
                self.state.submit.rejected_combos.insert(fp);
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
