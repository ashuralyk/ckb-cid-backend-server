//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
    ExecutionSkeleton,
};
use crate::code_deps::{cell_dep_code, merge_cell_deps};
const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::rpc::{LiveCell, Rpc, RpcError};
use crate::scan::ScannedIntention;
use crate::ssri::{
    build_batch_draft, draft_cell_deps_for_batch, draft_cells_for_batch, intention_draft_cell,
    SsriError, SsriHost, PROCESS_LOCK_ARGS_INVALID,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::worker_commands::{Submission, WorkerCommands};
use crate::workers::ProcessWorkerState;
use cid_process_executor::AssembledTx;
use cid_protocol::CidArgs;
use ckb_types::packed::{CellDep, OutPoint};
use ckb_types::prelude::{Entity, Unpack};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const LOG_TARGET: &str = "cid_backend";
const MAX_COMPARISON_PROBES: usize = 64;
const MAX_ARRIVAL_PROBES: usize = 16;

#[derive(Clone, Copy)]
pub(crate) struct ProbeBudget {
    pub(crate) remaining: usize,
    arrivals: usize,
}

impl Default for ProbeBudget {
    fn default() -> Self {
        Self {
            remaining: MAX_COMPARISON_PROBES,
            arrivals: MAX_ARRIVAL_PROBES,
        }
    }
}

fn hex_pref(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    SubmittedProcess,
    Submitted,
    Rejected,
    CachedRejected,
    Continue,
    ProcessOccupied,
    /// Leave cells live and advance to the next window without caching a verdict.
    RetryLater,
    ProbePending,
    RpcUnavailable,
}

#[derive(Debug, Default, Clone)]
pub struct BatchReportDelta {
    pub submitted: usize,
    pub skipped: usize,
    pub conflicts: usize,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchMode {
    Assemble,
    /// Check assembly for Process faults before a deferred refund gets its turn.
    CheckAssembly,
    FilterRefund,
}

pub struct ProcessBatchParams<'a> {
    pub hash: [u8; 32],
    /// Shared worker state — lock only around submit/cache mutations (P2-9).
    pub state: Arc<Mutex<ProcessWorkerState>>,
    pub process: &'a ResolvedProcess,
    pub batch: Vec<ScannedIntention>,
    pub rest: &'a [ScannedIntention],
    pub execution_code_key: &'a [u8],
    pub tick_cell_deps: &'a [CellDep],
    pub cid_locks: &'a [CidLockIdentity],
    pub refund_policy: &'a RefundPolicy,
    pub max_assembled_tx_bytes: usize,
    pub job_epoch: u64,
    pub epoch: Arc<AtomicU64>,
    pub min: u32,
    pub mode: BatchMode,
    pub commands: Option<Arc<WorkerCommands>>,
}

struct BatchLease<'a> {
    epoch: &'a AtomicU64,
    job_epoch: u64,
    commands: Option<&'a WorkerCommands>,
    hash: [u8; 32],
}

impl BatchLease<'_> {
    fn epoch_changed(&self) -> bool {
        self.epoch.load(Ordering::SeqCst) != self.job_epoch
    }

    fn cancelled(&self) -> bool {
        self.epoch_changed() || self.commands.is_some_and(|cmds| cmds.has_update(self.hash))
    }

    fn begin_submission(&self) -> Result<Option<Submission<'_>>, ()> {
        if self.epoch_changed() {
            return Err(());
        }
        self.commands
            .map(|cmds| cmds.begin_submission(self.hash).ok_or(()))
            .transpose()
    }
}

fn cancelled_delta(n: usize) -> (BatchOutcome, BatchReportDelta) {
    (
        BatchOutcome::Continue,
        BatchReportDelta {
            skipped: n,
            last_error: Some("worker: RunBatch cancelled".into()),
            ..BatchReportDelta::default()
        },
    )
}

pub fn window_combo_cached(
    worker: &ProcessWorkerState,
    batch: &[ScannedIntention],
    process: &ResolvedProcess,
    execution_code_key: &[u8],
) -> bool {
    let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let fp = combo_fingerprint(&ops, Some(&process.live_cell.out_point), execution_code_key);
    worker.submit.rejected_combos.contains(&fp)
}

pub fn run_process_batch<R, S>(
    rpc: Arc<R>,
    ssri: &S,
    params: ProcessBatchParams<'_>,
) -> (BatchOutcome, BatchReportDelta)
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    S: SsriHost,
{
    if params.mode == BatchMode::FilterRefund {
        return refund_filter_rejections(rpc.as_ref(), params);
    }
    let ProcessBatchParams {
        hash,
        state,
        process,
        mut batch,
        rest,
        execution_code_key,
        tick_cell_deps,
        cid_locks,
        refund_policy: _,
        max_assembled_tx_bytes,
        job_epoch,
        epoch,
        min,
        mode,
        commands,
    } = params;
    let lease = BatchLease {
        epoch: &epoch,
        job_epoch,
        commands: commands.as_deref(),
        hash,
    };
    let mut report = BatchReportDelta::default();

    // Narrow lock: cache / occupancy prechecks only.
    {
        let worker = state.lock().expect("process worker state");
        if worker.unhealthy_state.as_ref() == Some(&process.live_cell.out_point) {
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.isolation_probe.is_none()
            && window_combo_cached(&worker, &batch, process, execution_code_key)
        {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return (BatchOutcome::CachedRejected, report);
        }
        if worker.submit.pending_occupies(&process.live_cell.out_point)
            || worker.has_pending_filter_refund(&process.live_cell.out_point)
        {
            tracing::warn!(
                target: LOG_TARGET,
                event = "tick.skip",
                reason = "process_occupied",
            );
            report.skipped += batch.len();
            return (BatchOutcome::ProcessOccupied, report);
        }
    }

    let cell_deps = draft_cell_deps_for_batch(process, tick_cell_deps);
    if cell_deps.is_empty() {
        tracing::warn!(
            target: LOG_TARGET,
            event = "tick.skip",
            reason = "cell_deps_required",
        );
        report.last_error = Some(
            "cell_deps required for submit: resolve CID/Process code deps into the draft".into(),
        );
        report.skipped += batch.len();
        return (BatchOutcome::Continue, report);
    }
    let tip = match rpc.tip_block_number() {
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
            return (BatchOutcome::Continue, report);
        }
    };

    if lease.cancelled() {
        return cancelled_delta(batch.len());
    }

    let max_size = if max_assembled_tx_bytes == 0 {
        DEFAULT_MAX_ASSEMBLED_TX_BYTES
    } else {
        max_assembled_tx_bytes
    };

    // Assemble without holding the state mutex (P2-9). Live path passes
    // RpcHostQuery into ProcessExecutor::assemble (P1-3).
    let attempt = |items: &[ScannedIntention]| {
        let draft = build_batch_draft(items, process, &cell_deps);
        let cells = draft_cells_for_batch(items, process);
        ssri.assemble_live(rpc.clone(), &process.binary, &draft, &cells, tip)
    };
    let prepare = |assembled: &AssembledTx, items: &[ScannedIntention]| {
        prepare_execution(rpc.as_ref(), assembled, items, process, max_size, cid_locks)
    };
    let (probe, mut budget) = {
        let mut worker = state.lock().expect("process worker state");
        (worker.isolation_probe.take(), worker.probe_budget)
    };
    let prepared = if probe.is_none() {
        match attempt(&batch) {
            Ok(assembled) => match prepare(&assembled, &batch) {
                Err(PreparationError::NonLiveInput(op)) => {
                    report.last_error = Some(format!("guest-added input {op:?} is not live"));
                    None
                }
                result => Some(result),
            },
            Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
                return mark_process_unhealthy(hash, process, &state, batch.len());
            }
            Err(error) => {
                tracing::warn!(target: LOG_TARGET, event = "assemble.batch_failed",
                    process_type_hash = %hex_pref(&hash), %error,
                    intentions = ?batch.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                report.last_error = Some(format!("Process assembly failed ({error})"));
                None
            }
        }
    } else {
        None
    };
    let prepared = if let Some(prepared) = prepared {
        prepared
    } else {
        let candidates: HashMap<_, _> = batch
            .iter()
            .chain(rest)
            .map(|item| (&item.out_point, item))
            .collect();
        let mut probe = probe.unwrap_or_else(|| {
            IsolationProbe::new(&process.live_cell.out_point, &batch, rest, min as usize)
        });
        probe.max = probe.max.max(batch.len());
        probe.include_arrivals(batch.iter().chain(rest));
        let mut live = HashMap::new();
        let mut try_subset = |items: &[ScannedIntention]| {
            if window_combo_cached(
                &state.lock().expect("process worker state"),
                items,
                process,
                execution_code_key,
            ) {
                return Ok(None);
            }
            for item in items {
                let is_live = match live.get(&item.out_point) {
                    Some(&is_live) => is_live,
                    None => {
                        let is_live = rpc
                            .get_live_cell(&item.out_point)
                            .map_err(|error| SsriError::Unusable(error.to_string()))?
                            .is_some();
                        live.insert(item.out_point.clone(), is_live);
                        is_live
                    }
                };
                if !is_live {
                    return Ok(None);
                }
            }
            match attempt(items) {
                Ok(assembled) => match prepare(&assembled, items) {
                    Err(PreparationError::NonLiveInput(_)) => Ok(None),
                    Err(PreparationError::InputLookup(error)) => Err(SsriError::Unusable(format!(
                        "verify guest-added input: {error}"
                    ))),
                    result => Ok(Some(result)),
                },
                Err(error @ SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => Err(error),
                Err(error) => {
                    tracing::debug!(target: LOG_TARGET, event = "assemble.probe_failed",
                        process_type_hash = %hex_pref(&hash), %error,
                        intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                    Ok(None)
                }
            }
        };
        let result = probe.search(&candidates, &mut budget, &mut try_subset);
        let pending = probe.has_work();
        {
            let mut worker = state.lock().expect("process worker state");
            worker.probe_budget = budget;
            if pending {
                worker.isolation_probe = Some(probe);
            } else if matches!(result, Ok(None)) {
                worker.exhausted_isolation = Some(ExhaustedIsolation::new(
                    process,
                    execution_code_key,
                    batch.iter().chain(rest),
                ));
            }
        }
        match result {
            Ok(Some((items, prepared))) => {
                tracing::info!(target: LOG_TARGET, event = "assemble.batch_isolated",
                    process_type_hash = %hex_pref(&hash),
                    intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                batch = items;
                prepared
            }
            Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
                return mark_process_unhealthy(hash, process, &state, batch.len());
            }
            result => {
                if let Err(error) = result {
                    tracing::warn!(target: LOG_TARGET, event = "assemble.probe_deferred",
                        process_type_hash = %hex_pref(&hash), %error);
                    report.last_error = Some(format!("assemble isolation: {error}"));
                }
                report.skipped += batch.len();
                let outcome = if pending {
                    BatchOutcome::ProbePending
                } else {
                    BatchOutcome::RetryLater
                };
                return (outcome, report);
            }
        }
    };
    let skeleton = match prepared {
        Ok(skeleton) => skeleton,
        Err(PreparationError::Invalid(error)) => {
            let reason = match error {
                AssembleError::SizeLimit { .. } => "size_limit",
                _ => "guest_tx",
            };
            tracing::warn!(target: LOG_TARGET, event = "assemble.invalid", reason, %error);
            let mut worker = state.lock().expect("process worker state");
            return record_invalid_assembled_reject(
                &mut worker,
                &batch,
                process,
                execution_code_key,
                &mut report,
            );
        }
        Err(PreparationError::UnselectedIntention(out_point)) => {
            tracing::warn!(target: LOG_TARGET, event = "assemble.invalid",
                reason = "unselected_intention", ?out_point);
            let mut worker = state.lock().expect("process worker state");
            return record_invalid_assembled_reject(
                &mut worker,
                &batch,
                process,
                execution_code_key,
                &mut report,
            );
        }
        Err(PreparationError::InputLookup(error)) => {
            tracing::warn!(target: LOG_TARGET, event = "tick.skip",
                reason = "funding_input_lookup", %error);
            report.last_error = Some(format!("verify guest-added input: {error}"));
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        }
        Err(PreparationError::NonLiveInput(_)) => {
            unreachable!("non-live input starts or advances isolation")
        }
    };

    if mode == BatchMode::CheckAssembly {
        return (BatchOutcome::Continue, report);
    }

    let consumed_process = skeleton
        .tx
        .inputs()
        .into_iter()
        .any(|i| i.previous_output() == process.live_cell.out_point);
    let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let process_op = consumed_process.then(|| process.live_cell.out_point.clone());
    let fp = combo_fingerprint(&intent_ops, process_op.as_ref(), execution_code_key);

    {
        let worker = state.lock().expect("process worker state");
        if worker.submit.rejected_combos.contains(&fp) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return (BatchOutcome::CachedRejected, report);
        }
    }

    let consumed: Vec<_> = skeleton
        .tx
        .inputs()
        .into_iter()
        .map(|i| i.previous_output())
        .collect();

    // Dry-run without holding the state mutex.
    match dry_run_gate(rpc.as_ref(), &skeleton.tx) {
        DryRunGate::Rejected(m) => {
            let mut worker = state.lock().expect("process worker state");
            worker.submit.rejected_combos.insert(fp);
            tracing::warn!(
                target: LOG_TARGET,
                event = "dry_run.rejected",
                error = %m,
                n = batch.len(),
            );
            report.last_error = Some(format!("dry-run rejected: {m}"));
            return (BatchOutcome::Rejected, report);
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
            return (BatchOutcome::Continue, report);
        }
        DryRunGate::Proceed => {}
    }

    let mut worker = state.lock().expect("process worker state");
    let Ok(_submission) = lease.begin_submission() else {
        return cancelled_delta(batch.len());
    };
    match submit_execution(rpc.as_ref(), &skeleton, &mut worker.submit, &consumed) {
        Ok(tx_hash) => {
            tracing::info!(
                target: LOG_TARGET,
                event = "submit.ok",
                tx_hash = %hex_pref(tx_hash.as_slice()),
            );
            report.submitted += 1;
            if consumed_process {
                worker.isolation_probe = None;
                (BatchOutcome::SubmittedProcess, report)
            } else {
                (BatchOutcome::Submitted, report)
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
            (BatchOutcome::Continue, report)
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
            (BatchOutcome::Rejected, report)
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
            (BatchOutcome::Continue, report)
        }
    }
}

enum PreparationError {
    Invalid(AssembleError),
    UnselectedIntention(OutPoint),
    NonLiveInput(OutPoint),
    InputLookup(RpcError),
}

fn prepare_execution(
    rpc: &impl HostQueryRpc,
    assembled: &AssembledTx,
    batch: &[ScannedIntention],
    process: &ResolvedProcess,
    max_size: usize,
    cid_locks: &[CidLockIdentity],
) -> Result<ExecutionSkeleton, PreparationError> {
    validate_assembled_tx(assembled, batch, process, max_size)
        .map_err(PreparationError::Invalid)?;
    let skeleton = execution_from_assembled(assembled).map_err(PreparationError::Invalid)?;

    // Additional funding inputs must be live and cannot bypass Intention selection.
    for input in skeleton.tx.inputs().into_iter() {
        let op = input.previous_output();
        if op == process.live_cell.out_point || batch.iter().any(|item| item.out_point == op) {
            continue;
        }
        let Some(cell) = rpc
            .get_live_cell(&op)
            .map_err(PreparationError::InputLookup)?
        else {
            tracing::warn!(target: LOG_TARGET, event = "assemble.invalid",
                reason = "funding_input_not_live", out_point = ?op,
                process_type_hash = %hex_pref(&process.type_hash));
            return Err(PreparationError::NonLiveInput(op));
        };
        let lock = cell.lock();
        let code_hash: [u8; 32] = lock.code_hash().unpack();
        if cid_locks.iter().any(|identity| {
            code_hash == identity.code_hash && lock.hash_type() == identity.hash_type.into()
        }) || CidArgs::decode(&lock.args().raw_data()).is_ok()
        {
            return Err(PreparationError::UnselectedIntention(op));
        }
    }
    Ok(skeleton)
}

/// Back off after checking all subsets. The same candidate set may be rotated
/// by the window cursor, so key it independently of that order.
pub(crate) struct ExhaustedIsolation {
    fingerprint: Vec<u8>,
    pub(crate) polls_remaining: u8,
}

impl ExhaustedIsolation {
    fn new<'a>(
        process: &ResolvedProcess,
        execution_code_key: &[u8],
        items: impl Iterator<Item = &'a ScannedIntention>,
    ) -> Self {
        Self {
            fingerprint: Self::fingerprint(process, execution_code_key, items),
            // Skip the next three successful scans and retry on the fourth.
            polls_remaining: 4,
        }
    }

    pub(crate) fn matches(
        &self,
        process: &ResolvedProcess,
        execution_code_key: &[u8],
        items: &[ScannedIntention],
    ) -> bool {
        self.fingerprint == Self::fingerprint(process, execution_code_key, items.iter())
    }

    fn fingerprint<'a>(
        process: &ResolvedProcess,
        execution_code_key: &[u8],
        items: impl Iterator<Item = &'a ScannedIntention>,
    ) -> Vec<u8> {
        let mut ops: Vec<_> = items.map(|item| item.out_point.clone()).collect();
        ops.sort_unstable_by(|a, b| a.as_slice().cmp(b.as_slice()));
        combo_fingerprint(&ops, Some(&process.live_cell.out_point), execution_code_key)
    }
}

/// Keep positions stable across scans. The original search and each group of
/// arrivals cover disjoint subsets of the same list. Within the arrival
/// budget, give the newest group the first turn and rotate through the rest.
pub(crate) struct IsolationProbe {
    process: OutPoint,
    candidates: Vec<OutPoint>,
    min: usize,
    max: usize,
    older: SubsetSearch,
    arrivals: VecDeque<SubsetSearch>,
}

struct SubsetSearch {
    end: usize,
    /// If set, enumerate from the newest end and require an arrival.
    arrival_count: Option<usize>,
    indices: Option<Vec<usize>>,
}

impl IsolationProbe {
    fn new(
        process: &OutPoint,
        batch: &[ScannedIntention],
        rest: &[ScannedIntention],
        min: usize,
    ) -> Self {
        let candidates: Vec<_> = batch
            .iter()
            .chain(rest)
            .map(|item| item.out_point.clone())
            .collect();
        let max = batch.len();
        let mut older = SubsetSearch::new(candidates.len(), min, None);
        if max == min {
            older.advance(max);
        }
        Self {
            process: process.clone(),
            candidates,
            min,
            max,
            older,
            arrivals: VecDeque::new(),
        }
    }

    pub(crate) fn matches(&self, process: &OutPoint, min: u32) -> bool {
        self.process == *process && self.min == min as usize
    }

    fn has_work(&self) -> bool {
        self.older.has_work() || !self.arrivals.is_empty()
    }

    fn include_arrivals<'a>(&mut self, current: impl Iterator<Item = &'a ScannedIntention>) {
        let known: HashSet<_> = self.candidates.iter().collect();
        let arrivals: Vec<_> = current
            .filter(|item| !known.contains(&item.out_point))
            .map(|item| item.out_point.clone())
            .collect();
        if arrivals.is_empty() {
            return;
        }
        self.arrivals.push_front(SubsetSearch::new(
            self.candidates.len() + arrivals.len(),
            self.min,
            Some(arrivals.len()),
        ));
        self.candidates.extend(arrivals);
    }

    fn search<T>(
        &mut self,
        candidates: &HashMap<&OutPoint, &ScannedIntention>,
        budget: &mut ProbeBudget,
        attempt: &mut impl FnMut(&[ScannedIntention]) -> Result<Option<T>, SsriError>,
    ) -> Result<Option<(Vec<ScannedIntention>, T)>, SsriError> {
        let snapshot: Vec<_> = self
            .candidates
            .iter()
            .map(|op| candidates.get(op).copied())
            .collect();
        let arrival_budget = if self.older.has_work() {
            budget.remaining.min(budget.arrivals)
        } else {
            budget.remaining
        };
        for _ in 0..arrival_budget {
            let Some(mut group) = self.arrivals.pop_front() else {
                break;
            };
            let mut turn = 1;
            let result = group.search(&snapshot, self.max, &mut turn, attempt);
            let used = 1 - turn;
            budget.remaining -= used;
            budget.arrivals = budget.arrivals.saturating_sub(used);
            if group.has_work() {
                self.arrivals.push_back(group);
            }
            if let Some(found) = result? {
                return Ok(Some(found));
            }
        }
        self.older
            .search(&snapshot, self.max, &mut budget.remaining, attempt)
    }
}

impl SubsetSearch {
    fn new(end: usize, min: usize, arrival_count: Option<usize>) -> Self {
        Self {
            end,
            arrival_count,
            indices: Some((0..min).collect()),
        }
    }

    fn has_work(&self) -> bool {
        self.indices.is_some()
    }

    fn search<T>(
        &mut self,
        snapshot: &[Option<&ScannedIntention>],
        max: usize,
        budget: &mut usize,
        attempt: &mut impl FnMut(&[ScannedIntention]) -> Result<Option<T>, SsriError>,
    ) -> Result<Option<(Vec<ScannedIntention>, T)>, SsriError> {
        while let Some(indices) = self.indices.as_ref().filter(|_| *budget > 0) {
            *budget -= 1;
            let items: Option<Vec<_>> = if self.arrival_count.is_some() {
                indices
                    .iter()
                    .rev()
                    .map(|&i| snapshot[self.end - 1 - i].cloned())
                    .collect()
            } else {
                indices.iter().map(|&i| snapshot[i].cloned()).collect()
            };
            if let Some(items) = items {
                if let Some(assembled) = attempt(&items)? {
                    // Keep this position until submission. A cached transaction
                    // rejection skips the subset when the search resumes.
                    return Ok(Some((items, assembled)));
                }
            }
            self.advance(max);
        }
        Ok(None)
    }

    fn advance(&mut self, max: usize) {
        let indices = self.indices.as_mut().expect("active subset search");
        if !advance_probe_indices(indices, self.end, self.arrival_count.unwrap_or(self.end)) {
            let next_size = indices.len() + 1;
            self.indices = (next_size <= self.end.min(max)).then(|| (0..next_size).collect());
        }
    }
}

/// Exhaust nearby subsets before including a later candidate.
fn advance_probe_indices(
    indices: &mut [usize],
    candidate_count: usize,
    first_limit: usize,
) -> bool {
    for i in 0..indices.len() {
        let mut limit = indices.get(i + 1).copied().unwrap_or(candidate_count);
        if i == 0 {
            limit = limit.min(first_limit);
        }
        if indices[i] + 1 < limit {
            indices[i] += 1;
            for (initial, index) in indices[..i].iter_mut().enumerate() {
                *index = initial;
            }
            return true;
        }
    }
    false
}

fn mark_process_unhealthy(
    hash: [u8; 32],
    process: &ResolvedProcess,
    state: &Mutex<ProcessWorkerState>,
    skipped: usize,
) -> (BatchOutcome, BatchReportDelta) {
    tracing::error!(target: LOG_TARGET, event = "process.unhealthy",
        process_type_hash = %hex_pref(&hash), out_point = ?process.live_cell.out_point,
        code = PROCESS_LOCK_ARGS_INVALID, reason = "ProcessLockArgsInvalid");
    let mut worker = state.lock().expect("process worker state");
    worker.unhealthy_state = Some(process.live_cell.out_point.clone());
    worker.isolate_pending.clear();
    worker.isolate_resume = None;
    worker.isolation_probe = None;
    (
        BatchOutcome::Continue,
        BatchReportDelta {
            skipped,
            last_error: Some("Process assembly failed (exit 41: ProcessLockArgsInvalid)".into()),
            ..BatchReportDelta::default()
        },
    )
}

fn intention_snapshot(item: &ScannedIntention) -> LiveCell {
    let draft = intention_draft_cell(item);
    LiveCell {
        out_point: draft.out_point,
        output: draft.output,
        data: draft.data,
    }
}

fn verify_refund_cell(
    rpc: &impl HostQueryRpc,
    snapshot: &LiveCell,
    role: &str,
    count: usize,
    report: &mut BatchReportDelta,
) -> Result<(), BatchOutcome> {
    let outcome = match rpc.get_live_cell(&snapshot.out_point) {
        Ok(live) if live.as_ref() == Some(snapshot) => return Ok(()),
        Ok(_) => {
            tracing::warn!(target: LOG_TARGET, event = "refund.snapshot_mismatch",
                role, out_point = ?snapshot.out_point);
            BatchOutcome::Continue
        }
        Err(error) => {
            report.last_error = Some(format!("refund {role} lookup: {error}"));
            BatchOutcome::RpcUnavailable
        }
    };
    report.skipped += count;
    Err(outcome)
}

fn refund_filter_rejections<R: Rpc + HostQueryRpc>(
    rpc: &R,
    mut params: ProcessBatchParams<'_>,
) -> (BatchOutcome, BatchReportDelta) {
    const MAX_ATTEMPTS: usize = 16;
    let mut report = BatchReportDelta::default();
    let authorizing_state = &params.process.live_cell;
    let cursor = {
        let mut worker = params.state.lock().expect("process worker state");
        if worker.unhealthy_state.as_ref() == Some(&authorizing_state.out_point) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.submit.pending.values().any(|tx| {
            tx.consumed
                .iter()
                .any(|op| op.as_slice() == authorizing_state.out_point.as_slice())
        }) {
            worker.filter_refund_deferred = true;
            tracing::debug!(target: LOG_TARGET, event = "refund.process_state_pending",
                out_point = ?authorizing_state.out_point);
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.has_pending_filter_refund(&authorizing_state.out_point) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        worker.filter_refund_deferred = false;
        worker.refund_cursor.clone()
    };
    let Some(code) = params.process.verdict_code_cell() else {
        report.skipped = params.batch.len();
        report.last_error = Some("filter refund requires a live Process code-cell ELF".into());
        return (BatchOutcome::Continue, report);
    };
    for (snapshot, role) in [(code, "Process code"), (authorizing_state, "Process state")] {
        if let Err(outcome) =
            verify_refund_cell(rpc, snapshot, role, params.batch.len(), &mut report)
        {
            return (outcome, report);
        }
    }
    // Require both outpoints to remain live through transaction validation.
    let verdict_deps = [
        cell_dep_code(&code.out_point),
        cell_dep_code(&authorizing_state.out_point),
    ];
    let deps = match merge_cell_deps(&verdict_deps, params.tick_cell_deps) {
        Ok(deps) => deps,
        Err(error) => {
            report.skipped = params.batch.len();
            report.last_error = Some(format!("filter refund: {error}"));
            return (BatchOutcome::Continue, report);
        }
    };
    params
        .batch
        .sort_unstable_by(|a, b| a.out_point.as_slice().cmp(b.out_point.as_slice()));
    let start = cursor.map_or(0, |op| {
        params
            .batch
            .partition_point(|item| item.out_point.as_slice() <= op.as_slice())
    });
    let lease = BatchLease {
        epoch: &params.epoch,
        job_epoch: params.job_epoch,
        commands: params.commands.as_deref(),
        hash: params.hash,
    };
    let (before, after) = params.batch.split_at(start);
    for item in after.iter().chain(before).take(MAX_ATTEMPTS) {
        if lease.cancelled() {
            break;
        }
        params
            .state
            .lock()
            .expect("process worker state")
            .refund_cursor = Some(item.out_point.clone());
        let outcome = submit_filter_refund(rpc, &params, item, &deps, &mut report, &lease);
        if outcome == BatchOutcome::RpcUnavailable {
            break;
        }
    }
    let outcome = if report.submitted > 0 {
        BatchOutcome::Submitted
    } else {
        BatchOutcome::Continue
    };
    (outcome, report)
}

fn submit_filter_refund<R: Rpc + HostQueryRpc>(
    rpc: &R,
    params: &ProcessBatchParams<'_>,
    item: &ScannedIntention,
    deps: &[CellDep],
    report: &mut BatchReportDelta,
    lease: &BatchLease<'_>,
) -> BatchOutcome {
    let state = &params.state;
    let authorizing_state = &params.process.live_cell;
    let consumed = std::slice::from_ref(&item.out_point);
    let mut fp = combo_fingerprint(
        consumed,
        Some(&authorizing_state.out_point),
        params.execution_code_key,
    );
    // The Process is a dependency here. Keep refund rejections distinct
    // from digest rejections for the same Intention and Process state.
    fp.push(0);
    {
        let worker = state.lock().expect("process worker state");
        if worker.submit.pending_occupies(&item.out_point)
            || worker.submit.rejected_combos.contains(&fp)
        {
            report.skipped += 1;
            return BatchOutcome::Continue;
        }
    }
    // The CID refund verifies assets and fees, so dry-run cannot validate the
    // Intention data and lock args that were used to obtain the policy verdict.
    if let Err(outcome) = verify_refund_cell(rpc, &intention_snapshot(item), "Intention", 1, report)
    {
        return outcome;
    }
    let mode = params.refund_policy.mode_for(&item.lock_identity());
    let skeleton = match assemble_refund_with_policy(
        std::slice::from_ref(item),
        deps,
        mode,
        params.refund_policy.fee_rate,
    ) {
        Ok(skeleton) => {
            let returned = skeleton
                .tx
                .outputs()
                .get(0)
                .expect("refund output")
                .capacity()
                .unpack();
            let fee = item.capacity.saturating_sub(returned);
            tracing::info!(
                target: LOG_TARGET, event = "refund.path", path = "user_lock",
                out_point = ?item.out_point, fee,
            );
            skeleton
        }
        Err(error @ AssembleError::RefundFeeUnaffordable { .. }) => {
            tracing::warn!(target: LOG_TARGET, event = "refund.fee_unaffordable", %error);
            report.last_error = Some(error.to_string());
            return BatchOutcome::RetryLater;
        }
        Err(error) => {
            tracing::warn!(target: LOG_TARGET, event = "refund.path", path = "unbuildable", %error);
            report.skipped += 1;
            return BatchOutcome::Continue;
        }
    };

    match dry_run_gate(rpc, &skeleton.tx) {
        DryRunGate::Rejected(error) => {
            state
                .lock()
                .expect("process worker state")
                .submit
                .rejected_combos
                .insert(fp);
            tracing::warn!(target: LOG_TARGET, event = "dry_run.rejected", %error);
            report.last_error = Some(format!("dry-run rejected: {error}"));
            report.skipped += 1;
            return BatchOutcome::Continue;
        }
        DryRunGate::Skip(error) => {
            tracing::warn!(target: LOG_TARGET, event = "tick.skip",
                reason = "dry_run_unavailable", %error);
            report.last_error = Some(format!("dry-run: {error}"));
            report.skipped += 1;
            return if matches!(error, RpcError::Unavailable(_)) {
                BatchOutcome::RpcUnavailable
            } else {
                BatchOutcome::Continue
            };
        }
        DryRunGate::Proceed => {}
    }
    if let Err(outcome) = verify_refund_cell(rpc, authorizing_state, "Process state", 1, report) {
        return outcome;
    }
    let mut worker = state.lock().expect("process worker state");
    let Ok(_submission) = lease.begin_submission() else {
        report.skipped += 1;
        return BatchOutcome::Continue;
    };
    match submit_execution(rpc, &skeleton, &mut worker.submit, consumed) {
        Ok(tx_hash) => {
            worker.refund_state_deps.insert(
                tx_hash.as_slice().to_vec(),
                authorizing_state.out_point.clone(),
            );
            tracing::info!(target: LOG_TARGET, event = "submit.ok", tx_hash = %hex_pref(tx_hash.as_slice()));
            report.submitted += 1;
            BatchOutcome::Submitted
        }
        Err(RpcError::Conflict(error)) => {
            tracing::warn!(target: LOG_TARGET, event = "submit.conflict", %error);
            report.conflicts += 1;
            report.last_error = Some(format!("submit conflict: {error}"));
            BatchOutcome::Continue
        }
        Err(RpcError::SubmitRejected(error)) => {
            worker.submit.rejected_combos.insert(fp);
            tracing::warn!(target: LOG_TARGET, event = "submit.rejected", %error);
            report.last_error = Some(format!("submit rejected: {error}"));
            report.skipped += 1;
            BatchOutcome::Continue
        }
        Err(error) => {
            tracing::warn!(target: LOG_TARGET, event = "tick.skip", reason = "submit_failed", %error);
            report.last_error = Some(format!("submit failed: {error}"));
            report.skipped += 1;
            BatchOutcome::RpcUnavailable
        }
    }
}

fn record_invalid_assembled_reject(
    worker: &mut ProcessWorkerState,
    batch: &[ScannedIntention],
    process: &ResolvedProcess,
    execution_code_key: &[u8],
    report: &mut BatchReportDelta,
) -> (BatchOutcome, BatchReportDelta) {
    let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let fp = combo_fingerprint(
        &intent_ops,
        Some(&process.live_cell.out_point),
        execution_code_key,
    );
    worker.submit.rejected_combos.insert(fp);
    report.skipped += batch.len();
    (BatchOutcome::Rejected, report.clone())
}
