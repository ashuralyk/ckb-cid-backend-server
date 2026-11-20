//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
    ExecutionSkeleton,
};
use crate::code_deps::{cell_dep_code, merge_cell_deps};
const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;
use crate::fee::{cell_occupied_capacity, CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::rpc::{LiveCell, Rpc, RpcError};
use crate::scan::ScannedIntention;
use crate::ssri::{
    build_batch_draft, draft_cell_deps_for_batch, draft_cells_for_batch, intention_draft_cell,
    SsriError, SsriHost, PROCESS_LOCK_ARGS_INVALID, STATE_CAPACITY_INSUFFICIENT,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::worker_commands::{Submission, WorkerCommands};
use crate::workers::ProcessWorkerState;
use cid_process_executor::AssembledTx;
use cid_protocol::CidArgs;
use ckb_types::packed::{CellDep, CellOutput, OutPoint};
use ckb_types::prelude::{Entity, Unpack};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const LOG_TARGET: &str = "cid_backend";
const MAX_COMPARISON_PROBES: usize = 64;
const AUDITED_EXAMPLE_DATA_HASH: [u8; 32] = [
    0x23, 0x74, 0xe4, 0x55, 0xd8, 0xcd, 0xca, 0xe5, 0xd5, 0x38, 0x72, 0x57, 0xec, 0x0d, 0x5b, 0x33,
    0x11, 0x4d, 0x3b, 0x53, 0x38, 0xd8, 0x23, 0xd5, 0xe3, 0x88, 0xb5, 0xa0, 0xa7, 0x67, 0xf2, 0x26,
];

#[derive(Clone)]
pub(crate) struct ProbeBudget {
    pub(crate) remaining: usize,
    pub(crate) scan_number: u64,
    arrivals: Arc<HashSet<Vec<u8>>>,
    live_reads: usize,
}

impl ProbeBudget {
    pub(crate) fn for_scan(scan_number: u64, arrivals: Arc<HashSet<Vec<u8>>>) -> Self {
        Self {
            remaining: MAX_COMPARISON_PROBES,
            scan_number,
            arrivals,
            live_reads: 0,
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
    Continue,
    ProcessOccupied,
    /// Leave cells live and advance to the next window without caching a verdict.
    RetryLater,
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

fn mark_cancelled(report: &mut BatchReportDelta, skipped: usize) {
    report.skipped += skipped;
    report.last_error = Some("worker: RunBatch cancelled".into());
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
        ref state,
        process,
        ref batch,
        ..
    } = params;
    let lease = BatchLease {
        epoch: &params.epoch,
        job_epoch: params.job_epoch,
        commands: params.commands.as_deref(),
        hash,
    };
    let mut report = BatchReportDelta::default();

    // The digest loop shares this budget only among jobs in the current tick.
    let mut budget = {
        let worker = state.lock().expect("process worker state");
        if worker.is_process_quarantined(process) {
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
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
        worker
            .probe_budget
            .clone()
            .unwrap_or_else(|| ProbeBudget::for_scan(0, Arc::default()))
    };
    let result = scan_and_submit(rpc, ssri, &params, &lease, &mut budget, report);
    if let Some(current) = &mut state.lock().expect("process worker state").probe_budget {
        *current = budget;
    }
    result
}

fn scan_and_submit<R, S>(
    rpc: Arc<R>,
    ssri: &S,
    params: &ProcessBatchParams<'_>,
    lease: &BatchLease<'_>,
    budget: &mut ProbeBudget,
    mut report: BatchReportDelta,
) -> (BatchOutcome, BatchReportDelta)
where
    R: Rpc + HostQueryRpc + Send + Sync + 'static,
    S: SsriHost,
{
    let ProcessBatchParams {
        hash,
        state,
        process,
        batch,
        rest,
        execution_code_key,
        tick_cell_deps,
        cid_locks,
        max_assembled_tx_bytes,
        min,
        ..
    } = params;
    let (hash, process) = (*hash, *process);
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
        mark_cancelled(&mut report, batch.len());
        return (BatchOutcome::Continue, report);
    }

    let max_tx_bytes = if *max_assembled_tx_bytes == 0 {
        DEFAULT_MAX_ASSEMBLED_TX_BYTES
    } else {
        *max_assembled_tx_bytes
    };

    // Assemble without holding the state mutex (P2-9). Live path passes
    // RpcHostQuery into ProcessExecutor::assemble (P1-3).
    let attempt = |items: &[ScannedIntention]| {
        let draft = build_batch_draft(items, process, &cell_deps);
        let cells = draft_cells_for_batch(items, process);
        ssri.assemble_live(rpc.clone(), &process.binary, &draft, &cells, tip)
    };
    let candidates: Vec<_> = batch.iter().chain(*rest).collect();
    let max = batch.len();
    let mut probes = None;
    let mut live = HashMap::new();
    let mut try_initial = true;
    let mut initial_rejected = false;

    // Only the pinned guest always pays a positive fee from the Process state.
    let can_pause_on_exit_37 =
        cell_occupied_capacity(&process.live_cell.output, &process.live_cell.data)
            .is_some_and(|occupied| process.live_cell.capacity() <= occupied)
            && process.verdict_code_cell().is_some_and(|code| {
                let data_hash: [u8; 32] = CellOutput::calc_data_hash(&code.data).unpack();
                data_hash == AUDITED_EXAMPLE_DATA_HASH
            });

    'scan: while budget.remaining > 0 {
        let initial = std::mem::take(&mut try_initial);
        let positions = if initial {
            (0..max).collect()
        } else {
            let probes = probes.get_or_insert_with(|| {
                Subsets::new(&candidates, *min as usize, max, budget, initial_rejected)
            });
            let Some(positions) = probes.next() else {
                break;
            };
            positions
        };
        if lease.cancelled() {
            mark_cancelled(&mut report, max);
            return (BatchOutcome::Continue, report);
        }
        budget.remaining -= 1;
        let mut locks = HashSet::new();
        if !positions
            .iter()
            .all(|&index| locks.insert(candidates[index].lock_group_key()))
        {
            continue;
        }
        for &index in &positions {
            let op = &candidates[index].out_point;
            let is_live = if let Some(&known) = live.get(op) {
                known
            } else {
                if budget.live_reads >= max + MAX_COMPARISON_PROBES {
                    break 'scan;
                }
                budget.live_reads += 1;
                match rpc.get_live_cell(op) {
                    Ok(cell) => {
                        let is_live = cell.is_some();
                        live.insert(op.clone(), is_live);
                        is_live
                    }
                    Err(error) => {
                        tracing::warn!(target: LOG_TARGET, event = "assemble.probe_deferred",
                            process_type_hash = %hex_pref(&hash), %error);
                        report.last_error = Some(format!("verify isolation candidate: {error}"));
                        break 'scan;
                    }
                }
            };
            if !is_live {
                continue 'scan;
            }
        }
        let items: Vec<_> = positions
            .iter()
            .map(|&index| candidates[index].clone())
            .collect();
        if window_combo_cached(
            &state.lock().expect("process worker state"),
            &items,
            process,
            execution_code_key,
        ) {
            tracing::debug!(target: LOG_TARGET, event = "submit.rejected_combo", n = items.len());
            initial_rejected |= initial;
            continue;
        }
        let assembled = match attempt(&items) {
            Ok(assembled) => assembled,
            Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
                mark_process_unhealthy(hash, process, state, &mut report);
                break;
            }
            Err(SsriError::Incompatible(STATE_CAPACITY_INSUFFICIENT)) if can_pause_on_exit_37 => {
                tracing::warn!(target: LOG_TARGET, event = "assemble.process_unfunded",
                    process_type_hash = %hex_pref(&hash), out_point = ?process.live_cell.out_point,
                    code = STATE_CAPACITY_INSUFFICIENT);
                state
                    .lock()
                    .expect("process worker state")
                    .mark_process_unfunded(process);
                report.last_error =
                    Some("Process assembly failed (exit 37: no fee headroom)".into());
                break;
            }
            Err(error) => {
                if initial {
                    tracing::warn!(target: LOG_TARGET, event = "assemble.batch_failed",
                        process_type_hash = %hex_pref(&hash), %error,
                        intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                    report.last_error = Some(format!("Process assembly failed ({error})"));
                } else {
                    tracing::warn!(target: LOG_TARGET, event = "assemble.probe_failed",
                        process_type_hash = %hex_pref(&hash), %error,
                        intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                }
                continue;
            }
        };
        let skeleton = match prepare_execution(
            rpc.as_ref(),
            &assembled,
            &items,
            process,
            max_tx_bytes,
            cid_locks,
        ) {
            Ok(skeleton) => skeleton,
            Err(PreparationError::NonLiveInput(op)) => {
                report.last_error = Some(format!("guest-added input {op:?} is not live"));
                continue;
            }
            Err(PreparationError::Rejected) => {
                record_invalid_assembled_reject(
                    &mut state.lock().expect("process worker state"),
                    &items,
                    process,
                    execution_code_key,
                );
                initial_rejected |= initial;
                continue;
            }
            Err(PreparationError::UnsafeFunding) => {
                initial_rejected |= initial;
                continue;
            }
            Err(PreparationError::InputLookup(error)) => {
                tracing::warn!(target: LOG_TARGET, event = "tick.skip",
                    reason = "funding_input_lookup", %error);
                report.last_error = Some(format!("verify guest-added input: {error}"));
                break;
            }
        };
        if !initial {
            tracing::info!(target: LOG_TARGET, event = "assemble.batch_isolated",
                process_type_hash = %hex_pref(&hash),
                intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
        }
        if let Some(outcome) =
            submit_digest(rpc.as_ref(), params, lease, &items, &skeleton, &mut report)
        {
            return (outcome, report);
        }
        initial_rejected |= initial;
    }
    report.skipped += batch.len();
    (BatchOutcome::RetryLater, report)
}

fn submit_digest<R: Rpc>(
    rpc: &R,
    params: &ProcessBatchParams<'_>,
    lease: &BatchLease<'_>,
    batch: &[ScannedIntention],
    skeleton: &ExecutionSkeleton,
    report: &mut BatchReportDelta,
) -> Option<BatchOutcome> {
    let state = &params.state;
    let process = params.process;
    let consumed_process = skeleton
        .tx
        .inputs()
        .into_iter()
        .any(|i| i.previous_output() == process.live_cell.out_point);
    let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let process_op = consumed_process.then(|| process.live_cell.out_point.clone());
    let fp = combo_fingerprint(&intent_ops, process_op.as_ref(), params.execution_code_key);

    {
        let worker = state.lock().expect("process worker state");
        if worker.submit.rejected_combos.contains(&fp) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return None;
        }
    }

    let consumed: Vec<_> = skeleton
        .tx
        .inputs()
        .into_iter()
        .map(|i| i.previous_output())
        .collect();

    // Dry-run without holding the state mutex.
    match dry_run_gate(rpc, &skeleton.tx) {
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
            return None;
        }
        DryRunGate::Skip(RpcError::Conflict(m)) => {
            tracing::warn!(target: LOG_TARGET, event = "dry_run.conflict", error = %m);
            report.conflicts += 1;
            report.last_error = Some(format!("dry-run conflict: {m}"));
            return None;
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
            return Some(BatchOutcome::RetryLater);
        }
        DryRunGate::Proceed => {}
    }

    let mut worker = state.lock().expect("process worker state");
    let Ok(_submission) = lease.begin_submission() else {
        mark_cancelled(report, batch.len());
        return Some(BatchOutcome::Continue);
    };
    match submit_execution(rpc, skeleton, &mut worker.submit, &consumed) {
        Ok(tx_hash) => {
            tracing::info!(
                target: LOG_TARGET,
                event = "submit.ok",
                tx_hash = %hex_pref(tx_hash.as_slice()),
            );
            report.submitted += 1;
            if consumed_process {
                Some(BatchOutcome::SubmittedProcess)
            } else {
                Some(BatchOutcome::Submitted)
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
            None
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
            None
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
            Some(BatchOutcome::RetryLater)
        }
    }
}

enum PreparationError {
    Rejected,
    UnsafeFunding,
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
    let skeleton = validate_assembled_tx(assembled, batch, process, max_size)
        .and_then(|()| execution_from_assembled(assembled))
        .map_err(|error| {
            let reason = match error {
                AssembleError::SizeLimit { .. } => "size_limit",
                _ => "guest_tx",
            };
            tracing::warn!(target: LOG_TARGET, event = "assemble.invalid", reason, %error);
            PreparationError::Rejected
        })?;

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
            tracing::warn!(target: LOG_TARGET, event = "assemble.invalid",
                reason = "unselected_intention", out_point = ?op);
            return Err(PreparationError::UnsafeFunding);
        }
    }
    Ok(skeleton)
}

/// Enumerate legal subset sizes within one worker job.
struct Subsets {
    order: Vec<usize>,
    max: usize,
    sizes: VecDeque<(usize, Option<Vec<usize>>)>,
    interleave: bool,
    preferred: Vec<Vec<usize>>,
    next_preferred: usize,
}

impl Subsets {
    fn new(
        candidates: &[&ScannedIntention],
        min: usize,
        max: usize,
        budget: &ProbeBudget,
        rejected: bool,
    ) -> Self {
        let count = candidates.len();
        let interleave = exceeds_probe_budget(count, min, max);
        let mut order: Vec<_> = (0..count).collect();
        // Keep nearby candidates first and vary the rest independently of search results.
        if interleave && budget.scan_number != 0 {
            order[(min + 1).min(count)..].sort_by_cached_key(|&index| {
                let mut hasher = DefaultHasher::new();
                (budget.scan_number, index).hash(&mut hasher);
                hasher.finish()
            });
        }
        let mut preferred = Vec::new();
        if rejected && max > 1 {
            if max > min {
                preferred.push((0..max - 1).collect());
            }
            if count > min {
                preferred.push((1..(max + 1).min(count)).collect());
            }
        }
        // Sample combinations that include arrivals, while
        // leaving most of the budget to the normal per-size search.
        let size_count = max - min + 1;
        if interleave && !budget.arrivals.is_empty() {
            let priority: Vec<_> = candidates
                .iter()
                .enumerate()
                .rev()
                .filter_map(|(index, item)| {
                    budget
                        .arrivals
                        .contains(item.out_point.as_slice())
                        .then_some(index)
                })
                .take(MAX_COMPARISON_PROBES / 4)
                .collect();
            let sample_limit = (MAX_COMPARISON_PROBES / 4)
                .min(MAX_COMPARISON_PROBES.saturating_sub(size_count + preferred.len()));
            let priority_at_min = priority.iter().map(|&index| (min, index));
            let priority_by_size = (min + 1..=max)
                .flat_map(|size| priority.iter().take(2).map(move |&index| (size, index)));
            for (size, index) in priority_at_min
                .take(2)
                .chain(priority_by_size)
                .chain(priority.iter().skip(2).map(|&index| (min, index)))
                .take(sample_limit)
            {
                let lock = candidates[index].lock_group_key();
                let mut positions: Vec<_> = (0..max)
                    .filter(|&i| candidates[i].lock_group_key() != lock)
                    .take(size - 1)
                    .collect();
                positions.push(index);
                positions.sort_unstable();
                if !Self::is_initial(&positions, max) && !preferred.contains(&positions) {
                    preferred.push(positions);
                }
            }
        }
        let mut sizes: VecDeque<(usize, Option<Vec<usize>>)> =
            (min..=max).map(|size| (size, None)).collect();
        if sizes.len() > MAX_COMPARISON_PROBES - preferred.len() {
            let start = (budget.scan_number % sizes.len() as u64) as usize;
            sizes.rotate_left(start);
        }
        Self {
            order,
            max,
            sizes,
            interleave,
            preferred,
            next_preferred: 0,
        }
    }

    fn is_initial(positions: &[usize], max: usize) -> bool {
        positions.len() == max && positions.iter().copied().eq(0..max)
    }
}

impl Iterator for Subsets {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(positions) = self.preferred.get(self.next_preferred) {
            self.next_preferred += 1;
            return Some(positions.clone());
        }
        while let Some((size, indices)) = self.sizes.pop_front() {
            let mut indices = indices.unwrap_or_else(|| (0..size).collect());
            let mut positions: Vec<_> = indices.iter().map(|&i| self.order[i]).collect();
            positions.sort_unstable(); // Preserve the candidate order within each batch.
                                       // Exhaust nearby subsets before including a later candidate.
            for i in 0..indices.len() {
                let limit = indices.get(i + 1).copied().unwrap_or(self.order.len());
                if indices[i] + 1 < limit {
                    indices[i] += 1;
                    for (initial, index) in indices[..i].iter_mut().enumerate() {
                        *index = initial;
                    }
                    if self.interleave {
                        self.sizes.push_back((size, Some(indices)));
                    } else {
                        self.sizes.push_front((size, Some(indices)));
                    }
                    break;
                }
            }
            if !Self::is_initial(&positions, self.max) && !self.preferred.contains(&positions) {
                return Some(positions);
            }
        }
        None
    }
}

fn exceeds_probe_budget(count: usize, min: usize, max: usize) -> bool {
    let mut remaining = MAX_COMPARISON_PROBES;
    for size in min..=max {
        let mut subsets = 1usize;
        for i in 0..size.min(count - size) {
            subsets = subsets.saturating_mul(count - i) / (i + 1);
            if subsets > remaining {
                return true;
            }
        }
        let Some(next) = remaining.checked_sub(subsets) else {
            return true;
        };
        remaining = next;
    }
    false
}

fn mark_process_unhealthy(
    hash: [u8; 32],
    process: &ResolvedProcess,
    state: &Mutex<ProcessWorkerState>,
    report: &mut BatchReportDelta,
) {
    tracing::error!(target: LOG_TARGET, event = "process.unhealthy",
        process_type_hash = %hex_pref(&hash), out_point = ?process.live_cell.out_point,
        code = PROCESS_LOCK_ARGS_INVALID, reason = "ProcessLockArgsInvalid");
    state
        .lock()
        .expect("process worker state")
        .quarantine_process(process);
    report.last_error = Some("Process assembly failed (exit 41: ProcessLockArgsInvalid)".into());
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
    let scan_number = {
        let worker = params.state.lock().expect("process worker state");
        if worker.is_process_quarantined(params.process) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.submit.pending_occupies(&authorizing_state.out_point) {
            tracing::debug!(target: LOG_TARGET, event = "refund.process_state_pending",
                out_point = ?authorizing_state.out_point);
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.has_pending_filter_refund(&authorizing_state.out_point) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        worker
            .probe_budget
            .as_ref()
            .map_or(0, |budget| budget.scan_number)
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
    if !params.batch.is_empty() {
        let start = (scan_number % params.batch.len() as u64) as usize;
        params.batch.rotate_left(start);
    }
    let lease = BatchLease {
        epoch: &params.epoch,
        job_epoch: params.job_epoch,
        commands: params.commands.as_deref(),
        hash: params.hash,
    };
    for item in params.batch.iter().take(MAX_ATTEMPTS) {
        if lease.cancelled() {
            break;
        }
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
    let result = submit_execution(rpc, &skeleton, &mut worker.submit, consumed);
    let tracked_hash = match &result {
        Ok(hash) => Some(hash.clone()),
        Err(RpcError::Unavailable(_)) => Some(skeleton.tx.hash()),
        _ => None,
    };
    if let Some(hash) = tracked_hash {
        worker.refund_state_deps.insert(
            hash.as_slice().to_vec(),
            authorizing_state.out_point.clone(),
        );
    }
    match result {
        Ok(tx_hash) => {
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
) {
    let intent_ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let fp = combo_fingerprint(
        &intent_ops,
        Some(&process.live_cell.out_point),
        execution_code_key,
    );
    worker.submit.rejected_combos.insert(fp);
}
