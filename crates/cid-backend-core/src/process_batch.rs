//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
};
const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::rpc::{out_point_key, Rpc, RpcError};
use crate::scan::ScannedIntention;
use crate::ssri::{
    build_batch_draft, draft_cell_deps_for_batch, draft_cells_for_batch, SsriError, SsriHost,
    BENEFICIARY_MISSING, CID_LOCK_MISMATCH, DEPOSIT_BELOW_MINIMUM, INCOMPATIBLE_BATCH,
    PROCESS_LOCK_ARGS_INVALID, UNSUPPORTED_FUNDING_LOCK,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::workers::ProcessWorkerState;
use cid_intention_filter::intention_process_type_id_parts;
use cid_process_executor::AssembledTx;
use ckb_types::packed::{CellDep, OutPoint};
use ckb_types::prelude::{Entity, Unpack};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const LOG_TARGET: &str = "cid_backend";

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
    /// Soft retry (e.g. [`AssembleError::RefundFeeUnaffordable`]): do **not**
    /// cache `rejected_combos` and do **not** trigger isolate drop_first/shrink.
    RetryLater,
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
    RetryRefund,
    FilterRefund,
}

impl BatchMode {
    pub(crate) fn refund_only(self) -> bool {
        self != Self::Assemble
    }
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
    pub refund_policy: &'a RefundPolicy,
    pub max_assembled_tx_bytes: usize,
    pub job_epoch: u64,
    pub epoch: Arc<AtomicU64>,
    pub min: u32,
    pub mode: BatchMode,
}

fn batch_cancelled(epoch: &AtomicU64, job_epoch: u64) -> bool {
    epoch.load(Ordering::SeqCst) != job_epoch
}

fn cancelled_delta(n: usize) -> (BatchOutcome, BatchReportDelta) {
    (
        BatchOutcome::Continue,
        BatchReportDelta {
            skipped: n,
            last_error: Some("worker: RunBatch cancelled (epoch)".into()),
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

/// Refund txs fingerprint with `process = None` (see assemble_and_submit_refund).
pub fn refund_combo_cached(
    worker: &ProcessWorkerState,
    batch: &[ScannedIntention],
    execution_code_key: &[u8],
) -> bool {
    let ops: Vec<_> = batch.iter().map(|i| i.out_point.clone()).collect();
    let fp = combo_fingerprint(&ops, None, execution_code_key);
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
    let ProcessBatchParams {
        hash,
        state,
        process,
        mut batch,
        rest,
        execution_code_key,
        tick_cell_deps,
        refund_policy,
        max_assembled_tx_bytes,
        job_epoch,
        epoch,
        min,
        mode,
    } = params;
    let refund_only = mode.refund_only();
    let lease = (&*epoch, job_epoch);
    let mut report = BatchReportDelta::default();

    // Narrow lock: cache / occupancy prechecks only.
    {
        let worker = state.lock().expect("process worker state");
        if worker.unhealthy_state.as_ref() == Some(&process.live_cell.out_point) {
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
        if !refund_only && window_combo_cached(&worker, &batch, process, execution_code_key) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return (BatchOutcome::CachedRejected, report);
        }
        if mode != BatchMode::FilterRefund
            && worker.submit.pending_occupies(&process.live_cell.out_point)
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

    if mode == BatchMode::FilterRefund {
        for item in &batch {
            assemble_and_submit_refund(
                rpc.as_ref(),
                &state,
                std::slice::from_ref(item),
                INCOMPATIBLE_BATCH,
                execution_code_key,
                tick_cell_deps,
                refund_policy,
                &mut report,
                BatchMode::FilterRefund,
                lease,
            );
        }
        let outcome = if report.submitted > 0 {
            BatchOutcome::Submitted
        } else {
            BatchOutcome::Continue
        };
        return (outcome, report);
    }
    if refund_only {
        tracing::info!(
            target: LOG_TARGET,
            event = "refund.below_min_retry",
            n = batch.len(),
        );
        return handle_incompatible_refunds(
            rpc.as_ref(),
            &state,
            &batch,
            INCOMPATIBLE_BATCH,
            execution_code_key,
            tick_cell_deps,
            refund_policy,
            &mut report,
            true,
            lease,
        );
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

    if batch_cancelled(&epoch, job_epoch) {
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
    let mut assemble_outcome = attempt(&batch);
    if let Err(SsriError::Incompatible(code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH))) =
        assemble_outcome
    {
        let (good, bad) = match isolate_intentions(&batch, rest, min as usize, code, &attempt) {
            Ok(result) => result,
            Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
                return mark_process_unhealthy(hash, process, &state, batch.len());
            }
            Err(error) => {
                return (
                    BatchOutcome::RetryLater,
                    BatchReportDelta {
                        skipped: batch.len(),
                        last_error: Some(format!("assemble isolation: {error}")),
                        ..report
                    },
                );
            }
        };
        if bad.is_empty() {
            report.last_error = Some(format!(
                "assemble exit {code}: could not isolate an Intention"
            ));
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        }
        // Check the remaining group before issuing any refunds. Exit 41 at
        // any point invalidates the state snapshot and all pending refunds.
        batch = good;
        let remaining = (batch.len() >= min as usize).then(|| attempt(&batch));
        if matches!(
            remaining,
            Some(Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)))
        ) {
            return mark_process_unhealthy(hash, process, &state, batch.len() + bad.len());
        }
        for (item, code) in bad {
            tracing::warn!(target: LOG_TARGET, event = "assemble.intention_isolated",
                process_type_hash = %hex_pref(&hash), out_point = ?item.out_point, code);
            assemble_and_submit_refund(
                rpc.as_ref(),
                &state,
                std::slice::from_ref(&item),
                code,
                execution_code_key,
                tick_cell_deps,
                refund_policy,
                &mut report,
                BatchMode::RetryRefund,
                lease,
            );
        }
        let Some(remaining) = remaining else {
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        };
        assemble_outcome = remaining;
    }
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
                    let mut worker = state.lock().expect("process worker state");
                    return record_invalid_assembled_reject(
                        &mut worker,
                        &batch,
                        process,
                        execution_code_key,
                        &mut report,
                    );
                }
                Err(AssembleError::InvalidAssembled(m)) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "assemble.invalid",
                        reason = "guest_tx",
                        error = %m,
                    );
                    let mut worker = state.lock().expect("process worker state");
                    return record_invalid_assembled_reject(
                        &mut worker,
                        &batch,
                        process,
                        execution_code_key,
                        &mut report,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "tick.skip",
                        reason = "validate_assembled",
                        error = %e,
                    );
                    report.skipped += batch.len();
                    return (BatchOutcome::Continue, report);
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
                    let mut worker = state.lock().expect("process worker state");
                    return record_invalid_assembled_reject(
                        &mut worker,
                        &batch,
                        process,
                        execution_code_key,
                        &mut report,
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "tick.skip",
                        reason = "execution_from_assembled",
                        error = %e,
                    );
                    report.skipped += batch.len();
                    return (BatchOutcome::Continue, report);
                }
            }
        }
        Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
            return mark_process_unhealthy(hash, process, &state, batch.len());
        }
        Err(SsriError::Incompatible(code @ (UNSUPPORTED_FUNDING_LOCK | BENEFICIARY_MISSING))) => {
            tracing::warn!(target: LOG_TARGET, event = "ssri.assemble_rejected", code);
            let mut worker = state.lock().expect("process worker state");
            return record_invalid_assembled_reject(
                &mut worker,
                &batch,
                process,
                execution_code_key,
                &mut report,
            );
        }
        Err(SsriError::Incompatible(code)) if code == INCOMPATIBLE_BATCH => {
            tracing::info!(
                target: LOG_TARGET,
                event = "ssri.incompatible",
                code,
            );
            // Refund path: unlock around dry-run + submit like digest_ok (P2).
            return handle_incompatible_refunds(
                rpc.as_ref(),
                &state,
                &batch,
                code,
                execution_code_key,
                tick_cell_deps,
                refund_policy,
                &mut report,
                false,
                lease,
            );
        }
        Err(SsriError::Incompatible(code)) => {
            tracing::warn!(target: LOG_TARGET, event = "ssri.assemble_failed", code);
            report.last_error = Some(format!("Process assembly failed (exit {code})"));
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
        Err(error) => {
            tracing::warn!(
                target: LOG_TARGET,
                event = "tick.skip",
                reason = "ssri_unusable",
                %error,
            );
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
    };

    match find_unselected_intention(rpc.as_ref(), &skeleton, &batch, process) {
        Ok(None) => {}
        Ok(Some(out_point)) => {
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
        Err(error) => {
            tracing::warn!(target: LOG_TARGET, event = "tick.skip",
                reason = "funding_input_lookup", %error);
            report.last_error = Some(format!("verify guest-added input: {error}"));
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        }
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

    // Shared cancel/epoch checked before submit (P0-1 / P0-2).
    if batch_cancelled(&epoch, job_epoch) {
        return cancelled_delta(batch.len());
    }

    let mut worker = state.lock().expect("process worker state");
    match submit_execution(rpc.as_ref(), &skeleton, &mut worker.submit, &consumed) {
        Ok(tx_hash) => {
            tracing::info!(
                target: LOG_TARGET,
                event = "submit.ok",
                tx_hash = %hex_pref(tx_hash.as_slice()),
            );
            report.submitted += 1;
            if consumed_process {
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

/// Preserve guest-added funding inputs while preventing them from bypassing
/// routing, filtering and selection as additional CID Intention inputs.
fn find_unselected_intention<R: HostQueryRpc>(
    rpc: &R,
    skeleton: &crate::assemble::ExecutionSkeleton,
    batch: &[ScannedIntention],
    process: &ResolvedProcess,
) -> Result<Option<OutPoint>, RpcError> {
    for input in skeleton.tx.inputs().into_iter() {
        let op = input.previous_output();
        if op == process.live_cell.out_point || batch.iter().any(|item| item.out_point == op) {
            continue;
        }
        let cell = rpc.get_live_cell(&op)?.ok_or_else(|| {
            RpcError::Unavailable(format!("guest-added input {op:?} is no longer live"))
        })?;
        let lock = cell.lock();
        let code_hash: [u8; 32] = lock.code_hash().unpack();
        if code_hash == batch[0].lock_code_hash
            || intention_process_type_id_parts(
                &code_hash,
                lock.hash_type().into(),
                &lock.args().raw_data(),
                &code_hash,
            )
            .is_ok()
        {
            return Ok(Some(op));
        }
    }
    Ok(None)
}

type IsolatedIntentions = (Vec<ScannedIntention>, Vec<(ScannedIntention, i8)>);

fn isolate_intentions(
    batch: &[ScannedIntention],
    rest: &[ScannedIntention],
    min: usize,
    original_code: i8,
    attempt: &impl Fn(&[ScannedIntention]) -> Result<AssembledTx, SsriError>,
) -> Result<IsolatedIntentions, SsriError> {
    let mut good = Vec::new();
    let mut bad = Vec::new();
    if min == 1 {
        for item in batch {
            let result = if batch.len() == 1 {
                Err(SsriError::Incompatible(original_code))
            } else {
                attempt(std::slice::from_ref(item))
            };
            match result {
                Ok(_) => good.push(item.clone()),
                Err(SsriError::Incompatible(
                    code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH),
                )) => {
                    bad.push((item.clone(), code));
                }
                Err(error) => return Err(error),
            }
        }
        return Ok((good, bad));
    }

    // Find a valid comparison batch, keeping the search bounded. Include
    // later filter-accepted peers because bad cells can occur in every
    // contiguous minimum-size window.
    let candidates: Vec<_> = batch.iter().chain(rest).collect();
    let mut indices: Vec<_> = (0..min).collect();
    const MAX_PROBES: usize = 64;
    for _ in 0..MAX_PROBES {
        let control: Vec<_> = indices.iter().map(|&i| candidates[i].clone()).collect();
        match attempt(&control) {
            Ok(_) => {
                let end = batch.len().max(indices[min - 1] + 1);
                for (index, &item) in candidates[..end].iter().enumerate() {
                    if indices.contains(&index) {
                        good.push(item.clone());
                        continue;
                    }

                    let mut probe = indices[..min - 1].to_vec();
                    let position = probe.partition_point(|&i| i < index);
                    probe.insert(position, index);
                    let probe: Vec<_> = probe.iter().map(|&i| candidates[i].clone()).collect();
                    match attempt(&probe) {
                        Ok(_) => good.push(item.clone()),
                        Err(SsriError::Incompatible(
                            code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH),
                        )) => {
                            bad.push((item.clone(), code));
                        }
                        Err(error) => return Err(error),
                    }
                }
                good.truncate(batch.len());
                break;
            }
            Err(SsriError::Incompatible(DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH)) => {}
            Err(error) => return Err(error),
        }
        if !advance_probe_indices(&mut indices, candidates.len()) {
            break;
        }
    }
    Ok((good, bad))
}

/// Move to the next subset, exhausting nearby candidates before adding a
/// later one (e.g. [0,1], [0,2], [1,2], [0,3]).
fn advance_probe_indices(indices: &mut [usize], candidate_count: usize) -> bool {
    for i in 0..indices.len() {
        let limit = indices.get(i + 1).copied().unwrap_or(candidate_count);
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
    (
        BatchOutcome::Continue,
        BatchReportDelta {
            skipped,
            last_error: Some("Process assembly failed (exit 41: ProcessLockArgsInvalid)".into()),
            ..BatchReportDelta::default()
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn handle_incompatible_refunds<R: Rpc + HostQueryRpc>(
    rpc: &R,
    state: &Arc<Mutex<ProcessWorkerState>>,
    batch: &[ScannedIntention],
    code: i8,
    execution_code_key: &[u8],
    tick_cell_deps: &[CellDep],
    refund_policy: &RefundPolicy,
    report: &mut BatchReportDelta,
    refund_only: bool,
    lease: (&AtomicU64, u64),
) -> (BatchOutcome, BatchReportDelta) {
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
    let mut retry_later = false;
    let mut rejection = None;
    for (_id, group) in by_identity {
        if group.is_empty() {
            continue;
        }
        let outcome = assemble_and_submit_refund(
            rpc,
            state,
            &group,
            code,
            execution_code_key,
            tick_cell_deps,
            refund_policy,
            report,
            if refund_only {
                BatchMode::RetryRefund
            } else {
                BatchMode::Assemble
            },
            lease,
        );
        match outcome {
            BatchOutcome::Submitted => submitted = true,
            BatchOutcome::RetryLater => retry_later = true,
            BatchOutcome::Rejected => rejection = Some(outcome),
            BatchOutcome::CachedRejected => {
                rejection.get_or_insert(outcome);
            }
            _ => {}
        }
    }

    // Each group was authorized by the same guest result. Finish independent
    // refunds even when another identity has to be split and retried.
    // Fee-unaffordable groups stay marked pending; digest_loop strips them from
    // drop_first/shrink when a sibling identity is Rejected (P3).
    let outcome = rejection.unwrap_or(if retry_later {
        BatchOutcome::RetryLater
    } else if submitted {
        BatchOutcome::Submitted
    } else {
        BatchOutcome::Continue
    });
    (outcome, report.clone())
}

#[allow(clippy::too_many_arguments)]
fn assemble_and_submit_refund<R: Rpc + HostQueryRpc>(
    rpc: &R,
    state: &Arc<Mutex<ProcessWorkerState>>,
    group: &[ScannedIntention],
    code: i8,
    execution_code_key: &[u8],
    tick_cell_deps: &[CellDep],
    refund_policy: &RefundPolicy,
    report: &mut BatchReportDelta,
    attempt_mode: BatchMode,
    lease: (&AtomicU64, u64),
) -> BatchOutcome {
    let refund_only = attempt_mode.refund_only();
    if batch_cancelled(lease.0, lease.1) {
        report.skipped += group.len();
        return BatchOutcome::Continue;
    }
    {
        let worker = state.lock().expect("process worker state");
        if group
            .iter()
            .any(|item| worker.submit.pending_occupies(&item.out_point))
        {
            report.skipped += group.len();
            return BatchOutcome::Continue;
        }
    }
    let mode = refund_policy.mode_for(&group[0].lock_identity());
    let skeleton =
        match assemble_refund_with_policy(group, tick_cell_deps, mode, refund_policy.fee_rate) {
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
                if fee == 0 {
                    tracing::debug!(target: LOG_TARGET, event = "refund.zero_fee", n = group.len());
                }
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
            Err(e @ AssembleError::RefundFeeUnaffordable { .. }) => {
                tracing::warn!(target: LOG_TARGET, event = "refund.fee_unaffordable", error = %e);
                // Non-permanent: do not cache rejected_combos and do not isolate-split
                // (shrinking cannot raise fee budget). Mark for below-min retry.
                report.last_error = Some(e.to_string());
                // A filter rejection is always retried using a fresh verdict.
                if attempt_mode != BatchMode::FilterRefund {
                    let mut worker = state.lock().expect("process worker state");
                    for item in group {
                        worker
                            .pending_incompatible_refund
                            .insert(out_point_key(&item.out_point));
                    }
                }
                return BatchOutcome::RetryLater;
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
    let fp = combo_fingerprint(&intent_ops, None, execution_code_key);
    {
        let worker = state.lock().expect("process worker state");
        if worker.submit.rejected_combos.contains(&fp) {
            // refund_only: cached permanent failure — soft skip, no isolate.
            return if refund_only {
                report.skipped += group.len();
                BatchOutcome::Continue
            } else {
                BatchOutcome::CachedRejected
            };
        }
    }
    let consumed: Vec<_> = skeleton
        .tx
        .inputs()
        .into_iter()
        .map(|i| i.previous_output())
        .collect();
    // Dry-run without holding the state mutex (P2).
    match dry_run_gate(rpc, &skeleton.tx) {
        DryRunGate::Rejected(m) => {
            let mut worker = state.lock().expect("process worker state");
            worker.submit.rejected_combos.insert(fp);
            tracing::warn!(
                target: LOG_TARGET,
                event = "dry_run.rejected",
                error = %m,
                n = group.len(),
            );
            report.last_error = Some(format!("dry-run rejected: {m}"));
            // refund_only leftovers: cache combo, do not isolate-split (P3).
            return if refund_only {
                report.skipped += group.len();
                BatchOutcome::Continue
            } else {
                BatchOutcome::Rejected
            };
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
    if batch_cancelled(lease.0, lease.1) {
        report.skipped += group.len();
        return BatchOutcome::Continue;
    }
    let mut worker = state.lock().expect("process worker state");
    match submit_execution(rpc, &skeleton, &mut worker.submit, &consumed) {
        Ok(tx_hash) => {
            tracing::info!(
                target: LOG_TARGET,
                event = "submit.ok",
                tx_hash = %hex_pref(tx_hash.as_slice()),
            );
            // Keep pending_incompatible_refund until the out-point leaves
            // live_keys / pending occupancy (Drop/reorg must still retry).
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
            // refund_only: cache the combo but do not isolate-split (P3).
            if refund_only {
                report.skipped += group.len();
                BatchOutcome::Continue
            } else {
                BatchOutcome::Rejected
            }
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
