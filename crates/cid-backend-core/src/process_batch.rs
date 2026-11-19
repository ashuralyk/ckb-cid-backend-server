//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
};
const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::rpc::{Rpc, RpcError};
use crate::scan::ScannedIntention;
use crate::ssri::{
    build_batch_draft, draft_cell_deps_for_batch, draft_cells_for_batch, SsriError, SsriHost,
    INCOMPATIBLE_BATCH, UNSUPPORTED_FUNDING_LOCK,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::workers::ProcessWorkerState;
use ckb_types::packed::CellDep;
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
}

#[derive(Debug, Default, Clone)]
pub struct BatchReportDelta {
    pub submitted: usize,
    pub skipped: usize,
    pub conflicts: usize,
    pub last_error: Option<String>,
}

pub struct ProcessBatchParams<'a> {
    pub hash: [u8; 32],
    /// Shared worker state — lock only around submit/cache mutations (P2-9).
    pub state: Arc<Mutex<ProcessWorkerState>>,
    pub process: &'a ResolvedProcess,
    pub batch: Vec<ScannedIntention>,
    pub execution_code_key: &'a [u8],
    pub tick_cell_deps: &'a [CellDep],
    pub refund_policy: &'a RefundPolicy,
    pub max_assembled_tx_bytes: usize,
    pub job_epoch: u64,
    pub epoch: Arc<AtomicU64>,
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
        batch,
        execution_code_key,
        tick_cell_deps,
        refund_policy,
        max_assembled_tx_bytes,
        job_epoch,
        epoch,
    } = params;
    let mut report = BatchReportDelta::default();

    // Narrow lock: cache / occupancy prechecks only.
    {
        let worker = state.lock().expect("process worker state");
        if window_combo_cached(&worker, &batch, process, execution_code_key) {
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return (BatchOutcome::CachedRejected, report);
        }
        if worker.submit.pending_occupies(&process.live_cell.out_point) {
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
    let draft = build_batch_draft(&batch, process, &cell_deps);
    let cells = draft_cells_for_batch(&batch, process);
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
    let assemble_outcome = ssri.assemble_live(rpc.clone(), &process.binary, &draft, &cells, tip);
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
        Err(SsriError::Incompatible(code)) if code == UNSUPPORTED_FUNDING_LOCK => {
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
                hash,
                &state,
                &batch,
                code,
                execution_code_key,
                tick_cell_deps,
                refund_policy,
                &mut report,
            );
        }
        Err(SsriError::Incompatible(code)) => {
            tracing::warn!(target: LOG_TARGET, event = "ssri.assemble_failed", code);
            report.last_error = Some(format!("Process assembly failed (exit {code})"));
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
        Err(SsriError::Unusable(m)) => {
            tracing::warn!(
                target: LOG_TARGET,
                event = "tick.skip",
                reason = "ssri_unusable",
                error = %m,
            );
            report.skipped += batch.len();
            return (BatchOutcome::Continue, report);
        }
    };

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

#[allow(clippy::too_many_arguments)]
fn handle_incompatible_refunds<R: Rpc + HostQueryRpc>(
    rpc: &R,
    hash: [u8; 32],
    state: &Arc<Mutex<ProcessWorkerState>>,
    batch: &[ScannedIntention],
    code: i8,
    execution_code_key: &[u8],
    tick_cell_deps: &[CellDep],
    refund_policy: &RefundPolicy,
    report: &mut BatchReportDelta,
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
    let mut rejection = None;
    for (_id, group) in by_identity {
        if group.is_empty() {
            continue;
        }
        let outcome = assemble_and_submit_refund(
            rpc,
            hash,
            state,
            &group,
            code,
            execution_code_key,
            tick_cell_deps,
            refund_policy,
            report,
        );
        match outcome {
            BatchOutcome::Submitted => submitted = true,
            BatchOutcome::Rejected => rejection = Some(outcome),
            BatchOutcome::CachedRejected => {
                rejection.get_or_insert(outcome);
            }
            _ => {}
        }
    }

    // Each group was authorized by the same guest result. Finish independent
    // refunds even when another identity has to be split and retried.
    let outcome = rejection.unwrap_or(if submitted {
        BatchOutcome::Submitted
    } else {
        BatchOutcome::Continue
    });
    (outcome, report.clone())
}

#[allow(clippy::too_many_arguments)]
fn assemble_and_submit_refund<R: Rpc + HostQueryRpc>(
    rpc: &R,
    _hash: [u8; 32],
    state: &Arc<Mutex<ProcessWorkerState>>,
    group: &[ScannedIntention],
    code: i8,
    execution_code_key: &[u8],
    tick_cell_deps: &[CellDep],
    refund_policy: &RefundPolicy,
    report: &mut BatchReportDelta,
) -> BatchOutcome {
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
                // Reuse the split path without caching an unaffordable window.
                report.last_error = Some(e.to_string());
                return BatchOutcome::Rejected;
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
            return BatchOutcome::CachedRejected;
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
    let mut worker = state.lock().expect("process worker state");
    match submit_execution(rpc, &skeleton, &mut worker.submit, &consumed) {
        Ok(tx_hash) => {
            tracing::info!(
                target: LOG_TARGET,
                event = "submit.ok",
                tx_hash = %hex_pref(tx_hash.as_slice()),
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
