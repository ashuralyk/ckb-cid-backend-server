//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
};
use crate::code_deps::{cell_dep_code, merge_cell_deps};
const DEFAULT_MAX_ASSEMBLED_TX_BYTES: usize = 512 * 1024;
use crate::fee::{CidLockIdentity, RefundPolicy};
use crate::host_query::HostQueryRpc;
use crate::process::ResolvedProcess;
use crate::rpc::{out_point_key, LiveCell, Rpc, RpcError};
use crate::scan::ScannedIntention;
use crate::ssri::{
    build_batch_draft, draft_cell_deps_for_batch, draft_cells_for_batch, intention_draft_cell,
    SsriError, SsriHost, BENEFICIARY_MISSING, CID_LOCK_MISMATCH, DEPOSIT_BELOW_MINIMUM,
    INCOMPATIBLE_BATCH, PROCESS_LOCK_ARGS_INVALID, UNSUPPORTED_FUNDING_LOCK,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::worker_commands::{Submission, WorkerCommands};
use crate::workers::ProcessWorkerState;
use cid_protocol::CidArgs;
use ckb_types::packed::{CellDep, OutPoint};
use ckb_types::prelude::{Entity, Unpack};
use std::collections::HashMap;
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
    RetryRefund,
    PolicyRefund,
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
    if params.mode == BatchMode::PolicyRefund {
        return refund_policy_rejections(rpc.as_ref(), params);
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
        refund_policy,
        max_assembled_tx_bytes,
        job_epoch,
        epoch,
        min,
        mode,
        commands,
    } = params;
    let refund_only = mode.refund_only();
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
        if !refund_only && window_combo_cached(&worker, &batch, process, execution_code_key) {
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
    if refund_only {
        tracing::info!(
            target: LOG_TARGET,
            event = "refund.below_min_retry",
            n = batch.len(),
        );
        return handle_incompatible_refunds(
            rpc.as_ref(),
            process,
            &state,
            &batch,
            INCOMPATIBLE_BATCH,
            execution_code_key,
            &cell_deps,
            refund_policy,
            &mut report,
            true,
            &lease,
        );
    }

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
    let mut probe = state
        .lock()
        .expect("process worker state")
        .isolation_probe
        .take();
    // A resumed search already has its trigger. Re-assembling the changed
    // window could restart isolation before the saved comparisons run.
    let resuming = probe.as_ref().is_some_and(|saved| saved.indices.is_some());
    let initial = (!resuming).then(|| attempt(&batch));
    let original_code = match &initial {
        Some(Err(SsriError::Incompatible(code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH)))) => {
            Some(*code)
        }
        _ => None,
    };
    let assemble_outcome = if resuming || original_code.is_some() {
        if let Some(code) = process.verdict_code_cell() {
            if let Err(outcome) =
                verify_refund_cell(rpc.as_ref(), code, "Process code", batch.len(), &mut report)
            {
                state.lock().expect("process worker state").isolation_probe = probe;
                return (outcome, report);
            }
        }
        // Check node liveness before using a comparison cell as evidence,
        // once per candidate per job.
        let mut live = HashMap::new();
        let mut probe_attempt = |items: &[ScannedIntention]| {
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
                    return Ok(false);
                }
            }
            attempt(items).map(|_| true)
        };
        let result = isolate_intentions(
            &batch,
            rest,
            min as usize,
            original_code,
            &process.live_cell.out_point,
            &mut probe,
            &mut probe_attempt,
        );
        state.lock().expect("process worker state").isolation_probe = probe;
        let (good, bad) = match result {
            Ok(Some(result)) => result,
            Ok(None) => {
                state
                    .lock()
                    .expect("process worker state")
                    .isolation_probe_paused = true;
                report.skipped += batch.len();
                return (BatchOutcome::ProbePending, report);
            }
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
            report.last_error = Some("assemble: could not isolate an Intention".into());
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
        if process.verdict_code_cell().is_some() && !lease.cancelled() {
            let mut worker = state.lock().expect("process worker state");
            for (item, code) in bad {
                tracing::warn!(target: LOG_TARGET, event = "assemble.intention_isolated",
                    process_type_hash = %hex_pref(&hash), out_point = ?item.out_point, code);
                worker.isolated_refunds.insert(item.out_point, code);
            }
        }
        let Some(remaining) = remaining else {
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        };
        remaining
    } else {
        initial.expect("fresh assembly outcome")
    };
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
                process,
                &state,
                &batch,
                code,
                execution_code_key,
                &cell_deps,
                refund_policy,
                &mut report,
                false,
                &lease,
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

    match find_unselected_intention(rpc.as_ref(), &skeleton, &batch, process, cid_locks) {
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
    cid_locks: &[CidLockIdentity],
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
        if cid_locks.iter().any(|identity| {
            code_hash == identity.code_hash && lock.hash_type() == identity.hash_type.into()
        }) || CidArgs::decode(&lock.args().raw_data()).is_ok()
        {
            return Ok(Some(op));
        }
    }
    Ok(None)
}

type IsolatedIntentions = (Vec<ScannedIntention>, Vec<(ScannedIntention, i8)>);

/// Keep positions stable while walking combinations. Cells missing from a
/// later scan leave holes and arrivals wait for a subsequent search.
pub(crate) struct IsolationProbe {
    process: OutPoint,
    candidates: Vec<OutPoint>,
    window_len: usize,
    min: usize,
    indices: Option<Vec<usize>>,
}

impl IsolationProbe {
    pub(crate) fn resume_bucket(
        &self,
        process: &OutPoint,
        min: usize,
        bucket: &mut Vec<ScannedIntention>,
    ) -> bool {
        if self.process != *process || self.min != min {
            return false;
        }
        if self.indices.is_none() {
            return self
                .candidates
                .iter()
                .eq(bucket.iter().map(|item| &item.out_point));
        }
        let positions: HashMap<_, _> = self
            .candidates
            .iter()
            .enumerate()
            .map(|(index, op)| (op, index))
            .collect();
        if bucket
            .iter()
            .filter(|item| positions.contains_key(&item.out_point))
            .count()
            < min
        {
            return false;
        }
        bucket.retain(|item| positions.contains_key(&item.out_point));
        bucket.sort_unstable_by_key(|item| positions[&item.out_point]);
        true
    }
}

fn isolate_intentions(
    batch: &[ScannedIntention],
    rest: &[ScannedIntention],
    min: usize,
    original_code: Option<i8>,
    process: &OutPoint,
    probe: &mut Option<IsolationProbe>,
    attempt: &mut impl FnMut(&[ScannedIntention]) -> Result<bool, SsriError>,
) -> Result<Option<IsolatedIntentions>, SsriError> {
    let mut good = Vec::new();
    let mut bad = Vec::new();
    let mut classify = |item: &ScannedIntention, result: Result<bool, SsriError>| {
        match result {
            Ok(true) => good.push(item.clone()),
            Ok(false) => {}
            Err(SsriError::Incompatible(code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH))) => {
                bad.push((item.clone(), code));
            }
            Err(error @ SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => return Err(error),
            Err(error) => {
                tracing::debug!(target: LOG_TARGET, event = "assemble.isolation_skipped",
                    out_point = ?item.out_point, %error);
            }
        }
        Ok(())
    };
    if min == 1 {
        *probe = None;
        for item in batch {
            let result = match original_code {
                Some(code) if batch.len() == 1 => Err(SsriError::Incompatible(code)),
                _ => attempt(std::slice::from_ref(item)),
            };
            classify(item, result)?;
        }
        return Ok(Some((good, bad)));
    }

    let candidates: HashMap<_, _> = batch
        .iter()
        .chain(rest)
        .map(|item| (&item.out_point, item))
        .collect();
    let saved = probe.get_or_insert_with(|| IsolationProbe {
        process: process.clone(),
        candidates: batch
            .iter()
            .chain(rest)
            .map(|item| item.out_point.clone())
            .collect(),
        window_len: batch.len(),
        min,
        indices: Some((0..min).collect()),
    });
    let Some(indices) = &mut saved.indices else {
        return Ok(Some((good, bad)));
    };
    let snapshot: Vec<_> = saved
        .candidates
        .iter()
        .map(|op| candidates.get(op).copied())
        .collect();
    const MAX_PROBES: usize = 64;
    for _ in 0..MAX_PROBES {
        let control: Option<Vec<_>> = indices.iter().map(|&i| snapshot[i].cloned()).collect();
        let result = match control.as_deref() {
            Some(items) => attempt(items),
            None => Ok(false),
        };
        match result {
            Ok(true) => {
                let end = saved.window_len.max(indices[min - 1] + 1);
                for (index, item) in snapshot[..end].iter().enumerate() {
                    let Some(item) = item else { continue };
                    if indices.contains(&index) {
                        classify(item, Ok(true))?;
                        continue;
                    }

                    let mut comparison = control.as_ref().expect("successful control").clone();
                    comparison.pop();
                    let position = indices[..min - 1].partition_point(|&i| i < index);
                    comparison.insert(position, (*item).clone());
                    classify(item, attempt(&comparison))?;
                }
                good.truncate(batch.len());
                *probe = None;
                return Ok(Some((good, bad)));
            }
            Ok(false)
            | Err(SsriError::Incompatible(
                DEPOSIT_BELOW_MINIMUM
                | CID_LOCK_MISMATCH
                | INCOMPATIBLE_BATCH
                | UNSUPPORTED_FUNDING_LOCK
                | BENEFICIARY_MISSING,
            )) => {}
            Err(error) => return Err(error),
        }
        if !advance_probe_indices(indices, snapshot.len()) {
            saved.indices = None;
            return Ok(Some((good, bad)));
        }
    }
    Ok(None)
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
    worker.isolation_probe = None;
    worker.clear_isolated_refunds();
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

fn refund_policy_rejections<R: Rpc + HostQueryRpc>(
    rpc: &R,
    mut params: ProcessBatchParams<'_>,
) -> (BatchOutcome, BatchReportDelta) {
    const MAX_ATTEMPTS: usize = 16;
    let mut report = BatchReportDelta::default();
    let Some(code) = params.process.verdict_code_cell() else {
        report.skipped = params.batch.len();
        report.last_error = Some("policy refund requires a live Process code-cell ELF".into());
        return (BatchOutcome::Continue, report);
    };
    if let Err(outcome) =
        verify_refund_cell(rpc, code, "Process code", params.batch.len(), &mut report)
    {
        return (outcome, report);
    }
    let deps = match merge_cell_deps(&[cell_dep_code(&code.out_point)], params.tick_cell_deps) {
        Ok(deps) => deps,
        Err(error) => {
            report.skipped = params.batch.len();
            report.last_error = Some(format!("policy refund: {error}"));
            return (BatchOutcome::Continue, report);
        }
    };
    let cursor = {
        let worker = params.state.lock().expect("process worker state");
        if worker.unhealthy_state.as_ref() == Some(&params.process.live_cell.out_point) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        worker.refund_cursor.clone()
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
        let code = {
            let mut worker = params.state.lock().expect("process worker state");
            worker.refund_cursor = Some(item.out_point.clone());
            worker.isolated_refunds.get(&item.out_point).copied()
        };
        let outcome = assemble_and_submit_refund(
            rpc,
            &params.state,
            std::slice::from_ref(item),
            code.unwrap_or(INCOMPATIBLE_BATCH),
            params.execution_code_key,
            &deps,
            params.refund_policy,
            &mut report,
            BatchMode::PolicyRefund,
            &params.process.live_cell,
            &lease,
        );
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

#[allow(clippy::too_many_arguments)]
fn handle_incompatible_refunds<R: Rpc + HostQueryRpc>(
    rpc: &R,
    process: &ResolvedProcess,
    state: &Arc<Mutex<ProcessWorkerState>>,
    batch: &[ScannedIntention],
    code: i8,
    execution_code_key: &[u8],
    tick_cell_deps: &[CellDep],
    refund_policy: &RefundPolicy,
    report: &mut BatchReportDelta,
    refund_only: bool,
    lease: &BatchLease<'_>,
) -> (BatchOutcome, BatchReportDelta) {
    if let Some(code) = process.verdict_code_cell() {
        if let Err(outcome) = verify_refund_cell(rpc, code, "Process code", batch.len(), report) {
            return (outcome, report.clone());
        }
    }
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
            &process.live_cell,
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
    authorizing_state: &LiveCell,
    lease: &BatchLease<'_>,
) -> BatchOutcome {
    let refund_only = attempt_mode.refund_only();
    if lease.cancelled() {
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
    // The CID refund verifies assets and fees, so dry-run cannot validate the
    // Intention data and lock args that were used to obtain the policy verdict.
    for item in group {
        if let Err(outcome) = verify_refund_cell(
            rpc,
            &intention_snapshot(item),
            "Intention",
            group.len(),
            report,
        ) {
            return outcome;
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
                if attempt_mode != BatchMode::PolicyRefund {
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
            return if matches!(m, RpcError::Unavailable(_)) {
                BatchOutcome::RpcUnavailable
            } else {
                BatchOutcome::Continue
            };
        }
        DryRunGate::Proceed => {}
    }
    if let Err(outcome) =
        verify_refund_cell(rpc, authorizing_state, "Process state", group.len(), report)
    {
        return outcome;
    }
    let mut worker = state.lock().expect("process worker state");
    let Ok(_submission) = lease.begin_submission() else {
        report.skipped += group.len();
        return BatchOutcome::Continue;
    };
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
