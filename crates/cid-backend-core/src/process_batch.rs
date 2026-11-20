//! Process batch assembly / dry-run / submit (runs on per-Process worker threads).

use crate::assemble::{
    assemble_refund_with_policy, execution_from_assembled, validate_assembled_tx, AssembleError,
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
    SsriError, SsriHost, BENEFICIARY_MISSING, CID_LOCK_MISMATCH, DEPOSIT_BELOW_MINIMUM,
    HOST_QUERY_FAILED, INCOMPATIBLE_BATCH, PROCESS_LOCK_ARGS_INVALID, STATE_CAPACITY_INSUFFICIENT,
    UNSUPPORTED_FUNDING_LOCK,
};
use crate::submit::{combo_fingerprint, dry_run_gate, submit_execution, DryRunGate};
use crate::worker_commands::{Submission, WorkerCommands};
use crate::workers::ProcessWorkerState;
use cid_intention_filter::{FILTER_DEPOSIT_BELOW_MINIMUM, FILTER_MALFORMED_DIGEST};
use cid_process_example_policy::{classify_intention_excluding_process_lock, ProcessLockArgs};
use cid_protocol::CidArgs;
use ckb_types::packed::{CellDep, OutPoint, Script};
use ckb_types::prelude::{Entity, Unpack};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const LOG_TARGET: &str = "cid_backend";
pub(crate) const MAX_COMPARISON_PROBES: usize = 64;

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
    PolicyRefund,
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
        refund_policy: _,
        max_assembled_tx_bytes,
        job_epoch,
        epoch,
        min,
        mode: _,
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
    let lock = process.live_cell.lock();
    let (mut probe, mut probes_left, audited) = {
        let mut worker = state.lock().expect("process worker state");
        (
            worker.isolation_probe.take(),
            worker.comparison_probes_left,
            worker.audited_policy_lock.as_ref() == Some(&lock),
        )
    };
    // A resumed search already has its trigger. Re-assembling the changed
    // window could restart isolation before the saved comparisons run.
    let resuming = probe.as_ref().is_some_and(|saved| saved.indices.is_some());
    let initial = (!resuming).then(|| attempt(&batch));
    let original_code = match &initial {
        Some(Err(SsriError::Incompatible(code @ (DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH)))) => {
            Some(*code)
        }
        Some(Err(SsriError::Incompatible(BENEFICIARY_MISSING))) if audited => {
            Some(BENEFICIARY_MISSING)
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
        let mut check_live = |items: &[ScannedIntention]| {
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
            Ok(true)
        };
        let result = isolate_intentions(
            &batch,
            rest,
            min as usize,
            original_code,
            &process.live_cell.out_point,
            &mut probe,
            &mut probes_left,
            &mut check_live,
            &attempt,
        );
        {
            let mut worker = state.lock().expect("process worker state");
            worker.isolation_probe = probe;
            worker.comparison_probes_left = probes_left;
        }
        let IsolatedIntentions {
            accepted,
            rejected,
            control,
        } = match result {
            Ok(Some(result)) => result,
            Ok(None) => {
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
        if accepted.is_empty() && rejected.is_empty() {
            report.last_error = Some("assemble: could not isolate an Intention".into());
            report.skipped += batch.len();
            return (BatchOutcome::RetryLater, report);
        }
        // Check the remaining group before issuing any refunds. Exit 41 at
        // any point invalidates the state snapshot and all pending refunds.
        batch = accepted;
        let mut remaining = (batch.len() >= min as usize).then(|| attempt(&batch));
        if matches!(
            remaining,
            Some(Err(SsriError::Incompatible(
                INCOMPATIBLE_BATCH | STATE_CAPACITY_INSUFFICIENT
            )))
        ) && !control.is_empty()
            && !control
                .iter()
                .map(|item| &item.out_point)
                .eq(batch.iter().map(|item| &item.out_point))
        {
            tracing::info!(target: LOG_TARGET, event = "assemble.retry_control",
                batch_size = batch.len(), control_size = control.len());
            batch = control;
            remaining = Some(attempt(&batch));
        }
        if matches!(
            remaining,
            Some(Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)))
        ) {
            return mark_process_unhealthy(hash, process, &state, batch.len() + rejected.len());
        }
        if !lease.cancelled() {
            let mut worker = state.lock().expect("process worker state");
            for (item, code) in rejected {
                let can_refund = audited && audited_isolation_matches(&item, code, &lock);
                tracing::warn!(target: LOG_TARGET, event = "assemble.intention_isolated",
                    process_type_hash = %hex_pref(&hash), out_point = ?item.out_point, code, can_refund);
                if can_refund {
                    worker.isolated_refunds.insert(item.out_point, code);
                }
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

#[derive(Default)]
struct IsolatedIntentions {
    accepted: Vec<ScannedIntention>,
    rejected: Vec<(ScannedIntention, i8)>,
    control: Vec<ScannedIntention>,
}

enum ProbeVerdict {
    Accepted,
    Rejected(i8),
}

impl ProbeVerdict {
    fn record(self, item: &ScannedIntention, isolated: &mut IsolatedIntentions) {
        match self {
            Self::Accepted => isolated.accepted.push(item.clone()),
            Self::Rejected(code) => isolated.rejected.push((item.clone(), code)),
        }
    }
}

#[derive(Default)]
struct SubstitutionProbe {
    next: usize,
    verdicts: Vec<(usize, ProbeVerdict)>,
}

/// Keep positions stable while walking combinations. Cells missing from a
/// later scan leave holes and arrivals wait for a subsequent search.
pub(crate) struct IsolationProbe {
    process: OutPoint,
    candidates: Vec<OutPoint>,
    window_len: usize,
    min: usize,
    indices: Option<Vec<usize>>,
    // While substituting, `indices` identifies the successful control.
    substitution: Option<SubstitutionProbe>,
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

fn classify_probe(
    item: &ScannedIntention,
    result: Result<bool, SsriError>,
) -> Result<Option<ProbeVerdict>, SsriError> {
    match result {
        Ok(true) => Ok(Some(ProbeVerdict::Accepted)),
        Ok(false) => Ok(None),
        Err(SsriError::Incompatible(
            code @ (BENEFICIARY_MISSING | DEPOSIT_BELOW_MINIMUM | CID_LOCK_MISMATCH),
        )) => Ok(Some(ProbeVerdict::Rejected(code))),
        Err(error @ SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => Err(error),
        Err(error) => {
            tracing::debug!(target: LOG_TARGET, event = "assemble.isolation_skipped",
                out_point = ?item.out_point, %error);
            Ok(None)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn isolate_intentions<T>(
    batch: &[ScannedIntention],
    rest: &[ScannedIntention],
    min: usize,
    original_code: Option<i8>,
    process: &OutPoint,
    probe: &mut Option<IsolationProbe>,
    probes_left: &mut usize,
    check_live: &mut impl FnMut(&[ScannedIntention]) -> Result<bool, SsriError>,
    attempt: &impl Fn(&[ScannedIntention]) -> Result<T, SsriError>,
) -> Result<Option<IsolatedIntentions>, SsriError> {
    if min == 1 {
        *probe = None;
        let mut isolated = IsolatedIntentions::default();
        for item in batch {
            let single = std::slice::from_ref(item);
            let result = match original_code {
                Some(code) if batch.len() == 1 => Err(SsriError::Incompatible(code)),
                _ => check_live(single).and_then(|live| {
                    if live {
                        attempt(single).map(|_| true)
                    } else {
                        Ok(false)
                    }
                }),
            };
            if let Some(verdict) = classify_probe(item, result)? {
                verdict.record(item, &mut isolated);
            }
        }
        isolated.control.extend(isolated.accepted.first().cloned());
        return Ok(Some(isolated));
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
        substitution: None,
    });
    let Some(indices) = &mut saved.indices else {
        return Ok(Some(IsolatedIntentions::default()));
    };
    let snapshot: Vec<_> = saved
        .candidates
        .iter()
        .map(|op| candidates.get(op).copied())
        .collect();
    let control = loop {
        if *probes_left == 0 {
            return Ok(None);
        }
        let control: Option<Vec<_>> = indices.iter().map(|&i| snapshot[i].cloned()).collect();
        // Count missing or spent subsets too, so holes cannot make the
        // combination walk unbounded within a tick.
        *probes_left -= 1;
        match control {
            Some(items) if check_live(&items)? => {
                if saved.substitution.is_some() {
                    // Reuse the successful control without assembling it again.
                    *probes_left += 1;
                    break items;
                }
                match attempt(&items) {
                    Ok(_) => break items,
                    Err(SsriError::Incompatible(
                        DEPOSIT_BELOW_MINIMUM
                        | CID_LOCK_MISMATCH
                        | INCOMPATIBLE_BATCH
                        | UNSUPPORTED_FUNDING_LOCK
                        | BENEFICIARY_MISSING
                        | STATE_CAPACITY_INSUFFICIENT,
                    )) => {}
                    Err(error) => return Err(error),
                }
            }
            // Discard partial results if a saved control cell disappeared.
            _ => saved.substitution = None,
        }
        if !advance_probe_indices(indices, snapshot.len()) {
            saved.indices = None;
            return Ok(Some(IsolatedIntentions::default()));
        }
    };

    let substitution = saved
        .substitution
        .get_or_insert_with(SubstitutionProbe::default);
    let end = saved.window_len.max(indices[min - 1] + 1);
    for (index, item) in snapshot[..end].iter().enumerate().skip(substitution.next) {
        if let Some(item) = *item {
            let verdict = if indices.contains(&index) {
                Some(ProbeVerdict::Accepted)
            } else {
                if *probes_left == 0 {
                    return Ok(None);
                }
                *probes_left -= 1;
                let mut comparison = control.clone();
                comparison.pop();
                let position = indices[..min - 1].partition_point(|&i| i < index);
                comparison.insert(position, item.clone());
                if check_live(&comparison)? {
                    let result = match attempt(&comparison) {
                        Err(error @ SsriError::Incompatible(HOST_QUERY_FAILED)) => {
                            return Err(error)
                        }
                        result => result.map(|_| true),
                    };
                    classify_probe(item, result)?
                } else {
                    None
                }
            };
            if let Some(verdict) = verdict {
                substitution.verdicts.push((index, verdict));
            }
        }
        substitution.next = index + 1;
    }
    let mut isolated = IsolatedIntentions {
        control,
        ..IsolatedIntentions::default()
    };
    for (index, verdict) in std::mem::take(&mut substitution.verdicts) {
        if let Some(item) = snapshot[index] {
            verdict.record(item, &mut isolated);
        }
    }
    isolated.accepted.truncate(batch.len());
    *probe = None;
    Ok(Some(isolated))
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

/// Reproduce the audited example's per-cell assembly checks from this snapshot.
pub(crate) fn audited_isolation_matches(item: &ScannedIntention, code: i8, lock: &Script) -> bool {
    let Ok(args) = ProcessLockArgs::decode(&lock.args().raw_data()) else {
        return false;
    };
    if !args.matches_intention_lock(&item.lock_code_hash, item.lock_hash_type as u8) {
        return code == CID_LOCK_MISMATCH;
    }
    let expected_reason = match code {
        BENEFICIARY_MISSING => FILTER_MALFORMED_DIGEST,
        DEPOSIT_BELOW_MINIMUM => FILTER_DEPOSIT_BELOW_MINIMUM,
        _ => return false,
    };
    let asset = item
        .type_script
        .as_ref()
        .map(|typ| (typ.code_hash().unpack(), typ.hash_type().into()));
    classify_intention_excluding_process_lock(
        &item.protocol_lock(),
        item.capacity,
        asset.as_ref().map(|(hash, kind)| (hash, *kind)),
        &item.data,
        Some(&lock.code_hash().unpack()),
    ) == expected_reason
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
    // The filter is stateless and the audited example preserves its state lock.
    // Check the live state just before submit without competing with a pending digest.
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
        params
            .state
            .lock()
            .expect("process worker state")
            .refund_cursor = Some(item.out_point.clone());
        let outcome = submit_policy_refund(rpc, &params, item, &deps, &mut report, &lease);
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

fn submit_policy_refund<R: Rpc + HostQueryRpc>(
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
    let fp = combo_fingerprint(consumed, None, params.execution_code_key);
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
