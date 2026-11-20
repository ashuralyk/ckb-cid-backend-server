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
    SsriError, SsriHost, HOST_QUERY_FAILED, PROCESS_LOCK_ARGS_INVALID, STATE_CAPACITY_INSUFFICIENT,
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

#[derive(Clone, Copy)]
pub(crate) struct ProbeBudget {
    pub(crate) remaining: usize,
    scan_number: u64,
    live_reads: usize,
}

impl Default for ProbeBudget {
    fn default() -> Self {
        Self::for_scan(0)
    }
}

impl ProbeBudget {
    pub(crate) fn for_scan(scan_number: u64) -> Self {
        Self {
            remaining: MAX_COMPARISON_PROBES,
            scan_number,
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
    AssemblyChecked,
    Continue,
    ProcessOccupied,
    /// Leave cells live and advance to the next window without caching a verdict.
    RetryLater,
    /// Resume the current subset search within this tick.
    RetryIsolation,
    /// The current candidates have no more subsets.
    IsolationExhausted,
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
    let (previous, mut budget, arrivals, exhausted) = {
        let mut worker = state.lock().expect("process worker state");
        (
            worker.isolation_probe.take(),
            worker.probe_budget,
            std::mem::take(&mut worker.isolation_arrivals),
            worker.exhausted_assembly.take(),
        )
    };
    let candidates: Vec<_> = batch.iter().chain(rest).collect();
    let min_size = min as usize;
    let max_size = batch.len();
    let candidate_count = candidates.len();
    let lookup_failed = |error| {
        (
            BatchOutcome::RetryLater,
            BatchReportDelta {
                skipped: max_size,
                last_error: Some(format!("verify isolation candidate: {error}")),
                ..BatchReportDelta::default()
            },
        )
    };

    // An exhausted search only gets a backoff when its entire candidate
    // snapshot can be revalidated within the bounded lookup budget.
    let chain = (mode == BatchMode::Assemble && candidate_count <= MAX_COMPARISON_PROBES)
        .then(|| {
            rpc.get_block_hash(tip)
                .ok()
                .flatten()
                .map(|hash| (tip, hash))
        })
        .flatten();
    let mut live = HashMap::new();
    let mut watched_dead = VecDeque::new();
    let mut revived = Vec::new();
    if let Some(marker) = exhausted.filter(|marker| {
        marker.skip_scan == budget.scan_number
            && Some(marker.chain) == chain
            && marker.live.len() == candidate_count
    }) {
        let mut unchanged = true;
        for (candidate, was_live) in candidates.iter().zip(marker.live) {
            match rpc.get_live_cell(&candidate.out_point) {
                Ok(cell) => {
                    let is_live = cell.is_some();
                    unchanged &= is_live == was_live;
                    live.insert(candidate.out_point.clone(), is_live);
                    if is_live && !was_live {
                        revived.push(candidate.out_point.clone());
                    }
                    if !is_live {
                        watched_dead.push_back(candidate.out_point.clone());
                    }
                }
                Err(error) => return lookup_failed(error),
            }
        }
        if unchanged {
            report.skipped += batch.len();
            return (BatchOutcome::IsolationExhausted, report);
        }
    }

    let mut probe = previous;
    if let Some(previous) = &mut probe {
        if previous.scan_number != budget.scan_number {
            let watched = std::mem::take(&mut previous.watched_dead);
            let accepted: HashSet<_> = if watched.is_empty() {
                HashSet::new()
            } else {
                candidates.iter().map(|item| &item.out_point).collect()
            };
            for op in watched {
                if !accepted.contains(&op) {
                    continue;
                }
                match rpc.get_live_cell(&op) {
                    Ok(cell) => {
                        live.insert(op.clone(), cell.is_some());
                        if cell.is_some() {
                            revived.push(op);
                        } else {
                            watched_dead.push_back(op);
                        }
                    }
                    Err(error) => return lookup_failed(error),
                }
            }
            previous.live = std::mem::take(&mut live);
            previous.watched_dead = std::mem::take(&mut watched_dead);
            previous.scan_number = budget.scan_number;
            previous.cacheable &= previous.chain == chain;
        }
        if previous.restart
            || !revived.is_empty()
            || !previous.matches_shape(candidate_count, min_size, max_size)
        {
            let previous = probe.take().expect("active search");
            live = previous.live;
            watched_dead = previous.watched_dead;
        }
    }

    let priority = if revived.is_empty() && arrivals.is_empty() {
        Vec::new()
    } else {
        let positions: HashMap<_, _> = candidates
            .iter()
            .enumerate()
            .map(|(i, item)| (&item.out_point, i))
            .collect();
        // Include both a revival and an arrival early if they occur together.
        let mut seen = HashSet::new();
        revived
            .first()
            .into_iter()
            .chain(arrivals.iter().rev())
            .chain(&revived)
            .filter_map(|op| positions.get(op).copied())
            .filter(|index| seen.insert(*index))
            .collect()
    };
    let new_probe = |candidates: &[&ScannedIntention], budget: &ProbeBudget, rejected| {
        IsolationProbe::new(
            candidates,
            min_size,
            max_size,
            budget.scan_number,
            rejected,
            &priority,
        )
    };
    let cached = |items: &[ScannedIntention]| {
        let rejected = mode != BatchMode::CheckAssembly
            && window_combo_cached(
                &state.lock().expect("process worker state"),
                items,
                process,
                execution_code_key,
            );
        if rejected {
            tracing::debug!(target: LOG_TARGET, event = "submit.rejected_combo", n = items.len());
        }
        rejected
    };
    // The pinned guest preserves occupied capacity and pays a positive fee
    // from the Process state for every batch. A funded state can still return
    // exit 37 for some subsets, so only stop on verified zero headroom.
    let can_pause_on_exit_37 =
        cell_occupied_capacity(&process.live_cell.output, &process.live_cell.data)
            .is_some_and(|occupied| process.live_cell.capacity() <= occupied)
            && process.verdict_code_cell().is_some_and(|code| {
                let data_hash: [u8; 32] = CellOutput::calc_data_hash(&code.data).unpack();
                data_hash == AUDITED_EXAMPLE_DATA_HASH
            });
    let mark_unfunded = || {
        tracing::warn!(target: LOG_TARGET, event = "assemble.process_unfunded",
            process_type_hash = %hex_pref(&hash), out_point = ?process.live_cell.out_point,
            code = STATE_CAPACITY_INSUFFICIENT);
        state
            .lock()
            .expect("process worker state")
            .mark_process_unfunded(process);
        let outcome = if mode == BatchMode::CheckAssembly {
            BatchOutcome::AssemblyChecked
        } else {
            BatchOutcome::RetryLater
        };
        (
            outcome,
            BatchReportDelta {
                skipped: batch.len(),
                last_error: Some("Process assembly failed (exit 37: no fee headroom)".into()),
                ..BatchReportDelta::default()
            },
        )
    };
    let initial_cached = probe.is_none() && cached(&batch);
    let mut cacheable = chain.is_some() && !initial_cached;
    // The initial assembly validates its own draft inputs through the host.
    // Isolation skips stale or already rejected subsets before running a guest.
    let prepared = if probe.is_none() && !initial_cached {
        match attempt(&batch) {
            Ok(assembled) => {
                cacheable = false;
                match prepare(&assembled, &batch) {
                    Err(PreparationError::NonLiveInput(op)) => {
                        report.last_error = Some(format!("guest-added input {op:?} is not live"));
                        None
                    }
                    Err(PreparationError::Rejected | PreparationError::UnsafeFunding)
                        if mode == BatchMode::CheckAssembly =>
                    {
                        None
                    }
                    result => Some(result),
                }
            }
            Err(SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => {
                return mark_process_unhealthy(hash, process, &state, batch.len());
            }
            Err(SsriError::Incompatible(STATE_CAPACITY_INSUFFICIENT)) if can_pause_on_exit_37 => {
                return mark_unfunded();
            }
            Err(error) => {
                cacheable &= cacheable_assembly_exit(&error);
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
        let mut probe = probe.unwrap_or_else(|| {
            let mut probe = new_probe(&candidates, &budget, initial_cached);
            probe.live = live;
            probe.watched_dead = watched_dead;
            probe.cacheable = cacheable;
            probe.chain = chain;
            probe
        });
        let mut cacheable = probe.cacheable;
        let mut try_subset = |items: &[ScannedIntention]| {
            if cached(items) {
                return Ok(None);
            }
            match attempt(items) {
                Ok(assembled) => {
                    cacheable = false;
                    match prepare(&assembled, items) {
                        Err(PreparationError::NonLiveInput(_)) => Ok(None),
                        Err(PreparationError::InputLookup(error)) => Err(SsriError::Unusable(
                            format!("verify guest-added input: {error}"),
                        )),
                        Err(PreparationError::Rejected | PreparationError::UnsafeFunding)
                            if mode == BatchMode::CheckAssembly =>
                        {
                            Ok(None)
                        }
                        result => Ok(Some(result)),
                    }
                }
                Err(error @ SsriError::Incompatible(PROCESS_LOCK_ARGS_INVALID)) => Err(error),
                Err(error @ SsriError::Incompatible(STATE_CAPACITY_INSUFFICIENT))
                    if can_pause_on_exit_37 =>
                {
                    Err(error)
                }
                Err(error) => {
                    cacheable &= cacheable_assembly_exit(&error);
                    tracing::warn!(target: LOG_TARGET, event = "assemble.probe_failed",
                        process_type_hash = %hex_pref(&hash), %error,
                        intentions = ?items.iter().map(|item| &item.out_point).collect::<Vec<_>>());
                    Ok(None)
                }
            }
        };
        let result = probe.search(&candidates, rpc.as_ref(), &mut budget, &mut try_subset);
        probe.cacheable = cacheable;
        let pending = probe.has_work();
        let exhausted = if !pending && matches!(&result, Ok(None)) && cacheable {
            match probe.exhausted_snapshot(&candidates, rpc.as_ref(), &mut budget) {
                Ok(marker) => marker,
                Err(error) => return lookup_failed(error),
            }
        } else {
            None
        };
        {
            let mut worker = state.lock().expect("process worker state");
            worker.probe_budget = budget;
            worker.exhausted_assembly = exhausted;
            if pending && result.is_ok() {
                worker.isolation_probe = Some(probe);
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
            Err(SsriError::Incompatible(STATE_CAPACITY_INSUFFICIENT)) if can_pause_on_exit_37 => {
                return mark_unfunded();
            }
            result => {
                let searched = result.is_ok();
                let checked = mode == BatchMode::CheckAssembly && searched;
                if let Err(error) = result {
                    tracing::warn!(target: LOG_TARGET, event = "assemble.probe_deferred",
                        process_type_hash = %hex_pref(&hash), %error);
                    report.last_error = Some(format!("assemble isolation: {error}"));
                }
                report.skipped += batch.len();
                let outcome = if pending && searched {
                    BatchOutcome::ProbePending
                } else if checked {
                    BatchOutcome::AssemblyChecked
                } else {
                    BatchOutcome::IsolationExhausted
                };
                return (outcome, report);
            }
        }
    };
    let retry_isolation = |worker: &mut ProcessWorkerState, rejected| {
        let probe = match worker.isolation_probe.take() {
            Some(mut probe) => {
                probe.advance();
                probe
            }
            None => {
                let candidates: Vec<_> = batch.iter().chain(rest).collect();
                new_probe(&candidates, &budget, rejected)
            }
        };
        if probe.has_work() {
            worker.isolation_probe = Some(probe);
            BatchOutcome::RetryIsolation
        } else {
            BatchOutcome::IsolationExhausted
        }
    };

    let skeleton = match prepared {
        Ok(skeleton) => skeleton,
        Err(error @ (PreparationError::Rejected | PreparationError::UnsafeFunding)) => {
            let mut worker = state.lock().expect("process worker state");
            if matches!(error, PreparationError::Rejected) {
                record_invalid_assembled_reject(&mut worker, &batch, process, execution_code_key);
            }
            report.skipped += batch.len();
            return (retry_isolation(&mut worker, true), report);
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
        return (BatchOutcome::AssemblyChecked, report);
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
        let mut worker = state.lock().expect("process worker state");
        if worker.submit.rejected_combos.contains(&fp) {
            let outcome = retry_isolation(&mut worker, true);
            tracing::debug!(
                target: LOG_TARGET,
                event = "submit.rejected_combo",
                n = batch.len(),
            );
            return (outcome, report);
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
            let outcome = retry_isolation(&mut worker, true);
            worker.submit.rejected_combos.insert(fp);
            tracing::warn!(
                target: LOG_TARGET,
                event = "dry_run.rejected",
                error = %m,
                n = batch.len(),
            );
            report.last_error = Some(format!("dry-run rejected: {m}"));
            return (outcome, report);
        }
        DryRunGate::Skip(RpcError::Conflict(m)) => {
            let mut worker = state.lock().expect("process worker state");
            let outcome = retry_isolation(&mut worker, false);
            tracing::warn!(target: LOG_TARGET, event = "dry_run.conflict", error = %m);
            report.conflicts += 1;
            report.last_error = Some(format!("dry-run conflict: {m}"));
            return (outcome, report);
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
            worker.isolation_probe = None;
            if consumed_process {
                (BatchOutcome::SubmittedProcess, report)
            } else {
                (BatchOutcome::Submitted, report)
            }
        }
        Err(RpcError::Conflict(m)) => {
            let outcome = retry_isolation(&mut worker, false);
            tracing::warn!(
                target: LOG_TARGET,
                event = "submit.conflict",
                error = %m,
            );
            report.conflicts += 1;
            report.last_error = Some(format!("submit conflict: {m}"));
            (outcome, report)
        }
        Err(RpcError::SubmitRejected(m)) => {
            let outcome = retry_isolation(&mut worker, true);
            worker.submit.rejected_combos.insert(fp);
            tracing::warn!(
                target: LOG_TARGET,
                event = "submit.rejected",
                error = %m,
                n = batch.len(),
            );
            report.last_error = Some(format!("submit rejected: {m}"));
            (outcome, report)
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

fn cacheable_assembly_exit(error: &SsriError) -> bool {
    matches!(
        error,
        SsriError::Incompatible(code)
            if !matches!(*code, HOST_QUERY_FAILED | STATE_CAPACITY_INSUFFICIENT)
    )
}

pub(crate) struct ExhaustedAssembly {
    skip_scan: u64,
    chain: (u64, [u8; 32]),
    live: Vec<bool>,
}

/// Enumerate legal subsets of the initial window and the remaining candidates.
pub(crate) struct IsolationProbe {
    order: Vec<usize>,
    min: usize,
    max: usize,
    cursors: VecDeque<(usize, Option<Vec<usize>>)>,
    interleave: bool,
    preferred: Vec<Vec<usize>>,
    next_preferred: usize,
    live: HashMap<OutPoint, bool>,
    watched_dead: VecDeque<OutPoint>,
    scan_number: u64,
    cacheable: bool,
    chain: Option<(u64, [u8; 32])>,
    pub(crate) restart: bool,
}

impl IsolationProbe {
    fn new(
        candidates: &[&ScannedIntention],
        min: usize,
        max: usize,
        scan_number: u64,
        rejected: bool,
        priority: &[usize],
    ) -> Self {
        let count = candidates.len();
        let interleave = exceeds_probe_budget(count, min, max);
        let mut order: Vec<_> = (0..count).collect();
        // Keep nearby candidates first. Rotate the remainder when the search
        // restarts and give arrivals and observed revivals early probes.
        if interleave && (scan_number != 0 || !priority.is_empty()) {
            let priority_set: HashSet<_> = priority.iter().copied().collect();
            order[(min + 1).min(count)..].sort_by_cached_key(|&index| {
                let mut hasher = DefaultHasher::new();
                (scan_number, index).hash(&mut hasher);
                (!priority_set.contains(&index), hasher.finish())
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
        // Sample combinations that include new or revived candidates, while
        // leaving most of the budget to the normal per-size search.
        let size_count = max - min + 1;
        if interleave && !priority.is_empty() {
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
        let mut cursors: VecDeque<_> = if count == min {
            VecDeque::new()
        } else {
            (min..=max).map(|size| (size, None)).collect()
        };
        if cursors.len() > MAX_COMPARISON_PROBES - preferred.len() {
            // A range wider than the budget gets a different start on restart.
            let start = (scan_number % cursors.len() as u64) as usize;
            cursors.rotate_left(start);
        }
        Self {
            order,
            min,
            max,
            cursors,
            interleave,
            preferred,
            next_preferred: 0,
            live: HashMap::new(),
            watched_dead: VecDeque::new(),
            scan_number,
            cacheable: false,
            chain: None,
            restart: false,
        }
    }

    fn is_initial(positions: &[usize], max: usize) -> bool {
        positions.len() == max && positions.iter().copied().eq(0..max)
    }

    fn matches_shape(&self, count: usize, min: usize, max: usize) -> bool {
        self.order.len() == count && self.min == min && self.max == max
    }

    fn has_work(&self) -> bool {
        !self.cursors.is_empty()
    }

    fn candidate_live(
        &mut self,
        op: &OutPoint,
        rpc: &impl HostQueryRpc,
        budget: &mut ProbeBudget,
    ) -> Result<Option<bool>, RpcError> {
        if let Some(&live) = self.live.get(op) {
            return Ok(Some(live));
        }
        // Allow a full window and up to 64 additional reads even if every
        // reached subset adds new candidates. Never walk the whole remainder.
        if budget.live_reads >= self.max + MAX_COMPARISON_PROBES {
            return Ok(None);
        }
        budget.live_reads += 1;
        let live = rpc.get_live_cell(op)?.is_some();
        self.live.insert(op.clone(), live);
        if !live {
            if self.watched_dead.len() == MAX_COMPARISON_PROBES {
                self.watched_dead.pop_front();
            }
            self.watched_dead.push_back(op.clone());
        }
        Ok(Some(live))
    }

    fn exhausted_snapshot(
        &mut self,
        candidates: &[&ScannedIntention],
        rpc: &impl HostQueryRpc,
        budget: &mut ProbeBudget,
    ) -> Result<Option<ExhaustedAssembly>, RpcError> {
        let mut live = Vec::with_capacity(candidates.len());
        for item in candidates {
            let Some(is_live) = self.candidate_live(&item.out_point, rpc, budget)? else {
                return Ok(None);
            };
            live.push(is_live);
        }
        Ok(self.chain.map(|chain| ExhaustedAssembly {
            skip_scan: budget.scan_number.wrapping_add(1),
            chain,
            live,
        }))
    }

    fn search<T>(
        &mut self,
        candidates: &[&ScannedIntention],
        rpc: &impl HostQueryRpc,
        budget: &mut ProbeBudget,
        attempt: &mut impl FnMut(&[ScannedIntention]) -> Result<Option<T>, SsriError>,
    ) -> Result<Option<(Vec<ScannedIntention>, T)>, SsriError> {
        'probe: while self.has_work() && budget.remaining > 0 {
            let positions = if let Some(preferred) = self.preferred.get(self.next_preferred) {
                preferred.clone()
            } else {
                let (size, indices) = self.cursors.front_mut().expect("active subset search");
                let indices = indices.get_or_insert_with(|| (0..*size).collect());
                let mut positions: Vec<_> = indices.iter().map(|&i| self.order[i]).collect();
                positions.sort_unstable(); // Preserve the candidate order within each batch.
                if Self::is_initial(&positions, self.max) || self.preferred.contains(&positions) {
                    self.advance();
                    continue;
                }
                positions
            };
            budget.remaining -= 1;
            let mut locks = HashSet::new();
            if positions
                .iter()
                .all(|&i| locks.insert(candidates[i].lock_group_key()))
            {
                for &i in &positions {
                    match self.candidate_live(&candidates[i].out_point, rpc, budget) {
                        Ok(Some(true)) => {}
                        Ok(Some(false)) => {
                            self.advance();
                            continue 'probe;
                        }
                        Ok(None) => return Ok(None),
                        Err(error) => {
                            return Err(SsriError::Unusable(format!(
                                "verify isolation candidate: {error}"
                            )));
                        }
                    }
                }
                let items: Vec<_> = positions.iter().map(|&i| candidates[i].clone()).collect();
                if let Some(assembled) = attempt(&items)? {
                    return Ok(Some((items, assembled)));
                }
            }
            self.advance();
        }
        Ok(None)
    }

    fn advance(&mut self) {
        if self.next_preferred < self.preferred.len() {
            self.next_preferred += 1;
            return;
        }
        let (size, indices) = self.cursors.pop_front().expect("active subset search");
        let mut indices = indices.expect("initialized subset cursor");
        // Exhaust nearby subsets before including a later candidate.
        for i in 0..indices.len() {
            let limit = indices.get(i + 1).copied().unwrap_or(self.order.len());
            if indices[i] + 1 < limit {
                indices[i] += 1;
                for (initial, index) in indices[..i].iter_mut().enumerate() {
                    *index = initial;
                }
                if self.interleave {
                    self.cursors.push_back((size, Some(indices)));
                } else {
                    self.cursors.push_front((size, Some(indices)));
                }
                return;
            }
        }
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
    skipped: usize,
) -> (BatchOutcome, BatchReportDelta) {
    tracing::error!(target: LOG_TARGET, event = "process.unhealthy",
        process_type_hash = %hex_pref(&hash), out_point = ?process.live_cell.out_point,
        code = PROCESS_LOCK_ARGS_INVALID, reason = "ProcessLockArgsInvalid");
    let mut worker = state.lock().expect("process worker state");
    worker.quarantine_process(process);
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
        if worker.is_process_quarantined(params.process) {
            report.skipped = params.batch.len();
            return (BatchOutcome::Continue, report);
        }
        if worker.submit.pending_occupies(&authorizing_state.out_point) {
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
