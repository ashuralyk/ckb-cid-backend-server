use crate::assemble::{assemble_digest_incompatible, assemble_digest_ok, AssembleError};
use crate::process::{ProcessResolver, ResolvedProcess};
use crate::rpc::{out_point_key, Rpc, RpcError, TxStatus};
use crate::scan::{bucket_by_process_type_hash, scan_intentions, ScannedIntention};
use crate::ssri::{to_intention_input, SsriError, SsriHost};
use crate::submit::{submit_execution, SubmitState};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellDep};
use ckb_types::prelude::*;
use std::collections::HashSet;
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
    pub extra_cell_deps: Vec<CellDep>,
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
            },
        }
    }

    pub fn rpc(&self) -> &R {
        &self.rpc
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
        );
        report
    }

    fn tick_inner(&mut self) -> TickReport {
        let mut report = TickReport::default();
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
                    return report;
                }
                Err(e) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        event = "scan.unavailable",
                        error = %e,
                    );
                    report.last_error = Some(format!("scan error: {e}"));
                    return report;
                }
            };
        report.scan_ok = true;
        report.scanned = scanned.len();

        let live_keys: HashSet<Vec<u8>> = scanned
            .iter()
            .map(|i| out_point_key(&i.out_point))
            .collect();
        let tip = self.refresh_tip();
        self.reconcile_pending(tip);
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
        report
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
                    let Some(tip) = tip else {
                        continue;
                    };
                    let confirmations = confirmation_count(tip, block_number);
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
                            tip,
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
        let mut remaining = bucket;
        while remaining.len() >= min as usize {
            let n = remaining.len().min(max as usize);
            let batch: Vec<ScannedIntention> = remaining.drain(..n).collect();
            let consumed_process = self.process_batch(&process, batch, report);
            if consumed_process {
                // digest_ok spends the resolved Process outpoint. Leave later
                // batches for the next tick, which re-resolves the recreate.
                break;
            }
        }
        if !remaining.is_empty() {
            report.skipped += remaining.len();
        }
    }

    /// Returns true when a digest_ok submit consumed `process`.
    fn process_batch(
        &mut self,
        process: &ResolvedProcess,
        batch: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) -> bool {
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
                    return true;
                }
                match assemble_digest_ok(&batch, process, &partial, &self.config.extra_cell_deps) {
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
                        return false;
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: LOG_TARGET,
                            event = "tick.skip",
                            reason = "digest_ok",
                            error = %e,
                        );
                        report.skipped += batch.len();
                        return false;
                    }
                }
            }
            Err(SsriError::Incompatible(code)) => {
                tracing::info!(
                    target: LOG_TARGET,
                    event = "ssri.incompatible",
                    code,
                );
                match assemble_digest_incompatible(&batch, &self.config.extra_cell_deps) {
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
                        return false;
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
                return false;
            }
        };
        let consumed_process = skeleton
            .tx
            .inputs()
            .into_iter()
            .any(|i| i.previous_output() == process.live_cell.out_point);
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
                consumed_process
            }
            Err(RpcError::Conflict(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.conflict",
                    error = %m,
                );
                report.conflicts += 1;
                report.last_error = Some(format!("submit conflict: {m}"));
                false
            }
            Err(RpcError::SubmitRejected(m)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    event = "submit.rejected",
                    error = %m,
                );
                report.last_error = Some(format!("submit rejected: {m}"));
                report.skipped += batch.len();
                false
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
                false
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
