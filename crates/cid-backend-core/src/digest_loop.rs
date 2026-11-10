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
}

pub struct LoopState {
    pub submit: SubmitState,
    pub resolver: ProcessResolver,
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
        self.reconcile_pending();
        let pending_occupied = self.state.submit.pending_occupied_keys();
        self.state
            .submit
            .in_flight
            .retain(|k| live_keys.contains(k) || pending_occupied.contains(k));

        let eligible: Vec<ScannedIntention> = scanned
            .into_iter()
            .filter(|i| {
                !self
                    .state
                    .submit
                    .in_flight
                    .contains(&out_point_key(&i.out_point))
            })
            .collect();

        for (_hash, bucket) in bucket_by_process_type_hash(&eligible) {
            self.process_bucket(bucket, &mut report);
        }
        report
    }

    fn reconcile_pending(&mut self) {
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
                    tracing::info!(
                        target: LOG_TARGET,
                        event = "pending.dropped",
                        tx_hash = %tx_hex,
                    );
                    if let Some(ops) = self.state.submit.pending.remove(&hash_bytes) {
                        for k in ops {
                            self.state.submit.in_flight.remove(&k);
                        }
                    }
                }
                Ok(TxStatus::Committed) => {
                    tracing::info!(
                        target: LOG_TARGET,
                        event = "pending.committed",
                        tx_hash = %tx_hex,
                    );
                    self.state.submit.pending.remove(&hash_bytes);
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
