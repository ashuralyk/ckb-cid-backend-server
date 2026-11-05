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
use std::time::Duration;

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
    pub fn new(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        resolver: ProcessResolver,
    ) -> Self {
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
        let mut report = TickReport::default();
        let scanned = match scan_intentions(
            &self.rpc,
            &self.config.cid_code_hash,
            self.config.hash_type,
        ) {
            Ok(v) => v,
            Err(RpcError::Unavailable(m)) => {
                log::warn!("scan unavailable: {m}");
                return report;
            }
            Err(e) => {
                log::warn!("scan error: {e}");
                return report;
            }
        };
        report.scanned = scanned.len();

        let live_keys: HashSet<Vec<u8>> = scanned
            .iter()
            .map(|i| out_point_key(&i.out_point))
            .collect();
        self.reconcile_pending(&live_keys);
        self.state
            .submit
            .in_flight
            .retain(|k| live_keys.contains(k));

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

    fn reconcile_pending(&mut self, live_keys: &HashSet<Vec<u8>>) {
        let hashes: Vec<Vec<u8>> = self.state.submit.pending.keys().cloned().collect();
        for hash_bytes in hashes {
            let tx_hash = match Byte32::from_slice(&hash_bytes) {
                Ok(h) => h,
                Err(_) => {
                    self.state.submit.pending.remove(&hash_bytes);
                    continue;
                }
            };
            match self.rpc.get_transaction_status(&tx_hash) {
                Ok(TxStatus::Dropped) => {
                    if let Some(ops) = self.state.submit.pending.remove(&hash_bytes) {
                        for k in ops {
                            if live_keys.contains(&k) {
                                self.state.submit.in_flight.remove(&k);
                            }
                        }
                    }
                }
                Ok(TxStatus::Committed) => {
                    self.state.submit.pending.remove(&hash_bytes);
                }
                Ok(TxStatus::Pending) => {}
                Err(e) => {
                    log::warn!("tx status for pending submit: {e}");
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
                log::warn!("skip bucket: no usable Process binary");
                report.skipped += bucket.len();
                return;
            }
            Err(e) => {
                log::warn!("skip bucket: process resolve {e}");
                report.skipped += bucket.len();
                return;
            }
        };
        let (min, max) = match self.ssri.get_batch_bounds(&process.binary) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("skip bucket: batch bounds {e}");
                report.skipped += bucket.len();
                return;
            }
        };
        if min == 0 || max < min {
            log::warn!("skip bucket: malformed bounds min={min} max={max}");
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
            Ok(partial) => match assemble_digest_ok(
                &batch,
                process,
                &partial,
                &self.config.extra_cell_deps,
            ) {
                Ok(s) => s,
                Err(AssembleError::CapacityShortfall { inputs, outputs }) => {
                    log::warn!("skip batch: capacity in={inputs} out={outputs}");
                    report.skipped += batch.len();
                    return false;
                }
                Err(e) => {
                    log::warn!("skip batch: digest_ok {e}");
                    report.skipped += batch.len();
                    return false;
                }
            },
            Err(SsriError::Incompatible(code)) => {
                log::info!("ssri incompatible {code}; attempting user-lock refund");
                match assemble_digest_incompatible(&batch, &self.config.extra_cell_deps) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("skip batch: refund unbuildable {e}");
                        report.skipped += batch.len();
                        return false;
                    }
                }
            }
            Err(SsriError::Unusable(m)) => {
                log::warn!("skip batch: ssri unusable {m} (no refund)");
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
            Ok(_) => {
                report.submitted += 1;
                consumed_process
            }
            Err(RpcError::Conflict(m)) => {
                log::warn!("submit conflict: {m}");
                report.conflicts += 1;
                false
            }
            Err(e) => {
                log::warn!("submit failed: {e}");
                report.skipped += batch.len();
                false
            }
        }
    }
}
