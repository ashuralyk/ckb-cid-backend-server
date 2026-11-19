use crate::assemble::ExecutionSkeleton;
use crate::rpc::{out_point_key, Rpc, RpcError};
use ckb_types::core::TransactionView;
use ckb_types::packed::{Byte32, OutPoint};
use ckb_types::prelude::*;
use std::collections::{HashMap, HashSet};

/// In-memory submit bookkeeping. Restart drops this; the next scan is the
/// source of truth.
#[derive(Clone, Debug, Default)]
pub struct SubmitState {
    pub in_flight: HashSet<Vec<u8>>,
    /// Conflict (retry next tick if the cell is still live). Informational.
    pub failed: HashSet<Vec<u8>>,
    /// Legacy outpoint skip set. SubmitRejected no longer writes here: a
    /// transaction-level reject cannot prove an individual Intention is invalid.
    pub rejected: HashSet<Vec<u8>>,
    /// Intention-set (+ Process outpoint when the tx spends Process) plus the
    /// tick's Type-ID execution code identity that got `SubmitRejected`.
    /// Identical combos are not resubmitted while that code snapshot is live.
    /// A new Process outpoint **or** Type-ID code outpoint is a new combo.
    pub rejected_combos: HashSet<Vec<u8>>,
    /// Submitted tx hash bytes → consumed outpoints still treated as in-flight
    /// until the tx is dropped or reaches confirmation depth.
    pub pending: HashMap<Vec<u8>, PendingTx>,
}

#[derive(Clone, Debug)]
pub struct PendingTx {
    pub consumed: Vec<Vec<u8>>,
    /// Outputs of this tx (e.g. Process recreate). Occupied until settlement
    /// so a confirming digest_ok cannot be spent again on the same fork.
    pub created: Vec<Vec<u8>>,
    /// True after we have observed `TxStatus::Committed` at least once.
    pub seen_committed: bool,
}

impl SubmitState {
    pub fn pending_occupies(&self, op: &OutPoint) -> bool {
        let k = out_point_key(op);
        self.pending
            .values()
            .any(|p| p.consumed.iter().any(|o| o == &k) || p.created.iter().any(|o| o == &k))
    }
    pub fn pending_occupied_keys(&self) -> HashSet<Vec<u8>> {
        self.pending
            .values()
            .flat_map(|p| p.consumed.iter().chain(p.created.iter()).cloned())
            .collect()
    }
}

/// Result of a pre-submit dry-run gate.
pub enum DryRunGate {
    Proceed,
    Rejected(String),
    Skip(String),
}

pub fn dry_run_gate<R: Rpc>(rpc: &R, tx: &TransactionView) -> DryRunGate {
    match rpc.dry_run_transaction(tx) {
        Ok(()) => DryRunGate::Proceed,
        Err(RpcError::SubmitRejected(m)) => DryRunGate::Rejected(m),
        Err(e) => DryRunGate::Skip(e.to_string()),
    }
}

pub fn submit_execution<R: Rpc>(
    rpc: &R,
    skeleton: &ExecutionSkeleton,
    state: &mut SubmitState,
    consumed: &[OutPoint],
) -> Result<Byte32, RpcError> {
    // Fail closed if the sized empty-witness skeleton drifted before RPC.
    if let Some(guard) = &skeleton.fee_guard {
        if let Err(e) = guard.verify(&skeleton.tx) {
            return Err(RpcError::SubmitRejected(format!("fee_guard: {e}")));
        }
    }
    match rpc.submit_transaction(&skeleton.tx) {
        Ok(hash) => {
            let mut keys = Vec::with_capacity(consumed.len());
            for op in consumed {
                let k = out_point_key(op);
                state.in_flight.insert(k.clone());
                state.failed.remove(&k);
                state.rejected.remove(&k);
                keys.push(k);
            }
            let created = (0..skeleton.tx.outputs().len())
                .map(|i| {
                    let op = OutPoint::new_builder()
                        .tx_hash(hash.clone())
                        .index((i as u32).pack())
                        .build();
                    out_point_key(&op)
                })
                .collect();
            state.pending.insert(
                hash.as_slice().to_vec(),
                PendingTx {
                    consumed: keys,
                    created,
                    seen_committed: false,
                },
            );
            Ok(hash)
        }
        Err(e @ RpcError::Conflict(_)) => {
            for op in consumed {
                let k = out_point_key(op);
                state.failed.insert(k.clone());
                state.in_flight.remove(&k);
            }
            Err(e)
        }
        Err(e @ RpcError::SubmitRejected(_)) => {
            // Transaction-level reject: do not blacklist consumed outpoints.
            // DigestLoop records a combo fingerprint and isolates sub-batches.
            for op in consumed {
                state.in_flight.remove(&out_point_key(op));
            }
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// Ordered Intention outpoints, optional Process state outpoint, and the
/// Type-ID execution code identity for this tick. Used to skip resubmitting an
/// identical combo after `SubmitRejected` without blaming one input. When
/// Process (or other Type-ID) **code** cells upgrade, `execution_code_key`
/// changes so a previously rejected combo can be retried under the new guest.
pub fn combo_fingerprint(
    intentions: &[OutPoint],
    process: Option<&OutPoint>,
    execution_code_key: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(intentions.len() as u32).to_le_bytes());
    for op in intentions {
        buf.extend(out_point_key(op));
    }
    match process {
        Some(p) => {
            buf.push(1);
            buf.extend(out_point_key(p));
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&(execution_code_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(execution_code_key);
    buf
}

/// Stable key for Type-ID–resolved code CellDep outpoints (config order).
pub fn execution_code_key(type_id_outpoints: &[OutPoint]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + type_id_outpoints.len() * 36);
    buf.extend_from_slice(&(type_id_outpoints.len() as u32).to_le_bytes());
    for op in type_id_outpoints {
        buf.extend(out_point_key(op));
    }
    buf
}
