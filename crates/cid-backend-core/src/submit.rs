use crate::assemble::ExecutionSkeleton;
use crate::rpc::{out_point_key, Rpc, RpcError};
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
    /// Permanent submit rejection this process lifetime. Skipped, not occupied.
    pub rejected: HashSet<Vec<u8>>,
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

pub fn submit_execution<R: Rpc>(
    rpc: &R,
    skeleton: &ExecutionSkeleton,
    state: &mut SubmitState,
    consumed: &[OutPoint],
) -> Result<Byte32, RpcError> {
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
            for op in consumed {
                let k = out_point_key(op);
                state.rejected.insert(k.clone());
                state.in_flight.remove(&k);
            }
            Err(e)
        }
        Err(e) => Err(e),
    }
}
