use crate::assemble::ExecutionSkeleton;
use crate::rpc::{out_point_key, Rpc, RpcError};
use ckb_types::packed::{Byte32, OutPoint};
use ckb_types::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct SubmitState {
    pub in_flight: HashSet<Vec<u8>>,
    pub failed: HashSet<Vec<u8>>,
    /// Submitted tx hash bytes → consumed outpoint keys still treated as in-flight.
    pub pending: HashMap<Vec<u8>, Vec<Vec<u8>>>,
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
                keys.push(k);
            }
            state.pending.insert(hash.as_slice().to_vec(), keys);
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
        Err(e) => Err(e),
    }
}
