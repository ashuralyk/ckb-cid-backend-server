use crate::assemble::ExecutionSkeleton;
use crate::rpc::{out_point_key, Rpc, RpcError};
use ckb_types::packed::{Byte32, OutPoint};
use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct SubmitState {
    pub in_flight: HashSet<Vec<u8>>,
    pub failed: HashSet<Vec<u8>>,
}

pub fn submit_execution<R: Rpc>(
    rpc: &R,
    skeleton: &ExecutionSkeleton,
    state: &mut SubmitState,
    consumed: &[OutPoint],
) -> Result<Byte32, RpcError> {
    match rpc.submit_transaction(&skeleton.tx) {
        Ok(hash) => {
            for op in consumed {
                let k = out_point_key(op);
                state.in_flight.insert(k.clone());
                state.failed.remove(&k);
            }
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
