use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use std::fmt;

#[derive(Clone, Debug)]
pub struct LiveCell {
    pub out_point: OutPoint,
    pub output: CellOutput,
    pub data: Vec<u8>,
}

impl LiveCell {
    pub fn capacity(&self) -> u64 {
        self.output.capacity().unpack()
    }

    pub fn lock(&self) -> Script {
        self.output.lock()
    }

    pub fn type_script(&self) -> Option<Script> {
        self.output.type_().to_opt()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Unavailable(String),
    SubmitRejected(String),
    Conflict(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Unavailable(m) => write!(f, "rpc unavailable: {m}"),
            RpcError::SubmitRejected(m) => write!(f, "submit rejected: {m}"),
            RpcError::Conflict(m) => write!(f, "submit conflict: {m}"),
        }
    }
}

impl std::error::Error for RpcError {}

pub fn out_point_key(op: &OutPoint) -> Vec<u8> {
    op.as_slice().to_vec()
}

pub fn script_hash32(script: &Script) -> [u8; 32] {
    script.calc_script_hash().unpack()
}

pub trait Rpc {
    fn list_live_intentions(
        &self,
        cid_code_hash: &[u8; 32],
        hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError>;

    fn get_live_process_by_type_hash(
        &self,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError>;

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError>;
}
