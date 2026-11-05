use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::Byte32;
use cid_backend_core::rpc::{LiveCell, Rpc, RpcError};

pub struct PlaceholderRpc {
    pub node_url: String,
    pub indexer_url: String,
}

impl Rpc for PlaceholderRpc {
    fn list_live_intentions(
        &self,
        _cid_code_hash: &[u8; 32],
        _hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError> {
        Err(RpcError::Unavailable(format!(
            "v0 placeholder; no live JSON-RPC (configured node {})",
            self.node_url
        )))
    }

    fn get_live_process_by_type_hash(
        &self,
        _process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError> {
        Err(RpcError::Unavailable(format!(
            "v0 placeholder; no live JSON-RPC (configured indexer {})",
            self.indexer_url
        )))
    }

    fn submit_transaction(&self, _tx: &TransactionView) -> Result<Byte32, RpcError> {
        Err(RpcError::Unavailable(
            "v0 placeholder; no live JSON-RPC submit".into(),
        ))
    }
}
