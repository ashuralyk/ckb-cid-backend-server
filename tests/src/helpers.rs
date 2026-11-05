use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use cid_backend_core::rpc::LiveCell;

pub fn out_point(tx_fill: u8, index: u32) -> OutPoint {
    OutPoint::new_builder()
        .tx_hash(Byte32::from_slice(&[tx_fill; 32]).expect("32 bytes"))
        .index(index.pack())
        .build()
}

pub fn lock_script(code_hash: [u8; 32], hash_type: ScriptHashType, args: Vec<u8>) -> Script {
    Script::new_builder()
        .code_hash(code_hash.pack())
        .hash_type(hash_type.into())
        .args(args.pack())
        .build()
}

pub fn live_cell(
    op: OutPoint,
    lock: Script,
    type_script: Option<Script>,
    capacity: u64,
    data: Vec<u8>,
) -> LiveCell {
    let mut output = CellOutput::new_builder()
        .capacity(capacity.pack())
        .lock(lock);
    if let Some(ts) = type_script {
        output = output.type_(Some(ts).pack());
    }
    LiveCell {
        out_point: op,
        output: output.build(),
        data,
    }
}
