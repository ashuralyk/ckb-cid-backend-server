use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use cid_backend_core::rpc::LiveCell;
use cid_protocol::{CidArgs, Payload, Script as CidScript};

pub const CID_CODE: [u8; 32] = [0xC1; 32];

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

pub fn user_lock() -> CidScript {
    CidScript {
        code_hash: [0x42; 32],
        hash_type: 1,
        args: vec![0x01, 0x02],
    }
}

pub fn process_type_script(unique: u8) -> Script {
    lock_script([0x51; 32], ScriptHashType::Type, vec![unique; 32])
}

pub fn cid_lock_args(process_type_hash: [u8; 32]) -> Vec<u8> {
    CidArgs::new(
        process_type_hash,
        user_lock(),
        Payload { data: vec![0xAA] },
    )
    .encode()
    .unwrap()
}

pub fn intention_cell(fill: u8, process_type_hash: [u8; 32], capacity: u64) -> LiveCell {
    live_cell(
        out_point(fill, 0),
        lock_script(
            CID_CODE,
            ScriptHashType::Type,
            cid_lock_args(process_type_hash),
        ),
        None,
        capacity,
        vec![],
    )
}

pub fn process_cell(unique: u8, capacity: u64, data: Vec<u8>) -> LiveCell {
    live_cell(
        out_point(0x50, unique as u32),
        lock_script([0u8; 32], ScriptHashType::Type, vec![]),
        Some(process_type_script(unique)),
        capacity,
        data,
    )
}
