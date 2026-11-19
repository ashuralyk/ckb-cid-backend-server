use std::time::Duration;

use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
use cid_backend_core::process::{ProcessEntry, ProcessResolver};
use cid_backend_core::rpc::{LiveCell, Rpc};
use cid_backend_core::ssri::SsriHost;
use cid_backend_core::type_id::{
    type_id_script, type_id_script_hash, TypeIdIdentity, TYPE_ID_CODE_HASH,
};
use cid_protocol::{CidArgs, Payload, Script as CidScript};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;

pub const CID_CODE: [u8; 32] = [0xC1; 32];

/// Fixed TYPE_ID args for the Process **code** cell (lock on state cell).
pub const PROC_CODE_ARGS: [u8; 32] = [0xCD; 32];

pub const FIXTURE_ELF: &[u8] = &[0x7f, b'E', b'L', b'F', 1];

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

pub fn state_args(unique: u8) -> [u8; 32] {
    [unique; 32]
}

/// Process state TYPE_ID type script (identity = `calc_script_hash` = Intention `process_type_hash`).
pub fn process_type_script(unique: u8) -> Script {
    type_id_script(&state_args(unique))
}

pub fn process_type_hash(unique: u8) -> [u8; 32] {
    type_id_script_hash(&state_args(unique))
}

pub fn fixture_process_entry(unique: u8) -> ProcessEntry {
    fixture_process_entry_with_code(unique, PROC_CODE_ARGS)
}

pub fn fixture_process_entry_with_code(unique: u8, code_args: [u8; 32]) -> ProcessEntry {
    let state = state_args(unique);
    ProcessEntry {
        identity_type_hash: type_id_script_hash(&state),
        identity: TypeIdIdentity::new(state),
        code: TypeIdIdentity::new(code_args),
        ssri_elf_path: None,
    }
}

pub fn default_loop_config() -> LoopConfig {
    LoopConfig {
        cid_code_hash: CID_CODE,
        hash_type: ScriptHashType::Type,
        poll_interval: Duration::from_secs(1),
        extra_cell_deps: vec![],
        type_id_code_deps: vec![],
        confirmation_depth: 1,
        max_assembled_tx_bytes: 512 * 1024,
    }
}

pub fn digest_loop_empty<R: Rpc, S: SsriHost>(
    rpc: R,
    ssri: S,
    config: LoopConfig,
) -> DigestLoop<R, NopClock, S> {
    DigestLoop::new(rpc, NopClock, ssri, config)
}

pub fn register_fixture(loop_: &mut DigestLoop<impl Rpc, impl cid_backend_core::digest_loop::Clock, impl SsriHost>, unique: u8) {
    let entry = fixture_process_entry(unique);
    loop_.register_process(entry);
    if let Some(w) = loop_.workers_mut().get_mut(&fixture_process_entry(unique).identity_type_hash) {
        w.resolver = ProcessResolver::for_entry(fixture_process_entry(unique))
            .with_fallback_binary(FIXTURE_ELF.to_vec());
    }
}

pub fn loop_with_resolver<R: Rpc, S: SsriHost>(
    rpc: R,
    ssri: S,
    config: LoopConfig,
    unique: u8,
) -> DigestLoop<R, NopClock, S> {
    let entry = fixture_process_entry(unique);
    DigestLoop::new_with_resolver(
        rpc,
        NopClock,
        ssri,
        config,
        ProcessResolver::for_entry(entry).with_fallback_binary(FIXTURE_ELF.to_vec()),
    )
}

pub fn loop_with_resolver_and_code<R: Rpc, S: SsriHost>(
    rpc: R,
    ssri: S,
    config: LoopConfig,
    unique: u8,
    code_args: [u8; 32],
) -> DigestLoop<R, NopClock, S> {
    let entry = fixture_process_entry_with_code(unique, code_args);
    DigestLoop::new_with_resolver(
        rpc,
        NopClock,
        ssri,
        config,
        ProcessResolver::for_entry(entry).with_fallback_binary(FIXTURE_ELF.to_vec()),
    )
}

/// Process state cell: Process-as-lock (lock = Process code TYPE_ID type-script hash).
pub fn process_cell(unique: u8, capacity: u64, data: Vec<u8>) -> LiveCell {
    process_cell_for_code(unique, capacity, data, PROC_CODE_ARGS)
}

pub fn process_cell_for_code(
    unique: u8,
    capacity: u64,
    data: Vec<u8>,
    code_args: [u8; 32],
) -> LiveCell {
    let entry = fixture_process_entry_with_code(unique, code_args);
    let lock = Script::new_builder()
        .code_hash(entry.code.type_script_hash().pack())
        .hash_type(ScriptHashType::Type.into())
        .args(Vec::<u8>::new().pack())
        .build();
    let ts = type_id_script(&entry.identity.args);
    live_cell(
        out_point(0x50, unique as u32),
        lock,
        Some(ts),
        capacity,
        data,
    )
}

/// @deprecated — use `process_type_script` / `process_cell_for_code` with registry entry instead.
pub fn process_type_script_for_code(_code_hash: [u8; 32], unique: u8) -> Script {
    process_type_script(unique)
}

pub fn cid_lock_args(process_type_hash: [u8; 32]) -> Vec<u8> {
    CidArgs::new(process_type_hash, user_lock(), Payload { data: vec![0xAA] })
        .encode()
        .unwrap()
}

pub fn intention_cell(fill: u8, process_type_hash: [u8; 32], capacity: u64) -> LiveCell {
    intention_cell_with_lock(
        fill,
        process_type_hash,
        capacity,
        CID_CODE,
        ScriptHashType::Type,
    )
}

pub fn intention_cell_with_lock(
    fill: u8,
    process_type_hash: [u8; 32],
    capacity: u64,
    lock_code: [u8; 32],
    lock_hash_type: ScriptHashType,
) -> LiveCell {
    live_cell(
        out_point(fill, 0),
        lock_script(lock_code, lock_hash_type, cid_lock_args(process_type_hash)),
        None,
        capacity,
        vec![],
    )
}