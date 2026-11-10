//! Local isolated CKB *dev* chain closure (not HTTP-mock, not FakeRpc).
//!
//! Ignored by default. `scripts/local-dev-chain.sh` starts a pinned CKB v0.119.0
//! dev node (min_fee_rate=0, Indexer on, no public network) and runs this test.
//!
//! Flow: deploy always-success + CID lock + Process ELF → Process state cell →
//! Intention → `DigestLoop::tick` (real JSON-RPC + EmbeddedSsri ckb-vm) →
//! wait until the execution tx is **committed**.

use std::env;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use cid_backend_core::digest_loop::{DigestLoop, NopClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::rpc::{script_hash32, Rpc, TxStatus};
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_daemon::{CkbRpc, DaemonConfig};
use cid_protocol::{CidArgs, Payload, Script as CidScript};
use ckb_hash::{blake2b_256, new_blake2b};
use ckb_types::bytes::Bytes;
use ckb_types::core::{Capacity, DepType, ScriptHashType, TransactionBuilder, TransactionView};
use ckb_types::packed::{Byte32, CellDep, CellInput, CellOutput, OutPoint, Script, WitnessArgs};
use ckb_types::prelude::*;
use secp256k1::{Message, Secp256k1, SecretKey};
use serde_json::{json, Value};

const SECP_TYPE_HASH: [u8; 32] =
    hex32("9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
const ISSUED_ARGS: [u8; 20] = hex20("c8328aabcd9b9e8e64fbc566c4385c3bdeb219d7");
const ISSUED_PRIV: [u8; 32] =
    hex32("d00c06bfd800d27397002dca6fb0993d5ba6399b4238b2f29ee9deb97593d2bc");

const fn hex32(s: &str) -> [u8; 32] {
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = hex_byte(b[i * 2], b[i * 2 + 1]);
        i += 1;
    }
    out
}

const fn hex20(s: &str) -> [u8; 20] {
    let b = s.as_bytes();
    let mut out = [0u8; 20];
    let mut i = 0;
    while i < 20 {
        out[i] = hex_byte(b[i * 2], b[i * 2 + 1]);
        i += 1;
    }
    out
}

const fn hex_byte(a: u8, b: u8) -> u8 {
    (hex_nibble(a) << 4) | hex_nibble(b)
}

const fn hex_nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn decode_hex(s: &str) -> Vec<u8> {
    let t = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if t.is_empty() {
        Vec::new()
    } else {
        hex::decode(t).expect("hex")
    }
}

fn read_env_path(key: &str) -> PathBuf {
    PathBuf::from(env::var(key).unwrap_or_else(|_| panic!("missing {key}")))
}

fn secp_lock() -> Script {
    Script::new_builder()
        .code_hash(SECP_TYPE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(ISSUED_ARGS.to_vec().pack())
        .build()
}

fn data1_script(code_hash: [u8; 32], args: Vec<u8>) -> Script {
    Script::new_builder()
        .code_hash(code_hash.pack())
        .hash_type(ScriptHashType::Data1.into())
        .args(args.pack())
        .build()
}

fn json_script(v: &Value) -> Script {
    let code = decode_hex(v["code_hash"].as_str().unwrap());
    let ht = match v["hash_type"].as_str().unwrap() {
        "data" => ScriptHashType::Data,
        "type" => ScriptHashType::Type,
        "data1" => ScriptHashType::Data1,
        "data2" => ScriptHashType::Data2,
        other => panic!("hash_type {other}"),
    };
    let args = decode_hex(v["args"].as_str().unwrap_or("0x"));
    Script::new_builder()
        .code_hash(Byte32::from_slice(&code).unwrap())
        .hash_type(ht.into())
        .args(args.pack())
        .build()
}

fn occupied(lock: &Script, type_script: Option<&Script>, data: &[u8]) -> u64 {
    let mut b = CellOutput::new_builder().lock(lock.clone());
    if let Some(ts) = type_script {
        b = b.type_(Some(ts.clone()).pack());
    }
    b.build()
        .occupied_capacity(Capacity::bytes(data.len()).expect("data cap"))
        .expect("occupied")
        .as_u64()
}

fn cell_dep(op: OutPoint) -> CellDep {
    CellDep::new_builder()
        .out_point(op)
        .dep_type(DepType::Code.into())
        .build()
}

fn input(op: OutPoint) -> CellInput {
    CellInput::new_builder()
        .previous_output(op)
        .since(0u64.pack())
        .build()
}

fn placeholder_witness() -> WitnessArgs {
    WitnessArgs::new_builder()
        .lock(Some(Bytes::from(vec![0u8; 65])).pack())
        .build()
}

fn sign_secp(tx: TransactionView) -> TransactionView {
    let secp = Secp256k1::new();
    let sk = SecretKey::from_slice(&ISSUED_PRIV).expect("key");
    let wa = placeholder_witness();
    let wit_bytes = wa.as_bytes();
    let tx = tx
        .as_advanced_builder()
        .set_witnesses(vec![wit_bytes.pack()])
        .build();
    let mut hasher = new_blake2b();
    hasher.update(tx.hash().as_slice());
    hasher.update(&(wit_bytes.len() as u64).to_le_bytes());
    hasher.update(&wit_bytes);
    let mut message = [0u8; 32];
    hasher.finalize(&mut message);
    let sig = secp.sign_ecdsa_recoverable(&Message::from_digest(message), &sk);
    let (recid, compact) = sig.serialize_compact();
    let mut sig65 = [0u8; 65];
    sig65[..64].copy_from_slice(&compact);
    sig65[64] = recid.to_i32() as u8;
    let signed = WitnessArgs::new_builder()
        .lock(Some(Bytes::from(sig65.to_vec())).pack())
        .build();
    tx.as_advanced_builder()
        .set_witnesses(vec![signed.as_bytes().pack()])
        .build()
}

fn calc_type_id(first_input: &CellInput, output_index: u64) -> [u8; 32] {
    let mut hasher = new_blake2b();
    hasher.update(first_input.as_slice());
    hasher.update(&output_index.to_le_bytes());
    let mut out = [0u8; 32];
    hasher.finalize(&mut out);
    out
}

fn wait_committed(rpc: &CkbRpc, hash: &Byte32, timeout: Duration) -> TxStatus {
    let start = Instant::now();
    loop {
        match rpc.get_transaction_status(hash) {
            Ok(st @ TxStatus::Committed { .. }) => return st,
            Ok(st) if start.elapsed() > timeout => return st,
            Ok(_) => thread::sleep(Duration::from_millis(400)),
            Err(_) if start.elapsed() > timeout => return TxStatus::Dropped,
            Err(_) => thread::sleep(Duration::from_millis(400)),
        }
    }
}

fn submit_wait(rpc: &CkbRpc, tx: TransactionView) -> Byte32 {
    let hash = rpc
        .submit_transaction(&tx)
        .unwrap_or_else(|e| panic!("submit {}: {e}", hex_bytes(tx.hash().as_slice())));
    let st = wait_committed(rpc, &hash, Duration::from_secs(60));
    assert!(
        matches!(st, TxStatus::Committed { .. }),
        "tx {} not committed ({st:?})",
        hex_bytes(hash.as_slice())
    );
    hash
}

fn genesis_deps(rpc: &CkbRpc) -> (CellDep, CellDep) {
    let hash = rpc
        .call_node("get_block_hash", json!(["0x0"]))
        .expect("genesis hash");
    let block = rpc
        .call_node("get_block", json!([hash]))
        .expect("genesis block");
    let mut secp_code: Option<OutPoint> = None;
    let mut secp_data: Option<(OutPoint, usize)> = None;
    for tx in block["transactions"].as_array().unwrap() {
        let tx_hash = decode_hex(tx["hash"].as_str().unwrap());
        let txh = Byte32::from_slice(&tx_hash).unwrap();
        let outputs = tx["outputs"].as_array().unwrap();
        let datas = tx["outputs_data"].as_array().unwrap();
        for (i, out) in outputs.iter().enumerate() {
            let op = OutPoint::new_builder()
                .tx_hash(txh.clone())
                .index((i as u32).pack())
                .build();
            if let Some(ts) = out.get("type") {
                if !ts.is_null() {
                    let type_s = json_script(ts);
                    if script_hash32(&type_s) == SECP_TYPE_HASH {
                        secp_code = Some(op.clone());
                    }
                }
            }
            let data_len = decode_hex(datas[i].as_str().unwrap_or("0x")).len();
            if data_len > secp_data.as_ref().map(|(_, n)| *n).unwrap_or(0) {
                secp_data = Some((op, data_len));
            }
        }
    }
    (
        cell_dep(secp_code.expect("secp code cell")),
        cell_dep(secp_data.expect("secp data cell").0),
    )
}

fn live_funding_cell(rpc: &CkbRpc) -> (OutPoint, u64) {
    let key = json!({
        "script": {
            "code_hash": hex_bytes(&SECP_TYPE_HASH),
            "hash_type": "type",
            "args": hex_bytes(&ISSUED_ARGS),
        },
        "script_type": "lock",
        "script_search_mode": "exact",
        "with_data": false,
    });
    let result = rpc
        .call_node("get_cells", json!([key, "asc", "0x100", Value::Null]))
        .expect("get_cells funding");
    let mut best: Option<(OutPoint, u64)> = None;
    for obj in result["objects"].as_array().unwrap() {
        let cap = u64::from_str_radix(
            obj["output"]["capacity"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
            16,
        )
        .unwrap();
        if best.as_ref().map(|(_, c)| cap > *c).unwrap_or(true) {
            let tx_hash = decode_hex(obj["out_point"]["tx_hash"].as_str().unwrap());
            let index = u32::from_str_radix(
                obj["out_point"]["index"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            let op = OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&tx_hash).unwrap())
                .index(index.pack())
                .build();
            best = Some((op, cap));
        }
    }
    best.expect("no live secp funding cell")
}

fn change_output(lock: &Script, cap: u64) -> CellOutput {
    CellOutput::new_builder()
        .capacity(cap.pack())
        .lock(lock.clone())
        .build()
}

fn code_output(lock: &Script, data: &[u8]) -> (CellOutput, u64) {
    let cap = occupied(lock, None, data) + 200_0000_0000;
    (
        CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(lock.clone())
            .build(),
        cap,
    )
}

#[test]
#[ignore = "local CKB dev chain; run scripts/local-dev-chain.sh"]
fn scan_resolve_ssri_submit_confirms_on_local_dev_chain() {
    if env::var("CID_LOCAL_CHAIN_E2E").ok().as_deref() != Some("1") {
        eprintln!("skip: set CID_LOCAL_CHAIN_E2E=1 (use scripts/local-dev-chain.sh)");
        return;
    }
    let rpc_url = env::var("CID_CKB_RPC_URL").expect("CID_CKB_RPC_URL");
    let process_elf =
        std::fs::read(read_env_path("CID_PROCESS_EXAMPLE_PATH")).expect("process elf");
    let cid_elf = std::fs::read(read_env_path("CID_LOCK_ELF_PATH")).expect("cid elf");
    let always_elf =
        std::fs::read(read_env_path("CID_ALWAYS_SUCCESS_PATH")).expect("always_success");
    assert_eq!(&process_elf[..4], b"\x7fELF");
    assert_eq!(&cid_elf[..4], b"\x7fELF");
    assert_eq!(&always_elf[..4], b"\x7fELF");

    let process_code_hash = blake2b_256(&process_elf);
    let cid_code_hash = blake2b_256(&cid_elf);
    let always_code_hash = blake2b_256(&always_elf);

    let rpc = CkbRpc::with_process_code_hashes(&rpc_url, &rpc_url, vec![process_code_hash]);
    let secp_lock = secp_lock();
    let (secp_code_dep, secp_data_dep) = genesis_deps(&rpc);
    let (issued_op, issued_cap) = live_funding_cell(&rpc);

    let (as_out, as_cap) = code_output(&secp_lock, &always_elf);
    let (cid_out, cid_cap) = code_output(&secp_lock, &cid_elf);
    let (proc_code_out, proc_code_cap) = code_output(&secp_lock, &process_elf);
    let spent = as_cap + cid_cap + proc_code_cap;
    assert!(issued_cap > spent + 1_000_0000_0000, "genesis capacity");
    let change_cap = issued_cap - spent - 1_0000_0000; // 1 CKB fee buffer even with min_fee_rate=0

    let tx1 = TransactionBuilder::default()
        .input(input(issued_op.clone()))
        .output(as_out)
        .output(cid_out)
        .output(proc_code_out)
        .output(change_output(&secp_lock, change_cap))
        .output_data(always_elf.pack())
        .output_data(cid_elf.pack())
        .output_data(process_elf.pack())
        .output_data(Vec::<u8>::new().pack())
        .cell_dep(secp_code_dep.clone())
        .cell_dep(secp_data_dep.clone())
        .build();
    let tx1 = sign_secp(tx1);
    let tx1_hash = submit_wait(&rpc, tx1);
    let as_op = OutPoint::new_builder()
        .tx_hash(tx1_hash.clone())
        .index(0u32.pack())
        .build();
    let cid_op = OutPoint::new_builder()
        .tx_hash(tx1_hash.clone())
        .index(1u32.pack())
        .build();
    let proc_code_op = OutPoint::new_builder()
        .tx_hash(tx1_hash.clone())
        .index(2u32.pack())
        .build();
    let change_op = OutPoint::new_builder()
        .tx_hash(tx1_hash.clone())
        .index(3u32.pack())
        .build();

    let always_lock = data1_script(always_code_hash, vec![]);
    let first_in = input(change_op.clone());
    let type_id = calc_type_id(&first_in, 0);
    let process_type = data1_script(process_code_hash, type_id.to_vec());
    let process_type_hash = script_hash32(&process_type);

    let proc_data_cap = occupied(&always_lock, Some(&process_type), &process_elf) + 200_0000_0000;
    let proc_out = {
        let mut b = CellOutput::new_builder()
            .capacity(proc_data_cap.pack())
            .lock(always_lock.clone());
        b = b.type_(Some(process_type.clone()).pack());
        b.build()
    };
    let payload = Payload::from_script(&CidScript {
        code_hash: always_code_hash,
        hash_type: 2,
        args: vec![0xEE],
    })
    .unwrap();
    let user = CidScript {
        code_hash: always_code_hash,
        hash_type: 2,
        args: vec![],
    };
    let intent_args = CidArgs::new(process_type_hash, user, payload)
        .encode()
        .unwrap();
    let cid_lock = data1_script(cid_code_hash, intent_args);
    let intent_cap = occupied(&cid_lock, None, &[]) + 50_0000_0000;
    let intent_out = CellOutput::new_builder()
        .capacity(intent_cap.pack())
        .lock(cid_lock)
        .build();
    let spent2 = proc_data_cap + intent_cap;
    assert!(change_cap > spent2 + 1_000_0000_0000);
    let change2 = change_cap - spent2 - 1_0000_0000;

    let tx2 = TransactionBuilder::default()
        .input(first_in)
        .output(proc_out)
        .output(intent_out)
        .output(change_output(&secp_lock, change2))
        .output_data(process_elf.pack())
        .output_data(Vec::<u8>::new().pack())
        .output_data(Vec::<u8>::new().pack())
        .cell_dep(secp_code_dep)
        .cell_dep(secp_data_dep)
        .cell_dep(cell_dep(as_op.clone()))
        .cell_dep(cell_dep(cid_op.clone()))
        .cell_dep(cell_dep(proc_code_op.clone()))
        .build();
    let tx2 = sign_secp(tx2);
    let tx2_hash = submit_wait(&rpc, tx2);
    let process_op = OutPoint::new_builder()
        .tx_hash(tx2_hash.clone())
        .index(0u32.pack())
        .build();
    let _intent_op = OutPoint::new_builder()
        .tx_hash(tx2_hash.clone())
        .index(1u32.pack())
        .build();
    eprintln!(
        "local-chain deployed process={} intention={} type_hash={}",
        hex_bytes(process_op.tx_hash().as_slice()),
        hex_bytes(tx2_hash.as_slice()),
        hex_bytes(&process_type_hash)
    );

    thread::sleep(Duration::from_secs(1));
    let cfg = DaemonConfig {
        node_url: rpc_url.clone(),
        indexer_url: rpc_url.clone(),
        cid_lock_code_hash: cid_code_hash,
        hash_type: ScriptHashType::Data1,
        poll_interval: Duration::from_secs(1),
        extra_cell_deps: vec![cell_dep(cid_op), cell_dep(proc_code_op), cell_dep(as_op)],
        process_type_code_hashes: vec![process_code_hash],
        health_bind: None,
        confirmation_depth: 1,
    };
    let live_rpc = CkbRpc::from_config(&cfg);
    let mut loop_ = DigestLoop::new(
        live_rpc,
        NopClock,
        EmbeddedSsri,
        cfg.to_loop_config(),
        ProcessResolver::new(),
    );

    let mut submitted_hash: Option<Byte32> = None;
    for i in 0..20 {
        let report = loop_.tick();
        eprintln!(
            "tick {i}: scanned={} submitted={} skipped={}",
            report.scanned, report.submitted, report.skipped
        );
        if report.submitted > 0 {
            let pending = loop_.state().submit.pending.keys().next().cloned();
            submitted_hash = pending.map(|b| Byte32::from_slice(&b).unwrap());
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }
    let hash = submitted_hash.expect("daemon did not submit an execution tx");
    let st = wait_committed(loop_.rpc(), &hash, Duration::from_secs(60));
    assert!(matches!(st, TxStatus::Committed { .. }));
    eprintln!(
        "local-chain execution tx committed: {}",
        hex_bytes(hash.as_slice())
    );

    thread::sleep(Duration::from_secs(1));
    let intentions = loop_
        .rpc()
        .list_live_intentions(&cid_code_hash, ScriptHashType::Data1)
        .expect("rescan");
    assert!(
        intentions.is_empty(),
        "intention still live after committed digest_ok: {}",
        intentions.len()
    );
    let proc = loop_
        .rpc()
        .get_live_process_by_type_hash(&process_type_hash)
        .expect("process")
        .expect("process recreated");
    assert_ne!(proc.out_point, process_op, "process cell must be recreated");
    eprintln!(
        "local-chain closure ok: execution={} process_recreate={}",
        hex_bytes(hash.as_slice()),
        hex_bytes(proc.out_point.tx_hash().as_slice())
    );
}
