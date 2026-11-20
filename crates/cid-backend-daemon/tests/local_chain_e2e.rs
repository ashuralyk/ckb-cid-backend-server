//! Local isolated CKB *dev* chain closure (not HTTP-mock, not FakeRpc).
//!
//! Ignored by default. `scripts/local-dev-chain.sh` starts a pinned CKB v0.119.0
//! dev node (min_fee_rate=0, Indexer on, no public network) and runs this test.
//!
//! Flow: deploy always-success + CID lock + Process ELF → Process state cell →
//! Intention → `DigestLoop::tick` (real JSON-RPC + EmbeddedSsri ckb-vm) →
//! wait until the digest and xUDT refund are **committed**.

#![allow(clippy::inconsistent_digit_grouping)]

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use cid_backend_core::digest_loop::{DigestLoop, NopClock};
use cid_backend_core::host_query::HostQueryRpc;
use cid_backend_core::rpc::{script_hash32, Rpc, TxStatus};
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_core::{
    assemble_digest_incompatible, decode_cells, AssembleError, ProcessEntry, RpcError,
    TypeIdCodeDep, TypeIdIdentity,
};
use cid_backend_daemon::{CkbRpc, DaemonConfig};
use cid_protocol::{CidArgs, Payload, Script as CidScript, XUDT_CODE_HASH};
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

fn code_output(lock: &Script, type_script: Option<Script>, data: &[u8]) -> (CellOutput, u64) {
    let cap = occupied(lock, type_script.as_ref(), data) + 200_0000_0000;
    (
        CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(lock.clone())
            .type_(type_script.pack())
            .build(),
        cap,
    )
}

fn local_rpc_address(url: &str) -> Option<SocketAddr> {
    let authority = url.strip_prefix("http://")?;
    let addr: SocketAddr = authority
        .strip_suffix('/')
        .unwrap_or(authority)
        .parse()
        .ok()?;
    (addr.ip().is_loopback() && addr.port() != 0).then_some(addr)
}

fn is_dev_chain(info: &Value) -> bool {
    info["chain"].as_str() == Some("ckb_dev")
}

fn check_committed_execution(
    result: &Value,
    intention: &OutPoint,
    beneficiary: &Script,
    capacity: u64,
) -> Result<(), &'static str> {
    if result["tx_status"]["status"] != "committed" {
        return Err("execution transaction is not committed");
    }
    let tx = &result["transaction"];
    let tx_hash = hex_bytes(intention.tx_hash().as_slice());
    let index: u32 = intention.index().unpack();
    let index = format!("0x{index:x}");
    let inputs = tx["inputs"].as_array().ok_or("missing committed inputs")?;
    if !inputs.iter().any(|input| {
        let previous = &input["previous_output"];
        previous["tx_hash"] == tx_hash && previous["index"] == index
    }) {
        return Err("committed transaction did not consume the original Intention");
    }
    let outputs = tx["outputs"]
        .as_array()
        .ok_or("missing committed outputs")?;
    let data = tx["outputs_data"]
        .as_array()
        .ok_or("missing committed outputs_data")?;
    let capacity = format!("0x{capacity:x}");
    if !outputs.iter().zip(data).any(|(output, data)| {
        output["capacity"] == capacity
            && output["type"].is_null()
            && *data == "0x"
            && json_script(&output["lock"]) == *beneficiary
    }) {
        return Err("committed transaction did not pay the expected beneficiary");
    }
    Ok(())
}

#[test]
fn fixture_targets_only_a_loopback_dev_node() {
    for url in [
        "https://127.0.0.1:8114",
        "http://localhost:8114",
        "http://example.org:8114",
        "http://127.0.0.1:8114@example.org:8114",
        "http://127.0.0.1:8114/proxy",
    ] {
        assert!(local_rpc_address(url).is_none(), "{url}");
    }
    assert!(local_rpc_address("http://127.0.0.1:8114").is_some());
    assert!(local_rpc_address("http://[::1]:8114/").is_some());
    for chain in ["ckb", "ckb_testnet", "ckb_staging"] {
        assert!(!is_dev_chain(&json!({"chain": chain})));
    }
    assert!(!is_dev_chain(&json!({})));
    assert!(is_dev_chain(&json!({"chain": "ckb_dev"})));
}

#[test]
fn committed_execution_requires_the_intention_input_and_beneficiary_output() {
    let op = OutPoint::new_builder()
        .tx_hash([0x42; 32].pack())
        .index(1u32.pack())
        .build();
    let hash = hex_bytes(op.tx_hash().as_slice());
    let beneficiary = secp_lock();
    let mut result = json!({
        "tx_status": {"status": "committed"},
        "transaction": {
            "inputs": [{"previous_output": {"tx_hash": hash, "index": "0x1"}}],
            "outputs": [{
                "capacity": "0x64",
                "type": null,
                "lock": {
                    "code_hash": hex_bytes(beneficiary.code_hash().as_slice()),
                    "hash_type": "type",
                    "args": hex_bytes(&beneficiary.args().raw_data()),
                },
            }],
            "outputs_data": ["0x"],
        },
    });
    assert_eq!(
        check_committed_execution(&result, &op, &beneficiary, 100),
        Ok(())
    );
    assert!(check_committed_execution(&result, &op, &beneficiary, 101).is_err());
    result["transaction"]["inputs"][0]["previous_output"]["index"] = json!("0x0");
    assert!(check_committed_execution(&result, &op, &beneficiary, 100).is_err());
}

#[test]
#[ignore = "local CKB dev chain; run scripts/local-dev-chain.sh"]
fn scan_resolve_ssri_submit_confirms_on_local_dev_chain() {
    if env::var("CID_LOCAL_CHAIN_E2E").ok().as_deref() != Some("1") {
        eprintln!("skip: set CID_LOCAL_CHAIN_E2E=1 (use scripts/local-dev-chain.sh)");
        return;
    }
    let url = env::var("CID_CKB_RPC_URL").expect("CID_CKB_RPC_URL");
    let addr = local_rpc_address(&url).expect("fixture requires a numeric loopback HTTP RPC URL");
    let rpc_url = format!("http://{addr}");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(30))
        .redirects(0)
        .try_proxy_from_env(false)
        .build();
    let rpc = CkbRpc::with_agent(&rpc_url, &rpc_url, agent);
    let info = rpc
        .call_node("get_blockchain_info", json!([]))
        .expect("chain info");
    assert!(is_dev_chain(&info), "fixture requires ckb_dev");
    let process_elf =
        std::fs::read(read_env_path("CID_PROCESS_EXAMPLE_PATH")).expect("process elf");
    let cid_elf = std::fs::read(read_env_path("CID_LOCK_ELF_PATH")).expect("cid elf");
    let always_elf =
        std::fs::read(read_env_path("CID_ALWAYS_SUCCESS_PATH")).expect("always_success");
    assert_eq!(&process_elf[..4], b"\x7fELF");
    assert_eq!(&cid_elf[..4], b"\x7fELF");
    assert_eq!(&always_elf[..4], b"\x7fELF");

    let xudt_elf = std::fs::read(read_env_path("CID_XUDT_ELF_PATH")).expect("xUDT elf");
    assert_eq!(blake2b_256(&xudt_elf), XUDT_CODE_HASH);
    let always_code_hash = blake2b_256(&always_elf);

    let secp_lock = secp_lock();
    let (secp_code_dep, secp_data_dep) = genesis_deps(&rpc);
    let (issued_op, issued_cap) = live_funding_cell(&rpc);

    let funding_input = input(issued_op);
    let cid_code = TypeIdIdentity::new(calc_type_id(&funding_input, 1));
    let cid_code_hash = cid_code.type_script_hash();
    let process_code = TypeIdIdentity::new(calc_type_id(&funding_input, 2));
    let (as_out, as_cap) = code_output(&secp_lock, None, &always_elf);
    let (cid_out, cid_cap) = code_output(&secp_lock, Some(cid_code.type_script()), &cid_elf);
    let (proc_code_out, proc_code_cap) =
        code_output(&secp_lock, Some(process_code.type_script()), &process_elf);
    let (xudt_out, xudt_cap) = code_output(&secp_lock, None, &xudt_elf);
    let spent = as_cap + cid_cap + proc_code_cap + xudt_cap;
    assert!(issued_cap > spent + 1_000_0000_0000, "genesis capacity");
    let change_cap = issued_cap - spent - 1_0000_0000; // 1 CKB fee buffer even with min_fee_rate=0

    let tx1 = TransactionBuilder::default()
        .input(funding_input)
        .output(as_out)
        .output(cid_out)
        .output(proc_code_out)
        .output(xudt_out)
        .output(change_output(&secp_lock, change_cap))
        .output_data(always_elf.pack())
        .output_data(cid_elf.pack())
        .output_data(process_elf.pack())
        .output_data(xudt_elf.pack())
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
    let xudt_dep = cell_dep(
        OutPoint::new_builder()
            .tx_hash(tx1_hash.clone())
            .index(3u32.pack())
            .build(),
    );
    let change_op = OutPoint::new_builder()
        .tx_hash(tx1_hash)
        .index(4u32.pack())
        .build();

    let first_in = input(change_op);
    let process_state = TypeIdIdentity::new(calc_type_id(&first_in, 0));
    let process_type = process_state.type_script();
    let process_type_hash = script_hash32(&process_type);

    let process_lock = Script::new_builder()
        .code_hash(process_code.type_script_hash().pack())
        .hash_type(ScriptHashType::Type.into())
        .args([cid_code_hash.as_slice(), &[1, 0xEE]].concat().pack())
        .build();
    let (proc_out, proc_data_cap) = code_output(&process_lock, Some(process_type), &[]);
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
    let intent_args = CidArgs::new(process_type_hash, user.clone(), payload)
        .encode()
        .unwrap();
    let cid_lock = Script::new_builder()
        .code_hash(cid_code_hash.pack())
        .hash_type(ScriptHashType::Type.into())
        .args(intent_args.pack())
        .build();
    let intent_cap = 600_0000_0000_u64;
    let intent_out = CellOutput::new_builder()
        .capacity(intent_cap.pack())
        .lock(cid_lock)
        .build();
    let malformed_args = CidArgs::new(process_type_hash, user, Payload { data: vec![0xFF] })
        .encode()
        .unwrap();
    let malformed_lock = intent_out
        .lock()
        .as_builder()
        .args(malformed_args.pack())
        .build();
    // The secp funding input authorizes minting this xUDT.
    let xudt_type = Script::new_builder()
        .code_hash(XUDT_CODE_HASH.pack())
        .hash_type(ScriptHashType::Data1.into())
        .args(script_hash32(&secp_lock).as_slice().pack())
        .build();
    let xudt_intention = intent_out
        .clone()
        .as_builder()
        .lock(malformed_lock.clone())
        .type_(Some(xudt_type.clone()).pack())
        .build();
    let xudt_data = [100u128.to_le_bytes().as_slice(), &[0xAA, 0xBB]].concat();
    let untyped_data = [0xCCu8];
    let untyped_intention = intent_out.clone().as_builder().lock(malformed_lock).build();
    let spent2 = proc_data_cap + 3 * intent_cap;
    assert!(change_cap > spent2 + 1_000_0000_0000);
    let change2 = change_cap - spent2 - 1_0000_0000;

    let tx2 = TransactionBuilder::default()
        .input(first_in)
        .output(proc_out)
        .output(intent_out)
        .output(xudt_intention)
        .output(untyped_intention)
        .output(change_output(&secp_lock, change2))
        .output_data(Vec::<u8>::new().pack())
        .output_data(Vec::<u8>::new().pack())
        .output_data(xudt_data.pack())
        .output_data(untyped_data.as_slice().pack())
        .output_data(Vec::<u8>::new().pack())
        .cell_dep(secp_code_dep)
        .cell_dep(secp_data_dep)
        .cell_dep(cell_dep(as_op.clone()))
        .cell_dep(cell_dep(cid_op.clone()))
        .cell_dep(cell_dep(proc_code_op.clone()))
        .cell_dep(xudt_dep.clone())
        .build();
    let tx2 = sign_secp(tx2);
    let tx2_hash = submit_wait(&rpc, tx2);
    let process_op = OutPoint::new_builder()
        .tx_hash(tx2_hash.clone())
        .index(0u32.pack())
        .build();
    let intent_op = OutPoint::new_builder()
        .tx_hash(tx2_hash.clone())
        .index(1u32.pack())
        .build();
    let xudt_op = intent_op.clone().as_builder().index(2u32.pack()).build();
    let untyped_op = intent_op.clone().as_builder().index(3u32.pack()).build();
    let untyped = rpc
        .get_live_cell(&untyped_op)
        .unwrap()
        .expect("untyped fixture input");
    let deps = [cell_dep(cid_op), cell_dep(as_op.clone())];
    assert!(matches!(
        assemble_digest_incompatible(&decode_cells(vec![untyped.clone()]), &deps),
        Err(AssembleError::RefundUnbuildable(_))
    ));
    let user_lock = Script::new_builder()
        .code_hash(always_code_hash.pack())
        .hash_type(ScriptHashType::Data1.into())
        .build();
    // Handcraft the refund the old builder would have emitted and verify CID
    // rejects it at input asset validation (UnsupportedType, exit 29).
    let invalid = TransactionBuilder::default()
        .input(input(untyped_op.clone()))
        .output(untyped.output.as_builder().lock(user_lock.clone()).build())
        .output_data(untyped_data.as_slice().pack())
        .cell_deps(deps)
        .witness(Default::default())
        .build();
    assert!(matches!(
        rpc.dry_run_transaction(&invalid),
        Err(RpcError::SubmitRejected(ref message)) if message.contains("error code 29")
    ));
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
        hash_type: ScriptHashType::Type,
        poll_interval: Duration::from_secs(1),
        extra_cell_deps: vec![cell_dep(as_op), xudt_dep],
        type_id_code_deps: vec![TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: cid_code,
        }],
        process_type_code_hashes: vec![],
        health_bind: None,
        api_bind: None,
        allow_non_loopback_api: false,
        registry_path: std::env::temp_dir().join("cid-local-e2e-registry.json"),
        registry_max_entries: 64,
        confirmation_depth: 1,
        allow_missing_filter: false,
        example_policy_data_hashes: vec![],
        fee_rate: cid_backend_core::DEFAULT_FEE_RATE,
        bounded_refund_locks: vec![],
        extra_scan_locks: vec![],
        network_name: Some("local-dev".into()),
        genesis_hash: None,
        validate_genesis: false,
        log_format: cid_backend_daemon::logging::LogFormat::Json,
    };
    let mut loop_ = DigestLoop::new(rpc, NopClock, EmbeddedSsri, cfg.to_loop_config());
    assert!(loop_.register_process(ProcessEntry {
        identity_type_hash: process_type_hash,
        identity: process_state,
        code: process_code,
        ssri_elf_path: None,
    }));

    let mut digest_hash = None;
    let mut refund_hash = None;
    for i in 0..20 {
        let report = loop_.tick();
        eprintln!(
            "tick {i}: scanned={} submitted={} skipped={}",
            report.scanned, report.submitted, report.skipped
        );
        assert_eq!(report.last_error, None);
        if let Some(state) = loop_.state().submit {
            for (hash, pending) in state.pending {
                if pending
                    .consumed
                    .iter()
                    .any(|op| op.as_slice() == intent_op.as_slice())
                {
                    digest_hash = Some(Byte32::from_slice(&hash).unwrap());
                }
                if pending
                    .consumed
                    .iter()
                    .any(|op| op.as_slice() == xudt_op.as_slice())
                {
                    refund_hash = Some(Byte32::from_slice(&hash).unwrap());
                }
            }
        }
        if digest_hash.is_some() && refund_hash.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }
    let hash = digest_hash.expect("daemon did not submit an execution tx");
    let refund_hash = refund_hash.expect("daemon did not submit an xUDT refund");
    assert!(matches!(
        wait_committed(loop_.rpc(), &refund_hash, Duration::from_secs(60)),
        TxStatus::Committed { .. }
    ));
    let refund_op = OutPoint::new_builder().tx_hash(refund_hash).build();
    let refund = loop_
        .rpc()
        .get_live_cell(&refund_op)
        .unwrap()
        .expect("xUDT payout");
    assert_eq!(refund.lock(), user_lock);
    assert_eq!(refund.capacity(), intent_cap);
    assert_eq!(refund.type_script(), Some(xudt_type));
    assert_eq!(refund.data, xudt_data);
    assert!(loop_.rpc().get_live_cell(&xudt_op).unwrap().is_none());
    assert!(loop_.rpc().get_live_cell(&untyped_op).unwrap().is_some());

    let st = wait_committed(loop_.rpc(), &hash, Duration::from_secs(60));
    assert!(matches!(st, TxStatus::Committed { .. }));
    eprintln!(
        "local-chain execution tx committed: {}",
        hex_bytes(hash.as_slice())
    );

    let committed = loop_
        .rpc()
        .call_node("get_transaction", json!([hex_bytes(hash.as_slice())]))
        .expect("committed transaction");
    let beneficiary = Script::new_builder()
        .code_hash(always_code_hash.pack())
        .hash_type(ScriptHashType::Data1.into())
        .args([0xEEu8].pack())
        .build();
    check_committed_execution(&committed, &intent_op, &beneficiary, intent_cap)
        .expect("execution matches the intended spend");
    assert!(loop_
        .rpc()
        .get_live_cell(&intent_op)
        .expect("Intention liveness")
        .is_none());
    thread::sleep(Duration::from_secs(1));
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
