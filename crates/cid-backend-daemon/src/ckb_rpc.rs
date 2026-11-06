//! Live CKB node + indexer JSON-RPC adapter (`Rpc`).
//!
//! HTTP only. No disk state: every scan is a fresh `get_cells`. Process cells
//! are found by type-script prefix (`CID_PROCESS_CODE_HASH`) then filtered to
//! `calc_script_hash == process_type_hash` from CID args.

use crate::config::DaemonConfig;
use cid_backend_core::rpc::{script_hash32, LiveCell, Rpc, RpcError, TxStatus};
use ckb_types::core::{DepType, ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, CellInput, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use serde_json::{json, Value};
use std::time::Duration;

const PAGE: &str = "0x64";
const MAX_PAGES: usize = 64;

const HASH_TYPES: [ScriptHashType; 4] = [
    ScriptHashType::Type,
    ScriptHashType::Data,
    ScriptHashType::Data1,
    ScriptHashType::Data2,
];

pub struct CkbRpc {
    node_url: String,
    indexer_url: String,
    process_type_code_hashes: Vec<[u8; 32]>,
    agent: ureq::Agent,
}

impl CkbRpc {
    pub fn new(node_url: impl Into<String>, indexer_url: impl Into<String>) -> Self {
        Self::with_process_code_hashes(node_url, indexer_url, Vec::new())
    }

    pub fn with_process_code_hashes(
        node_url: impl Into<String>,
        indexer_url: impl Into<String>,
        process_type_code_hashes: Vec<[u8; 32]>,
    ) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        Self {
            node_url: node_url.into(),
            indexer_url: indexer_url.into(),
            process_type_code_hashes,
            agent,
        }
    }

    pub fn from_config(cfg: &DaemonConfig) -> Self {
        Self::with_process_code_hashes(
            cfg.node_url.clone(),
            cfg.indexer_url.clone(),
            cfg.process_type_code_hashes.clone(),
        )
    }

    /// Node JSON-RPC helper for local-chain bootstrap / confirmation polls.
    pub fn call_node(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        self.rpc_call(&self.node_url, method, params)
    }

    fn rpc_call(&self, url: &str, method: &str, params: Value) -> Result<Value, RpcError> {
        let body = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let resp = self
            .agent
            .post(url)
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| RpcError::Unavailable(format!("{method}: {e}")))?;
        let v: Value = resp
            .into_json()
            .map_err(|e| RpcError::Unavailable(format!("{method} decode: {e}")))?;
        if let Some(err) = v.get("error") {
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("rpc error");
            return Err(map_rpc_error(method, msg));
        }
        match v.get("result") {
            Some(r) => Ok(r.clone()),
            None => Err(RpcError::Unavailable(format!("{method}: missing result"))),
        }
    }

    fn get_cells(&self, search_key: &Value) -> Result<Vec<LiveCell>, RpcError> {
        let mut after = Value::Null;
        let mut out = Vec::new();
        for _ in 0..MAX_PAGES {
            let result = self.rpc_call(
                &self.indexer_url,
                "get_cells",
                json!([search_key, "asc", PAGE, after]),
            )?;
            let objects = result
                .get("objects")
                .and_then(|o| o.as_array())
                .ok_or_else(|| RpcError::Unavailable("get_cells: missing objects".into()))?;
            for obj in objects {
                out.push(live_cell_from_indexer(obj)?);
            }
            let cursor = result
                .get("last_cursor")
                .and_then(|c| c.as_str())
                .unwrap_or("");
            if objects.len() < 100 || cursor.is_empty() {
                break;
            }
            after = Value::String(cursor.to_string());
        }
        Ok(out)
    }

    fn cells_by_type_prefix(
        &self,
        code_hash: &[u8; 32],
        hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError> {
        let key = json!({
            "script": json_script(code_hash, hash_type, &[]),
            "script_type": "type",
            "script_search_mode": "prefix",
            "with_data": true,
        });
        self.get_cells(&key)
    }
}

impl Rpc for CkbRpc {
    fn list_live_intentions(
        &self,
        cid_code_hash: &[u8; 32],
        hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError> {
        let key = json!({
            "script": json_script(cid_code_hash, hash_type, &[]),
            "script_type": "lock",
            "script_search_mode": "prefix",
            "with_data": true,
        });
        self.get_cells(&key)
    }

    fn get_live_process_by_type_hash(
        &self,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError> {
        let mut candidates = Vec::new();
        let mut prefixes: Vec<[u8; 32]> = vec![*process_type_hash];
        prefixes.extend_from_slice(&self.process_type_code_hashes);
        for code_hash in prefixes {
            for ht in HASH_TYPES {
                candidates.extend(self.cells_by_type_prefix(&code_hash, ht)?);
            }
        }
        Ok(candidates.into_iter().find(|cell| {
            cell.type_script()
                .map(|ts| &script_hash32(&ts) == process_type_hash)
                .unwrap_or(false)
        }))
    }

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "send_transaction",
            json!([tx_view_to_json(tx), "passthrough"]),
        )?;
        let hex = result
            .as_str()
            .ok_or_else(|| RpcError::SubmitRejected("send_transaction: result not hex".into()))?;
        let bytes = decode_hex(hex)
            .map_err(|e| RpcError::SubmitRejected(format!("send_transaction hash: {e}")))?;
        Byte32::from_slice(&bytes)
            .map_err(|_| RpcError::SubmitRejected("send_transaction hash not 32 bytes".into()))
    }

    fn get_transaction_status(&self, tx_hash: &Byte32) -> Result<TxStatus, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "get_transaction",
            json!([hex_bytes(tx_hash.as_slice())]),
        )?;
        if result.is_null() {
            return Ok(TxStatus::Dropped);
        }
        let status = result
            .pointer("/tx_status/status")
            .and_then(|s| s.as_str())
            .or_else(|| result.pointer("/status").and_then(|s| s.as_str()))
            .unwrap_or("unknown");
        Ok(match status {
            "pending" | "proposed" => TxStatus::Pending,
            "committed" => TxStatus::Committed,
            _ => TxStatus::Dropped,
        })
    }
}

fn map_rpc_error(method: &str, msg: &str) -> RpcError {
    if method == "send_transaction" {
        if is_conflict(msg) {
            RpcError::Conflict(msg.into())
        } else {
            RpcError::SubmitRejected(msg.into())
        }
    } else {
        RpcError::Unavailable(msg.into())
    }
}

fn is_conflict(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    const NEEDLES: &[&str] = &[
        "unknowninput",
        "unknown input",
        "unknowninputs",
        "deadcell",
        "dead cell",
        "duplicat",
        "conflict",
        "unresolvable",
        "failedtoresolve",
        "already been consumed",
        "outpoint",
        "is not live",
    ];
    let compact: String = m.chars().filter(|c| !c.is_whitespace()).collect();
    NEEDLES
        .iter()
        .any(|n| m.contains(n) || compact.contains(&n.replace(' ', "")))
}

fn json_script(code_hash: &[u8; 32], hash_type: ScriptHashType, args: &[u8]) -> Value {
    json!({
        "code_hash": hex_bytes(code_hash),
        "hash_type": hash_type_str(hash_type),
        "args": hex_bytes(args),
    })
}

fn hash_type_str(ht: ScriptHashType) -> &'static str {
    match ht {
        ScriptHashType::Data => "data",
        ScriptHashType::Type => "type",
        ScriptHashType::Data1 => "data1",
        ScriptHashType::Data2 => "data2",
    }
}

fn parse_hash_type(s: &str) -> Result<ScriptHashType, RpcError> {
    match s {
        "data" => Ok(ScriptHashType::Data),
        "type" => Ok(ScriptHashType::Type),
        "data1" => Ok(ScriptHashType::Data1),
        "data2" => Ok(ScriptHashType::Data2),
        other => Err(RpcError::Unavailable(format!("hash_type {other}"))),
    }
}

fn live_cell_from_indexer(obj: &Value) -> Result<LiveCell, RpcError> {
    let op = obj
        .get("out_point")
        .ok_or_else(|| RpcError::Unavailable("indexer cell missing out_point".into()))?;
    let output = obj
        .get("output")
        .ok_or_else(|| RpcError::Unavailable("indexer cell missing output".into()))?;
    let data = match obj.get("output_data") {
        Some(Value::String(s)) => decode_hex(s)?,
        _ => Vec::new(),
    };
    Ok(LiveCell {
        out_point: json_to_out_point(op)?,
        output: json_to_output(output)?,
        data,
    })
}

fn json_to_out_point(v: &Value) -> Result<OutPoint, RpcError> {
    let hash = decode_hex(
        v.get("tx_hash")
            .and_then(|h| h.as_str())
            .ok_or_else(|| RpcError::Unavailable("out_point.tx_hash".into()))?,
    )?;
    let index = parse_uint(
        v.get("index")
            .ok_or_else(|| RpcError::Unavailable("out_point.index".into()))?,
    )?;
    Ok(OutPoint::new_builder()
        .tx_hash(Byte32::from_slice(&hash).map_err(|_| RpcError::Unavailable("tx_hash".into()))?)
        .index((index as u32).pack())
        .build())
}

fn json_to_output(v: &Value) -> Result<CellOutput, RpcError> {
    let cap = parse_uint(
        v.get("capacity")
            .ok_or_else(|| RpcError::Unavailable("capacity".into()))?,
    )?;
    let lock = json_to_script(
        v.get("lock")
            .ok_or_else(|| RpcError::Unavailable("lock".into()))?,
    )?;
    let mut b = CellOutput::new_builder().capacity(cap.pack()).lock(lock);
    if let Some(ts) = v.get("type") {
        if !ts.is_null() {
            b = b.type_(Some(json_to_script(ts)?).pack());
        }
    }
    Ok(b.build())
}

fn json_to_script(v: &Value) -> Result<Script, RpcError> {
    let code = decode_hex(
        v.get("code_hash")
            .and_then(|h| h.as_str())
            .ok_or_else(|| RpcError::Unavailable("script.code_hash".into()))?,
    )?;
    let ht = parse_hash_type(
        v.get("hash_type")
            .and_then(|h| h.as_str())
            .ok_or_else(|| RpcError::Unavailable("script.hash_type".into()))?,
    )?;
    let args = decode_hex(v.get("args").and_then(|h| h.as_str()).unwrap_or("0x"))?;
    Ok(Script::new_builder()
        .code_hash(
            Byte32::from_slice(&code).map_err(|_| RpcError::Unavailable("code_hash".into()))?,
        )
        .hash_type(ht.into())
        .args(args.pack())
        .build())
}

fn tx_view_to_json(tx: &TransactionView) -> Value {
    let cell_deps: Vec<Value> = tx
        .cell_deps()
        .into_iter()
        .map(|d| {
            let dt: DepType = d.dep_type().try_into().unwrap_or(DepType::Code);
            json!({
                "out_point": out_point_json(&d.out_point()),
                "dep_type": match dt {
                    DepType::Code => "code",
                    DepType::DepGroup => "dep_group",
                },
            })
        })
        .collect();
    let header_deps: Vec<Value> = tx
        .header_deps()
        .into_iter()
        .map(|h| Value::String(hex_bytes(h.as_slice())))
        .collect();
    let inputs: Vec<Value> = tx
        .inputs()
        .into_iter()
        .map(|i: CellInput| {
            let since: u64 = i.since().unpack();
            json!({
                "previous_output": out_point_json(&i.previous_output()),
                "since": hex_u64(since),
            })
        })
        .collect();
    let outputs: Vec<Value> = tx.outputs().into_iter().map(|o| output_json(&o)).collect();
    let outputs_data: Vec<Value> = tx
        .outputs_data()
        .into_iter()
        .map(|d| {
            let b: Vec<u8> = d.unpack();
            Value::String(hex_bytes(&b))
        })
        .collect();
    let witnesses: Vec<Value> = tx
        .witnesses()
        .into_iter()
        .map(|w| {
            let b: Vec<u8> = w.unpack();
            Value::String(hex_bytes(&b))
        })
        .collect();
    json!({
        "version": hex_u32(tx.version()),
        "cell_deps": cell_deps,
        "header_deps": header_deps,
        "inputs": inputs,
        "outputs": outputs,
        "outputs_data": outputs_data,
        "witnesses": witnesses,
    })
}

fn out_point_json(op: &OutPoint) -> Value {
    let index: u32 = op.index().unpack();
    json!({
        "tx_hash": hex_bytes(op.tx_hash().as_slice()),
        "index": hex_u32(index),
    })
}

fn output_json(out: &CellOutput) -> Value {
    let cap: u64 = out.capacity().unpack();
    json!({
        "capacity": hex_u64(cap),
        "lock": script_json(&out.lock()),
        "type": out.type_().to_opt().map(|t| script_json(&t)),
    })
}

fn script_json(s: &Script) -> Value {
    let ht: ScriptHashType = s.hash_type().try_into().unwrap_or(ScriptHashType::Type);
    let args: Vec<u8> = s.args().unpack();
    json!({
        "code_hash": hex_bytes(s.code_hash().as_slice()),
        "hash_type": hash_type_str(ht),
        "args": hex_bytes(&args),
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn hex_u64(n: u64) -> String {
    format!("0x{n:x}")
}

fn hex_u32(n: u32) -> String {
    format!("0x{n:x}")
}

fn decode_hex(s: &str) -> Result<Vec<u8>, RpcError> {
    let t = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if t.is_empty() {
        return Ok(Vec::new());
    }
    hex::decode(t).map_err(|e| RpcError::Unavailable(format!("hex: {e}")))
}

fn parse_uint(v: &Value) -> Result<u64, RpcError> {
    match v {
        Value::String(s) => {
            let t = s.trim().trim_start_matches("0x").trim_start_matches("0X");
            if t.is_empty() {
                return Ok(0);
            }
            u64::from_str_radix(t, 16).map_err(|_| RpcError::Unavailable(format!("uint {s}")))
        }
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| RpcError::Unavailable("uint".into())),
        _ => Err(RpcError::Unavailable("uint type".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::ssri::{SsriError, SsriHost};
    use cid_process_executor::{IntentionInput, PartialTx};
    use cid_protocol::{CidArgs, Payload, Script as CidScript};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    const CID_CODE: [u8; 32] = [0xC1; 32];
    const PROC_CODE: [u8; 32] = [0x51; 32];

    type Handler = Arc<dyn Fn(&str, Value) -> Result<Value, (i64, String)> + Send + Sync>;

    fn spawn_mock(handler: Handler) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(false).expect("blocking");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = match stream {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                let req = match read_http_json(&mut stream) {
                    Some(v) => v,
                    None => continue,
                };
                let method = req
                    .get("method")
                    .and_then(|m| m.as_str())
                    .unwrap_or("")
                    .to_string();
                let params = req.get("params").cloned().unwrap_or(Value::Null);
                let body = match handler(&method, params) {
                    Ok(result) => json!({"jsonrpc":"2.0","id":1,"result":result}),
                    Err((code, message)) => json!({
                        "jsonrpc":"2.0",
                        "id":1,
                        "error":{"code":code,"message":message}
                    }),
                };
                let raw = serde_json::to_vec(&body).unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    raw.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&raw);
                let _ = stream.flush();
            }
        });
        format!("http://{addr}")
    }

    fn read_http_json(stream: &mut std::net::TcpStream) -> Option<Value> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = stream.read(&mut tmp).ok()?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = find_headers_end(&buf) {
                let headers = std::str::from_utf8(&buf[..pos]).ok()?.to_ascii_lowercase();
                let len = headers.lines().find_map(|l| {
                    l.strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })?;
                let start = pos + 4;
                while buf.len() < start + len {
                    let n = stream.read(&mut tmp).ok()?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                return serde_json::from_slice(&buf[start..start + len]).ok();
            }
            if buf.len() > 64 * 1024 {
                return None;
            }
        }
        None
    }

    fn find_headers_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn cells_result(objects: Vec<Value>) -> Value {
        json!({"objects": objects, "last_cursor": "0x"})
    }

    fn indexer_cell(
        tx_fill: u8,
        index: u32,
        lock: Value,
        type_script: Option<Value>,
        cap: u64,
        data: &[u8],
    ) -> Value {
        json!({
            "block_number": "0x1",
            "tx_index": "0x0",
            "out_point": {
                "tx_hash": hex_bytes(&[tx_fill; 32]),
                "index": hex_u32(index),
            },
            "output": {
                "capacity": hex_u64(cap),
                "lock": lock,
                "type": type_script,
            },
            "output_data": hex_bytes(data),
        })
    }

    fn lock_json(code: [u8; 32], args: &[u8]) -> Value {
        json_script(&code, ScriptHashType::Type, args)
    }

    fn process_type_script(unique: u8) -> Script {
        Script::new_builder()
            .code_hash(PROC_CODE.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(vec![unique; 32].pack())
            .build()
    }

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0x01, 0x02],
        }
    }

    fn cid_args(process_hash: [u8; 32]) -> Vec<u8> {
        CidArgs::new(process_hash, user_lock(), Payload { data: vec![0xAA] })
            .encode()
            .unwrap()
    }

    struct StubSsri;
    impl SsriHost for StubSsri {
        fn get_batch_bounds(&self, _: &[u8]) -> Result<(u32, u32), SsriError> {
            Ok((1, 1))
        }
        fn assemble(&self, _: &[u8], _: &[IntentionInput]) -> Result<PartialTx, SsriError> {
            let bytes = ckb_types::packed::Transaction::new_builder()
                .build()
                .as_slice()
                .to_vec();
            Ok(PartialTx::from_bytes(&bytes).expect("empty partial"))
        }
    }

    #[test]
    fn lists_live_intentions_in_indexer_asc_order() {
        let url = spawn_mock(Arc::new(|method, params| {
            assert_eq!(method, "get_cells");
            let key = &params[0];
            assert_eq!(key["script_type"], "lock");
            assert_eq!(key["script"]["hash_type"], "type");
            assert_eq!(params[1], "asc");
            Ok(cells_result(vec![
                indexer_cell(1, 0, lock_json(CID_CODE, &[0x01]), None, 100, &[]),
                indexer_cell(2, 0, lock_json(CID_CODE, &[0x02]), None, 200, &[]),
            ]))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let cells = rpc
            .list_live_intentions(&CID_CODE, ScriptHashType::Type)
            .expect("list");
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0].capacity(), 100);
        assert_eq!(cells[1].capacity(), 200);
        let i0: u32 = cells[0].out_point.index().unpack();
        assert_eq!(i0, 0);
    }

    #[test]
    fn finds_process_cell_by_script_hash_via_code_hash_prefix() {
        let ts = process_type_script(7);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts);
        let url = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            let code = params[0]["script"]["code_hash"].as_str().unwrap();
            if code == hex_bytes(&PROC_CODE) {
                Ok(cells_result(vec![indexer_cell(
                    0x50,
                    0,
                    lock_json([0; 32], &[]),
                    Some(type_json.clone()),
                    200,
                    &[0x7f, b'E', b'L', b'F'],
                )]))
            } else {
                Ok(cells_result(vec![]))
            }
        }));
        let rpc = CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]);
        let cell = rpc
            .get_live_process_by_type_hash(&hash)
            .expect("rpc")
            .expect("found");
        assert_eq!(cell.data, vec![0x7f, b'E', b'L', b'F']);
        assert_eq!(script_hash32(&cell.type_script().unwrap()), hash);
    }

    #[test]
    fn submit_maps_unknown_inputs_to_conflict() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((
                -1107,
                "TransactionFailedToResolve: UnknownInputs(0xab)".into(),
            ))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(matches!(err, RpcError::Conflict(_)), "{err:?}");
    }

    #[test]
    fn tx_status_pending_committed_and_dropped() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let url = spawn_mock(Arc::new(move |method, _| {
            assert_eq!(method, "get_transaction");
            let i = n2.fetch_add(1, Ordering::SeqCst);
            Ok(match i {
                0 => json!({"tx_status":{"status":"pending"}}),
                1 => json!({"tx_status":{"status":"proposed"}}),
                2 => json!({"tx_status":{"status":"committed","block_hash":"0x00"}}),
                3 => json!({"tx_status":{"status":"unknown"}}),
                4 => json!({"tx_status":{"status":"rejected"}}),
                _ => Value::Null,
            })
        }));
        let rpc = CkbRpc::new(&url, &url);
        let hash = Byte32::from_slice(&[1u8; 32]).unwrap();
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Pending
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Pending
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Committed
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Dropped
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Dropped
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Dropped
        );
    }

    #[test]
    fn indexer_lag_lists_spent_cell_until_cursor_catches_up() {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls2 = calls.clone();
        let url = spawn_mock(Arc::new(move |method, _params| {
            calls2.lock().unwrap().push(method.to_string());
            match method {
                "get_cells" => {
                    let n = calls2
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|m| *m == "get_cells")
                        .count();
                    if n <= 2 {
                        Ok(cells_result(vec![indexer_cell(
                            1,
                            0,
                            lock_json(CID_CODE, &[0x01]),
                            None,
                            100,
                            &[],
                        )]))
                    } else {
                        Ok(cells_result(vec![]))
                    }
                }
                "send_transaction" => Ok(Value::String(hex_bytes(&[0xab; 32]))),
                _ => Err((-1, format!("unexpected {method}"))),
            }
        }));
        let rpc = CkbRpc::new(&url, &url);
        assert_eq!(
            rpc.list_live_intentions(&CID_CODE, ScriptHashType::Type)
                .unwrap()
                .len(),
            1
        );
        let tx = ckb_types::core::TransactionBuilder::default().build();
        rpc.submit_transaction(&tx).unwrap();
        assert_eq!(
            rpc.list_live_intentions(&CID_CODE, ScriptHashType::Type)
                .unwrap()
                .len(),
            1,
            "indexer lag: cell still listed after submit"
        );
        assert_eq!(
            rpc.list_live_intentions(&CID_CODE, ScriptHashType::Type)
                .unwrap()
                .len(),
            0,
            "after indexer catch-up the cell disappears"
        );
    }

    #[test]
    fn restart_full_rescan_has_no_adapter_memory() {
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts);
        let args = cid_args(hash);
        let intent = indexer_cell(1, 0, lock_json(CID_CODE, &args), None, 100_0000_0000, &[]);
        let proc = indexer_cell(
            0x50,
            0,
            lock_json([0; 32], &[]),
            Some(type_json),
            200_0000_0000,
            &[0x7f, b'E', b'L', b'F', 1],
        );
        let submitted = Arc::new(Mutex::new(0usize));
        let submitted2 = submitted.clone();
        let url = spawn_mock(Arc::new(move |method, params| match method {
            "get_cells" => {
                let script_type = params[0]["script_type"].as_str().unwrap_or("");
                if script_type == "lock" {
                    Ok(cells_result(vec![intent.clone()]))
                } else {
                    Ok(cells_result(vec![proc.clone()]))
                }
            }
            "send_transaction" => {
                *submitted2.lock().unwrap() += 1;
                let n = *submitted2.lock().unwrap();
                Ok(Value::String(hex_bytes(&[n as u8; 32])))
            }
            "get_transaction" => Ok(json!({"tx_status":{"status":"pending"}})),
            other => Err((-1, format!("unexpected {other}"))),
        }));

        // The handler above always returns both cells for every get_cells, including
        // type-script searches. Filter happens in the adapter by script hash.
        let rpc = CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]);
        let cfg = LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
        };
        let mut first = DigestLoop::new(
            CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]),
            NopClock,
            StubSsri,
            cfg,
            ProcessResolver::new(),
        );
        let r1 = first.tick();
        assert_eq!(r1.submitted, 1, "first process submits");
        assert_eq!(*submitted.lock().unwrap(), 1);

        // New process: empty in_flight / pending. Adapter has no disk state, so
        // a full indexer rescan sees the same live cells (Pending / lag).
        let mut second = DigestLoop::new(
            rpc,
            NopClock,
            StubSsri,
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
            ProcessResolver::new(),
        );
        let r2 = second.tick();
        assert_eq!(r2.submitted, 1, "restart resubmits; no persisted occupancy");
        assert_eq!(*submitted.lock().unwrap(), 2);
    }
}
