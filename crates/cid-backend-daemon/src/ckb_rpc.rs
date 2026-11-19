//! Live CKB node + indexer JSON-RPC adapter (`Rpc`).
//!
//! HTTP only. No disk state: every scan is a fresh `get_cells` paged until the
//! indexer is exhausted. A truncated page window is never treated as a complete
//! live-set. Legacy Process discovery walks TYPE_ID type-script prefix only,
//! then filters `calc_script_hash == process_type_hash`. Workers use registry +
//! exact TYPE_ID args for state cells.

use crate::config::{ConfigError, DaemonConfig};
use cid_backend_core::host_query::{
    HostQueryRpc, DEFAULT_HOST_QUERY_PAGE_LIMIT, DEFAULT_HOST_QUERY_TIMEOUT_SECS,
};
use cid_backend_core::rpc::{script_hash32, LiveCell, Rpc, RpcError, TxStatus};
use ckb_types::core::{DepType, ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, CellInput, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use serde_json::{json, Value};
use std::time::Duration;

/// Indexer `limit` per `get_cells` call. Pagination continues until a short
/// page (or empty objects). There is no page cap that drops the remainder.
const PAGE_LIMIT: usize = 100;
const PAGE_LIMIT_HEX: &str = "0x64";

const HASH_TYPES: [ScriptHashType; 4] = [
    ScriptHashType::Type,
    ScriptHashType::Data,
    ScriptHashType::Data1,
    ScriptHashType::Data2,
];

pub struct CkbRpc {
    node_url: String,
    indexer_url: String,
    agent: ureq::Agent,
}

impl CkbRpc {
    pub fn new(node_url: impl Into<String>, indexer_url: impl Into<String>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        Self {
            node_url: node_url.into(),
            indexer_url: indexer_url.into(),
            agent,
        }
    }

    /// Kept for test fixtures; process code-hash prefixes are no longer used.
    pub fn with_process_code_hashes(
        node_url: impl Into<String>,
        indexer_url: impl Into<String>,
        _process_type_code_hashes: Vec<[u8; 32]>,
    ) -> Self {
        Self::new(node_url, indexer_url)
    }

    pub fn from_config(cfg: &DaemonConfig) -> Self {
        let _ = &cfg.process_type_code_hashes;
        Self::new(cfg.node_url.clone(), cfg.indexer_url.clone())
    }

    /// Node JSON-RPC helper for local-chain bootstrap / confirmation polls.
    pub fn call_node(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        self.rpc_call(&self.node_url, method, params)
    }

    /// Fail-closed genesis identity: `get_block_hash("0x0")`.
    pub fn genesis_block_hash(&self) -> Result<[u8; 32], RpcError> {
        let result = self.call_node("get_block_hash", json!(["0x0"]))?;
        let hex = result
            .as_str()
            .ok_or_else(|| RpcError::Unavailable("get_block_hash: result not hex".into()))?;
        let bytes = decode_hex(hex)?;
        <[u8; 32]>::try_from(bytes.as_slice())
            .map_err(|_| RpcError::Unavailable("get_block_hash: not 32 bytes".into()))
    }

    /// Compare declared genesis to the node. RPC errors and mismatches both fail.
    pub fn verify_declared_genesis(&self, expected: &[u8; 32]) -> Result<(), ConfigError> {
        match self.genesis_block_hash() {
            Ok(got) if &got == expected => Ok(()),
            Ok(got) => Err(ConfigError::Invalid(format!(
                "genesis hash mismatch (fail-closed): declared 0x{} node 0x{}",
                hex::encode(expected),
                hex::encode(got)
            ))),
            Err(e) => Err(ConfigError::Invalid(format!(
                "genesis hash check failed (fail-closed): {e}"
            ))),
        }
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
            .map_err(|e| RpcError::Unavailable(format_rpc_transport_error(method, e)))?;
        let v: Value = resp.into_json().map_err(|_e| {
            // Decode failures must not echo body/URL fragments via Display.
            RpcError::Unavailable(format!("{method}: response decode failed"))
        })?;
        if let Some(err) = v.get("error") {
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("rpc error");
            let code = json_rpc_code(err);
            return Err(map_rpc_error(method, code, msg));
        }
        match v.get("result") {
            Some(r) => Ok(r.clone()),
            None => Err(RpcError::Unavailable(format!("{method}: missing result"))),
        }
    }

    fn get_cells(&self, search_key: &Value) -> Result<Vec<LiveCell>, RpcError> {
        let mut after = Value::Null;
        let mut out = Vec::new();
        loop {
            let result = self.rpc_call(
                &self.indexer_url,
                "get_cells",
                json!([search_key, "asc", PAGE_LIMIT_HEX, after.clone()]),
            )?;
            let objects = result
                .get("objects")
                .and_then(|o| o.as_array())
                .ok_or_else(|| RpcError::Unavailable("get_cells: missing objects".into()))?;
            for obj in objects {
                out.push(live_cell_from_indexer(obj)?);
            }
            if objects.len() < PAGE_LIMIT {
                break;
            }
            let cursor = result
                .get("last_cursor")
                .and_then(|c| c.as_str())
                .unwrap_or("");
            if cursor_is_empty(cursor) {
                return Err(RpcError::Unavailable(format!(
                    "get_cells: full page of {PAGE_LIMIT} without last_cursor; refusing truncated live-set"
                )));
            }
            let next = Value::String(cursor.to_string());
            if next == after {
                return Err(RpcError::Unavailable(
                    "get_cells: last_cursor did not advance; refusing truncated live-set".into(),
                ));
            }
            after = next;
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

    /// Exact TYPE_ID / type-script lookup with early stop and page bound.
    ///
    /// `with_data = true` so a unique Process code cell carries ELF bytes for
    /// SSRI in the same snapshot as the CellDep. Stops once `max_results`
    /// matches are collected (use 2 for unique-vs-ambiguous). Full pages
    /// without an advancing cursor, or exceeding `MAX_TYPE_ID_PAGES` before
    /// the set is exhausted while still under `max_results`, fail closed —
    /// never report a unique cell from a truncated walk.
    fn find_cells_by_exact_type_script(
        &self,
        type_script: &Script,
        max_results: usize,
    ) -> Result<Vec<LiveCell>, RpcError> {
        let ht: ScriptHashType = type_script
            .hash_type()
            .try_into()
            .map_err(|_| RpcError::Unavailable("type script hash_type".into()))?;
        let code: [u8; 32] = type_script.code_hash().unpack();
        let args: Vec<u8> = type_script.args().unpack();
        let search_key = json!({
            "script": json_script(&code, ht, &args),
            "script_type": "type",
            "script_search_mode": "exact",
            "with_data": true,
        });
        let mut after = Value::Null;
        let mut out = Vec::new();
        let mut pages = 0usize;
        loop {
            if out.len() >= max_results {
                out.truncate(max_results);
                return Ok(out);
            }
            if pages >= MAX_TYPE_ID_PAGES {
                return Err(RpcError::Unavailable(format!(
                    "get_cells exact type-id: exceeded {MAX_TYPE_ID_PAGES} pages before \
                     uniqueness settled; refusing truncated live-set"
                )));
            }
            let result = self.rpc_call(
                &self.indexer_url,
                "get_cells",
                json!([&search_key, "asc", PAGE_LIMIT_HEX, after.clone()]),
            )?;
            pages += 1;
            let objects = result
                .get("objects")
                .and_then(|o| o.as_array())
                .ok_or_else(|| RpcError::Unavailable("get_cells: missing objects".into()))?;
            let want = script_hash32(type_script);
            for obj in objects {
                let cell = live_cell_from_indexer(obj)?;
                // Local exact filter even if indexer "exact" is loose.
                if cell
                    .type_script()
                    .map(|ts| script_hash32(&ts) == want)
                    .unwrap_or(false)
                {
                    out.push(cell);
                    if out.len() >= max_results {
                        out.truncate(max_results);
                        return Ok(out);
                    }
                }
            }
            if objects.len() < PAGE_LIMIT {
                break;
            }
            let cursor = result
                .get("last_cursor")
                .and_then(|c| c.as_str())
                .unwrap_or("");
            if cursor_is_empty(cursor) {
                return Err(RpcError::Unavailable(format!(
                    "get_cells: full page of {PAGE_LIMIT} without last_cursor; refusing truncated live-set"
                )));
            }
            let next = Value::String(cursor.to_string());
            if next == after {
                return Err(RpcError::Unavailable(
                    "get_cells: last_cursor did not advance; refusing truncated live-set".into(),
                ));
            }
            after = next;
        }
        Ok(out)
    }
}

/// Safety bound for Type-ID uniqueness walks (not for intention live-set scans).
const MAX_TYPE_ID_PAGES: usize = 16;

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
        for ht in HASH_TYPES {
            candidates.extend(self.cells_by_type_prefix(
                &cid_backend_core::TYPE_ID_CODE_HASH,
                ht,
            )?);
        }
        Ok(candidates.into_iter().find(|cell| {
            cell.type_script()
                .map(|ts| &script_hash32(&ts) == process_type_hash)
                .unwrap_or(false)
        }))
    }

    fn dry_run_transaction(&self, tx: &TransactionView) -> Result<(), RpcError> {
        let tx_json = tx_view_to_json(tx)?;
        match self.dry_run_rpc_call("test_tx_pool_accept", json!([tx_json.clone(), "passthrough"])) {
            Ok(()) => Ok(()),
            Err((code, msg)) if dry_run_should_fallback(code, &msg) => {
                self.dry_run_rpc_call("estimate_cycles", json!([tx_json]))
                    .map_err(|(c, m)| map_verify_rpc_error("estimate_cycles", c, &m))
            }
            Err((code, msg)) => Err(map_verify_rpc_error("test_tx_pool_accept", code, &msg)),
        }
    }

    fn find_live_cells_by_type_script(
        &self,
        type_script: &Script,
        max_results: usize,
    ) -> Result<Vec<LiveCell>, RpcError> {
        self.find_cells_by_exact_type_script(type_script, max_results)
    }

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "send_transaction",
            json!([tx_view_to_json(tx)?, "passthrough"]),
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
        match status {
            "pending" | "proposed" => Ok(TxStatus::Pending),
            "committed" => {
                let block_number = committed_block_number(self, &result)?;
                Ok(TxStatus::Committed { block_number })
            }
            _ => Ok(TxStatus::Dropped),
        }
    }

    fn tip_block_number(&self) -> Result<u64, RpcError> {
        let result = self.rpc_call(&self.node_url, "get_tip_block_number", json!([]))?;
        parse_uint(&result)
    }
}

impl HostQueryRpc for CkbRpc {
    fn get_live_cell(&self, out_point: &OutPoint) -> Result<Option<LiveCell>, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "get_live_cell",
            json!([out_point_json(out_point), true]),
        )?;
        let status = result
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("unknown");
        if status != "live" {
            return Ok(None);
        }
        let Some(cell) = result.get("cell") else {
            return Ok(None);
        };
        let data = match result.get("data") {
            Some(Value::String(s)) => decode_hex(s)?,
            _ => Vec::new(),
        };
        Ok(Some(LiveCell {
            out_point: out_point.clone(),
            output: json_to_output(cell)?,
            data,
        }))
    }

    fn get_block_hash(&self, number: u64) -> Result<Option<[u8; 32]>, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "get_block_hash",
            json!([hex_u64(number)]),
        )?;
        if result.is_null() {
            return Ok(None);
        }
        let hex = result.as_str().ok_or_else(|| {
            RpcError::Unavailable("get_block_hash: result not hex".into())
        })?;
        let bytes = decode_hex(hex)?;
        <[u8; 32]>::try_from(bytes.as_slice())
            .map(Some)
            .map_err(|_| RpcError::Unavailable("get_block_hash: not 32 bytes".into()))
    }

    fn get_header_by_number(&self, number: u64) -> Result<Option<Vec<u8>>, RpcError> {
        let result = self.rpc_call(
            &self.node_url,
            "get_header_by_number",
            json!([hex_u64(number)]),
        )?;
        if result.is_null() {
            return Ok(None);
        }
        serde_json::to_vec(&result)
            .map(Some)
            .map_err(|e| RpcError::Unavailable(format!("get_header_by_number encode: {e}")))
    }

    fn get_header(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, RpcError> {
        let result = self.rpc_call(&self.node_url, "get_header", json!([hex_bytes(hash)]))?;
        if result.is_null() {
            return Ok(None);
        }
        serde_json::to_vec(&result)
            .map(Some)
            .map_err(|e| RpcError::Unavailable(format!("get_header encode: {e}")))
    }

    fn get_cells_page(
        &self,
        search_key_molecule: &[u8],
        limit: u32,
        after: &[u8],
    ) -> Result<(Vec<LiveCell>, Vec<u8>), RpcError> {
        let _ = DEFAULT_HOST_QUERY_TIMEOUT_SECS;
        let search_key = search_key_json_from_molecule(search_key_molecule)?;
        let page_limit = u64::from(limit.min(DEFAULT_HOST_QUERY_PAGE_LIMIT).max(1));
        let after_val = json_after_cursor(after);
        let result = self.rpc_call(
            &self.indexer_url,
            "get_cells",
            json!([search_key, "asc", hex_u64(page_limit), after_val]),
        )?;
        let objects = result
            .get("objects")
            .and_then(|o| o.as_array())
            .ok_or_else(|| RpcError::Unavailable("get_cells: missing objects".into()))?;
        let mut cells = Vec::with_capacity(objects.len());
        for obj in objects {
            cells.push(live_cell_from_indexer(obj)?);
        }
        let next = result
            .get("last_cursor")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        Ok((cells, next.as_bytes().to_vec()))
    }
}

fn search_key_json_from_molecule(bytes: &[u8]) -> Result<Value, RpcError> {
    serde_json::from_slice(bytes).map_err(|_| {
        RpcError::Unavailable(
            "get_cells_page: search_key must be JSON bytes (molecule decode pending catalog)"
                .into(),
        )
    })
}

fn json_after_cursor(after: &[u8]) -> Value {
    if after.is_empty() {
        return Value::Null;
    }
    if let Ok(s) = std::str::from_utf8(after) {
        Value::String(s.to_string())
    } else {
        Value::String(hex_bytes(after))
    }
}

/// Map ureq failures to log-safe text. ureq `Display` embeds the full request URL
/// (userinfo / query tokens); never forward that into `RpcError` / tick logs.
fn format_rpc_transport_error(method: &str, err: ureq::Error) -> String {
    match err {
        ureq::Error::Status(code, _) => format!("{method}: HTTP {code}"),
        ureq::Error::Transport(t) => format!("{method}: transport ({})", t.kind()),
    }
}

fn committed_block_number(rpc: &CkbRpc, tx: &Value) -> Result<u64, RpcError> {
    if let Some(n) = tx.pointer("/tx_status/block_number") {
        if let Ok(v) = parse_uint(n) {
            return Ok(v);
        }
    }
    let Some(hash) = tx.pointer("/tx_status/block_hash").and_then(|h| h.as_str()) else {
        return Err(RpcError::Unavailable(
            "committed tx missing block_number and block_hash".into(),
        ));
    };
    let bytes = match decode_hex(hash) {
        Ok(b) if b.len() == 32 => b,
        _ => {
            return Err(RpcError::Unavailable(
                "committed tx invalid block_hash".into(),
            ));
        }
    };
    let header = rpc.rpc_call(&rpc.node_url, "get_header", json!([hex_bytes(&bytes)]))?;
    if header.is_null() {
        return Err(RpcError::Unavailable(
            "get_header returned null (non-canonical or reorg)".into(),
        ));
    }
    header
        .get("number")
        .map(parse_uint)
        .transpose()?
        .ok_or_else(|| RpcError::Unavailable("get_header: missing number".into()))
}

/// CKB `send_transaction` JSON-RPC codes (rpc/README.md). Only positively
/// identified permanent failures become `SubmitRejected`.
const TX_FAILED_TO_RESOLVE: i64 = -301;
const TX_FAILED_TO_VERIFY: i64 = -302;
const POOL_REJECTED_BY_OUTPUTS_VALIDATOR: i64 = -1102;
const POOL_REJECTED_BY_MIN_FEE_RATE: i64 = -1104;
const POOL_REJECTED_BY_MAX_ANCESTORS: i64 = -1105;
const POOL_IS_FULL: i64 = -1106;
const POOL_REJECTED_DUPLICATED: i64 = -1107;
const POOL_REJECTED_MALFORMED: i64 = -1108;

fn json_rpc_code(err: &Value) -> Option<i64> {
    let c = err.get("code")?;
    c.as_i64()
        .or_else(|| c.as_u64().and_then(|u| i64::try_from(u).ok()))
        .or_else(|| c.as_str().and_then(|s| s.parse().ok()))
}

const JSON_RPC_METHOD_NOT_FOUND: i64 = -32601;

fn map_verify_rpc_error(method: &str, code: Option<i64>, msg: &str) -> RpcError {
    if is_conflict(msg) {
        return RpcError::Conflict(msg.into());
    }
    match code {
        Some(TX_FAILED_TO_RESOLVE) | Some(POOL_REJECTED_DUPLICATED) => {
            RpcError::Conflict(msg.into())
        }
        Some(TX_FAILED_TO_VERIFY)
        | Some(POOL_REJECTED_BY_OUTPUTS_VALIDATOR)
        | Some(POOL_REJECTED_MALFORMED) => RpcError::SubmitRejected(msg.into()),
        Some(POOL_IS_FULL)
        | Some(POOL_REJECTED_BY_MAX_ANCESTORS)
        | Some(POOL_REJECTED_BY_MIN_FEE_RATE) => RpcError::Unavailable(format!("{method} {code:?}: {msg}")),
        _ => RpcError::Unavailable(format!("{method} {code:?}: {msg}")),
    }
}

fn map_rpc_error(method: &str, code: Option<i64>, msg: &str) -> RpcError {
    match method {
        "send_transaction" | "test_tx_pool_accept" | "estimate_cycles" => {
            map_verify_rpc_error(method, code, msg)
        }
        _ => RpcError::Unavailable(msg.into()),
    }
}

fn dry_run_should_fallback(code: Option<i64>, msg: &str) -> bool {
    if code == Some(JSON_RPC_METHOD_NOT_FOUND) {
        return true;
    }
    let m = msg.to_ascii_lowercase();
    m.contains("method not found") || m.contains("test_tx_pool_accept")
}

impl CkbRpc {
    fn dry_run_rpc_call(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(), (Option<i64>, String)> {
        let body = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let resp = self
            .agent
            .post(&self.node_url)
            .set("Content-Type", "application/json")
            .send_json(body)
            .map_err(|e| (None, format_rpc_transport_error(method, e)))?;
        let v: Value = resp
            .into_json()
            .map_err(|_| (None, format!("{method}: response decode failed")))?;
        if let Some(err) = v.get("error") {
            let msg = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("rpc error")
                .to_string();
            let code = json_rpc_code(err);
            return Err((code, msg));
        }
        if v.get("result").is_some() {
            Ok(())
        } else {
            Err((None, format!("{method}: missing result")))
        }
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

fn dep_type_str(dt: DepType) -> &'static str {
    match dt {
        DepType::Code => "code",
        DepType::DepGroup => "dep_group",
    }
}

fn tx_view_to_json(tx: &TransactionView) -> Result<Value, RpcError> {
    let cell_deps: Vec<Value> = tx
        .cell_deps()
        .into_iter()
        .map(|d| {
            let dt: DepType = d
                .dep_type()
                .try_into()
                .map_err(|_| RpcError::Unavailable("cell_dep.dep_type".into()))?;
            Ok(json!({
                "out_point": out_point_json(&d.out_point()),
                "dep_type": dep_type_str(dt),
            }))
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
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
    let outputs: Vec<Value> = tx
        .outputs()
        .into_iter()
        .map(|o| output_json(&o))
        .collect::<Result<Vec<_>, RpcError>>()?;
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
    Ok(json!({
        "version": hex_u32(tx.version()),
        "cell_deps": cell_deps,
        "header_deps": header_deps,
        "inputs": inputs,
        "outputs": outputs,
        "outputs_data": outputs_data,
        "witnesses": witnesses,
    }))
}

fn out_point_json(op: &OutPoint) -> Value {
    let index: u32 = op.index().unpack();
    json!({
        "tx_hash": hex_bytes(op.tx_hash().as_slice()),
        "index": hex_u32(index),
    })
}

fn output_json(out: &CellOutput) -> Result<Value, RpcError> {
    let cap: u64 = out.capacity().unpack();
    Ok(json!({
        "capacity": hex_u64(cap),
        "lock": script_json(&out.lock())?,
        "type": out
            .type_()
            .to_opt()
            .map(|t| script_json(&t))
            .transpose()?,
    }))
}

fn script_json(s: &Script) -> Result<Value, RpcError> {
    let ht: ScriptHashType = s
        .hash_type()
        .try_into()
        .map_err(|_| RpcError::Unavailable("script hash_type".into()))?;
    let args: Vec<u8> = s.args().unpack();
    Ok(json!({
        "code_hash": hex_bytes(s.code_hash().as_slice()),
        "hash_type": hash_type_str(ht),
        "args": hex_bytes(&args),
    }))
}

fn cursor_is_empty(cursor: &str) -> bool {
    let t = cursor
        .trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X");
    t.is_empty()
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
    use cid_backend_core::digest_loop::{Clock, DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::rpc::out_point_key;
    use cid_backend_core::ssri::{SsriError, SsriHost};
    use cid_process_executor::{AssembledTx, MockHost};
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
            .code_hash(cid_backend_core::TYPE_ID_CODE_HASH.pack())
            .hash_type(ScriptHashType::Type.into())
            .args([unique; 32].to_vec().pack())
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

    const PROC_CODE_ARGS: [u8; 32] = [0xCDu8; 32];

    fn register_fixture_process<R, C, S>(loop_: &mut DigestLoop<R, C, S>, unique: u8)
    where
        R: Rpc + HostQueryRpc + Send + Sync + 'static,
        C: Clock,
        S: SsriHost + Send + Sync + 'static,
    {
        let identity = cid_backend_core::TypeIdIdentity::new([unique; 32]);
        let code = cid_backend_core::TypeIdIdentity::new(PROC_CODE_ARGS);
        let entry = cid_backend_core::ProcessEntry {
            identity_type_hash: identity.type_script_hash(),
            identity: identity.clone(),
            code,
            ssri_elf_path: None,
        };
        let hash = entry.identity_type_hash;
        loop_.register_process(entry.clone());
        if let Some(w) = loop_.workers().get(&hash) {
            w.with_state_mut(|state| {
                state.resolver = cid_backend_core::ProcessResolver::for_entry(entry)
                    .with_fallback_binary(vec![0x7f, b'E', b'L', b'F', 1]);
            });
        }
    }

    struct StubSsri;
    impl SsriHost for StubSsri {
        fn get_batch_bounds(&self, _: &[u8]) -> Result<(u32, u32), SsriError> {
            Ok((1, 1))
        }
        fn assemble(
            &self,
            _: &[u8],
            draft_tx: &[u8],
            host: MockHost,
        ) -> Result<AssembledTx, SsriError> {
            use ckb_types::packed::{CellInput, CellOutput, RawTransaction, Transaction};
            use ckb_types::prelude::*;
            let draft = Transaction::from_slice(draft_tx)
                .map_err(|e| SsriError::Unusable(format!("draft: {e:?}")))?;
            let mut inputs = Vec::new();
            let mut outputs = Vec::new();
            let mut outputs_data = Vec::new();
            let mut process_out: Option<(CellOutput, Vec<u8>)> = None;
            for input in draft.raw().inputs().into_iter() {
                let op = input.previous_output();
                let cell = host
                    .get_cell(op.as_slice())
                    .ok_or_else(|| SsriError::Unusable("missing host cell".into()))?;
                inputs.push(CellInput::new_builder().previous_output(op).build());
                if cell.output.type_().to_opt().is_some()
                    && cid_protocol::CidArgs::decode(&cell.output.lock().args().raw_data()).is_err()
                {
                    process_out = Some((cell.output.clone(), cell.data.clone()));
                } else {
                    outputs.push(cell.output.clone());
                    outputs_data.push(cell.data.clone());
                }
            }
            let (proc_out, proc_data) =
                process_out.ok_or_else(|| SsriError::Unusable("no process".into()))?;
            // Process recreate first (TYPE_ID continuity), then beneficiaries.
            let mut final_outputs = vec![proc_out];
            let mut final_data = vec![proc_data];
            final_outputs.extend(outputs);
            final_data.extend(outputs_data);
            let witnesses: Vec<ckb_types::packed::Bytes> =
                (0..inputs.len()).map(|_| Default::default()).collect();
            let bytes = Transaction::new_builder()
                .raw(
                    RawTransaction::new_builder()
                        .inputs(inputs.pack())
                        .outputs(final_outputs.pack())
                        .outputs_data(final_data.iter().map(|d| d.as_slice().pack()).pack())
                        .build(),
                )
                .witnesses(witnesses.pack())
                .build()
                .as_slice()
                .to_vec();
            AssembledTx::from_bytes(&bytes).map_err(|e| SsriError::Unusable(e.to_string()))
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
    fn finds_code_cell_by_exact_type_id_script_with_pagination() {
        use cid_backend_core::type_id::{type_id_script, TYPE_ID_CODE_HASH};
        let args = [0x7eu8; 32];
        let ts = type_id_script(&args);
        let type_json = script_json(&ts).unwrap();
        let type_json_full = type_json.clone();
        let page = Arc::new(AtomicUsize::new(0));
        let page2 = page.clone();
        let url = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            let key = &params[0];
            assert_eq!(key["script_type"], "type");
            assert_eq!(key["script_search_mode"], "exact");
            assert_eq!(key["script"]["hash_type"], "type");
            assert_eq!(
                key["script"]["code_hash"].as_str().unwrap(),
                hex_bytes(&TYPE_ID_CODE_HASH)
            );
            assert_eq!(key["script"]["args"].as_str().unwrap(), hex_bytes(&args));
            let n = page2.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // Full page then a short page — must not truncate.
                let mut objs = Vec::new();
                for i in 0..100 {
                    objs.push(indexer_cell(
                        0x10,
                        i as u32,
                        lock_json([0; 32], &[]),
                        Some(type_json.clone()),
                        1,
                        &[0x7f],
                    ));
                }
                Ok(json!({"objects": objs, "last_cursor": "0xc0ffee"}))
            } else {
                Ok(cells_result(vec![indexer_cell(
                    0x11,
                    0,
                    lock_json([0; 32], &[]),
                    Some(type_json.clone()),
                    42,
                    &[0x7f, b'E', b'L', b'F'],
                )]))
            }
        }));
        let rpc = CkbRpc::new(&url, &url);
        // Uniqueness walk stops at 2 matches (ambiguous) without draining pages.
        let cells = rpc
            .find_live_cells_by_type_script(&ts, 2)
            .expect("exact type-id search");
        assert_eq!(cells.len(), 2);
        assert_eq!(
            page.load(Ordering::SeqCst),
            1,
            "must not fetch page 2 once ambiguous"
        );

        // Explicit larger bound still paginates and includes cell data.
        let page_full = Arc::new(AtomicUsize::new(0));
        let page_full2 = page_full.clone();
        let url2 = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            assert_eq!(params[0]["script_search_mode"], "exact");
            assert_eq!(params[0]["with_data"], true);
            let n = page_full2.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                let mut objs = Vec::new();
                for i in 0..100 {
                    objs.push(indexer_cell(
                        0x10,
                        i as u32,
                        lock_json([0; 32], &[]),
                        Some(type_json_full.clone()),
                        1,
                        &[0x7f],
                    ));
                }
                Ok(json!({"objects": objs, "last_cursor": "0xc0ffee"}))
            } else {
                Ok(cells_result(vec![indexer_cell(
                    0x11,
                    0,
                    lock_json([0; 32], &[]),
                    Some(type_json_full.clone()),
                    42,
                    &[0x7f, b'E', b'L', b'F'],
                )]))
            }
        }));
        let rpc2 = CkbRpc::new(&url2, &url2);
        let all = rpc2
            .find_live_cells_by_type_script(&ts, 200)
            .expect("paged exact search");
        assert_eq!(all.len(), 101);
        assert_eq!(all[100].capacity(), 42);
        assert_eq!(&all[100].data, &[0x7f, b'E', b'L', b'F']);
    }

    #[test]
    fn finds_process_cell_by_script_hash_via_code_hash_prefix() {
        let ts = process_type_script(7);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts).unwrap();
        let url = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            let code = params[0]["script"]["code_hash"].as_str().unwrap();
            if code == hex_bytes(&cid_backend_core::TYPE_ID_CODE_HASH) {
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
        let rpc = CkbRpc::new(&url, &url);
        let cell = rpc
            .get_live_process_by_type_hash(&hash)
            .expect("rpc")
            .expect("found");
        assert_eq!(cell.data, vec![0x7f, b'E', b'L', b'F']);
        assert_eq!(script_hash32(&cell.type_script().unwrap()), hash);
    }

    #[test]
    fn dry_run_prefers_test_tx_pool_accept() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "test_tx_pool_accept");
            Ok(json!({"cycles":"0x1", "fee":"0x0"}))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        rpc.dry_run_transaction(&tx).expect("accept");
    }

    #[test]
    fn dry_run_falls_back_to_estimate_cycles_when_accept_missing() {
        let url = spawn_mock(Arc::new(|method, _| match method {
            "test_tx_pool_accept" => Err((-32601, "Method not found".into())),
            "estimate_cycles" => Ok(json!({"cycles":"0x1"})),
            other => panic!("unexpected {other}"),
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        rpc.dry_run_transaction(&tx).expect("estimate ok");
    }

    #[test]
    fn dry_run_maps_verify_failure_to_submit_rejected() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "test_tx_pool_accept");
            Err((-302, "TransactionFailedToVerify".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.dry_run_transaction(&tx).unwrap_err();
        assert!(matches!(err, RpcError::SubmitRejected(_)), "{err:?}");
    }

    #[test]
    fn script_hash_type_molecule_json_round_trip() {
        use super::{json_to_script, script_json};
        for ht in [
            ScriptHashType::Type,
            ScriptHashType::Data,
            ScriptHashType::Data1,
            ScriptHashType::Data2,
        ] {
            let s = Script::new_builder()
                .code_hash(Byte32::from_slice(&[ht as u8; 32]).unwrap())
                .hash_type(ht.into())
                .args(vec![0xab, 0xcd].pack())
                .build();
            let j = script_json(&s).unwrap();
            let back = json_to_script(&j).unwrap();
            assert_eq!(back, s);
        }
        let bad = json!({
            "code_hash": hex_bytes(&[0u8; 32]),
            "hash_type": "not_a_real_hash_type",
            "args": "0x",
        });
        assert!(json_to_script(&bad).is_err());
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
                2 => json!({"tx_status":{"status":"unknown"}}),
                3 => json!({"tx_status":{"status":"rejected"}}),
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
    fn committed_status_reads_block_number_and_header() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let url = spawn_mock(Arc::new(move |method, _| {
            let i = n2.fetch_add(1, Ordering::SeqCst);
            match (i, method) {
                (0, "get_transaction") => Ok(json!({
                    "tx_status": {"status": "committed", "block_number": "0xa"}
                })),
                (1, "get_transaction") => Ok(json!({
                    "tx_status": {
                        "status": "committed",
                        "block_hash": format!("0x{}", "ab".repeat(32))
                    }
                })),
                (2, "get_header") => Ok(json!({"number": "0x15"})),
                (3, "get_tip_block_number") => Ok(json!("0x20")),
                other => Err((-1, format!("unexpected {other:?}"))),
            }
        }));
        let rpc = CkbRpc::new(&url, &url);
        let hash = Byte32::from_slice(&[1u8; 32]).unwrap();
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Committed { block_number: 10 }
        );
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Committed { block_number: 21 }
        );
        assert_eq!(rpc.tip_block_number().unwrap(), 32);
    }

    fn free_localhost_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        port
    }

    #[test]
    fn rpc_transport_error_omits_url_credentials_and_query_tokens() {
        let port = free_localhost_port();
        let secret_url =
            format!("http://user:SECRET_MARKER@127.0.0.1:{port}/v1?api_key=prefix@TOKEN_MARKER");
        let rpc = CkbRpc::new(&secret_url, &secret_url);
        let err = rpc.tip_block_number().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("get_tip_block_number"),
            "method should remain: {msg}"
        );
        assert!(
            msg.contains("transport") || msg.contains("HTTP"),
            "expected transport/HTTP category: {msg}"
        );
        for leak in [
            "SECRET_MARKER",
            "TOKEN_MARKER",
            "user:",
            "api_key",
            "prefix@",
            &format!("127.0.0.1:{port}"),
        ] {
            assert!(
                !msg.contains(leak),
                "RpcError must not embed {leak:?}: {msg}"
            );
        }
    }

    #[test]
    fn rpc_http_status_error_omits_url() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf);
                let body = b"nope";
                let header = format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(body);
            }
        });
        let secret_url = format!("http://user:SECRET_MARKER@{addr}/v1?api_key=prefix@TOKEN_MARKER");
        // Brief settle for accept thread.
        thread::sleep(Duration::from_millis(20));
        let rpc = CkbRpc::new(&secret_url, &secret_url);
        let err = rpc.tip_block_number().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("HTTP 500") || msg.contains("transport"),
            "{msg}"
        );
        assert!(!msg.contains("SECRET_MARKER"), "{msg}");
        assert!(!msg.contains("TOKEN_MARKER"), "{msg}");
        assert!(!msg.contains("user:"), "{msg}");
    }

    #[test]
    fn send_transaction_pool_is_full_is_unavailable_not_rejected() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((-1106, "PoolIsFull: the pool has reached its limit".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(
            matches!(err, RpcError::Unavailable(ref m) if m.to_ascii_lowercase().contains("pool")),
            "PoolIsFull must be retryable, got {err:?}"
        );
    }

    #[test]
    fn send_transaction_ancestors_limit_is_unavailable() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((
                -1105,
                "PoolRejectedTransactionByMaxAncestorsCountLimit".into(),
            ))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(
            matches!(err, RpcError::Unavailable(_)),
            "max-ancestors is retryable, got {err:?}"
        );
    }

    #[test]
    fn send_transaction_malformed_is_permanent_reject() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((-1108, "PoolRejectedMalformedTransaction".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(
            matches!(err, RpcError::SubmitRejected(_)),
            "malformed tx is permanent, got {err:?}"
        );
    }

    #[test]
    fn send_transaction_verify_failed_is_permanent_reject() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((-302, "TransactionFailedToVerify: script".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(
            matches!(err, RpcError::SubmitRejected(_)),
            "verify failure is permanent, got {err:?}"
        );
    }

    #[test]
    fn send_transaction_unknown_code_is_unavailable_not_rejected() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "send_transaction");
            Err((-9999, "mysterious node error".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let tx = ckb_types::core::TransactionBuilder::default().build();
        let err = rpc.submit_transaction(&tx).unwrap_err();
        assert!(
            matches!(err, RpcError::Unavailable(_)),
            "unknown codes must not blacklist, got {err:?}"
        );
    }

    #[test]
    fn committed_explicit_height_zero_is_genesis_not_unknown() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "get_transaction");
            Ok(json!({"tx_status":{"status":"committed","block_number":"0x0"}}))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let hash = Byte32::from_slice(&[1u8; 32]).unwrap();
        assert_eq!(
            rpc.get_transaction_status(&hash).unwrap(),
            TxStatus::Committed { block_number: 0 }
        );
    }

    #[test]
    fn committed_missing_or_invalid_hash_is_unavailable() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let url = spawn_mock(Arc::new(move |method, _| {
            assert_eq!(method, "get_transaction");
            let i = n2.fetch_add(1, Ordering::SeqCst);
            Ok(match i {
                0 => json!({"tx_status":{"status":"committed"}}),
                _ => json!({"tx_status":{"status":"committed","block_hash":"0x00"}}),
            })
        }));
        let rpc = CkbRpc::new(&url, &url);
        let hash = Byte32::from_slice(&[1u8; 32]).unwrap();
        let missing = rpc.get_transaction_status(&hash).unwrap_err();
        let invalid = rpc.get_transaction_status(&hash).unwrap_err();
        assert!(
            matches!(missing, RpcError::Unavailable(_)),
            "missing hash must not be Committed(0), got {missing:?}"
        );
        assert!(
            matches!(invalid, RpcError::Unavailable(_)),
            "invalid hash 0x00 must not be Committed(0), got {invalid:?}"
        );
    }

    #[test]
    fn committed_get_header_null_is_unavailable() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let url = spawn_mock(Arc::new(move |method, _| {
            let i = n2.fetch_add(1, Ordering::SeqCst);
            match (i, method) {
                (0, "get_transaction") => Ok(json!({
                    "tx_status": {
                        "status": "committed",
                        "block_hash": format!("0x{}", "ab".repeat(32))
                    }
                })),
                (1, "get_header") => Ok(Value::Null),
                other => Err((-1, format!("unexpected {other:?}"))),
            }
        }));
        let rpc = CkbRpc::new(&url, &url);
        let hash = Byte32::from_slice(&[1u8; 32]).unwrap();
        let err = rpc.get_transaction_status(&hash).unwrap_err();
        assert!(
            matches!(err, RpcError::Unavailable(_)),
            "get_header null must not be Committed(0), got {err:?}"
        );
    }

    fn digest_loop_cfg(depth: u64) -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: depth,
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    fn indexer_cell_rpc_live(obj: &Value) -> Value {
        json!({
            "status": "live",
            "cell": obj["output"].clone(),
            "data": obj.get("output_data").cloned().unwrap_or(Value::String("0x".into())),
        })
    }

    fn out_point_matches_indexer(obj: &Value, op: &Value) -> bool {
        obj.get("out_point")
            .map(|oop| oop["tx_hash"] == op["tx_hash"] && oop["index"] == op["index"])
            .unwrap_or(false)
    }

    fn dry_run_accept(method: &str) -> Option<Result<Value, (i64, String)>> {
        match method {
            "test_tx_pool_accept" | "estimate_cycles" => {
                Some(Ok(json!({"cycles":"0x1","fee":"0x0"})))
            }
            _ => None,
        }
    }

    fn host_query_or_dry_run(
        method: &str,
        params: &Value,
        cells: &[Value],
    ) -> Result<Value, (i64, String)> {
        dry_run_accept(method)
            .or_else(|| host_query_for_indexer_cells(method, params, cells))
            .unwrap_or_else(|| Err((-1, format!("unexpected {method}"))))
    }

    fn host_query_for_indexer_cells(
        method: &str,
        params: &Value,
        cells: &[Value],
    ) -> Option<Result<Value, (i64, String)>> {
        match method {
            "get_block_hash" => Some(Ok(Value::String(format!("0x{}", "ee".repeat(32))))),
            "get_live_cell" => {
                let op = &params[0];
                let found = cells.iter().find(|c| out_point_matches_indexer(c, op));
                found.map(|c| Ok(indexer_cell_rpc_live(c)))
            }
            _ => None,
        }
    }

    fn live_intent_and_process() -> (Value, Value, [u8; 32]) {
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts).unwrap();
        let args = cid_args(hash);
        let intent = indexer_cell(1, 0, lock_json(CID_CODE, &args), None, 100_0000_0000, &[]);
        let code_hash = cid_backend_core::type_id_script_hash(&PROC_CODE_ARGS);
        let proc = indexer_cell(
            0x50,
            0,
            lock_json(code_hash, &[]),
            Some(type_json),
            200_0000_0000,
            &[0x7f, b'E', b'L', b'F', 1],
        );
        (intent, proc, hash)
    }

    #[test]
    fn digest_loop_retries_after_pool_is_full_without_rejecting() {
        let (intent, proc, _) = live_intent_and_process();
        let sends = Arc::new(AtomicUsize::new(0));
        let sends2 = sends.clone();
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
                let n = sends2.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err((-1106, "PoolIsFull".into()))
                } else {
                    Ok(Value::String(hex_bytes(&[0xab; 32])))
                }
            }
            "get_transaction" => Ok(json!({"tx_status":{"status":"pending"}})),
            "get_tip_block_number" => Ok(json!("0x0")),
            other => host_query_or_dry_run(other, &params, &[intent.clone(), proc.clone()]),
        }));
        let mut loop_ = DigestLoop::new(
            CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]),
            NopClock,
            StubSsri,
            digest_loop_cfg(1),
        );
        register_fixture_process(&mut loop_, 1);
        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "pool full must not count as submit");
        assert_eq!(sends.load(Ordering::SeqCst), 1);
        let intent_op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[1u8; 32]).unwrap())
            .index(0u32.pack())
            .build();
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .map(|s| s.rejected.contains(&out_point_key(&intent_op)))
                .unwrap_or(false),
            "PoolIsFull must not blacklist the Intention"
        );
        let second = loop_.tick();
        assert_eq!(second.submitted, 1, "retry after PoolIsFull must succeed");
        assert_eq!(sends.load(Ordering::SeqCst), 2);
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .map(|s| s.rejected.contains(&out_point_key(&intent_op)))
            .unwrap_or(false));
    }

    #[test]
    fn digest_loop_header_null_does_not_settle_then_recovers() {
        let (intent, proc, _) = live_intent_and_process();
        let headers = Arc::new(AtomicUsize::new(0));
        let headers2 = headers.clone();
        let tip = Arc::new(AtomicUsize::new(0x64));
        let tip2 = tip.clone();
        let url = spawn_mock(Arc::new(move |method, params| match method {
            "get_cells" => {
                let script_type = params[0]["script_type"].as_str().unwrap_or("");
                if script_type == "lock" {
                    Ok(cells_result(vec![intent.clone()]))
                } else {
                    Ok(cells_result(vec![proc.clone()]))
                }
            }
            "send_transaction" => Ok(Value::String(hex_bytes(&[0xab; 32]))),
            "get_transaction" => Ok(json!({
                "tx_status": {
                    "status": "committed",
                    "block_hash": format!("0x{}", "cd".repeat(32))
                }
            })),
            "get_header" => {
                let n = headers2.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(json!({"number": "0x5a"}))
                }
            }
            "get_tip_block_number" => Ok(json!(format!("0x{:x}", tip2.load(Ordering::SeqCst)))),
            other => host_query_or_dry_run(other, &params, &[intent.clone(), proc.clone()]),
        }));
        let mut loop_ = DigestLoop::new(
            CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]),
            NopClock,
            StubSsri,
            digest_loop_cfg(24),
        );
        register_fixture_process(&mut loop_, 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            loop_
                .state()
                .submit
                .as_ref()
                .map(|s| s.pending.len())
                .unwrap_or(0),
            1
        );

        let after_null = loop_.tick();
        assert_eq!(after_null.submitted, 0);
        assert_eq!(
            loop_
                .state()
                .submit
                .as_ref()
                .map(|s| s.pending.len())
                .unwrap_or(0),
            1,
            "get_header null must not settle (would be Committed(0) vs tip=100)"
        );
        assert_eq!(headers.load(Ordering::SeqCst), 1);

        let after_height = loop_.tick();
        assert_eq!(after_height.submitted, 0);
        assert_eq!(
            loop_
                .state()
                .submit
                .as_ref()
                .map(|s| s.pending.len())
                .unwrap_or(0),
            1,
            "commit height 90 / tip 100 / depth 24 still confirming"
        );

        tip.store(0x78, Ordering::SeqCst);
        let settled = loop_.tick();
        assert_eq!(settled.submitted, 0);
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .map(|s| s.pending.is_empty())
                .unwrap_or(true),
            "known height + enough confirmations must settle"
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
        let type_json = script_json(&ts).unwrap();
        let args = cid_args(hash);
        let intent = indexer_cell(1, 0, lock_json(CID_CODE, &args), None, 100_0000_0000, &[]);
        let code_hash = cid_backend_core::type_id_script_hash(&PROC_CODE_ARGS);
        let proc = indexer_cell(
            0x50,
            0,
            lock_json(code_hash, &[]),
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
            "get_tip_block_number" => Ok(json!("0x0")),
            other => host_query_or_dry_run(other, &params, &[intent.clone(), proc.clone()]),
        }));

        // The handler above always returns both cells for every get_cells, including
        // type-script searches. Filter happens in the adapter by script hash.
        let rpc = CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]);
        let cfg = LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            max_assembled_tx_bytes: 512 * 1024,
        };
        let mut first = DigestLoop::new(
            CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]),
            NopClock,
            StubSsri,
            cfg,
        );
        register_fixture_process(&mut first, 1);
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
                type_id_code_deps: vec![],
                confirmation_depth: 1,
                max_assembled_tx_bytes: 512 * 1024,
            },
        );
        register_fixture_process(&mut second, 1);
        let r2 = second.tick();
        assert_eq!(r2.submitted, 1, "restart resubmits; no persisted occupancy");
        assert_eq!(*submitted.lock().unwrap(), 2);
    }

    fn cursor_start(params: &Value) -> usize {
        match params.get(3) {
            None | Some(Value::Null) => 0,
            Some(Value::String(s)) => {
                let t = s.trim().trim_start_matches("0x").trim_start_matches("0X");
                if t.is_empty() {
                    0
                } else {
                    usize::from_str_radix(t, 16).unwrap_or(0)
                }
            }
            _ => 0,
        }
    }

    fn paged_objects(total: usize, start: usize, cell: impl Fn(u32) -> Value) -> Value {
        let end = (start + PAGE_LIMIT).min(total);
        let objects: Vec<Value> = (start..end).map(|i| cell(i as u32)).collect();
        let last_cursor = if end < total || objects.len() == PAGE_LIMIT {
            format!("0x{end:x}")
        } else {
            "0x".into()
        };
        json!({ "objects": objects, "last_cursor": last_cursor })
    }

    /// Full page with no usable cursor must not be reported as a complete scan.
    #[test]
    fn full_page_without_cursor_is_unavailable_not_silent_truncate() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "get_cells");
            let objects: Vec<Value> = (0..PAGE_LIMIT as u32)
                .map(|i| indexer_cell(1, i, lock_json(CID_CODE, &[0x01]), None, 1, &[]))
                .collect();
            Ok(json!({ "objects": objects, "last_cursor": "0x" }))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let err = rpc
            .list_live_intentions(&CID_CODE, ScriptHashType::Type)
            .unwrap_err();
        assert!(
            matches!(err, RpcError::Unavailable(ref m) if m.contains("truncated")),
            "{err:?}"
        );
    }

    /// Old MAX_PAGES=64 dropped cells 6400+. Consecutive scans both return the
    /// full live set (adapter has no sticky cursor; each call starts at Null).
    #[test]
    fn lists_more_than_6400_matching_cells_across_consecutive_scans() {
        const TOTAL: usize = 6500;
        let starts = Arc::new(Mutex::new(Vec::<Option<usize>>::new()));
        let starts2 = starts.clone();
        let url = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            let start = cursor_start(&params);
            starts2.lock().unwrap().push(params.get(3).and_then(|v| {
                if v.is_null() {
                    None
                } else {
                    Some(start)
                }
            }));
            Ok(paged_objects(TOTAL, start, |i| {
                indexer_cell(
                    1,
                    i,
                    lock_json(CID_CODE, &[0x01]),
                    None,
                    100 + i as u64,
                    &[],
                )
            }))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let first = rpc
            .list_live_intentions(&CID_CODE, ScriptHashType::Type)
            .expect("scan 1");
        let second = rpc
            .list_live_intentions(&CID_CODE, ScriptHashType::Type)
            .expect("scan 2");
        assert_eq!(first.len(), TOTAL, "first scan must not stop at 6400");
        assert_eq!(second.len(), TOTAL, "second scan must also be complete");
        assert_eq!(first.last().unwrap().capacity(), 100 + 6499);
        let log = starts.lock().unwrap().clone();
        let null_resets = log.iter().filter(|s| s.is_none()).count();
        assert_eq!(null_resets, 2, "each scan starts after=Null, got {log:?}");
        assert!(
            log.len() >= 66 * 2,
            "expected >=66 pages per scan (65 full + remainder), got {}",
            log.len()
        );
    }

    #[test]
    fn finds_process_cell_on_page_65() {
        let ts = process_type_script(7);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts).unwrap();
        const TARGET: u32 = 6400; // first cell of page 65 (0-based, PAGE=100)
        const TOTAL: usize = 6401;
        let url = spawn_mock(Arc::new(move |method, params| {
            assert_eq!(method, "get_cells");
            let code = params[0]["script"]["code_hash"].as_str().unwrap();
            let ht = params[0]["script"]["hash_type"].as_str().unwrap();
            if code == hex_bytes(&cid_backend_core::TYPE_ID_CODE_HASH) && ht == "type" {
                let start = cursor_start(&params);
                Ok(paged_objects(TOTAL, start, |i| {
                    let ty = if i == TARGET {
                        type_json.clone()
                    } else {
                        json_script(
                            &cid_backend_core::TYPE_ID_CODE_HASH,
                            ScriptHashType::Type,
                            &i.to_be_bytes(),
                        )
                    };
                    indexer_cell(
                        0x50,
                        i,
                        lock_json([0; 32], &[]),
                        Some(ty),
                        200,
                        &[0x7f, b'E', b'L', b'F'],
                    )
                }))
            } else {
                Ok(cells_result(vec![]))
            }
        }));
        let rpc = CkbRpc::new(&url, &url);
        let cell = rpc
            .get_live_process_by_type_hash(&hash)
            .expect("rpc")
            .expect("process on page 65 must be found");
        let idx: u32 = cell.out_point.index().unpack();
        assert_eq!(idx, TARGET);
        assert_eq!(script_hash32(&cell.type_script().unwrap()), hash);
    }

    /// Sticky unprocessable CID cells occupy the old 6400 window. The valid
    /// Intention after that window must still be scanned and submitted, and
    /// must remain visible on the next tick (not permanently starved).
    #[test]
    fn intention_beyond_6400_not_starved_by_sticky_unprocessable() {
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let type_json = script_json(&ts).unwrap();
        let args = cid_args(hash);
        let code_hash = cid_backend_core::type_id_script_hash(&PROC_CODE_ARGS);
        let proc = indexer_cell(
            0x50,
            0,
            lock_json(code_hash, &[]),
            Some(type_json),
            200_0000_0000,
            &[0x7f, b'E', b'L', b'F', 1],
        );
        const STICKY: usize = 6400;
        let valid_intent =
            indexer_cell(2, STICKY as u32, lock_json(CID_CODE, &args), None, 100_0000_0000, &[]);
        let submitted = Arc::new(Mutex::new(0usize));
        let submitted2 = submitted.clone();
        let url = spawn_mock(Arc::new(move |method, params| match method {
            "get_cells" => {
                let script_type = params[0]["script_type"].as_str().unwrap_or("");
                if script_type == "lock" {
                    let start = cursor_start(&params);
                    Ok(paged_objects(STICKY + 1, start, |i| {
                        if i as usize == STICKY {
                            indexer_cell(2, i, lock_json(CID_CODE, &args), None, 100_0000_0000, &[])
                        } else {
                            indexer_cell(
                                1,
                                i,
                                lock_json(CID_CODE, &[0xFF]),
                                None,
                                100_0000_0000,
                                &[],
                            )
                        }
                    }))
                } else {
                    Ok(cells_result(vec![proc.clone()]))
                }
            }
            "send_transaction" => {
                *submitted2.lock().unwrap() += 1;
                Ok(Value::String(hex_bytes(&[0xab; 32])))
            }
            "get_transaction" => Ok(json!({"tx_status": {"status": "pending"}})),
            "get_tip_block_number" => Ok(json!("0x0")),
            other => host_query_or_dry_run(other, &params, &[valid_intent.clone(), proc.clone()]),
        }));
        let cfg = LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            max_assembled_tx_bytes: 512 * 1024,
        };
        let mut loop_ = DigestLoop::new(
            CkbRpc::with_process_code_hashes(&url, &url, vec![PROC_CODE]),
            NopClock,
            StubSsri,
            cfg,
        );
        register_fixture_process(&mut loop_, 1);
        let r1 = loop_.tick();
        assert_eq!(
            r1.scanned, 1,
            "decoded live-set must include the Intention past the old 6400 window"
        );
        assert_eq!(r1.submitted, 1, "valid Intention must not be starved");
        assert_eq!(*submitted.lock().unwrap(), 1);

        let r2 = loop_.tick();
        assert_eq!(
            r2.scanned, 1,
            "consecutive tick still sees the Intention (complete rescan, not a stuck 6400 window)"
        );
        assert_eq!(
            r2.submitted, 0,
            "pending occupancy holds; sticky cells must not hide it from scan"
        );
    }

    #[test]
    fn host_query_get_live_cell_block_hash_and_cells_page() {
        let url = spawn_mock(Arc::new(|method, params| match method {
            "get_live_cell" => {
                let op = &params[0];
                assert_eq!(
                    op.get("index").and_then(|v| v.as_str()),
                    Some("0x0")
                );
                Ok(json!({
                    "cell": {
                        "capacity": "0x64",
                        "lock": lock_json(CID_CODE, &[]),
                        "type": null,
                    },
                    "data": "0xab",
                    "status": "live",
                }))
            }
            "get_block_hash" => Ok(Value::String(format!("0x{}", "cd".repeat(32)))),
            "get_cells" => {
                assert_eq!(params[2].as_str(), Some("0x32"));
                Ok(cells_result(vec![indexer_cell(
                    1,
                    0,
                    lock_json(CID_CODE, &[]),
                    None,
                    100,
                    &[0x01],
                )]))
            }
            other => Err((-1, format!("unexpected {other}"))),
        }));
        let rpc = CkbRpc::new(&url, &url);
        let op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x11; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let live = HostQueryRpc::get_live_cell(&rpc, &op).unwrap().expect("cell");
        assert_eq!(live.data, vec![0xab]);
        let hash = HostQueryRpc::get_block_hash(&rpc, 9).unwrap().expect("hash");
        assert_eq!(hash, [0xcd; 32]);
        let key = json!({
            "script": json_script(&CID_CODE, ScriptHashType::Type, &[]),
            "script_type": "lock",
            "script_search_mode": "prefix",
            "with_data": true,
        });
        let (page, cursor) = HostQueryRpc::get_cells_page(
            &rpc,
            &serde_json::to_vec(&key).unwrap(),
            DEFAULT_HOST_QUERY_PAGE_LIMIT,
            &[],
        )
        .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(cursor, b"0x");
    }

    #[test]
    fn verify_declared_genesis_matches() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "get_block_hash");
            Ok(Value::String(format!("0x{}", "ab".repeat(32))))
        }));
        let rpc = CkbRpc::new(&url, &url);
        rpc.verify_declared_genesis(&[0xabu8; 32]).unwrap();
    }

    #[test]
    fn verify_declared_genesis_mismatch_fails_closed() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "get_block_hash");
            Ok(Value::String(format!("0x{}", "ab".repeat(32))))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let err = rpc.verify_declared_genesis(&[0xcdu8; 32]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fail-closed"), "{msg}");
        assert!(msg.contains("mismatch"), "{msg}");
    }

    #[test]
    fn verify_declared_genesis_rpc_error_fails_closed() {
        let url = spawn_mock(Arc::new(|method, _| {
            assert_eq!(method, "get_block_hash");
            Err((-1, "unavailable".into()))
        }));
        let rpc = CkbRpc::new(&url, &url);
        let err = rpc.verify_declared_genesis(&[0xabu8; 32]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fail-closed"), "{msg}");
    }
}
