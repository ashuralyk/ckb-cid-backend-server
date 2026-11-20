//! HTTP API for Process registry: register / delete / list / get.
//!
//! Keyless. No secrets in request or response bodies. Bind via `observe.api_bind`
//! / `CID_API_BIND`. Updates the on-disk registry and enqueues live worker
//! spawn/stop commands for the digest loop.

use cid_backend_core::process::{state_is_process_lock, validate_state_cid_binding};
use cid_backend_core::rpc::{LiveCell, RpcError};
use cid_backend_core::{
    type_id_script_hash, CidLockIdentity, ProcessBusyMap, ProcessRegistration, ProcessRegistry,
    RegisterOutcome, RegistryError, WorkerCommand,
};
use ckb_types::packed::Script;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MAX_BODY_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_API_WORKERS: usize = 32;

type FindProcessState = dyn Fn(&Script) -> Result<Vec<LiveCell>, RpcError> + Send + Sync;

#[derive(Clone)]
pub struct RegistryApiState {
    pub registry: Arc<Mutex<ProcessRegistry>>,
    pub worker_cmds: Arc<Mutex<Vec<WorkerCommand>>>,
    /// Shared with the digest loop for pending/idle checks (P1-6).
    pub busy: ProcessBusyMap,
    pub cid: CidLockIdentity,
    pub find_process_state: Arc<FindProcessState>,
}

pub struct RegistryApiHandle {
    pub local_addr: SocketAddr,
}

pub fn spawn_registry_api(
    bind: SocketAddr,
    state: RegistryApiState,
) -> io::Result<RegistryApiHandle> {
    let listener = TcpListener::bind(bind)?;
    let local_addr = listener.local_addr()?;
    let workers = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name("cid-registry-api".into())
        .spawn(move || {
            for incoming in listener.incoming() {
                let Ok(stream) = incoming else {
                    continue;
                };
                let prev = workers.fetch_add(1, Ordering::AcqRel);
                if prev >= MAX_API_WORKERS {
                    workers.fetch_sub(1, Ordering::AcqRel);
                    refuse_busy(stream);
                    continue;
                }
                let state = state.clone();
                let worker_slot = workers.clone();
                match thread::Builder::new()
                    .name("cid-registry-api-conn".into())
                    .spawn(move || {
                        let _guard = WorkerGuard(worker_slot);
                        handle_stream(stream, &state);
                    }) {
                    Ok(_) => {}
                    Err(_) => {
                        workers.fetch_sub(1, Ordering::AcqRel);
                    }
                }
            }
        })?;
    Ok(RegistryApiHandle { local_addr })
}

struct WorkerGuard(Arc<AtomicUsize>);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn refuse_busy(mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_nonblocking(true);
    write_http(
        &mut stream,
        503,
        "Service Unavailable",
        "text/plain; charset=utf-8",
        b"busy\n",
    );
    close_stream(stream);
}

#[derive(Deserialize)]
struct RegisterBody {
    identity_type_id_args: String,
    #[serde(default)]
    identity_type_id: Option<String>,
    code_type_id_args: String,
    #[serde(default)]
    code_type_id: Option<String>,
}

#[derive(Serialize)]
struct ProcessJson {
    identity_type_hash: String,
    identity_type_id_args: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_type_id: Option<String>,
    code_type_id_args: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code_type_id: Option<String>,
}

fn handle_stream(mut stream: TcpStream, state: &RegistryApiState) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let parsed = match read_request(&mut stream) {
        Ok(p) => p,
        Err(HeadError::Timeout) => {
            write_http(
                &mut stream,
                408,
                "Request Timeout",
                "text/plain; charset=utf-8",
                b"timeout\n",
            );
            close_stream(stream);
            return;
        }
        Err(HeadError::TooLarge) => {
            write_http(
                &mut stream,
                413,
                "Payload Too Large",
                "text/plain; charset=utf-8",
                b"body too large\n",
            );
            close_stream(stream);
            return;
        }
        Err(_) => {
            close_stream(stream);
            return;
        }
    };
    let path = parsed.path.split('?').next().unwrap_or("/");
    let (status, reason, ctype, body) =
        match route(parsed.method.as_str(), path, &parsed.body, state) {
            Ok(r) => r,
            Err(()) => (
                500,
                "Internal Server Error",
                "text/plain; charset=utf-8",
                b"error\n".to_vec(),
            ),
        };
    write_http(&mut stream, status, reason, ctype, &body);
    close_stream(stream);
}

fn route(
    method: &str,
    path: &str,
    body: &[u8],
    state: &RegistryApiState,
) -> Result<(u16, &'static str, &'static str, Vec<u8>), ()> {
    if path == "/v1/processes" {
        return match method {
            "GET" => list_processes(state),
            "POST" | "PUT" => register_process(body, state),
            _ => Ok(method_not_allowed()),
        };
    }
    if let Some(id) = path.strip_prefix("/v1/processes/") {
        if id.is_empty() || id.contains('/') {
            return Ok(not_found());
        }
        return match method {
            "GET" => get_process(id, state),
            "DELETE" => delete_process(id, state),
            _ => Ok(method_not_allowed()),
        };
    }
    Ok(not_found())
}

fn list_processes(
    state: &RegistryApiState,
) -> Result<(u16, &'static str, &'static str, Vec<u8>), ()> {
    let reg = state.registry.lock().map_err(|_| ())?;
    let list: Vec<ProcessJson> = reg
        .list()
        .into_iter()
        .map(|p| to_json(p.identity_type_hash(), p))
        .collect();
    let bytes = serde_json::to_vec(&list).map_err(|_| ())?;
    Ok((200, "OK", "application/json", bytes))
}

fn get_process(
    id: &str,
    state: &RegistryApiState,
) -> Result<(u16, &'static str, &'static str, Vec<u8>), ()> {
    let hash = match parse_hex32(id) {
        Ok(h) => h,
        Err(_) => return Ok(bad_request(b"invalid identity_type_hash\n")),
    };
    let reg = state.registry.lock().map_err(|_| ())?;
    match reg.get(&hash) {
        Some(p) => {
            let bytes = serde_json::to_vec(&to_json(hash, p)).map_err(|_| ())?;
            Ok((200, "OK", "application/json", bytes))
        }
        None => Ok(not_found()),
    }
}

fn register_process(
    body: &[u8],
    state: &RegistryApiState,
) -> Result<(u16, &'static str, &'static str, Vec<u8>), ()> {
    let req: RegisterBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return Ok(bad_request(b"invalid json\n")),
    };
    let reg_entry = match parse_registration(&req) {
        Ok(r) => r,
        Err(msg) => return Ok(bad_request(msg.as_bytes())),
    };
    let entry = match reg_entry.to_process_entry() {
        Ok(e) => e,
        Err(e) => {
            return Ok(bad_request(format!("{e}\n").as_bytes()));
        }
    };
    let cells = match (state.find_process_state)(&entry.identity.type_script()) {
        Ok(cells) => cells,
        Err(error) => {
            tracing::warn!(target: "cid_backend", event = "registry.state_lookup_failed", %error);
            return Ok((
                503,
                "Service Unavailable",
                "text/plain; charset=utf-8",
                b"could not look up live Process state; retry later\n".to_vec(),
            ));
        }
    };
    let [cell] = cells.as_slice() else {
        return Ok(bad_request(
            b"expected one live Process state cell for identity_type_id_args\n",
        ));
    };
    if !state_is_process_lock(&entry, cell) {
        return Ok(bad_request(
            b"live Process state must use the registered Process code Type-ID lock\n",
        ));
    }
    if let Err(reason) = validate_state_cid_binding(cell, state.cid) {
        return Ok(bad_request(format!("{reason}\n").as_bytes()));
    }

    // Single critical section: registry persist + worker-cmd enqueue (P0-2).
    // Lock order: registry → workers → worker_cmds.
    let outcome = {
        let mut reg = state.registry.lock().map_err(|_| ())?;
        let hash = reg_entry.identity_type_hash();
        if let Some(existing) = reg.get(&hash) {
            let code_changing = existing.code_type_id_args != reg_entry.code_type_id_args
                || existing.code_type_id != reg_entry.code_type_id;
            if code_changing && state.busy.is_pending(&hash) {
                return Ok((
                    409,
                    "Conflict",
                    "text/plain; charset=utf-8",
                    b"process has in-flight submits; defer code re-bind until idle\n".to_vec(),
                ));
            }
        }
        let outcome = match reg.register(reg_entry.clone()) {
            Ok(o) => o,
            Err(RegistryError::Full) => {
                return Ok((
                    409,
                    "Conflict",
                    "text/plain; charset=utf-8",
                    b"registry full\n".to_vec(),
                ));
            }
            Err(e) => return Ok(bad_request(format!("{e}\n").as_bytes())),
        };
        let mut q = state.worker_cmds.lock().map_err(|_| ())?;
        // Cancel pending deletes for this identity before enqueueing Register (P0-1).
        q.retain(|c| {
            !matches!(
                c,
                WorkerCommand::Delete { hash: h, .. } if *h == hash
            )
        });
        q.push(WorkerCommand::Register(entry));
        outcome
    };
    let hash = reg_entry.identity_type_hash();
    tracing::info!(
        target: "cid_backend",
        event = "registry.api.register",
        outcome = ?outcome,
        process_type_hash = %hex_pref(&hash),
    );
    let bytes = serde_json::to_vec(&to_json(hash, &reg_entry)).map_err(|_| ())?;
    let (code, reason) = match outcome {
        RegisterOutcome::Created => (201, "Created"),
        RegisterOutcome::Updated => (200, "OK"),
    };
    Ok((code, reason, "application/json", bytes))
}

fn delete_process(
    id: &str,
    state: &RegistryApiState,
) -> Result<(u16, &'static str, &'static str, Vec<u8>), ()> {
    let hash = match parse_hex32(id) {
        Ok(h) => h,
        Err(_) => return Ok(bad_request(b"invalid identity_type_hash\n")),
    };
    {
        let mut reg = state.registry.lock().map_err(|_| ())?;
        if !reg.delete(&hash).map_err(|_| ())? {
            return Ok(not_found());
        }
        let mut q = state.worker_cmds.lock().map_err(|_| ())?;
        q.push(WorkerCommand::delete(hash));
    }
    tracing::info!(
        target: "cid_backend",
        event = "registry.api.delete",
        process_type_hash = %hex_pref(&hash),
    );
    Ok((204, "No Content", "text/plain; charset=utf-8", Vec::new()))
}

fn parse_registration(req: &RegisterBody) -> Result<ProcessRegistration, String> {
    Ok(ProcessRegistration {
        identity_type_id_args: parse_hex32(&req.identity_type_id_args)
            .map_err(|_| "identity_type_id_args must be 32-byte hex".to_string())?,
        identity_type_id: match &req.identity_type_id {
            Some(s) => Some(
                parse_hex32(s).map_err(|_| "identity_type_id must be 32-byte hex".to_string())?,
            ),
            None => None,
        },
        code_type_id_args: parse_hex32(&req.code_type_id_args)
            .map_err(|_| "code_type_id_args must be 32-byte hex".to_string())?,
        code_type_id: match &req.code_type_id {
            Some(s) => {
                Some(parse_hex32(s).map_err(|_| "code_type_id must be 32-byte hex".to_string())?)
            }
            None => None,
        },
    })
}

fn to_json(hash: [u8; 32], p: &ProcessRegistration) -> ProcessJson {
    ProcessJson {
        identity_type_hash: hex_pref(&hash),
        identity_type_id_args: hex_pref(&p.identity_type_id_args),
        identity_type_id: p.identity_type_id.as_ref().map(|h| hex_pref(h)),
        code_type_id_args: hex_pref(&p.code_type_id_args),
        code_type_id: p.code_type_id.as_ref().map(|h| hex_pref(h)),
    }
}

fn hex_pref(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s
}

fn parse_hex32(s: &str) -> Result<[u8; 32], ()> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if s.len() != 64 {
        return Err(());
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| ())?;
    }
    // Ensure callers that pass a full type-id as "args" by mistake still get
    // length validation only here; registry.validate checks derived match.
    let _ = type_id_script_hash(&out);
    Ok(out)
}

fn not_found() -> (u16, &'static str, &'static str, Vec<u8>) {
    (
        404,
        "Not Found",
        "text/plain; charset=utf-8",
        b"not found\n".to_vec(),
    )
}

fn method_not_allowed() -> (u16, &'static str, &'static str, Vec<u8>) {
    (
        405,
        "Method Not Allowed",
        "text/plain; charset=utf-8",
        b"method not allowed\n".to_vec(),
    )
}

fn bad_request(msg: &[u8]) -> (u16, &'static str, &'static str, Vec<u8>) {
    (
        400,
        "Bad Request",
        "text/plain; charset=utf-8",
        msg.to_vec(),
    )
}

struct RequestParsed {
    method: String,
    path: String,
    body: Vec<u8>,
}

enum HeadError {
    Timeout,
    Closed,
    TooLarge,
    Invalid,
}

fn read_request(stream: &mut TcpStream) -> Result<RequestParsed, HeadError> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    let header_end;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(HeadError::Timeout);
        }
        let _ = stream.set_read_timeout(Some(remaining));
        match stream.read(&mut tmp) {
            Ok(0) => return Err(HeadError::Closed),
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() > MAX_HEADER_BYTES {
                    return Err(HeadError::TooLarge);
                }
                if let Some(pos) = find_header_end(&buf) {
                    header_end = pos;
                    break;
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                return Err(HeadError::Timeout);
            }
            Err(_) => return Err(HeadError::Closed),
        }
    }
    let head = std::str::from_utf8(&buf[..header_end]).map_err(|_| HeadError::Invalid)?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or(HeadError::Invalid)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or(HeadError::Invalid)?.to_string();
    let path = parts.next().ok_or(HeadError::Invalid)?.to_string();
    let mut body_len = 0usize;
    let mut chunked = false;
    for line in lines {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            body_len = v.trim().parse().map_err(|_| HeadError::Invalid)?;
        }
        if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            chunked = true;
        }
    }
    if chunked {
        return Err(HeadError::Invalid);
    }
    if body_len > MAX_BODY_BYTES {
        return Err(HeadError::TooLarge);
    }
    let mut body = buf[header_end..].to_vec();
    while body.len() < body_len {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(HeadError::Timeout);
        }
        let _ = stream.set_read_timeout(Some(remaining));
        let mut tmp = [0u8; 512];
        match stream.read(&mut tmp) {
            Ok(0) => return Err(HeadError::Closed),
            Ok(n) => body.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                return Err(HeadError::Timeout);
            }
            Err(_) => return Err(HeadError::Closed),
        }
        if body.len() > MAX_BODY_BYTES {
            return Err(HeadError::TooLarge);
        }
    }
    body.truncate(body_len);
    Ok(RequestParsed { method, path, body })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn write_http(stream: &mut TcpStream, status: u16, reason: &str, ctype: &str, body: &[u8]) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn close_stream(stream: TcpStream) {
    let _ = stream.shutdown(Shutdown::Both);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid_backend_core::{type_id_script_hash, DEFAULT_MAX_REGISTRY_ENTRIES};
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{CellOutput, OutPoint};
    use ckb_types::prelude::*;
    use std::net::TcpStream;

    const CID: CidLockIdentity = CidLockIdentity {
        code_hash: [0xC1; 32],
        hash_type: ScriptHashType::Type,
    };

    fn api_state(registry: ProcessRegistry, lock_args: Vec<u8>) -> RegistryApiState {
        RegistryApiState {
            registry: Arc::new(Mutex::new(registry)),
            worker_cmds: Arc::new(Mutex::new(Vec::new())),
            busy: ProcessBusyMap::new(),
            cid: CID,
            find_process_state: Arc::new(move |typ| {
                let lock = Script::new_builder()
                    .code_hash(type_id_script_hash(&[0x22; 32]).pack())
                    .hash_type(ScriptHashType::Type.into())
                    .args(lock_args.pack())
                    .build();
                Ok(vec![LiveCell {
                    out_point: OutPoint::default(),
                    output: CellOutput::new_builder()
                        .lock(lock)
                        .type_(Some(typ.clone()).pack())
                        .build(),
                    data: vec![],
                }])
            }),
        }
    }

    #[test]
    fn registry_rejects_legacy_state_without_persisting_or_starting_worker() {
        let (reg, path) = temp_registry();
        let state = api_state(reg, vec![1]);
        let body = serde_json::json!({
            "identity_type_id_args": hex_pref(&[0x11; 32]),
            "code_type_id_args": hex_pref(&[0x22; 32]),
        })
        .to_string();
        let (status, _, _, response) =
            route("POST", "/v1/processes", body.as_bytes(), &state).unwrap();
        assert_eq!(status, 400);
        assert!(String::from_utf8(response)
            .unwrap()
            .contains("Process state lock args (TooShort)"));
        assert!(state.registry.lock().unwrap().is_empty());
        assert!(state.worker_cmds.lock().unwrap().is_empty());
        assert!(!path.exists());
    }
    use std::time::Duration;

    fn temp_registry() -> (ProcessRegistry, std::path::PathBuf) {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("cid-reg-api-{n}.json"));
        let reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        (reg, path)
    }

    fn http_exchange(addr: SocketAddr, raw: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream.write_all(raw.as_bytes()).unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
            if buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() > 32 {
                // keep reading a bit for body
                if buf.len() > 200 {
                    break;
                }
            }
        }
        let text = String::from_utf8_lossy(&buf);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (status, text.into_owned())
    }

    #[test]
    fn register_list_get_delete_round_trip() {
        let (reg, path) = temp_registry();
        let state = api_state(reg, [CID.code_hash.as_slice(), &[1]].concat());
        let handle = spawn_registry_api("127.0.0.1:0".parse().unwrap(), state.clone()).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let args = [0x11u8; 32];
        let code = [0x22u8; 32];
        let body = format!(
            r#"{{"identity_type_id_args":"0x{}","code_type_id_args":"0x{}"}}"#,
            hex::encode(args),
            hex::encode(code)
        );
        let raw = format!(
            "POST /v1/processes HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let (status, resp) = http_exchange(handle.local_addr, &raw);
        assert_eq!(status, 201, "{resp}");
        assert_eq!(state.worker_cmds.lock().unwrap().len(), 1);

        let (status, resp) = http_exchange(
            handle.local_addr,
            "GET /v1/processes HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        assert_eq!(status, 200, "{resp}");
        assert!(resp.contains(&hex::encode(type_id_script_hash(&args))));

        let id = format!("0x{}", hex::encode(type_id_script_hash(&args)));
        let (status, resp) = http_exchange(
            handle.local_addr,
            &format!("GET /v1/processes/{id} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        );
        assert_eq!(status, 200, "{resp}");

        let (status, resp) = http_exchange(
            handle.local_addr,
            &format!("DELETE /v1/processes/{id} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        );
        assert_eq!(status, 204, "{resp}");
        assert_eq!(state.worker_cmds.lock().unwrap().len(), 2);

        let (status, _) = http_exchange(
            handle.local_addr,
            &format!("GET /v1/processes/{id} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        );
        assert_eq!(status, 404);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_hex_rejected() {
        let (reg, path) = temp_registry();
        let state = api_state(reg, [CID.code_hash.as_slice(), &[1]].concat());
        let handle = spawn_registry_api("127.0.0.1:0".parse().unwrap(), state).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let body = r#"{"identity_type_id_args":"0xzz","code_type_id_args":"0x11"}"#;
        let raw = format!(
            "POST /v1/processes HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let (status, _) = http_exchange(handle.local_addr, &raw);
        assert_eq!(status, 400);
        let _ = std::fs::remove_file(path);
    }
}
