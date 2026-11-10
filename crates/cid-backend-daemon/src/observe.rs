//! In-process counters and an optional tiny HTTP health/stats server.
//!
//! Bind with `CID_HEALTH_BIND` (unset/empty = disabled). The digest loop is
//! not blocked: the listener runs on a background thread. State is memory-only.

use cid_backend_core::digest_loop::TickReport;
use serde::Serialize;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Health is GET-only. Any declared request body is rejected without draining
/// it (tiny_http 0.12 would block forever in `EqualReader::drop`).
const MAX_BODY_BYTES: usize = 0;
const MAX_HEADER_BYTES: usize = 8 * 1024;
/// Absolute deadline for reading the request head (not a per-read window).
const IO_TIMEOUT: Duration = Duration::from_secs(2);
/// Opt-in liveness port: a few dozen workers is enough for probes + scrape.
const MAX_HEALTH_WORKERS: usize = 32;

#[derive(Debug)]
pub struct DaemonStats {
    ticks: AtomicU64,
    scanned: AtomicU64,
    skipped: AtomicU64,
    submitted: AtomicU64,
    conflicts: AtomicU64,
    pending_in_flight: AtomicU64,
    last_tick_at_ms: AtomicU64,
    ready: AtomicBool,
    last_error: Mutex<Option<String>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StatsSnapshot {
    pub ticks: u64,
    pub scanned: u64,
    pub skipped: u64,
    pub submitted: u64,
    pub conflicts: u64,
    pub pending_in_flight: u64,
    pub last_tick_at: u64,
    pub last_error: Option<String>,
    pub ready: bool,
}

impl DaemonStats {
    pub fn new() -> Self {
        Self {
            ticks: AtomicU64::new(0),
            scanned: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
            pending_in_flight: AtomicU64::new(0),
            last_tick_at_ms: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            last_error: Mutex::new(None),
        }
    }

    pub fn observe(&self, report: &TickReport, pending_in_flight: usize) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
        self.scanned
            .fetch_add(report.scanned as u64, Ordering::Relaxed);
        self.skipped
            .fetch_add(report.skipped as u64, Ordering::Relaxed);
        self.submitted
            .fetch_add(report.submitted as u64, Ordering::Relaxed);
        self.conflicts
            .fetch_add(report.conflicts as u64, Ordering::Relaxed);
        self.pending_in_flight
            .store(pending_in_flight as u64, Ordering::Relaxed);
        self.last_tick_at_ms.store(now_ms(), Ordering::Relaxed);
        self.ready.store(report.scan_ok, Ordering::Relaxed);
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = report.last_error.clone();
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            ticks: self.ticks.load(Ordering::Relaxed),
            scanned: self.scanned.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            submitted: self.submitted.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            pending_in_flight: self.pending_in_flight.load(Ordering::Relaxed),
            last_tick_at: self.last_tick_at_ms.load(Ordering::Relaxed),
            last_error: self.last_error.lock().ok().and_then(|g| g.clone()),
            ready: self.is_ready(),
        }
    }

    fn prometheus(&self) -> String {
        let s = self.snapshot();
        let ready = if s.ready { 1 } else { 0 };
        format!(
            "# HELP cid_backend_ticks_total Digest loop ticks\n\
             # TYPE cid_backend_ticks_total counter\n\
             cid_backend_ticks_total {}\n\
             # TYPE cid_backend_scanned_total counter\n\
             cid_backend_scanned_total {}\n\
             # TYPE cid_backend_skipped_total counter\n\
             cid_backend_skipped_total {}\n\
             # TYPE cid_backend_submitted_total counter\n\
             cid_backend_submitted_total {}\n\
             # TYPE cid_backend_conflicts_total counter\n\
             cid_backend_conflicts_total {}\n\
             # TYPE cid_backend_pending_in_flight gauge\n\
             cid_backend_pending_in_flight {}\n\
             # TYPE cid_backend_last_tick_at_ms gauge\n\
             cid_backend_last_tick_at_ms {}\n\
             # TYPE cid_backend_ready gauge\n\
             cid_backend_ready {}\n",
            s.ticks,
            s.scanned,
            s.skipped,
            s.submitted,
            s.conflicts,
            s.pending_in_flight,
            s.last_tick_at,
            ready
        )
    }
}

impl Default for DaemonStats {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct HealthHandle {
    pub local_addr: SocketAddr,
    #[cfg(test)]
    live_workers: Arc<AtomicUsize>,
}

/// Start `/healthz`, `/readyz`, `/stats`, `/metrics` on `bind`.
///
/// Accept runs on `cid-health`; each connection is a worker with an absolute
/// 2s request-head deadline so a slow drip of bytes cannot occupy it forever.
/// At most `MAX_HEALTH_WORKERS` workers run at once; further sockets are
/// refused on the accept thread (503) and shut down. Bodies over
/// `MAX_BODY_BYTES` are rejected and the socket is shut down without draining.
pub fn spawn_health(bind: SocketAddr, stats: Arc<DaemonStats>) -> io::Result<HealthHandle> {
    let listener = TcpListener::bind(bind)?;
    let local_addr = listener.local_addr()?;
    let workers = Arc::new(AtomicUsize::new(0));
    #[cfg(test)]
    let live_workers = workers.clone();
    thread::Builder::new()
        .name("cid-health".into())
        .spawn(move || {
            for incoming in listener.incoming() {
                let Ok(stream) = incoming else {
                    continue;
                };
                let prev = workers.fetch_add(1, Ordering::AcqRel);
                if prev >= MAX_HEALTH_WORKERS {
                    workers.fetch_sub(1, Ordering::AcqRel);
                    refuse_busy(stream);
                    continue;
                }
                let stats = stats.clone();
                let worker_slot = workers.clone();
                match thread::Builder::new()
                    .name("cid-health-conn".into())
                    .spawn(move || {
                        let _guard = WorkerGuard(worker_slot);
                        handle_stream(stream, &stats);
                    }) {
                    Ok(_) => {}
                    Err(_) => {
                        // Closure (and stream) dropped without running WorkerGuard.
                        workers.fetch_sub(1, Ordering::AcqRel);
                    }
                }
            }
        })?;
    Ok(HealthHandle {
        local_addr,
        #[cfg(test)]
        live_workers,
    })
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

fn handle_stream(mut stream: TcpStream, stats: &DaemonStats) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let parsed = match read_request_head(&mut stream) {
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
        Err(_) => {
            close_stream(stream);
            return;
        }
    };
    if parsed.body_len > MAX_BODY_BYTES || parsed.chunked {
        // Do not read the declared body. Shutdown so a client that never
        // sends it cannot pin this worker.
        write_http(
            &mut stream,
            413,
            "Payload Too Large",
            "text/plain; charset=utf-8",
            b"body not allowed\n",
        );
        close_stream(stream);
        return;
    }
    let path = parsed.path.split('?').next().unwrap_or("/");
    let (status, reason, ctype, body) = match (parsed.method.as_str(), path) {
        ("GET", "/healthz") => (200, "OK", "text/plain; charset=utf-8", b"ok\n".to_vec()),
        ("GET", "/readyz") => {
            if stats.is_ready() {
                (200, "OK", "text/plain; charset=utf-8", b"ready\n".to_vec())
            } else {
                (
                    503,
                    "Service Unavailable",
                    "text/plain; charset=utf-8",
                    b"not ready\n".to_vec(),
                )
            }
        }
        ("GET", "/stats") => {
            let json = serde_json::to_string(&stats.snapshot()).unwrap_or_else(|_| "{}".into());
            (200, "OK", "application/json", json.into_bytes())
        }
        ("GET", "/metrics") => (
            200,
            "OK",
            "text/plain; version=0.0.4; charset=utf-8",
            stats.prometheus().into_bytes(),
        ),
        _ => (
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            b"not found\n".to_vec(),
        ),
    };
    write_http(&mut stream, status, reason, ctype, &body);
    close_stream(stream);
}

struct RequestHead {
    method: String,
    path: String,
    body_len: usize,
    chunked: bool,
}

enum HeadError {
    Timeout,
    Closed,
    TooLarge,
    Invalid,
}

fn read_request_head(stream: &mut TcpStream) -> Result<RequestHead, HeadError> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 512];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        // SO_RCVTIMEO of zero means "wait forever" on some platforms.
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
                if let Some(pos) = find_headers_end(&buf) {
                    return parse_head(&buf[..pos]);
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock =>
            {
                return Err(HeadError::Timeout);
            }
            Err(_) => return Err(HeadError::Closed),
        }
    }
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_head(head: &[u8]) -> Result<RequestHead, HeadError> {
    let text = std::str::from_utf8(head).map_err(|_| HeadError::Invalid)?;
    let mut lines = text.split("\r\n");
    let req_line = lines.next().ok_or(HeadError::Invalid)?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next().ok_or(HeadError::Invalid)?.to_string();
    let path = parts.next().ok_or(HeadError::Invalid)?.to_string();
    let mut body_len = 0usize;
    let mut chunked = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("Content-Length") {
            body_len = value.parse().map_err(|_| HeadError::Invalid)?;
        } else if name.eq_ignore_ascii_case("Transfer-Encoding") {
            chunked = value
                .to_ascii_lowercase()
                .split(',')
                .any(|t| t.trim() == "chunked");
        }
    }
    Ok(RequestHead {
        method,
        path,
        body_len,
        chunked,
    })
}

fn write_http(stream: &mut TcpStream, status: u16, reason: &str, ctype: &str, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    if !body.is_empty() {
        let _ = stream.write_all(body);
    }
    let _ = stream.flush();
}

fn close_stream(stream: TcpStream) {
    let _ = stream.shutdown(Shutdown::Both);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{LiveCell, Rpc, RpcError, TxStatus};
    use cid_backend_core::ssri::{SsriError, SsriHost};
    use cid_process_executor::{IntentionInput, PartialTx};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::Byte32;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::{Duration, Instant};

    fn get(url: &str) -> (u16, String) {
        let resp = ureq::get(url).call().map_err(|e| match e {
            ureq::Error::Status(code, resp) => {
                let body = resp.into_string().unwrap_or_default();
                (code, body)
            }
            other => panic!("request {url}: {other}"),
        });
        match resp {
            Ok(r) => (r.status(), r.into_string().unwrap_or_default()),
            Err((code, body)) => (code, body),
        }
    }

    #[test]
    fn healthz_live_readyz_requires_successful_scan() {
        let stats = Arc::new(DaemonStats::new());
        let h = spawn_health("127.0.0.1:0".parse().unwrap(), stats.clone()).expect("bind");
        let base = format!("http://{}", h.local_addr);

        let (code, body) = get(&format!("{base}/healthz"));
        assert_eq!(code, 200);
        assert_eq!(body, "ok\n");

        let (code, body) = get(&format!("{base}/readyz"));
        assert_eq!(code, 503);
        assert!(body.contains("not ready"));

        stats.observe(
            &TickReport {
                scan_ok: true,
                scanned: 3,
                skipped: 1,
                submitted: 1,
                conflicts: 0,
                last_error: None,
            },
            2,
        );

        let (code, _) = get(&format!("{base}/readyz"));
        assert_eq!(code, 200);

        let (code, body) = get(&format!("{base}/stats"));
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["ticks"], 1);
        assert_eq!(v["scanned"], 3);
        assert_eq!(v["skipped"], 1);
        assert_eq!(v["submitted"], 1);
        assert_eq!(v["pending_in_flight"], 2);
        assert_eq!(v["ready"], true);

        let (code, body) = get(&format!("{base}/metrics"));
        assert_eq!(code, 200);
        assert!(body.contains("cid_backend_ticks_total 1"));
        assert!(body.contains("cid_backend_ready 1"));
        assert!(body.contains("cid_backend_pending_in_flight 2"));
    }

    /// A client that declares a body and never sends it must not pin `/healthz`.
    #[test]
    fn incomplete_request_body_does_not_stall_healthz() {
        let stats = Arc::new(DaemonStats::new());
        let h = spawn_health("127.0.0.1:0".parse().unwrap(), stats).expect("bind");
        let mut slow = TcpStream::connect(h.local_addr).expect("slow connect");
        let _ = slow.set_nodelay(true);
        slow.write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2048\r\n\r\n")
            .expect("slow headers");
        let _ = slow.flush();

        let start = Instant::now();
        let (code, body) = get(&format!("http://{}/healthz", h.local_addr));
        let elapsed = start.elapsed();
        assert_eq!(code, 200, "second /healthz: {body}");
        assert_eq!(body, "ok\n");
        assert!(
            elapsed < Duration::from_millis(800),
            "second /healthz stalled for {elapsed:?} behind an incomplete body"
        );
        drop(slow);
    }

    /// A client that drips header bytes slower than the *total* 2s deadline
    /// (but faster than a naive per-read 2s timeout) must be closed with 408
    /// without pinning a second `/healthz`.
    #[test]
    fn drip_feed_headers_past_deadline_does_not_stall_healthz() {
        let stats = Arc::new(DaemonStats::new());
        let h = spawn_health("127.0.0.1:0".parse().unwrap(), stats).expect("bind");
        let mut slow = TcpStream::connect(h.local_addr).expect("slow connect");
        let _ = slow.set_nodelay(true);
        slow.set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        slow.set_write_timeout(Some(Duration::from_secs(5)))
            .expect("write timeout");
        let mut writer = slow.try_clone().expect("clone");
        let started = Instant::now();
        let drip = thread::spawn(move || {
            // Interval is well under IO_TIMEOUT so a per-read reset would not
            // fire; the absolute deadline must.
            for &b in b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Slow: " {
                if writer.write_all(&[b]).is_err() {
                    return;
                }
                let _ = writer.flush();
                thread::sleep(Duration::from_millis(400));
            }
        });

        thread::sleep(Duration::from_millis(50));
        let probe_start = Instant::now();
        let (code, body) = get(&format!("http://{}/healthz", h.local_addr));
        let probe_elapsed = probe_start.elapsed();
        assert_eq!(code, 200, "second /healthz: {body}");
        assert_eq!(body, "ok\n");
        assert!(
            probe_elapsed < Duration::from_millis(800),
            "second /healthz stalled for {probe_elapsed:?} behind a drip-feed client"
        );

        let mut resp = Vec::new();
        let read_res = slow.read_to_end(&mut resp);
        let elapsed = started.elapsed();
        let _ = drip.join();
        let text = String::from_utf8_lossy(&resp);
        assert!(
            read_res.is_ok() && text.contains("408"),
            "slow client should get 408 after the total deadline, got {read_res:?} {text:?}"
        );
        assert!(
            elapsed < Duration::from_millis(3500),
            "worker hung for {elapsed:?} under drip-feed headers (absolute deadline is 2s)"
        );
        assert!(
            elapsed >= Duration::from_millis(500),
            "408 arrived too fast ({elapsed:?}); drip should occupy the remaining-time budget"
        );
    }

    #[test]
    fn health_worker_cap_refuses_extra_connections() {
        let stats = Arc::new(DaemonStats::new());
        let h = spawn_health("127.0.0.1:0".parse().unwrap(), stats).expect("bind");
        let mut held = Vec::with_capacity(MAX_HEALTH_WORKERS);
        for i in 0..MAX_HEALTH_WORKERS {
            let s = TcpStream::connect(h.local_addr).unwrap_or_else(|e| panic!("hold {i}: {e}"));
            let _ = s.set_nodelay(true);
            held.push(s);
        }
        let wait_cap = Instant::now() + Duration::from_secs(1);
        while h.live_workers.load(Ordering::Acquire) < MAX_HEALTH_WORKERS {
            assert!(
                Instant::now() < wait_cap,
                "workers did not reach cap ({}/{})",
                h.live_workers.load(Ordering::Acquire),
                MAX_HEALTH_WORKERS
            );
            thread::sleep(Duration::from_millis(5));
        }

        let mut extra = TcpStream::connect(h.local_addr).expect("extra connect");
        let _ = extra.set_nodelay(true);
        extra
            .set_read_timeout(Some(Duration::from_millis(500)))
            .expect("extra read timeout");
        extra
            .set_write_timeout(Some(Duration::from_millis(500)))
            .expect("extra write timeout");
        let start = Instant::now();
        let _ = extra.write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
        let _ = extra.flush();
        let mut buf = [0u8; 256];
        let n = extra
            .read(&mut buf)
            .expect("extra should be refused, not hang");
        let elapsed = start.elapsed();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(
            text.contains("503"),
            "over-cap connection should get 503, got {text:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "over-cap refuse stalled for {elapsed:?}"
        );

        drop(held);
        let drain = Instant::now() + Duration::from_secs(1);
        while h.live_workers.load(Ordering::Acquire) != 0 {
            assert!(
                Instant::now() < drain,
                "workers did not drain after clients closed ({})",
                h.live_workers.load(Ordering::Acquire)
            );
            thread::sleep(Duration::from_millis(5));
        }
        let (code, body) = get(&format!("http://{}/healthz", h.local_addr));
        assert_eq!(code, 200, "healthz after cap drain: {body}");
    }

    #[test]
    fn failed_scan_clears_readiness_and_records_last_error() {
        let stats = DaemonStats::new();
        stats.observe(
            &TickReport {
                scan_ok: true,
                ..TickReport::default()
            },
            0,
        );
        assert!(stats.is_ready());
        stats.observe(
            &TickReport {
                scan_ok: false,
                last_error: Some("scan unavailable: down".into()),
                ..TickReport::default()
            },
            0,
        );
        assert!(!stats.is_ready());
        assert_eq!(
            stats.snapshot().last_error.as_deref(),
            Some("scan unavailable: down")
        );
        assert_eq!(stats.snapshot().ticks, 2);
    }

    struct EmptyRpc;
    impl Rpc for EmptyRpc {
        fn list_live_intentions(
            &self,
            _: &[u8; 32],
            _: ScriptHashType,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(Vec::new())
        }
        fn get_live_process_by_type_hash(
            &self,
            _: &[u8; 32],
        ) -> Result<Option<LiveCell>, RpcError> {
            Ok(None)
        }
        fn submit_transaction(&self, _: &TransactionView) -> Result<Byte32, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
        fn get_transaction_status(&self, _: &Byte32) -> Result<TxStatus, RpcError> {
            Ok(TxStatus::Dropped)
        }
        fn tip_block_number(&self) -> Result<u64, RpcError> {
            Ok(0)
        }
    }

    struct DownRpc;
    impl Rpc for DownRpc {
        fn list_live_intentions(
            &self,
            _: &[u8; 32],
            _: ScriptHashType,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Err(RpcError::Unavailable("indexer down".into()))
        }
        fn get_live_process_by_type_hash(
            &self,
            _: &[u8; 32],
        ) -> Result<Option<LiveCell>, RpcError> {
            Err(RpcError::Unavailable("indexer down".into()))
        }
        fn submit_transaction(&self, _: &TransactionView) -> Result<Byte32, RpcError> {
            Err(RpcError::Unavailable("down".into()))
        }
        fn get_transaction_status(&self, _: &Byte32) -> Result<TxStatus, RpcError> {
            Err(RpcError::Unavailable("down".into()))
        }
        fn tip_block_number(&self) -> Result<u64, RpcError> {
            Err(RpcError::Unavailable("down".into()))
        }
    }

    struct NoopSsri;
    impl SsriHost for NoopSsri {
        fn get_batch_bounds(&self, _: &[u8]) -> Result<(u32, u32), SsriError> {
            Ok((1, 1))
        }
        fn assemble(&self, _: &[u8], _: &[IntentionInput]) -> Result<PartialTx, SsriError> {
            Err(SsriError::Unusable("noop".into()))
        }
    }

    fn loop_cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: [0xC1; 32],
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            confirmation_depth: 1,
        }
    }

    #[test]
    fn mock_tick_increments_metrics_and_sets_ready() {
        let mut loop_ = DigestLoop::new(
            EmptyRpc,
            NopClock,
            NoopSsri,
            loop_cfg(),
            ProcessResolver::new(),
        );
        let stats = DaemonStats::new();
        let report = loop_.tick();
        stats.observe(&report, loop_.state().submit.pending.len());
        let snap = stats.snapshot();
        assert_eq!(snap.ticks, 1);
        assert!(report.scan_ok);
        assert!(snap.ready);
        assert_eq!(snap.scanned, 0);
        assert!(snap.last_tick_at > 0);
    }

    #[test]
    fn unavailable_mock_tick_is_not_ready() {
        let mut loop_ = DigestLoop::new(
            DownRpc,
            NopClock,
            NoopSsri,
            loop_cfg(),
            ProcessResolver::new(),
        );
        let stats = DaemonStats::new();
        let report = loop_.tick();
        stats.observe(&report, 0);
        assert!(!report.scan_ok);
        assert!(!stats.is_ready());
        assert!(stats
            .snapshot()
            .last_error
            .as_deref()
            .unwrap()
            .contains("indexer down"));
    }
}
