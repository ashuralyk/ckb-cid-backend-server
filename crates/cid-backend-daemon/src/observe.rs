//! In-process counters and an optional tiny HTTP health/stats server.
//!
//! Bind with `CID_HEALTH_BIND` (unset/empty = disabled). The digest loop is
//! not blocked: the listener runs on a background thread. State is memory-only.

use cid_backend_core::digest_loop::TickReport;
use serde::Serialize;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Response, Server, StatusCode};

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
}

/// Start `/healthz`, `/readyz`, `/stats`, `/metrics` on `bind`. The caller
/// keeps `HealthHandle`; the server thread owns the listener until process exit.
pub fn spawn_health(bind: SocketAddr, stats: Arc<DaemonStats>) -> io::Result<HealthHandle> {
    let server = Server::http(bind).map_err(|e| io::Error::other(e.to_string()))?;
    let local_addr = match server.server_addr().to_ip() {
        Some(a) => a,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "health bind is not an IP socket",
            ));
        }
    };
    thread::Builder::new()
        .name("cid-health".into())
        .spawn(move || {
            for req in server.incoming_requests() {
                handle_request(req, &stats);
            }
        })?;
    Ok(HealthHandle { local_addr })
}

fn handle_request(req: tiny_http::Request, stats: &DaemonStats) {
    let path = req.url().split('?').next().unwrap_or("/");
    let (status, ctype, body) = match (req.method(), path) {
        (Method::Get, "/healthz") => (200, "text/plain; charset=utf-8", "ok\n".into()),
        (Method::Get, "/readyz") => {
            if stats.is_ready() {
                (200, "text/plain; charset=utf-8", "ready\n".into())
            } else {
                (503, "text/plain; charset=utf-8", "not ready\n".into())
            }
        }
        (Method::Get, "/stats") => {
            let json = serde_json::to_string(&stats.snapshot()).unwrap_or_else(|_| "{}".into());
            (200, "application/json", json)
        }
        (Method::Get, "/metrics") => (
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            stats.prometheus(),
        ),
        _ => (404, "text/plain; charset=utf-8", "not found\n".into()),
    };
    let mut response = Response::from_string(body).with_status_code(StatusCode(status));
    if let Ok(h) = Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()) {
        response = response.with_header(h);
    }
    let _ = req.respond(response);
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
    use std::time::Duration;

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
