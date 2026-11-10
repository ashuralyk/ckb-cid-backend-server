use cid_backend_core::digest_loop::{DigestLoop, NopClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_daemon::observe::{spawn_health, DaemonStats};
use cid_backend_daemon::{CkbRpc, DaemonConfig};
use std::sync::Arc;

fn main() {
    cid_backend_daemon::logging::init_tracing();
    let config = match DaemonConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: "cid_backend",
                event = "config.load_failed",
                error = %e,
            );
            eprintln!("cid-backend-daemon invalid config: {e}");
            std::process::exit(1);
        }
    };
    let stats = Arc::new(DaemonStats::new());
    if let Some(addr) = config.health_bind {
        match spawn_health(addr, stats.clone()) {
            Ok(h) => tracing::info!(
                target: "cid_backend",
                event = "health.listen",
                bind = %h.local_addr,
            ),
            Err(e) => {
                tracing::error!(
                    target: "cid_backend",
                    event = "config.load_failed",
                    error = %e,
                );
                eprintln!("cid-backend-daemon health bind failed: {e}");
                std::process::exit(1);
            }
        }
    }
    let rpc = CkbRpc::from_config(&config);
    let loop_config = config.to_loop_config();
    let poll = config.poll_interval;
    let mut digest = DigestLoop::new(
        rpc,
        NopClock,
        EmbeddedSsri,
        loop_config,
        ProcessResolver::new(),
    );
    loop {
        let report = digest.tick();
        stats.observe(&report, digest.state().submit.pending.len());
        std::thread::sleep(poll);
    }
}
