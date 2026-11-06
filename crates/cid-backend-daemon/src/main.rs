use cid_backend_core::digest_loop::{DigestLoop, RealClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_daemon::{CkbRpc, DaemonConfig};

fn main() {
    env_logger::init();
    let config = match DaemonConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cid-backend-daemon invalid config: {e}");
            std::process::exit(1);
        }
    };
    let rpc = CkbRpc::from_config(&config);
    let loop_config = config.to_loop_config();
    let mut digest = DigestLoop::new(
        rpc,
        RealClock,
        EmbeddedSsri,
        loop_config,
        ProcessResolver::new(),
    );
    digest.run();
}
