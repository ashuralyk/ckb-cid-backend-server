mod config;
mod placeholder_rpc;

use cid_backend_core::digest_loop::{DigestLoop, RealClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::ssri::EmbeddedSsri;
use config::DaemonConfig;
use placeholder_rpc::PlaceholderRpc;

fn main() {
    env_logger::init();
    let config = match DaemonConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cid-backend-daemon invalid config: {e}");
            std::process::exit(1);
        }
    };
    let rpc = PlaceholderRpc {
        node_url: config.node_url.clone(),
        indexer_url: config.indexer_url.clone(),
    };
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
