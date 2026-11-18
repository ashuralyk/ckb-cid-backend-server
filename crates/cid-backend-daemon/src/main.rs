use cid_backend_core::code_deps::PROCESS_CODE_LABEL;
use cid_backend_core::digest_loop::{DigestLoop, NopClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_daemon::observe::{spawn_health, DaemonStats};
use cid_backend_daemon::{
    parse_cli, resolve_config_path, rpc_endpoint_for_log, usage, CkbRpc, CliAction, DaemonConfig,
};
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cli = match parse_cli(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cid-backend-daemon invalid config: {e}");
            eprintln!("{}", usage());
            std::process::exit(1);
        }
    };
    if matches!(cli, CliAction::Help) {
        print!("{}", usage());
        return;
    }
    let cid_config = std::env::var("CID_CONFIG").ok();
    let path = resolve_config_path(&cli, cid_config);
    let config = match DaemonConfig::from_file_and_vars(path.as_deref(), |k| std::env::var(k).ok())
    {
        Ok(c) => c,
        Err(e) => {
            cid_backend_daemon::logging::init_tracing();
            tracing::error!(
                target: "cid_backend",
                event = "config.load_failed",
                error = %e,
            );
            eprintln!("cid-backend-daemon invalid config: {e}");
            std::process::exit(1);
        }
    };
    cid_backend_daemon::logging::init_tracing_with(config.log_format);
    let genesis_hex = config.genesis_hash.map(|g| format!("0x{}", hex::encode(g)));
    tracing::info!(
        target: "cid_backend",
        event = "config.loaded",
        path = path.as_ref().map(|p| p.display().to_string()),
        network_name = config.network_name.as_deref(),
        genesis_hash = genesis_hex.as_deref(),
        genesis_status = if config.validate_genesis {
            "declared_will_verify"
        } else if config.genesis_hash.is_some() {
            "declared_unverified"
        } else {
            "unset"
        },
        rpc = %rpc_endpoint_for_log(&config.node_url),
        indexer = %rpc_endpoint_for_log(&config.indexer_url),
        static_cell_deps = config.extra_cell_deps.len(),
        type_id_code_deps = config.type_id_code_deps.len(),
        process_prefixes = config.process_type_code_hashes.len(),
    );
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
    if config.validate_genesis {
        let expected = config
            .genesis_hash
            .expect("validate_genesis requires genesis_hash");
        if let Err(e) = rpc.verify_declared_genesis(&expected) {
            tracing::error!(
                target: "cid_backend",
                event = "genesis.mismatch",
                error = %e,
            );
            eprintln!("cid-backend-daemon genesis check failed: {e}");
            std::process::exit(1);
        }
        tracing::info!(
            target: "cid_backend",
            event = "genesis.verified",
            genesis_hash = genesis_hex.as_deref(),
        );
    }
    let loop_config = config.to_loop_config();
    let poll = config.poll_interval;
    // Type-ID Process code → SSRI executes the resolved code-cell ELF (same
    // snapshot as the CellDep); state type script must reference that TYPE_ID.
    // Legacy / local data1 keeps state-cell ELF.
    let resolver = match config
        .type_id_code_deps
        .iter()
        .find(|d| d.label == PROCESS_CODE_LABEL)
    {
        Some(dep) => ProcessResolver::with_type_id_process_code(dep.identity.type_script_hash()),
        None => ProcessResolver::new(),
    };
    let mut digest = DigestLoop::new(rpc, NopClock, EmbeddedSsri, loop_config, resolver);
    loop {
        let report = digest.tick();
        stats.observe(&report, digest.state().submit.pending.len());
        std::thread::sleep(poll);
    }
}
