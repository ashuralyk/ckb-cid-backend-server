use cid_backend_core::digest_loop::{DigestLoop, NopClock, WorkerCommand};
use cid_backend_core::ssri::EmbeddedSsri;
use cid_backend_core::{ProcessBusyMap, ProcessRegistry, WorkerPool};
use cid_backend_daemon::observe::{spawn_health, DaemonStats};
use cid_backend_daemon::registry_api::{spawn_registry_api, RegistryApiState};
use cid_backend_daemon::{
    parse_cli, resolve_config_path, rpc_endpoint_for_log, usage, CkbRpc, CliAction, DaemonConfig,
};
use std::sync::{Arc, Mutex};

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
        registry_path = %config.registry_path.display(),
        registry_max_entries = config.registry_max_entries,
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

    let registry = match ProcessRegistry::open(&config.registry_path, config.registry_max_entries) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                target: "cid_backend",
                event = "registry.load_failed",
                error = %e,
            );
            eprintln!("cid-backend-daemon registry load failed: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!(
        target: "cid_backend",
        event = "registry.loaded",
        path = %config.registry_path.display(),
        entries = registry.len(),
    );

    let worker_cmds: Arc<Mutex<Vec<WorkerCommand>>> = Arc::new(Mutex::new(Vec::new()));
    let busy = ProcessBusyMap::new();
    let registry = Arc::new(Mutex::new(registry));

    if let Some(addr) = config.api_bind {
        let state = RegistryApiState {
            registry: registry.clone(),
            worker_cmds: worker_cmds.clone(),
            busy: busy.clone(),
        };
        match spawn_registry_api(addr, state) {
            Ok(h) => tracing::info!(
                target: "cid_backend",
                event = "registry.api.listen",
                bind = %h.local_addr,
            ),
            Err(e) => {
                tracing::error!(
                    target: "cid_backend",
                    event = "config.load_failed",
                    error = %e,
                );
                eprintln!("cid-backend-daemon registry API bind failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let rpc = Arc::new(CkbRpc::from_config(&config));
    if config.validate_genesis {
        let expected = config
            .genesis_hash
            .expect("validate_genesis requires genesis_hash");
        if let Err(e) = rpc.as_ref().verify_declared_genesis(&expected) {
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
    let ssri = Arc::new(EmbeddedSsri);
    let mut workers = WorkerPool::new(rpc.clone(), ssri.clone());
    for reg in registry.list() {
        match reg.to_process_entry() {
            Ok(entry) => {
                let _ = workers.register(entry);
            }
            Err(e) => {
                tracing::warn!(
                    target: "cid_backend",
                    event = "registry.entry_invalid",
                    error = %e,
                );
            }
        }
    }

    let loop_config = config.to_loop_config();
    let poll = config.poll_interval;
    let mut digest = DigestLoop::with_workers(
        rpc,
        NopClock,
        ssri,
        loop_config,
        workers,
        Some(worker_cmds),
    )
    .with_busy_map(busy);
    loop {
        let report = digest.tick();
        let pending = digest
            .workers()
            .hashes()
            .iter()
            .filter_map(|h| digest.workers().get(h))
            .map(|w| w.with_state(|s| s.submit.pending.len()))
            .sum();
        stats.observe(&report, pending);
        std::thread::sleep(poll);
    }
}
