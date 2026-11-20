//! Thin daemon library: TOML/`CID_*` config, CKB JSON-RPC/Indexer adapter, and
//! optional health/metrics HTTP. Business rules stay in `cid-backend-core`.
//! The daemon is keyless (no signing, no dotenv).

pub mod ckb_rpc;
pub mod config;
pub mod logging;
pub mod observe;
pub mod registry_api;

pub use ckb_rpc::CkbRpc;
pub use config::{
    parse_cli, resolve_config_path, rpc_endpoint_for_log, usage, CliAction, ConfigError,
    DaemonConfig,
};
pub use observe::{spawn_health, DaemonStats, HealthHandle};
pub use registry_api::{spawn_registry_api, RegistryApiHandle, RegistryApiState};
