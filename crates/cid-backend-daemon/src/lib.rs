//! Thin daemon library: env config, CKB JSON-RPC/Indexer adapter, and optional
//! health/metrics HTTP. Business rules stay in `cid-backend-core`.

pub mod ckb_rpc;
pub mod config;
pub mod logging;
pub mod observe;

pub use ckb_rpc::CkbRpc;
pub use config::{ConfigError, DaemonConfig};
pub use observe::{spawn_health, DaemonStats, HealthHandle};
