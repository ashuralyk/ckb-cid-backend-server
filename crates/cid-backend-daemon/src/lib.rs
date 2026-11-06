//! Thin daemon library: env config and the live CKB JSON-RPC/Indexer adapter.
//!
//! Business rules stay in `cid-backend-core`. This crate talks HTTP.

pub mod ckb_rpc;
pub mod config;

pub use ckb_rpc::CkbRpc;
pub use config::{ConfigError, DaemonConfig};
