use cid_backend_core::digest_loop::{LoopConfig, DEFAULT_CONFIRMATION_DEPTH};
use ckb_types::core::{DepType, ScriptHashType};
use ckb_types::packed::{Byte32, CellDep, OutPoint};
use ckb_types::prelude::*;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub node_url: String,
    pub indexer_url: String,
    pub cid_lock_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
    /// Code cell deps for CID lock, Process ELF, always-success, etc.
    pub extra_cell_deps: Vec<CellDep>,
    /// Indexer type-script `code_hash` prefixes used to find Process cells by
    /// `calc_script_hash` (the 32-byte value in CID args is a script hash, not
    /// a code hash). Local-chain sets this to the Process ELF data hash.
    pub process_type_code_hashes: Vec<[u8; 32]>,
    /// Optional health/stats bind. Unset/empty disables the listener.
    pub health_bind: Option<SocketAddr>,
    /// Confirmation depth for submitted execution txs. Default 24.
    pub confirmation_depth: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Missing(String),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Missing(k) => write!(f, "missing env {k}"),
            ConfigError::Invalid(m) => write!(f, "invalid config: {m}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl DaemonConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, ConfigError> {
        let node_url = get("CID_CKB_RPC_URL")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ConfigError::Missing("CID_CKB_RPC_URL".into()))?;
        let indexer_url = get("CID_INDEXER_URL")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| node_url.clone());
        let hash_raw = get("CID_LOCK_CODE_HASH")
            .ok_or_else(|| ConfigError::Missing("CID_LOCK_CODE_HASH".into()))?;
        let cid_lock_code_hash = parse_hex32(&hash_raw)?;
        let hash_type = match get("CID_LOCK_HASH_TYPE") {
            None => ScriptHashType::Type,
            Some(s) => parse_hash_type(&s)?,
        };
        let secs_raw = get("CID_POLL_INTERVAL_SECS")
            .ok_or_else(|| ConfigError::Missing("CID_POLL_INTERVAL_SECS".into()))?;
        let secs: u64 = secs_raw
            .parse()
            .map_err(|_| ConfigError::Invalid("CID_POLL_INTERVAL_SECS not u64".into()))?;
        if secs == 0 {
            return Err(ConfigError::Invalid(
                "CID_POLL_INTERVAL_SECS must be >= 1".into(),
            ));
        }
        let extra_cell_deps = parse_cell_deps(&[
            get("CID_LOCK_CELL_DEP"),
            get("CID_PROCESS_CODE_CELL_DEP"),
            get("CID_PROCESS_LOCK_CELL_DEP"),
            get("CID_EXTRA_CELL_DEPS"),
        ])?;
        let process_type_code_hashes = parse_hash_list(get("CID_PROCESS_CODE_HASH"))?;
        let health_bind = match get("CID_HEALTH_BIND").filter(|s| !s.trim().is_empty()) {
            None => None,
            Some(s) => Some(s.parse::<SocketAddr>().map_err(|_| {
                ConfigError::Invalid(format!("CID_HEALTH_BIND={s} (want host:port)"))
            })?),
        };
        let confirmation_depth =
            match get("CID_CONFIRMATION_DEPTH").filter(|s| !s.trim().is_empty()) {
                None => DEFAULT_CONFIRMATION_DEPTH,
                Some(s) => {
                    let n: u64 = s.parse().map_err(|_| {
                        ConfigError::Invalid("CID_CONFIRMATION_DEPTH not u64".into())
                    })?;
                    if n == 0 {
                        return Err(ConfigError::Invalid(
                            "CID_CONFIRMATION_DEPTH must be >= 1".into(),
                        ));
                    }
                    n
                }
            };
        Ok(Self {
            node_url,
            indexer_url,
            cid_lock_code_hash,
            hash_type,
            poll_interval: Duration::from_secs(secs),
            extra_cell_deps,
            process_type_code_hashes,
            health_bind,
            confirmation_depth,
        })
    }

    pub fn to_loop_config(&self) -> LoopConfig {
        LoopConfig {
            cid_code_hash: self.cid_lock_code_hash,
            hash_type: self.hash_type,
            poll_interval: self.poll_interval,
            extra_cell_deps: self.extra_cell_deps.clone(),
            confirmation_depth: self.confirmation_depth,
        }
    }
}

fn parse_hash_type(s: &str) -> Result<ScriptHashType, ConfigError> {
    match s.to_ascii_lowercase().as_str() {
        "type" => Ok(ScriptHashType::Type),
        "data" => Ok(ScriptHashType::Data),
        "data1" => Ok(ScriptHashType::Data1),
        "data2" => Ok(ScriptHashType::Data2),
        other => Err(ConfigError::Invalid(format!("CID_LOCK_HASH_TYPE={other}"))),
    }
}

fn parse_cell_deps(raws: &[Option<String>]) -> Result<Vec<CellDep>, ConfigError> {
    let mut out = Vec::new();
    for raw in raws.iter().flatten() {
        for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            out.push(parse_cell_dep(part)?);
        }
    }
    Ok(out)
}

fn parse_cell_dep(s: &str) -> Result<CellDep, ConfigError> {
    let (hash_part, index_part) = s
        .split_once(':')
        .ok_or_else(|| ConfigError::Invalid("cell dep must be <tx_hash>:<index>".into()))?;
    let tx_hash = parse_hex32(hash_part)?;
    let index = parse_index(index_part)?;
    let out_point = OutPoint::new_builder()
        .tx_hash(Byte32::from_slice(&tx_hash).expect("32 bytes"))
        .index(index.pack())
        .build();
    Ok(CellDep::new_builder()
        .out_point(out_point)
        .dep_type(DepType::Code.into())
        .build())
}

fn parse_index(s: &str) -> Result<u32, ConfigError> {
    let t = s.trim();
    if let Some(hex) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        u32::from_str_radix(hex, 16)
            .map_err(|_| ConfigError::Invalid("cell dep index not hex".into()))
    } else {
        t.parse()
            .map_err(|_| ConfigError::Invalid("cell dep index not u32".into()))
    }
}

fn parse_hash_list(raw: Option<String>) -> Result<Vec<[u8; 32]>, ConfigError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        out.push(parse_hex32(part)?);
    }
    Ok(out)
}

fn parse_hex32(s: &str) -> Result<[u8; 32], ConfigError> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if s.len() != 64 {
        return Err(ConfigError::Invalid(
            "CID_LOCK_CODE_HASH must be 32 bytes hex".into(),
        ));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| ConfigError::Invalid("CID_LOCK_CODE_HASH not hex".into()))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_types::core::ScriptHashType;

    #[test]
    fn from_vars_parses_required_and_defaults() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_LOCK_CODE_HASH" => Some("11".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.node_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.indexer_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.cid_lock_code_hash, [0x11u8; 32]);
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
        assert!(cfg.extra_cell_deps.is_empty());
        assert!(cfg.process_type_code_hashes.is_empty());
        assert!(cfg.health_bind.is_none());
        assert_eq!(cfg.confirmation_depth, DEFAULT_CONFIRMATION_DEPTH);
    }

    #[test]
    fn from_vars_parses_health_bind() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_LOCK_CODE_HASH" => Some("11".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_HEALTH_BIND" => Some("127.0.0.1:9100".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.health_bind.unwrap().to_string(), "127.0.0.1:9100");
    }

    #[test]
    fn invalid_health_bind_fails() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://n".into()),
            "CID_LOCK_CODE_HASH" => Some("00".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("1".into()),
            "CID_HEALTH_BIND" => Some("not-an-addr".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn from_vars_parses_cell_deps_and_process_code_hash() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_LOCK_CODE_HASH" => Some("11".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_CELL_DEP" => Some(format!("{}:0", "aa".repeat(32))),
            "CID_PROCESS_CODE_CELL_DEP" => Some(format!("0x{}:0x1", "bb".repeat(32))),
            "CID_PROCESS_CODE_HASH" => Some("0x".to_string() + &"cc".repeat(32)),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.extra_cell_deps.len(), 2);
        assert_eq!(cfg.process_type_code_hashes, vec![[0xccu8; 32]]);
        let loop_cfg = cfg.to_loop_config();
        assert_eq!(loop_cfg.extra_cell_deps.len(), 2);
    }

    #[test]
    fn from_vars_reads_optional_indexer_and_hash_type() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://node".into()),
            "CID_INDEXER_URL" => Some("http://indexer".into()),
            "CID_LOCK_CODE_HASH" => Some("0x".to_string() + &"ab".repeat(32)),
            "CID_LOCK_HASH_TYPE" => Some("data1".into()),
            "CID_POLL_INTERVAL_SECS" => Some("2".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.indexer_url, "http://indexer");
        assert_eq!(cfg.hash_type, ScriptHashType::Data1);
        assert_eq!(cfg.cid_lock_code_hash[0], 0xab);
    }

    #[test]
    fn missing_required_fails() {
        let err = DaemonConfig::from_vars(|_| None).unwrap_err();
        assert!(matches!(err, ConfigError::Missing(_)));
    }

    #[test]
    fn from_vars_parses_confirmation_depth() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://n".into()),
            "CID_LOCK_CODE_HASH" => Some("00".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("1".into()),
            "CID_CONFIRMATION_DEPTH" => Some("8".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.confirmation_depth, 8);
        assert_eq!(cfg.to_loop_config().confirmation_depth, 8);
    }

    #[test]
    fn zero_confirmation_depth_invalid() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://n".into()),
            "CID_LOCK_CODE_HASH" => Some("00".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("1".into()),
            "CID_CONFIRMATION_DEPTH" => Some("0".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn zero_interval_invalid() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://n".into()),
            "CID_LOCK_CODE_HASH" => Some("00".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("0".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }
}
