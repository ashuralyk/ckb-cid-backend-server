use cid_backend_core::digest_loop::LoopConfig;
use ckb_types::core::ScriptHashType;
use std::fmt;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub node_url: String,
    pub indexer_url: String,
    pub cid_lock_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
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
        Ok(Self {
            node_url,
            indexer_url,
            cid_lock_code_hash,
            hash_type,
            poll_interval: Duration::from_secs(secs),
        })
    }

    pub fn to_loop_config(&self) -> LoopConfig {
        LoopConfig {
            cid_code_hash: self.cid_lock_code_hash,
            hash_type: self.hash_type,
            poll_interval: self.poll_interval,
            extra_cell_deps: vec![],
        }
    }
}

fn parse_hash_type(s: &str) -> Result<ScriptHashType, ConfigError> {
    match s.to_ascii_lowercase().as_str() {
        "type" => Ok(ScriptHashType::Type),
        "data" => Ok(ScriptHashType::Data),
        "data1" => Ok(ScriptHashType::Data1),
        "data2" => Ok(ScriptHashType::Data2),
        other => Err(ConfigError::Invalid(format!(
            "CID_LOCK_HASH_TYPE={other}"
        ))),
    }
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
