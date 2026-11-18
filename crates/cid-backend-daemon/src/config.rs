use crate::logging::LogFormat;
use cid_backend_core::digest_loop::{LoopConfig, DEFAULT_CONFIRMATION_DEPTH};
use ckb_types::core::{DepType, ScriptHashType};
use ckb_types::packed::{Byte32, CellDep, OutPoint};
use ckb_types::prelude::*;
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub node_url: String,
    pub indexer_url: String,
    pub cid_lock_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
    /// Code/dep-group cell deps for CID lock, Process type, Process lock, etc.
    pub extra_cell_deps: Vec<CellDep>,
    /// Indexer type-script `code_hash` prefixes used to find Process cells by
    /// `calc_script_hash` (the 32-byte value in CID args is a script hash, not
    /// a code hash).
    pub process_type_code_hashes: Vec<[u8; 32]>,
    /// Optional health/stats bind. Unset/empty disables the listener.
    pub health_bind: Option<SocketAddr>,
    /// Confirmation depth for submitted execution txs. Default 24.
    pub confirmation_depth: u64,
    /// Human label for logs only. Never used to select RPC or contracts.
    pub network_name: Option<String>,
    /// Declared genesis block hash. File mode requires it. Not verified unless
    /// `validate_genesis` is true.
    pub genesis_hash: Option<[u8; 32]>,
    /// When true, daemon fail-closes after comparing `get_block_hash(0x0)`.
    pub validate_genesis: bool,
    pub log_format: LogFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Missing(String),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Missing(k) => write!(f, "missing {k}"),
            ConfigError::Invalid(m) => write!(f, "invalid config: {m}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliAction {
    Help,
    Run { config_path: Option<PathBuf> },
}

/// Strip userinfo / path / query so logs never echo credential-bearing RPC URLs.
pub fn rpc_endpoint_for_log(raw: &str) -> String {
    let Some(scheme_end) = raw.find("://") else {
        return "<invalid-rpc-url>".into();
    };
    let scheme = &raw[..scheme_end];
    if scheme != "http" && scheme != "https" {
        return "<invalid-rpc-url>".into();
    }
    let rest = &raw[scheme_end + 3..];
    let after_auth = rest.split_once('@').map(|(_, host)| host).unwrap_or(rest);
    let hostport = after_auth
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_auth);
    if hostport.is_empty() || hostport.contains('@') {
        return "<invalid-rpc-url>".into();
    }
    format!("{scheme}://{hostport}")
}

impl DaemonConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, ConfigError> {
        Self::from_file_and_vars(None, get)
    }

    pub fn from_file_and_vars<F: Fn(&str) -> Option<String>>(
        path: Option<&Path>,
        get: F,
    ) -> Result<Self, ConfigError> {
        let file = match path {
            None => None,
            Some(p) => Some(read_file_config(p)?),
        };
        merge(file, get, path.is_some())
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

/// Parse argv. `--config PATH` / `--config=PATH`. `CID_CONFIG` is applied later.
pub fn parse_cli(args: &[String]) -> Result<CliAction, ConfigError> {
    let mut config_path = None;
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(CliAction::Help);
        }
        if let Some(p) = arg.strip_prefix("--config=") {
            if p.is_empty() {
                return Err(ConfigError::Invalid(
                    "--config requires a file path".into(),
                ));
            }
            if config_path.replace(PathBuf::from(p)).is_some() {
                return Err(ConfigError::Invalid("duplicate --config".into()));
            }
            continue;
        }
        if arg == "--config" {
            let Some(p) = iter.next() else {
                return Err(ConfigError::Invalid(
                    "--config requires a file path".into(),
                ));
            };
            if p.starts_with('-') {
                return Err(ConfigError::Invalid(
                    "--config requires a file path".into(),
                ));
            }
            if config_path.replace(PathBuf::from(p)).is_some() {
                return Err(ConfigError::Invalid("duplicate --config".into()));
            }
            continue;
        }
        return Err(ConfigError::Invalid(format!(
            "unknown argument {arg} (want --config PATH)"
        )));
    }
    Ok(CliAction::Run { config_path })
}

/// `--config` wins over non-empty `CID_CONFIG`.
pub fn resolve_config_path(cli: &CliAction, cid_config: Option<String>) -> Option<PathBuf> {
    match cli {
        CliAction::Help => None,
        CliAction::Run { config_path: Some(p) } => Some(p.clone()),
        CliAction::Run { config_path: None } => cid_config
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from),
    }
}

pub fn usage() -> &'static str {
    "cid-backend-daemon [--config PATH]\n\
     \n\
     Public digest daemon. No keys, no signing.\n\
     Config: --config PATH overrides CID_CONFIG. Neither: legacy CID_* env only.\n\
     Precedence: defaults < TOML file < non-empty CID_* process env.\n\
     File mode requires [network].genesis_hash (declared identity, not verified\n\
     unless network.validate_genesis = true).\n"
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    network: Option<NetworkSection>,
    #[serde(default)]
    rpc: Option<RpcSection>,
    #[serde(default)]
    cid_lock: Option<CidLockSection>,
    #[serde(default)]
    process: Option<ProcessSection>,
    #[serde(default)]
    cell_deps: Vec<CellDepFile>,
    #[serde(rename = "loop", default)]
    digest: Option<LoopSection>,
    #[serde(default)]
    observe: Option<ObserveSection>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NetworkSection {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    genesis_hash: Option<String>,
    #[serde(default)]
    validate_genesis: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcSection {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    indexer_url: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CidLockSection {
    #[serde(default)]
    code_hash: Option<String>,
    #[serde(default)]
    hash_type: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessSection {
    #[serde(default)]
    type_code_hashes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CellDepFile {
    #[serde(default)]
    #[allow(dead_code)]
    label: Option<String>,
    tx_hash: String,
    index: u32,
    #[serde(default)]
    dep_type: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoopSection {
    #[serde(default)]
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    confirmation_depth: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserveSection {
    #[serde(default)]
    health_bind: Option<String>,
    #[serde(default)]
    log_format: Option<String>,
}

fn read_file_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        ConfigError::Invalid(format!("cannot read config {}: {e}", path.display()))
    })?;
    toml::from_str(&raw).map_err(|e| {
        ConfigError::Invalid(format!("TOML {}: {e}", path.display()))
    })
}

fn nonempty(get: &impl Fn(&str) -> Option<String>, key: &str) -> Option<String> {
    get(key).filter(|s| !s.trim().is_empty())
}

fn merge<F: Fn(&str) -> Option<String>>(
    file: Option<FileConfig>,
    get: F,
    from_file: bool,
) -> Result<DaemonConfig, ConfigError> {
    let file = file.unwrap_or_default();
    let network = file.network.unwrap_or_default();
    let rpc = file.rpc.unwrap_or_default();
    let cid_lock = file.cid_lock.unwrap_or_default();
    let process = file.process.unwrap_or_default();
    let digest = file.digest.unwrap_or_default();
    let observe = file.observe.unwrap_or_default();

    let node_url = match nonempty(&get, "CID_CKB_RPC_URL") {
        Some(v) => v,
        None => rpc.url.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
            ConfigError::Missing("CID_CKB_RPC_URL (or rpc.url in --config file)".into())
        })?,
    };
    let indexer_url = match nonempty(&get, "CID_INDEXER_URL") {
        Some(v) => v,
        None => rpc
            .indexer_url
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| node_url.clone()),
    };

    let cid_lock_code_hash = match nonempty(&get, "CID_LOCK_CODE_HASH") {
        Some(v) => parse_hex32(&v)?,
        None => match cid_lock.code_hash.filter(|s| !s.trim().is_empty()) {
            Some(v) => parse_hex32(&v)?,
            None => {
                return Err(ConfigError::Missing(
                    "CID_LOCK_CODE_HASH (or cid_lock.code_hash in --config file)".into(),
                ));
            }
        },
    };
    let hash_type = match nonempty(&get, "CID_LOCK_HASH_TYPE") {
        Some(s) => parse_hash_type(&s)?,
        None => match cid_lock.hash_type {
            Some(s) => parse_hash_type(&s)?,
            None => ScriptHashType::Type,
        },
    };

    let secs = match nonempty(&get, "CID_POLL_INTERVAL_SECS") {
        Some(raw) => parse_poll_secs(&raw)?,
        None => match digest.poll_interval_secs {
            Some(n) => {
                if n == 0 {
                    return Err(ConfigError::Invalid(
                        "loop.poll_interval_secs must be >= 1".into(),
                    ));
                }
                n
            }
            None => {
                return Err(ConfigError::Missing(
                    "CID_POLL_INTERVAL_SECS (or loop.poll_interval_secs in --config file)"
                        .into(),
                ));
            }
        },
    };

    let env_cell_dep_raws = [
        nonempty(&get, "CID_LOCK_CELL_DEP"),
        nonempty(&get, "CID_PROCESS_CODE_CELL_DEP"),
        nonempty(&get, "CID_PROCESS_LOCK_CELL_DEP"),
        nonempty(&get, "CID_EXTRA_CELL_DEPS"),
    ];
    let extra_cell_deps = if env_cell_dep_raws.iter().any(|v| v.is_some()) {
        parse_cell_deps(&env_cell_dep_raws)?
    } else {
        file_cell_deps(&file.cell_deps)?
    };

    let process_type_code_hashes = match nonempty(&get, "CID_PROCESS_CODE_HASH") {
        Some(raw) => parse_hash_list(Some(raw))?,
        None => {
            let mut out = Vec::new();
            for h in process.type_code_hashes {
                out.push(parse_hex32(&h)?);
            }
            out
        }
    };

    let health_bind = match nonempty(&get, "CID_HEALTH_BIND") {
        Some(s) => Some(parse_health_bind(&s)?),
        None => match observe.health_bind.filter(|s| !s.trim().is_empty()) {
            Some(s) => Some(parse_health_bind(&s)?),
            None => None,
        },
    };

    let confirmation_depth = match nonempty(&get, "CID_CONFIRMATION_DEPTH") {
        Some(s) => parse_confirmation_depth(&s)?,
        None => match digest.confirmation_depth {
            Some(n) => {
                if n == 0 {
                    return Err(ConfigError::Invalid(
                        "loop.confirmation_depth must be >= 1".into(),
                    ));
                }
                n
            }
            None => DEFAULT_CONFIRMATION_DEPTH,
        },
    };

    let network_name = network.name.filter(|s| !s.trim().is_empty());

    let genesis_hash = match nonempty(&get, "CID_GENESIS_HASH") {
        Some(s) => Some(parse_hex32(&s)?),
        None => match network.genesis_hash.filter(|s| !s.trim().is_empty()) {
            Some(s) => Some(parse_hex32(&s)?),
            None => None,
        },
    };

    let validate_genesis = match nonempty(&get, "CID_VALIDATE_GENESIS") {
        Some(s) => parse_bool_flag(&s, "CID_VALIDATE_GENESIS")?,
        None => network.validate_genesis,
    };

    let log_format = match nonempty(&get, "CID_LOG_FORMAT") {
        Some(s) => LogFormat::from_var(Some(s)),
        None => LogFormat::from_var(observe.log_format),
    };

    if from_file && genesis_hash.is_none() {
        return Err(ConfigError::Missing(
            "network.genesis_hash (file mode) or CID_GENESIS_HASH".into(),
        ));
    }
    if validate_genesis && genesis_hash.is_none() {
        return Err(ConfigError::Invalid(
            "validate_genesis requires network.genesis_hash or CID_GENESIS_HASH".into(),
        ));
    }

    Ok(DaemonConfig {
        node_url,
        indexer_url,
        cid_lock_code_hash,
        hash_type,
        poll_interval: Duration::from_secs(secs),
        extra_cell_deps,
        process_type_code_hashes,
        health_bind,
        confirmation_depth,
        network_name,
        genesis_hash,
        validate_genesis,
        log_format,
    })
}

fn file_cell_deps(rows: &[CellDepFile]) -> Result<Vec<CellDep>, ConfigError> {
    let mut out = Vec::new();
    for row in rows {
        let tx_hash = parse_hex32(&row.tx_hash)?;
        let dep_type = match row.dep_type.as_deref() {
            None => DepType::Code,
            Some(s) => parse_dep_type(s)?,
        };
        let out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&tx_hash).expect("32 bytes"))
            .index(row.index.pack())
            .build();
        out.push(
            CellDep::new_builder()
                .out_point(out_point)
                .dep_type(dep_type.into())
                .build(),
        );
    }
    Ok(out)
}

fn parse_bool_flag(s: &str, key: &str) -> Result<bool, ConfigError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(ConfigError::Invalid(format!(
            "{key}={other} (want true/false)"
        ))),
    }
}

fn parse_health_bind(s: &str) -> Result<SocketAddr, ConfigError> {
    s.parse::<SocketAddr>()
        .map_err(|_| ConfigError::Invalid(format!("CID_HEALTH_BIND={s} (want host:port)")))
}

fn parse_poll_secs(raw: &str) -> Result<u64, ConfigError> {
    let secs: u64 = raw
        .parse()
        .map_err(|_| ConfigError::Invalid("CID_POLL_INTERVAL_SECS not u64".into()))?;
    if secs == 0 {
        return Err(ConfigError::Invalid(
            "CID_POLL_INTERVAL_SECS must be >= 1".into(),
        ));
    }
    Ok(secs)
}

fn parse_confirmation_depth(s: &str) -> Result<u64, ConfigError> {
    let n: u64 = s
        .parse()
        .map_err(|_| ConfigError::Invalid("CID_CONFIRMATION_DEPTH not u64".into()))?;
    if n == 0 {
        return Err(ConfigError::Invalid(
            "CID_CONFIRMATION_DEPTH must be >= 1".into(),
        ));
    }
    Ok(n)
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

fn parse_dep_type(s: &str) -> Result<DepType, ConfigError> {
    match s.to_ascii_lowercase().as_str() {
        "code" => Ok(DepType::Code),
        "dep_group" | "depgroup" => Ok(DepType::DepGroup),
        other => Err(ConfigError::Invalid(format!(
            "dep_type={other} (want code|dep_group)"
        ))),
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
        return Err(ConfigError::Invalid("value must be 32 bytes hex".into()));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| ConfigError::Invalid("value is not hex".into()))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_types::core::ScriptHashType;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_N: AtomicU64 = AtomicU64::new(0);

    fn write_toml(body: &str) -> PathBuf {
        let n = TMP_N.fetch_add(1, Ordering::SeqCst);
        let p = std::env::temp_dir().join(format!(
            "cid-cfg-{}-{}.toml",
            std::process::id(),
            n
        ));
        std::fs::write(&p, body).expect("write toml");
        p
    }

    fn required_env(k: &str) -> Option<String> {
        match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_LOCK_CODE_HASH" => Some("11".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            _ => None,
        }
    }

    fn minimal_toml() -> String {
        format!(
            r#"
[network]
name = "local-dev"
genesis_hash = "{g}"

[rpc]
url = "http://127.0.0.1:8114"

[cid_lock]
code_hash = "{lock}"
hash_type = "data1"

[process]
type_code_hashes = ["{proc}"]

[[cell_deps]]
label = "cid_lock"
tx_hash = "{dep}"
index = 0
dep_type = "code"

[loop]
poll_interval_secs = 5
confirmation_depth = 8

[observe]
health_bind = "127.0.0.1:9100"
log_format = "pretty"
"#,
            g = "aa".repeat(32),
            lock = "bb".repeat(32),
            proc = "cc".repeat(32),
            dep = "dd".repeat(32),
        )
    }

    #[test]
    fn from_vars_parses_required_and_defaults() {
        let cfg = DaemonConfig::from_vars(required_env).unwrap();
        assert_eq!(cfg.node_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.indexer_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.cid_lock_code_hash, [0x11u8; 32]);
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
        assert!(cfg.extra_cell_deps.is_empty());
        assert!(cfg.process_type_code_hashes.is_empty());
        assert!(cfg.health_bind.is_none());
        assert_eq!(cfg.confirmation_depth, DEFAULT_CONFIRMATION_DEPTH);
        assert!(cfg.genesis_hash.is_none());
        assert!(!cfg.validate_genesis);
        assert_eq!(cfg.log_format, LogFormat::Json);
    }

    #[test]
    fn from_vars_parses_health_bind() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_HEALTH_BIND" => Some("127.0.0.1:9100".into()),
            other => required_env(other),
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
            "CID_LOCK_CELL_DEP" => Some(format!("{}:0", "aa".repeat(32))),
            "CID_PROCESS_CODE_CELL_DEP" => Some(format!("0x{}:0x1", "bb".repeat(32))),
            "CID_PROCESS_CODE_HASH" => Some("0x".to_string() + &"cc".repeat(32)),
            other => required_env(other),
        })
        .unwrap();
        assert_eq!(cfg.extra_cell_deps.len(), 2);
        assert_eq!(cfg.process_type_code_hashes, vec![[0xccu8; 32]]);
        let loop_cfg = cfg.to_loop_config();
        assert_eq!(loop_cfg.extra_cell_deps.len(), 2);
        assert_eq!(
            cfg.extra_cell_deps[0].dep_type(),
            DepType::Code.into()
        );
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
        let msg = err.to_string();
        assert!(msg.contains("CID_CKB_RPC_URL"), "{msg}");
    }

    #[test]
    fn from_vars_parses_confirmation_depth() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CONFIRMATION_DEPTH" => Some("8".into()),
            other => required_env(other),
        })
        .unwrap();
        assert_eq!(cfg.confirmation_depth, 8);
        assert_eq!(cfg.to_loop_config().confirmation_depth, 8);
    }

    #[test]
    fn zero_confirmation_depth_invalid() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CONFIRMATION_DEPTH" => Some("0".into()),
            other => required_env(other),
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

    #[test]
    fn file_parses_sections() {
        let p = write_toml(&minimal_toml());
        let cfg = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap();
        assert_eq!(cfg.network_name.as_deref(), Some("local-dev"));
        assert_eq!(cfg.genesis_hash, Some([0xaau8; 32]));
        assert!(!cfg.validate_genesis);
        assert_eq!(cfg.node_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.cid_lock_code_hash, [0xbbu8; 32]);
        assert_eq!(cfg.hash_type, ScriptHashType::Data1);
        assert_eq!(cfg.process_type_code_hashes, vec![[0xccu8; 32]]);
        assert_eq!(cfg.extra_cell_deps.len(), 1);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
        assert_eq!(cfg.confirmation_depth, 8);
        assert_eq!(cfg.health_bind.unwrap().to_string(), "127.0.0.1:9100");
        assert_eq!(cfg.log_format, LogFormat::Pretty);
    }

    #[test]
    fn file_dep_group() {
        let body = format!(
            r#"
[network]
genesis_hash = "{g}"
[rpc]
url = "http://127.0.0.1:1"
[cid_lock]
code_hash = "{lock}"
[loop]
poll_interval_secs = 1
[[cell_deps]]
tx_hash = "{dep}"
index = 2
dep_type = "dep_group"
"#,
            g = "11".repeat(32),
            lock = "22".repeat(32),
            dep = "33".repeat(32),
        );
        let p = write_toml(&body);
        let cfg = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap();
        assert_eq!(
            cfg.extra_cell_deps[0].dep_type(),
            DepType::DepGroup.into()
        );
        let idx: u32 = cfg.extra_cell_deps[0].out_point().index().unpack();
        assert_eq!(idx, 2);
    }

    #[test]
    fn env_overrides_file() {
        let p = write_toml(&minimal_toml());
        let cfg = DaemonConfig::from_file_and_vars(Some(&p), |k| match k {
            "CID_CKB_RPC_URL" => Some("http://override:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("9".into()),
            "CID_CONFIRMATION_DEPTH" => Some("3".into()),
            "CID_LOG_FORMAT" => Some("json".into()),
            "CID_PROCESS_CODE_HASH" => Some("ee".repeat(32)),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.node_url, "http://override:8114");
        assert_eq!(cfg.poll_interval, Duration::from_secs(9));
        assert_eq!(cfg.confirmation_depth, 3);
        assert_eq!(cfg.log_format, LogFormat::Json);
        assert_eq!(cfg.process_type_code_hashes, vec![[0xeeu8; 32]]);
        // file cell deps kept: no env cell-dep vars
        assert_eq!(cfg.extra_cell_deps.len(), 1);
    }

    #[test]
    fn env_cell_deps_replace_file_list() {
        let p = write_toml(&minimal_toml());
        let cfg = DaemonConfig::from_file_and_vars(Some(&p), |k| match k {
            "CID_LOCK_CELL_DEP" => Some(format!("{}:3", "99".repeat(32))),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.extra_cell_deps.len(), 1);
        let idx: u32 = cfg.extra_cell_deps[0].out_point().index().unpack();
        assert_eq!(idx, 3);
        assert_eq!(
            cfg.extra_cell_deps[0].dep_type(),
            DepType::Code.into()
        );
    }

    #[test]
    fn file_mode_requires_genesis() {
        let body = r#"
[rpc]
url = "http://127.0.0.1:1"
[cid_lock]
code_hash = "11"
[loop]
poll_interval_secs = 1
"#
        .replace("11", &"11".repeat(32));
        let p = write_toml(&body);
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        assert!(matches!(err, ConfigError::Missing(ref m) if m.contains("genesis_hash")));
    }

    #[test]
    fn unknown_key_rejected() {
        let p = write_toml("[network]\ngenesis_hash = \"aa\"\nprivate_key = \"nope\"\n");
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("TOML") || msg.contains("unknown"), "{msg}");
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        let p = write_toml("secret = \"x\"\n");
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.to_ascii_lowercase().contains("unknown") || msg.contains("TOML"), "{msg}");
    }

    #[test]
    fn missing_config_file() {
        let err = DaemonConfig::from_file_and_vars(
            Some(Path::new("/no/such/cid-config-xyz.toml")),
            |_| None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot read") || msg.contains("No such"), "{msg}");
    }

    #[test]
    fn invalid_hex_in_file() {
        let p = write_toml(
            r#"
[network]
genesis_hash = "<GENESIS_BLOCK_HASH_32>"
[rpc]
url = "http://127.0.0.1:1"
[cid_lock]
code_hash = "00"
[loop]
poll_interval_secs = 1
"#,
        );
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn example_placeholders_cannot_load() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/cid.network.toml.example");
        let err = DaemonConfig::from_file_and_vars(Some(&root), |_| None).unwrap_err();
        assert!(
            matches!(err, ConfigError::Invalid(_) | ConfigError::Missing(_)),
            "{err}"
        );
    }

    #[test]
    fn validate_genesis_without_hash_fails() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_VALIDATE_GENESIS" => Some("true".into()),
            other => required_env(other),
        })
        .unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(ref m) if m.contains("validate_genesis")));
    }

    #[test]
    fn parse_cli_config_and_help() {
        assert_eq!(
            parse_cli(&["d".into(), "--help".into()]).unwrap(),
            CliAction::Help
        );
        assert_eq!(
            parse_cli(&[
                "d".into(),
                "--config".into(),
                "n.toml".into()
            ])
            .unwrap(),
            CliAction::Run {
                config_path: Some(PathBuf::from("n.toml"))
            }
        );
        assert_eq!(
            parse_cli(&["d".into(), "--config=n.toml".into()]).unwrap(),
            CliAction::Run {
                config_path: Some(PathBuf::from("n.toml"))
            }
        );
        assert_eq!(
            parse_cli(&["d".into()]).unwrap(),
            CliAction::Run {
                config_path: None
            }
        );
        assert!(parse_cli(&["d".into(), "--nope".into()]).is_err());
        assert!(parse_cli(&["d".into(), "--config".into()]).is_err());
    }

    #[test]
    fn config_path_cli_wins_over_env() {
        let cli = CliAction::Run {
            config_path: Some(PathBuf::from("cli.toml")),
        };
        assert_eq!(
            resolve_config_path(&cli, Some("env.toml".into())),
            Some(PathBuf::from("cli.toml"))
        );
        let cli = CliAction::Run { config_path: None };
        assert_eq!(
            resolve_config_path(&cli, Some("env.toml".into())),
            Some(PathBuf::from("env.toml"))
        );
        assert_eq!(resolve_config_path(&cli, Some("".into())), None);
        assert_eq!(resolve_config_path(&cli, None), None);
    }

    #[test]
    fn rpc_endpoint_for_log_strips_userinfo() {
        let redacted = rpc_endpoint_for_log("http://user:s3cret@127.0.0.1:8114/rpc?x=1");
        assert_eq!(redacted, "http://127.0.0.1:8114");
        assert!(!redacted.contains("s3cret"));
        assert!(!redacted.contains("user:"));
        assert_eq!(
            rpc_endpoint_for_log("not-a-url"),
            "<invalid-rpc-url>"
        );
        assert_eq!(
            rpc_endpoint_for_log("ftp://127.0.0.1"),
            "<invalid-rpc-url>"
        );
    }
}
