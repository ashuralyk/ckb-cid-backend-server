use crate::logging::LogFormat;
use cid_backend_core::digest_loop::{
    LoopConfig, DEFAULT_CONFIRMATION_DEPTH, DEFAULT_MAX_ASSEMBLED_TX_BYTES,
};
use cid_backend_core::type_id::{TypeIdCodeDep, TypeIdIdentity};
use cid_backend_core::DEFAULT_MAX_REGISTRY_ENTRIES;
use cid_backend_core::{CidLockIdentity, DEFAULT_FEE_RATE, MAX_FEE_RATE};
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
    /// Primary CID lock scan `code_hash`. When `type_id_args` is set this is the
    /// derived TYPE_ID type-script hash (`hash_type` forced to `type`). Binary
    /// data hashes are not derived here — list them under `extra_scan_locks`
    /// / `legacy_scan_locks` after a Type-ID redeploy.
    pub cid_lock_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
    /// Static CellDeps (extras / local fixtures). Merged after Type-ID resolve.
    pub extra_cell_deps: Vec<CellDep>,
    /// Type-ID identities for code cells (CID lock / Process code / Process lock).
    pub type_id_code_deps: Vec<TypeIdCodeDep>,
    /// **Deprecated / ignored at runtime.** Process workers discover state via
    /// on-disk registry + exact TYPE_ID args (`find_live_cells_by_type_script`).
    /// Parsed only for backward-compatible config; not passed to `CkbRpc`.
    pub process_type_code_hashes: Vec<[u8; 32]>,
    /// Optional health/stats bind. Unset/empty disables the listener.
    pub health_bind: Option<SocketAddr>,
    /// Optional Process registry HTTP API bind. Unset/empty disables the listener.
    pub api_bind: Option<SocketAddr>,
    /// When false (default), `api_bind` must be loopback. Explicit opt-in required
    /// for non-loopback binds (P1-7).
    pub allow_non_loopback_api: bool,
    /// On-disk Process registry JSON path (atomic write+rename).
    pub registry_path: PathBuf,
    /// Max registered Processes.
    pub registry_max_entries: usize,
    /// Confirmation depth for submitted execution txs. Default 24.
    pub confirmation_depth: u64,
    pub allow_missing_filter: bool,
    pub example_policy_data_hashes: Vec<[u8; 32]>,
    /// Fee rate for allowlisted bounded refunds. The guest sets DigestOk fees.
    pub fee_rate: u64,
    /// Audited CID lock identities that may use bounded refund fee mode.
    /// Empty (default) ⇒ legacy exact refunds. Each entry must already be a
    /// scanned identity (primary or `extra_scan_locks`); this list never
    /// expands the indexer scan set. Never insert undeployed candidate hashes.
    pub bounded_refund_locks: Vec<CidLockIdentity>,
    /// Extra CID lock identities to scan (e.g. legacy data-hash locks after
    /// a Type-ID redeploy). Sole way to discover non-primary Intentions.
    pub extra_scan_locks: Vec<CidLockIdentity>,
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
///
/// Only `@` inside the URI authority is treated as userinfo. `@` in path, query,
/// or fragment must not be mistaken for credentials (would leak the suffix as host).
pub fn rpc_endpoint_for_log(raw: &str) -> String {
    let Some(scheme_end) = raw.find("://") else {
        return "<invalid-rpc-url>".into();
    };
    let scheme = &raw[..scheme_end];
    if scheme != "http" && scheme != "https" {
        return "<invalid-rpc-url>".into();
    }
    let rest = &raw[scheme_end + 3..];
    // RFC 3986: authority ends at the first `/`, `?`, or `#`.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let hostport = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);
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
            // CID lock Type-ID only; Process code Type IDs come from the registry.
            type_id_code_deps: self
                .type_id_code_deps
                .iter()
                .filter(|d| d.label == "cid_lock")
                .cloned()
                .collect(),
            confirmation_depth: self.confirmation_depth,
            extra_scan_locks: self.extra_scan_locks.clone(),
            refund_policy: cid_backend_core::fee::RefundPolicy {
                fee_rate: self.fee_rate,
                bounded_locks: self.bounded_refund_locks.clone(),
            },
            max_assembled_tx_bytes: DEFAULT_MAX_ASSEMBLED_TX_BYTES,
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
                return Err(ConfigError::Invalid("--config requires a file path".into()));
            }
            if config_path.replace(PathBuf::from(p)).is_some() {
                return Err(ConfigError::Invalid("duplicate --config".into()));
            }
            continue;
        }
        if arg == "--config" {
            let Some(p) = iter.next() else {
                return Err(ConfigError::Invalid("--config requires a file path".into()));
            };
            if p.starts_with('-') {
                return Err(ConfigError::Invalid("--config requires a file path".into()));
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
        CliAction::Run {
            config_path: Some(p),
        } => Some(p.clone()),
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
     unless network.validate_genesis = true).\n\
     Routing uses cid_lock.type_id_args (+ optional type_id full hash).\n\
     Register Process state and code Type IDs through the HTTP API.\n\
     Type-ID CellDeps refresh each tick. Static [[cell_deps]] supply extras.\n\
     loop.allow_missing_filter defaults to false.\n"
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
    process_lock: Option<ProcessLockSection>,
    #[serde(default)]
    cell_deps: Vec<CellDepFile>,
    #[serde(rename = "loop", default)]
    digest: Option<LoopSection>,
    #[serde(default)]
    observe: Option<ObserveSection>,
    #[serde(default)]
    registry: Option<RegistrySection>,
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
    /// Explicit lock `code_hash` when not using Type-ID. With `type_id_args`,
    /// must equal the derived TYPE_ID script hash (or be omitted).
    #[serde(default)]
    code_hash: Option<String>,
    /// The primary CID lock requires `type` (default).
    #[serde(default)]
    hash_type: Option<String>,
    /// TYPE_ID type-script args (32 bytes). Preferred: derives scan identity
    /// (`code_hash` = type-script hash, `hash_type=type`) and live CellDep refresh.
    #[serde(default)]
    type_id_args: Option<String>,
    /// Optional full type-script hash for fail-closed check. Not args.
    #[serde(default)]
    type_id: Option<String>,
    /// Audited `(code_hash, hash_type)` pairs that may use bounded refunds.
    /// Empty (default) ⇒ legacy exact for all scanned Intentions. Must be a
    /// subset of primary + `legacy_scan_locks` (fee mode only; not a scan source).
    #[serde(default)]
    bounded_refund_locks: Vec<LockIdentityFile>,
    /// Extra locks to scan besides the Type-ID / primary identity (e.g. old
    /// data1 Intentions after a Type-ID redeploy).
    #[serde(default)]
    legacy_scan_locks: Vec<LockIdentityFile>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LockIdentityFile {
    code_hash: String,
    #[serde(default)]
    hash_type: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessSection {
    #[serde(default)]
    example_policy_data_hashes: Vec<String>,
    #[serde(default)]
    type_code_hashes: Vec<String>,
    #[serde(default)]
    type_id_args: Option<String>,
    #[serde(default)]
    type_id: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessLockSection {
    #[serde(default)]
    type_id_args: Option<String>,
    #[serde(default)]
    type_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CellDepFile {
    /// Optional role label (`cid_lock` / `process_code` / `process_lock`).
    /// Used to reject static role deps when the same role uses Type-ID discovery.
    /// Unlabeled rows are treated as unrelated extras (secp / xUDT / …).
    #[serde(default)]
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
    #[serde(default)]
    allow_missing_filter: bool,
    #[serde(default)]
    fee_rate: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserveSection {
    #[serde(default)]
    health_bind: Option<String>,
    /// Process registry HTTP API bind (POST/GET/DELETE /v1/processes).
    #[serde(default)]
    api_bind: Option<String>,
    /// Explicit opt-in for non-loopback `api_bind` (default false).
    #[serde(default)]
    allow_non_loopback_api: Option<bool>,
    #[serde(default)]
    log_format: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySection {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    max_entries: Option<usize>,
}

fn read_file_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Invalid(format!("cannot read config {}: {e}", path.display())))?;
    toml::from_str(&raw).map_err(|e| format_toml_parse_error(path, &raw, e))
}

/// Build a fail-closed TOML diagnostic: category + line/column only.
///
/// Never emit `toml::de::Error` Display (raw line + caret) or `message()` text.
/// Serde messages embed typed string values (`invalid type: string "…"`) and
/// attacker-controlled unknown field names; both must stay out of logs/stderr.
fn format_toml_parse_error(path: &Path, raw: &str, err: toml::de::Error) -> ConfigError {
    let category = toml_error_category(err.message());
    let mut msg = format!("TOML {}: {category}", path.display());
    if let Some(span) = err.span() {
        let (line, col) = line_col_of(raw, span.start);
        msg.push_str(&format!(" (line {line}, column {col})"));
    }
    ConfigError::Invalid(msg)
}

/// Map serde/toml message keywords to a fixed safe label. Message text is only
/// classified — never copied into the returned diagnostic.
fn toml_error_category(message: &str) -> &'static str {
    let m = message.to_ascii_lowercase();
    if m.contains("unknown field") {
        "unknown field"
    } else if m.contains("missing field") {
        "missing field"
    } else if m.contains("duplicate") {
        "duplicate key"
    } else if m.contains("invalid type")
        || m.contains("invalid value")
        || (m.contains("expected")
            && (m.contains("u64")
                || m.contains("u32")
                || m.contains("i64")
                || m.contains("bool")
                || m.contains("integer")
                || m.contains("string")
                || m.contains("map")
                || m.contains("sequence")))
    {
        "invalid value type"
    } else {
        "parse error"
    }
}

/// 1-based line and column for `byte_index` into `raw` (UTF-8 safe for column).
fn line_col_of(raw: &str, byte_index: usize) -> (usize, usize) {
    let byte_index = byte_index.min(raw.len());
    let before = &raw[..byte_index];
    let line = before.bytes().filter(|&b| b == b'\n').count() + 1;
    let col = before
        .rsplit('\n')
        .next()
        .map(|s| s.chars().count())
        .unwrap_or(0)
        + 1;
    (line, col)
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
    let process_lock = file.process_lock.unwrap_or_default();
    let digest = file.digest.unwrap_or_default();
    let observe = file.observe.unwrap_or_default();
    let registry = file.registry.unwrap_or_default();

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

    let cid_lock_identity = parse_type_id_identity(
        &get,
        "CID_LOCK_TYPE_ID_ARGS",
        "CID_LOCK_TYPE_ID",
        cid_lock.type_id_args.as_deref(),
        cid_lock.type_id.as_deref(),
        "cid_lock",
    )?;
    let process_code_identity = parse_type_id_identity(
        &get,
        "CID_PROCESS_TYPE_ID_ARGS",
        "CID_PROCESS_TYPE_ID",
        process.type_id_args.as_deref(),
        process.type_id.as_deref(),
        "process",
    )?;
    let process_lock_identity = parse_type_id_identity(
        &get,
        "CID_PROCESS_LOCK_TYPE_ID_ARGS",
        "CID_PROCESS_LOCK_TYPE_ID",
        process_lock.type_id_args.as_deref(),
        process_lock.type_id.as_deref(),
        "process_lock",
    )?;

    let hash_type = match nonempty(&get, "CID_LOCK_HASH_TYPE").or(cid_lock.hash_type) {
        Some(value) => parse_hash_type(&value)?,
        None => ScriptHashType::Type,
    };
    if hash_type != ScriptHashType::Type {
        return Err(ConfigError::Invalid(
            "primary CID lock requires hash_type=type and the CID code Type-ID script hash; \
             move data-hash identities to cid_lock.legacy_scan_locks / CID_LEGACY_SCAN_LOCKS (scan-only)"
                .into(),
        ));
    }
    let cid_lock_code_hash = if let Some(ref id) = cid_lock_identity {
        let derived = id.type_script_hash();
        let explicit_hash = match nonempty(&get, "CID_LOCK_CODE_HASH") {
            Some(v) => Some(parse_hex32(&v)?),
            None => match cid_lock.code_hash.filter(|s| !s.trim().is_empty()) {
                Some(v) => Some(parse_hex32(&v)?),
                None => None,
            },
        };
        if let Some(h) = explicit_hash {
            if h != derived {
                return Err(ConfigError::Invalid(format!(
                    "cid_lock.code_hash / CID_LOCK_CODE_HASH 0x{} != type_id-derived 0x{}",
                    hex::encode(h),
                    hex::encode(derived)
                )));
            }
        }
        derived
    } else {
        match nonempty(&get, "CID_LOCK_CODE_HASH") {
            Some(v) => parse_hex32(&v)?,
            None => match cid_lock.code_hash.filter(|s| !s.trim().is_empty()) {
                Some(v) => parse_hex32(&v)?,
                None => {
                    return Err(ConfigError::Missing(
                        "CID_LOCK_CODE_HASH / cid_lock.code_hash, or cid_lock.type_id_args".into(),
                    ));
                }
            },
        }
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
                    "CID_POLL_INTERVAL_SECS (or loop.poll_interval_secs in --config file)".into(),
                ));
            }
        },
    };

    // Role-specific static code deps conflict with Type-ID discovery for the
    // same role (stale outpoints would survive upgrades). Unlabeled extras
    // (secp / xUDT / always-success) remain allowed — we cannot infer their
    // historical role from outpoint alone.
    if cid_lock_identity.is_some() {
        if nonempty(&get, "CID_LOCK_CELL_DEP").is_some() {
            return Err(ConfigError::Invalid(
                "CID_LOCK_CELL_DEP conflicts with cid_lock.type_id_args / CID_LOCK_TYPE_ID_ARGS; \
                 omit the static role dep (Type-ID resolves the code cell each tick)"
                    .into(),
            ));
        }
        reject_labeled_static_role(&file.cell_deps, "cid_lock")?;
    }
    if process_code_identity.is_some() {
        if nonempty(&get, "CID_PROCESS_CODE_CELL_DEP").is_some() {
            return Err(ConfigError::Invalid(
                "CID_PROCESS_CODE_CELL_DEP conflicts with process.type_id_args / \
                 CID_PROCESS_TYPE_ID_ARGS; omit the static role dep"
                    .into(),
            ));
        }
        reject_labeled_static_role(&file.cell_deps, "process_code")?;
    }
    if process_lock_identity.is_some() {
        if nonempty(&get, "CID_PROCESS_LOCK_CELL_DEP").is_some() {
            return Err(ConfigError::Invalid(
                "CID_PROCESS_LOCK_CELL_DEP conflicts with process_lock.type_id_args / \
                 CID_PROCESS_LOCK_TYPE_ID_ARGS; omit the static role dep"
                    .into(),
            ));
        }
        reject_labeled_static_role(&file.cell_deps, "process_lock")?;
    }

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

    let mut process_type_code_hashes = match nonempty(&get, "CID_PROCESS_CODE_HASH") {
        Some(raw) => parse_hash_list(Some(raw))?,
        None => {
            let mut out = Vec::new();
            for h in process.type_code_hashes {
                out.push(parse_hex32(&h)?);
            }
            out
        }
    };
    if !process_type_code_hashes.is_empty() {
        log::warn!(
            "CID_PROCESS_CODE_HASH / process.type_code_hashes is deprecated: \
             Process discovery uses the registry; indexer prefixes are ignored"
        );
        process_type_code_hashes.clear();
    }

    let mut type_id_code_deps = Vec::new();
    if let Some(id) = cid_lock_identity {
        type_id_code_deps.push(TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: id,
        });
    }
    if let Some(id) = process_code_identity {
        type_id_code_deps.push(TypeIdCodeDep {
            label: "process_code".into(),
            identity: id,
        });
    }
    if let Some(id) = process_lock_identity {
        type_id_code_deps.push(TypeIdCodeDep {
            label: "process_lock".into(),
            identity: id,
        });
    }

    let health_bind = match nonempty(&get, "CID_HEALTH_BIND") {
        Some(s) => Some(parse_health_bind(&s)?),
        None => match observe.health_bind.filter(|s| !s.trim().is_empty()) {
            Some(s) => Some(parse_health_bind(&s)?),
            None => None,
        },
    };

    let allow_non_loopback_api = match nonempty(&get, "CID_ALLOW_NON_LOOPBACK_API") {
        Some(s) => parse_bool_flag(&s, "CID_ALLOW_NON_LOOPBACK_API")?,
        None => observe.allow_non_loopback_api.unwrap_or(false),
    };

    let api_bind = match nonempty(&get, "CID_API_BIND") {
        Some(s) => Some(parse_socket_bind(&s, "CID_API_BIND")?),
        None => match observe.api_bind.filter(|s| !s.trim().is_empty()) {
            Some(s) => Some(parse_socket_bind(&s, "observe.api_bind")?),
            None => None,
        },
    };
    if let Some(addr) = api_bind {
        if !addr.ip().is_loopback() && !allow_non_loopback_api {
            return Err(ConfigError::Invalid(
                "api_bind / CID_API_BIND is not loopback; set                  observe.allow_non_loopback_api = true or CID_ALLOW_NON_LOOPBACK_API=true                  to expose the unauthenticated registry API (keyless; no signing keys,                  but register/delete enables Process workers)".into(),
            ));
        }
    }

    let registry_path = match nonempty(&get, "CID_REGISTRY_PATH") {
        Some(s) => PathBuf::from(s),
        None => match registry.path.filter(|s| !s.trim().is_empty()) {
            Some(s) => PathBuf::from(s),
            None => PathBuf::from("data/process-registry.json"),
        },
    };
    let registry_max_entries = match nonempty(&get, "CID_REGISTRY_MAX_ENTRIES") {
        Some(s) => {
            let n: usize = s
                .parse()
                .map_err(|_| ConfigError::Invalid("CID_REGISTRY_MAX_ENTRIES not usize".into()))?;
            if n == 0 {
                return Err(ConfigError::Invalid(
                    "CID_REGISTRY_MAX_ENTRIES must be >= 1".into(),
                ));
            }
            n
        }
        None => match registry.max_entries {
            Some(0) => {
                return Err(ConfigError::Invalid(
                    "registry.max_entries must be >= 1".into(),
                ));
            }
            Some(n) => n,
            None => DEFAULT_MAX_REGISTRY_ENTRIES,
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

    let fee_rate = match nonempty(&get, "CID_FEE_RATE") {
        Some(s) => parse_fee_rate(&s, "CID_FEE_RATE")?,
        None => match digest.fee_rate {
            Some(n) => validate_fee_rate(n, "loop.fee_rate")?,
            None => DEFAULT_FEE_RATE,
        },
    };
    // Capability allowlist: empty ⇒ legacy exact. Env overrides TOML lists.
    let bounded_refund_locks = match nonempty(&get, "CID_BOUNDED_REFUND_LOCKS") {
        Some(s) => parse_lock_identity_list(&s, "CID_BOUNDED_REFUND_LOCKS")?,
        None => parse_lock_identity_files(
            &cid_lock.bounded_refund_locks,
            "cid_lock.bounded_refund_locks",
        )?,
    };
    let extra_scan_locks = match nonempty(&get, "CID_LEGACY_SCAN_LOCKS") {
        Some(s) => parse_lock_identity_list(&s, "CID_LEGACY_SCAN_LOCKS")?,
        None => {
            parse_lock_identity_files(&cid_lock.legacy_scan_locks, "cid_lock.legacy_scan_locks")?
        }
    };
    validate_bounded_refund_subset(
        cid_lock_code_hash,
        hash_type,
        &extra_scan_locks,
        &bounded_refund_locks,
    )?;

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

    let allow_missing_filter = match nonempty(&get, "CID_ALLOW_MISSING_FILTER") {
        Some(s) => parse_bool_flag(&s, "CID_ALLOW_MISSING_FILTER")?,
        None => digest.allow_missing_filter,
    };
    let example_policy_data_hashes = match nonempty(&get, "CID_EXAMPLE_POLICY_DATA_HASHES") {
        Some(raw) => parse_hash_list(Some(raw))?,
        None => process
            .example_policy_data_hashes
            .iter()
            .map(|hash| parse_hex32(hash))
            .collect::<Result<_, _>>()?,
    };

    Ok(DaemonConfig {
        node_url,
        indexer_url,
        cid_lock_code_hash,
        hash_type,
        poll_interval: Duration::from_secs(secs),
        extra_cell_deps,
        type_id_code_deps,
        process_type_code_hashes,
        health_bind,
        api_bind,
        allow_non_loopback_api,
        registry_path,
        registry_max_entries,
        confirmation_depth,
        allow_missing_filter,
        example_policy_data_hashes,
        fee_rate,
        bounded_refund_locks,
        extra_scan_locks,
        network_name,
        genesis_hash,
        validate_genesis,
        log_format,
    })
}

fn parse_type_id_identity(
    get: &impl Fn(&str) -> Option<String>,
    args_env: &str,
    hash_env: &str,
    file_args: Option<&str>,
    file_hash: Option<&str>,
    label: &str,
) -> Result<Option<TypeIdIdentity>, ConfigError> {
    let args_raw = match nonempty(get, args_env) {
        Some(v) => Some(v),
        None => file_args
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
    };
    let hash_raw = match nonempty(get, hash_env) {
        Some(v) => Some(v),
        None => file_hash
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
    };
    match (args_raw, hash_raw) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ConfigError::Invalid(format!(
            "{label}.type_id (full type-script hash) set without type_id_args; \
             do not treat type_id as args — set {args_env} / {label}.type_id_args"
        ))),
        (Some(args), expected) => {
            let args = parse_hex32(&args)?;
            let expected = match expected {
                Some(h) => Some(parse_hex32(&h)?),
                None => None,
            };
            let id = TypeIdIdentity {
                args,
                expected_type_hash: expected,
            };
            id.validate_expected().map_err(ConfigError::Invalid)?;
            Ok(Some(id))
        }
    }
}

fn reject_labeled_static_role(rows: &[CellDepFile], role: &str) -> Result<(), ConfigError> {
    for row in rows {
        if row
            .label
            .as_deref()
            .map(|l| l.trim() == role)
            .unwrap_or(false)
        {
            return Err(ConfigError::Invalid(format!(
                "[[cell_deps]] label=\"{role}\" conflicts with {role}.type_id_args; \
                 remove the static role dep or the Type-ID identity (unlabeled extras \
                 like xUDT/secp are fine)"
            )));
        }
    }
    Ok(())
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

fn parse_health_bind(s: &str) -> Result<SocketAddr, ConfigError> {
    parse_socket_bind(s, "CID_HEALTH_BIND")
}

fn parse_socket_bind(s: &str, label: &str) -> Result<SocketAddr, ConfigError> {
    s.parse::<SocketAddr>().map_err(|_| {
        ConfigError::Invalid(format!("{label}={s} (want host:port, e.g. 127.0.0.1:9101)"))
    })
}

fn parse_bool_flag(s: &str, label: &str) -> Result<bool, ConfigError> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(ConfigError::Invalid(format!(
            "{label}={other} (want true/false)"
        ))),
    }
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

fn parse_lock_identity_files(
    rows: &[LockIdentityFile],
    label: &str,
) -> Result<Vec<CidLockIdentity>, ConfigError> {
    let mut out = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let code_hash = parse_hex32(&row.code_hash).map_err(|_| {
            ConfigError::Invalid(format!("{label}[{i}].code_hash must be 32 bytes hex"))
        })?;
        let hash_type = match row.hash_type.as_deref() {
            None | Some("") => ScriptHashType::Type,
            Some(s) => parse_hash_type(s)?,
        };
        out.push(CidLockIdentity::new(code_hash, hash_type));
    }
    Ok(out)
}

/// `bounded_refund_locks` selects fee mode only; every entry must already be
/// a scanned identity (primary or `legacy_scan_locks` / `extra_scan_locks`).
fn validate_bounded_refund_subset(
    primary_code_hash: [u8; 32],
    primary_hash_type: ScriptHashType,
    extra_scan_locks: &[CidLockIdentity],
    bounded_refund_locks: &[CidLockIdentity],
) -> Result<(), ConfigError> {
    let primary = CidLockIdentity::new(primary_code_hash, primary_hash_type);
    for id in bounded_refund_locks {
        let scanned = *id == primary || extra_scan_locks.iter().any(|x| x == id);
        if !scanned {
            return Err(ConfigError::Invalid(format!(
                "cid_lock.bounded_refund_locks / CID_BOUNDED_REFUND_LOCKS entry \
                 0x{}:{:?} is not a scanned identity; list it under the primary \
                 cid_lock identity or cid_lock.legacy_scan_locks / CID_LEGACY_SCAN_LOCKS \
                 (allowlist selects fee mode only and never expands the scan set)",
                hex::encode(id.code_hash),
                id.hash_type,
            )));
        }
    }
    Ok(())
}

/// `0xhash:hash_type,0xhash2:data1` (hash_type optional, default `type`).
fn parse_lock_identity_list(s: &str, label: &str) -> Result<Vec<CidLockIdentity>, ConfigError> {
    let mut out = Vec::new();
    for (i, part) in s.split(',').enumerate() {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (hash_s, ht_s) = match part.rsplit_once(':') {
            Some((h, t)) if looks_like_hash_type(t) => (h, Some(t)),
            _ => (part, None),
        };
        let code_hash = parse_hex32(hash_s).map_err(|_| {
            ConfigError::Invalid(format!("{label}[{i}] code_hash must be 32 bytes hex"))
        })?;
        let hash_type = match ht_s {
            None => ScriptHashType::Type,
            Some(t) => parse_hash_type(t)?,
        };
        out.push(CidLockIdentity::new(code_hash, hash_type));
    }
    Ok(out)
}

fn looks_like_hash_type(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "type" | "data" | "data1" | "data2"
    )
}

fn parse_fee_rate(s: &str, label: &str) -> Result<u64, ConfigError> {
    let n = s
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError::Invalid(format!("{label} not u64")))?;
    validate_fee_rate(n, label)
}

fn validate_fee_rate(n: u64, label: &str) -> Result<u64, ConfigError> {
    if n == 0 {
        return Err(ConfigError::Invalid(format!("{label} must be > 0")));
    }
    if n > MAX_FEE_RATE {
        return Err(ConfigError::Invalid(format!(
            "{label} must be <= {MAX_FEE_RATE}"
        )));
    }
    Ok(n)
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
        let p = std::env::temp_dir().join(format!("cid-cfg-{}-{}.toml", std::process::id(), n));
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
hash_type = "type"

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
        assert!(cfg.type_id_code_deps.is_empty());
        assert!(cfg.process_type_code_hashes.is_empty());
        assert!(cfg.health_bind.is_none());
        assert_eq!(cfg.confirmation_depth, DEFAULT_CONFIRMATION_DEPTH);
        assert_eq!(cfg.fee_rate, DEFAULT_FEE_RATE);
        assert!(
            cfg.bounded_refund_locks.is_empty(),
            "legacy exact refund must remain the default (empty allowlist)"
        );
        assert_eq!(
            cfg.to_loop_config().max_assembled_tx_bytes,
            DEFAULT_MAX_ASSEMBLED_TX_BYTES
        );
        assert!(cfg.extra_scan_locks.is_empty());
        assert!(cfg.genesis_hash.is_none());
        assert!(!cfg.validate_genesis);
        assert_eq!(cfg.log_format, LogFormat::Json);
    }

    #[test]
    fn audited_example_hashes_load_from_toml_and_env() {
        let body = minimal_toml().replace(
            "[process]",
            &format!(
                "[process]\nexample_policy_data_hashes = [\"0x{}\"]",
                "11".repeat(32)
            ),
        );
        let path = write_toml(&body);
        assert_eq!(
            DaemonConfig::from_file_and_vars(Some(&path), |_| None)
                .unwrap()
                .example_policy_data_hashes,
            [[0x11; 32]],
        );
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |key| {
            (key == "CID_EXAMPLE_POLICY_DATA_HASHES")
                .then(|| format!("{},{}", "22".repeat(32), "33".repeat(32)))
        })
        .unwrap();
        assert_eq!(cfg.example_policy_data_hashes, [[0x22; 32], [0x33; 32]]);
        assert!(DaemonConfig::from_vars(|key| match key {
            "CID_EXAMPLE_POLICY_DATA_HASHES" => Some("invalid".into()),
            _ => required_env(key),
        })
        .is_err());
    }

    #[test]
    fn fee_rate_defaults_and_toml_env_override() {
        let path = write_toml(&minimal_toml().replace(
            "confirmation_depth = 8",
            "confirmation_depth = 8\nfee_rate = 2500",
        ));
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert_eq!(cfg.fee_rate, 2500);
        assert_eq!(cfg.to_loop_config().refund_policy.fee_rate, 2500);

        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |k| match k {
            "CID_FEE_RATE" => Some("4000".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.to_loop_config().refund_policy.fee_rate, 4000);
    }

    #[test]
    fn bounded_refund_allowlist_toml_and_env() {
        let path = write_toml(&minimal_toml());
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert!(cfg.bounded_refund_locks.is_empty());

        // Allowlist primary (type) + scan a legacy data1; allowlist may also
        // name the legacy identity once it is in legacy_scan_locks.
        let path = write_toml(&format!(
            "{base}\n[[cid_lock.bounded_refund_locks]]\ncode_hash = \"0x{primary}\"\nhash_type = \"type\"\n[[cid_lock.legacy_scan_locks]]\ncode_hash = \"0x{legacy}\"\nhash_type = \"data1\"\n[[cid_lock.bounded_refund_locks]]\ncode_hash = \"0x{legacy}\"\nhash_type = \"data1\"\n",
            base = minimal_toml(),
            primary = "bb".repeat(32),
            legacy = "22".repeat(32),
        ));
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert_eq!(cfg.bounded_refund_locks.len(), 2);
        assert_eq!(cfg.bounded_refund_locks[0].code_hash, [0xbb; 32]);
        assert_eq!(cfg.bounded_refund_locks[0].hash_type, ScriptHashType::Type);
        assert_eq!(cfg.extra_scan_locks.len(), 1);
        assert_eq!(cfg.extra_scan_locks[0].hash_type, ScriptHashType::Data1);
        assert_eq!(cfg.bounded_refund_locks.len(), 2);
        let scan = cfg.to_loop_config().scan_lock_identities();
        assert_eq!(
            scan.len(),
            2,
            "scan must include the configured legacy lock"
        );
        let legacy = CidLockIdentity::new([0x22; 32], ScriptHashType::Data1);
        assert!(scan.contains(&legacy));
        assert_eq!(
            cfg.to_loop_config().refund_policy.mode_for(&legacy),
            cid_backend_core::RefundFeeMode::Bounded,
        );

        // Env allowlist must still be a scanned identity (primary type here).
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |k| match k {
            "CID_BOUNDED_REFUND_LOCKS" => Some(format!("0x{}:type", "bb".repeat(32))),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.bounded_refund_locks.len(), 1);
        assert_eq!(cfg.bounded_refund_locks[0].code_hash, [0xbb; 32]);
        assert_eq!(cfg.bounded_refund_locks[0].hash_type, ScriptHashType::Type);
    }

    #[test]
    fn bounded_refund_only_data1_not_in_scan_set_rejected() {
        let path = write_toml(&format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
type_id_args = "0x{args}"
[[cid_lock.bounded_refund_locks]]
code_hash = "0x{data1}"
hash_type = "data1"
[loop]
poll_interval_secs = 5
"#,
            g = "11".repeat(32),
            args = "aa".repeat(32),
            data1 = "d1".repeat(32),
        ));
        let err = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bounded_refund_locks") || msg.contains("CID_BOUNDED_REFUND_LOCKS"),
            "{msg}"
        );
        assert!(
            msg.contains("not a scanned identity") || msg.contains("legacy_scan_locks"),
            "{msg}"
        );
    }

    #[test]
    fn bounded_refund_data1_ok_when_also_legacy_scan() {
        let path = write_toml(&format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
type_id_args = "0x{args}"
[[cid_lock.legacy_scan_locks]]
code_hash = "0x{data1}"
hash_type = "data1"
[[cid_lock.bounded_refund_locks]]
code_hash = "0x{data1}"
hash_type = "data1"
[loop]
poll_interval_secs = 5
"#,
            g = "11".repeat(32),
            args = "aa".repeat(32),
            data1 = "d1".repeat(32),
        ));
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.extra_scan_locks.len(), 1);
        assert_eq!(cfg.bounded_refund_locks.len(), 1);
        assert_eq!(cfg.bounded_refund_locks[0].hash_type, ScriptHashType::Data1);
        let loop_cfg = cfg.to_loop_config();
        assert_eq!(loop_cfg.scan_lock_identities().len(), 2);
        assert_eq!(
            loop_cfg.scan_lock_identities()[0].hash_type,
            ScriptHashType::Type
        );
    }

    #[test]
    fn bounded_refund_type_id_primary_ok() {
        use cid_backend_core::type_id::type_id_script_hash;
        let derived = type_id_script_hash(&[0xaau8; 32]);
        let path = write_toml(&format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
type_id_args = "0x{args}"
[[cid_lock.bounded_refund_locks]]
code_hash = "0x{tid}"
hash_type = "type"
[loop]
poll_interval_secs = 5
"#,
            g = "11".repeat(32),
            args = "aa".repeat(32),
            tid = hex::encode(derived),
        ));
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert_eq!(cfg.cid_lock_code_hash, derived);
        assert_eq!(cfg.bounded_refund_locks.len(), 1);
        assert_eq!(cfg.bounded_refund_locks[0].code_hash, derived);
        assert_eq!(
            cfg.to_loop_config().scan_lock_identities(),
            vec![CidLockIdentity::new(derived, ScriptHashType::Type)]
        );
    }

    #[test]
    fn fee_rate_zero_and_too_large_rejected() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_FEE_RATE" => Some("0".into()),
            other => required_env(other),
        })
        .unwrap_err();
        assert!(err.to_string().contains("must be > 0"), "{err}");

        let err = DaemonConfig::from_vars(|k| match k {
            "CID_FEE_RATE" => Some((MAX_FEE_RATE + 1).to_string()),
            other => required_env(other),
        })
        .unwrap_err();
        assert!(err.to_string().contains("must be <="), "{err}");

        let path = write_toml(&minimal_toml().replace(
            "confirmation_depth = 8",
            "confirmation_depth = 8\nfee_rate = 0",
        ));
        let err = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap_err();
        assert!(err.to_string().contains("must be > 0"), "{err}");
    }

    #[test]
    fn type_id_args_derives_code_hash_and_code_deps() {
        use cid_backend_core::type_id::type_id_script_hash;
        let args = "aa".repeat(32);
        let expected = type_id_script_hash(&[0xaau8; 32]);
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some(args.clone()),
            "CID_LOCK_TYPE_ID" => Some(hex::encode(expected)),
            "CID_PROCESS_TYPE_ID_ARGS" => Some("bb".repeat(32)),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.cid_lock_code_hash, expected);
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.type_id_code_deps.len(), 2);
        assert_eq!(cfg.type_id_code_deps[0].label, "cid_lock");
        assert_eq!(cfg.type_id_code_deps[1].label, "process_code");
        assert!(cfg.process_type_code_hashes.is_empty());
        let loop_cfg = cfg.to_loop_config();
        // Loop only carries CID lock Type-ID; Process code deps come from registry.
        assert_eq!(loop_cfg.type_id_code_deps.len(), 1);
        assert_eq!(loop_cfg.type_id_code_deps[0].label, "cid_lock");
        assert_eq!(loop_cfg.cid_code_hash, expected);
    }

    #[test]
    fn type_id_without_args_rejected() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_LOCK_TYPE_ID" => Some("cc".repeat(32)),
            other => required_env(other),
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("type_id_args"), "{msg}");
        assert!(
            msg.contains("not treat type_id as args") || msg.contains("without type_id_args"),
            "{msg}"
        );
    }

    #[test]
    fn type_id_process_clears_deprecated_process_code_hash() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some("aa".repeat(32)),
            "CID_PROCESS_TYPE_ID_ARGS" => Some("bb".repeat(32)),
            "CID_PROCESS_CODE_HASH" => Some("51".repeat(32)),
            _ => None,
        })
        .unwrap();
        assert!(cfg.process_type_code_hashes.is_empty());
    }

    #[test]
    fn type_id_role_rejects_static_env_cell_dep() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some("aa".repeat(32)),
            "CID_LOCK_CELL_DEP" => Some(format!("{}:0", "dd".repeat(32))),
            _ => None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("CID_LOCK_CELL_DEP"), "{msg}");
        assert!(msg.contains("conflicts"), "{msg}");
    }

    #[test]
    fn type_id_role_rejects_labeled_file_cell_dep() {
        let path = write_toml(&format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
type_id_args = "0x{args}"
[[cell_deps]]
label = "cid_lock"
tx_hash = "0x{dep}"
index = 0
[loop]
poll_interval_secs = 5
"#,
            g = "11".repeat(32),
            args = "aa".repeat(32),
            dep = "dd".repeat(32),
        ));
        let err = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cid_lock"), "{msg}");
        assert!(msg.contains("conflicts"), "{msg}");
    }

    #[test]
    fn type_id_hash_mismatch_fail_closed() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("1".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some("11".repeat(32)),
            "CID_LOCK_TYPE_ID" => Some("22".repeat(32)),
            _ => None,
        })
        .unwrap_err();
        assert!(err.to_string().contains("mismatch"), "{err}");
    }

    #[test]
    fn type_id_args_rejects_data1_hash_type() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some("aa".repeat(32)),
            "CID_LOCK_HASH_TYPE" => Some("data1".into()),
            _ => None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("hash_type=type"), "{msg}");
    }

    #[test]
    fn type_id_args_rejects_code_hash_equal_to_unrelated_data_hash() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            "CID_LOCK_TYPE_ID_ARGS" => Some("aa".repeat(32)),
            // Binary data_hash must not be used as the Type-ID scan identity.
            "CID_LOCK_CODE_HASH" => Some("d1".repeat(32)),
            _ => None,
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("!=") || msg.contains("mismatch"), "{msg}");
        assert!(
            msg.contains("type_id-derived") || msg.contains("derived"),
            "{msg}"
        );
    }

    #[test]
    fn primary_data_hash_requires_migration_in_env_and_toml() {
        for hash_type in ["data", "data1", "data2"] {
            let err = DaemonConfig::from_vars(|key| match key {
                "CID_LOCK_HASH_TYPE" => Some(hash_type.into()),
                _ => required_env(key),
            })
            .unwrap_err();
            assert!(err.to_string().contains("legacy_scan_locks"), "{err}");

            let path = write_toml(&minimal_toml().replace(
                "hash_type = \"type\"",
                &format!("hash_type = \"{hash_type}\""),
            ));
            let err = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap_err();
            assert!(err.to_string().contains("legacy_scan_locks"), "{err}");
        }
    }

    #[test]
    fn type_id_primary_scan_identities_exclude_legacy_from_loop() {
        use cid_backend_core::type_id::type_id_script_hash;
        let derived = type_id_script_hash(&[0xaau8; 32]);
        let path = write_toml(&format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
type_id_args = "0x{args}"
type_id = "0x{tid}"
[[cid_lock.legacy_scan_locks]]
code_hash = "0x{legacy}"
hash_type = "data1"
[loop]
poll_interval_secs = 5
fee_rate = 1000
"#,
            g = "11".repeat(32),
            args = "aa".repeat(32),
            tid = hex::encode(derived),
            legacy = "d1".repeat(32),
        ));
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert_eq!(cfg.cid_lock_code_hash, derived);
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.extra_scan_locks.len(), 1);
        let ids = cfg.to_loop_config().scan_lock_identities();
        assert_eq!(
            ids,
            vec![
                CidLockIdentity::new(derived, ScriptHashType::Type),
                CidLockIdentity::new([0xd1; 32], ScriptHashType::Data1),
            ]
        );
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
        assert!(cfg.process_type_code_hashes.is_empty());
        let loop_cfg = cfg.to_loop_config();
        assert_eq!(loop_cfg.extra_cell_deps.len(), 2);
        assert_eq!(cfg.extra_cell_deps[0].dep_type(), DepType::Code.into());
    }

    #[test]
    fn from_vars_reads_optional_indexer_and_hash_type() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://node".into()),
            "CID_INDEXER_URL" => Some("http://indexer".into()),
            "CID_LOCK_CODE_HASH" => Some("0x".to_string() + &"ab".repeat(32)),
            "CID_LOCK_HASH_TYPE" => Some("type".into()),
            "CID_POLL_INTERVAL_SECS" => Some("2".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.indexer_url, "http://indexer");
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
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
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert!(cfg.process_type_code_hashes.is_empty());
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
        assert_eq!(cfg.extra_cell_deps[0].dep_type(), DepType::DepGroup.into());
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
        assert!(cfg.process_type_code_hashes.is_empty());
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
        assert_eq!(cfg.extra_cell_deps[0].dep_type(), DepType::Code.into());
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

    fn assert_toml_diag_safe(msg: &str, secret: &str) {
        assert!(msg.contains("TOML"), "{msg}");
        assert!(
            !msg.contains(secret),
            "TOML diagnostic must not echo secret: {msg}"
        );
        assert!(
            !msg.contains(" | "),
            "TOML diagnostic must not embed source gutter: {msg}"
        );
    }

    #[test]
    fn unknown_key_rejected() {
        let p = write_toml("[network]\ngenesis_hash = \"aa\"\nprivate_key = \"SECRET_MARKER\"\n");
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        let msg = err.to_string();
        assert_toml_diag_safe(&msg, "SECRET_MARKER");
        assert!(
            msg.contains("unknown field"),
            "expected unknown-field category: {msg}"
        );
        assert!(
            !msg.contains("private_key"),
            "unknown field name must not be echoed: {msg}"
        );
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        let p = write_toml("secret = \"SECRET_MARKER\"\n");
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        let msg = err.to_string();
        assert_toml_diag_safe(&msg, "SECRET_MARKER");
        assert!(msg.contains("unknown field"), "{msg}");
        assert!(
            !msg.contains("secret =") && !msg.contains("`secret`"),
            "unknown field name must not be echoed: {msg}"
        );
    }

    #[test]
    fn malformed_toml_diagnostic_omits_secret_bearing_line() {
        // Trailing garbage after a credential-looking URL value: toml Display would
        // reprint the line; we must keep only category + line/column.
        let body = format!(
            r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://user:SECRET_MARKER@127.0.0.1:8114/"
<<<
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = 1
"#,
            g = "11".repeat(32),
            lock = "22".repeat(32),
        );
        let p = write_toml(&body);
        let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
        let msg = err.to_string();
        assert_toml_diag_safe(&msg, "SECRET_MARKER");
        assert!(
            msg.contains("line") && msg.contains("column"),
            "expected safe location hint: {msg}"
        );
        assert!(
            !msg.contains("user:"),
            "TOML diagnostic must not echo userinfo: {msg}"
        );
        assert!(
            msg.contains("parse error") || msg.contains("invalid value type"),
            "expected safe category: {msg}"
        );
    }

    #[test]
    fn typed_string_value_mismatches_omit_secret_from_diagnostic() {
        let cases = [
            (
                "u64",
                format!(
                    r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = "SECRET_MARKER_U64"
"#,
                    g = "11".repeat(32),
                    lock = "22".repeat(32),
                ),
                "SECRET_MARKER_U64",
            ),
            (
                "bool",
                format!(
                    r#"
[network]
genesis_hash = "0x{g}"
validate_genesis = "SECRET_MARKER_BOOL"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[loop]
poll_interval_secs = 1
"#,
                    g = "11".repeat(32),
                    lock = "22".repeat(32),
                ),
                "SECRET_MARKER_BOOL",
            ),
            (
                "u32_index",
                format!(
                    r#"
[network]
genesis_hash = "0x{g}"
[rpc]
url = "http://127.0.0.1:8114"
[cid_lock]
code_hash = "0x{lock}"
[[cell_deps]]
tx_hash = "0x{dep}"
index = "SECRET_MARKER_U32"
[loop]
poll_interval_secs = 1
"#,
                    g = "11".repeat(32),
                    lock = "22".repeat(32),
                    dep = "33".repeat(32),
                ),
                "SECRET_MARKER_U32",
            ),
        ];
        for (label, body, secret) in cases {
            let p = write_toml(&body);
            let err = DaemonConfig::from_file_and_vars(Some(&p), |_| None).unwrap_err();
            let msg = err.to_string();
            assert_toml_diag_safe(&msg, secret);
            assert!(
                msg.contains("invalid value type"),
                "{label}: expected invalid value type category: {msg}"
            );
            assert!(
                msg.contains("line") && msg.contains("column"),
                "{label}: expected location: {msg}"
            );
        }
    }

    #[test]
    fn missing_config_file() {
        let err = DaemonConfig::from_file_and_vars(
            Some(Path::new("/no/such/cid-config-xyz.toml")),
            |_| None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("cannot read") || msg.contains("No such"),
            "{msg}"
        );
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
    fn testnet_v014_example_loads_with_current_cid_type_id() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/cid.testnet.toml");
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |_| None).unwrap();
        assert!(cfg.validate_genesis);
        assert!(!cfg.allow_missing_filter);
        assert_eq!(
            hex::encode(cfg.cid_lock_code_hash),
            "f3f8a56c2594030f99f05b09ff1b6a1b07e6e07ed04309c1c4e246561faad326"
        );
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(
            cfg.bounded_refund_locks,
            [CidLockIdentity::new(
                cfg.cid_lock_code_hash,
                ScriptHashType::Type
            )]
        );
        let cfg = DaemonConfig::from_file_and_vars(Some(&path), |key| {
            (key == "CID_ALLOW_MISSING_FILTER").then(|| "true".into())
        })
        .unwrap();
        assert!(cfg.allow_missing_filter);
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
            parse_cli(&["d".into(), "--config".into(), "n.toml".into()]).unwrap(),
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
            CliAction::Run { config_path: None }
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
        assert_eq!(rpc_endpoint_for_log("not-a-url"), "<invalid-rpc-url>");
        assert_eq!(rpc_endpoint_for_log("ftp://127.0.0.1"), "<invalid-rpc-url>");
    }

    #[test]
    fn rpc_endpoint_for_log_ignores_at_outside_authority() {
        // Review repro: `@` in query must not be treated as userinfo.
        let q = rpc_endpoint_for_log("https://rpc.example/v1?api_key=prefix@TOKEN_MARKER");
        assert_eq!(q, "https://rpc.example");
        assert!(!q.contains("TOKEN_MARKER"));
        assert!(!q.contains("prefix"));
        assert!(!q.contains("api_key"));

        let path = rpc_endpoint_for_log("https://rpc.example/v1/@TOKEN_MARKER/rpc");
        assert_eq!(path, "https://rpc.example");
        assert!(!path.contains("TOKEN_MARKER"));

        let frag = rpc_endpoint_for_log("https://rpc.example/rpc#user@TOKEN_MARKER");
        assert_eq!(frag, "https://rpc.example");
        assert!(!frag.contains("TOKEN_MARKER"));

        // Authority userinfo still stripped; path/query/fragment discarded.
        let mixed = rpc_endpoint_for_log(
            "https://user:s3cret@rpc.example:8443/v1?api_key=prefix@TOKEN_MARKER#x@y",
        );
        assert_eq!(mixed, "https://rpc.example:8443");
        assert!(!mixed.contains("s3cret"));
        assert!(!mixed.contains("TOKEN_MARKER"));
        assert!(!mixed.contains("user"));
    }
}
