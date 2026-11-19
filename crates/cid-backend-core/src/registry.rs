//! Disk-backed registry of configured Process TYPE_ID identities.

use crate::process::ProcessEntry;
use crate::type_id::{type_id_script_hash, TypeIdIdentity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const DEFAULT_MAX_REGISTRY_ENTRIES: usize = 64;

const REGISTRY_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessRegistration {
    pub identity_type_id_args: [u8; 32],
    pub identity_type_id: Option<[u8; 32]>,
    pub code_type_id_args: [u8; 32],
    pub code_type_id: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
    Created,
    Updated,
}

#[derive(Debug)]
pub enum RegistryError {
    Invalid(String),
    Full,
    Io(std::io::Error),
    Corrupt(String),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Invalid(msg) => write!(f, "invalid registration: {msg}"),
            RegistryError::Full => write!(f, "registry full"),
            RegistryError::Io(e) => write!(f, "io error: {e}"),
            RegistryError::Corrupt(msg) => write!(f, "corrupt registry: {msg}"),
        }
    }
}

impl std::error::Error for RegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RegistryError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(e: std::io::Error) -> Self {
        RegistryError::Io(e)
    }
}

impl ProcessRegistration {
    pub fn identity_type_hash(&self) -> [u8; 32] {
        type_id_script_hash(&self.identity_type_id_args)
    }

    pub fn validate(&self) -> Result<(), RegistryError> {
        if let Some(expected) = self.identity_type_id {
            let derived = type_id_script_hash(&self.identity_type_id_args);
            if derived != expected {
                return Err(RegistryError::Invalid(format!(
                    "identity_type_id mismatch: configured 0x{} derived 0x{}",
                    hex32(&expected),
                    hex32(&derived)
                )));
            }
        }
        if let Some(expected) = self.code_type_id {
            let derived = type_id_script_hash(&self.code_type_id_args);
            if derived != expected {
                return Err(RegistryError::Invalid(format!(
                    "code_type_id mismatch: configured 0x{} derived 0x{}",
                    hex32(&expected),
                    hex32(&derived)
                )));
            }
        }
        Ok(())
    }

    pub fn to_process_entry(&self) -> Result<ProcessEntry, RegistryError> {
        self.validate()?;
        let identity = match self.identity_type_id {
            Some(h) => TypeIdIdentity::with_expected_hash(self.identity_type_id_args, h)
                .map_err(RegistryError::Invalid)?,
            None => TypeIdIdentity::new(self.identity_type_id_args),
        };
        let code = match self.code_type_id {
            Some(h) => TypeIdIdentity::with_expected_hash(self.code_type_id_args, h)
                .map_err(RegistryError::Invalid)?,
            None => TypeIdIdentity::new(self.code_type_id_args),
        };
        Ok(ProcessEntry {
            identity_type_hash: self.identity_type_hash(),
            identity,
            code,
            ssri_elf_path: None,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct OnDiskRegistry {
    version: u32,
    processes: Vec<ProcessRegistrationSerde>,
}

#[derive(Serialize, Deserialize)]
struct ProcessRegistrationSerde {
    identity_type_id_args: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity_type_id: Option<String>,
    code_type_id_args: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    code_type_id: Option<String>,
}

impl TryFrom<ProcessRegistrationSerde> for ProcessRegistration {
    type Error = RegistryError;

    fn try_from(s: ProcessRegistrationSerde) -> Result<Self, Self::Error> {
        Ok(ProcessRegistration {
            identity_type_id_args: parse_hex32(&s.identity_type_id_args, "identity_type_id_args")?,
            identity_type_id: match s.identity_type_id {
                Some(v) => Some(parse_hex32(&v, "identity_type_id")?),
                None => None,
            },
            code_type_id_args: parse_hex32(&s.code_type_id_args, "code_type_id_args")?,
            code_type_id: match s.code_type_id {
                Some(v) => Some(parse_hex32(&v, "code_type_id")?),
                None => None,
            },
        })
    }
}

impl From<&ProcessRegistration> for ProcessRegistrationSerde {
    fn from(r: &ProcessRegistration) -> Self {
        Self {
            identity_type_id_args: format_hex32(&r.identity_type_id_args),
            identity_type_id: r
                .identity_type_id
                .map(|h| format_hex32(&h)),
            code_type_id_args: format_hex32(&r.code_type_id_args),
            code_type_id: r.code_type_id.map(|h| format_hex32(&h)),
        }
    }
}

pub struct ProcessRegistry {
    path: PathBuf,
    max_entries: usize,
    by_identity: HashMap<[u8; 32], ProcessRegistration>,
}

impl ProcessRegistry {
    pub fn open(path: impl AsRef<Path>, max_entries: usize) -> Result<Self, RegistryError> {
        let path = path.as_ref().to_path_buf();
        let by_identity = if path.exists() {
            load_from_disk(&path)?
        } else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            HashMap::new()
        };
        if by_identity.len() > max_entries {
            return Err(RegistryError::Corrupt(format!(
                "registry has {} entries, max is {max_entries}",
                by_identity.len()
            )));
        }
        Ok(Self {
            path,
            max_entries,
            by_identity,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn len(&self) -> usize {
        self.by_identity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_identity.is_empty()
    }

    pub fn get(&self, identity_hash: &[u8; 32]) -> Option<&ProcessRegistration> {
        self.by_identity.get(identity_hash)
    }

    pub fn list(&self) -> Vec<&ProcessRegistration> {
        let mut out: Vec<_> = self.by_identity.values().collect();
        out.sort_by(|a, b| a.identity_type_id_args.cmp(&b.identity_type_id_args));
        out
    }

    pub fn register(
        &mut self,
        reg: ProcessRegistration,
    ) -> Result<RegisterOutcome, RegistryError> {
        reg.validate()?;
        let key = reg.identity_type_hash();
        let outcome = match self.by_identity.get(&key) {
            None => {
                if self.by_identity.len() >= self.max_entries {
                    return Err(RegistryError::Full);
                }
                RegisterOutcome::Created
            }
            Some(existing) => {
                if existing == &reg {
                    return Ok(RegisterOutcome::Updated);
                }
                RegisterOutcome::Updated
            }
        };
        self.by_identity.insert(key, reg);
        self.persist()?;
        Ok(outcome)
    }

    pub fn delete(&mut self, identity_hash: &[u8; 32]) -> Result<bool, RegistryError> {
        let removed = self.by_identity.remove(identity_hash).is_some();
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    fn persist(&self) -> Result<(), RegistryError> {
        let mut processes: Vec<ProcessRegistrationSerde> = self
            .by_identity
            .values()
            .map(ProcessRegistrationSerde::from)
            .collect();
        processes.sort_by(|a, b| a.identity_type_id_args.cmp(&b.identity_type_id_args));
        let doc = OnDiskRegistry {
            version: REGISTRY_VERSION,
            processes,
        };
        let json = serde_json::to_string_pretty(&doc)
            .map_err(|e| RegistryError::Corrupt(e.to_string()))?;
        let tmp = tmp_path(&self.path);
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(json.as_bytes())?;
            f.sync_all()?;
        }
        fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

fn load_from_disk(path: &Path) -> Result<HashMap<[u8; 32], ProcessRegistration>, RegistryError> {
    let data = fs::read_to_string(path)?;
    let doc: OnDiskRegistry =
        serde_json::from_str(&data).map_err(|e| RegistryError::Corrupt(e.to_string()))?;
    if doc.version != REGISTRY_VERSION {
        return Err(RegistryError::Corrupt(format!(
            "unsupported registry version {}",
            doc.version
        )));
    }
    let mut map = HashMap::new();
    for serde in doc.processes {
        let reg = ProcessRegistration::try_from(serde)?;
        reg.validate()?;
        let key = reg.identity_type_hash();
        if map.contains_key(&key) {
            return Err(RegistryError::Corrupt(
                "duplicate process identity in registry file".into(),
            ));
        }
        map.insert(key, reg);
    }
    Ok(map)
}

fn format_hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s
}

fn parse_hex32(s: &str, field: &str) -> Result<[u8; 32], RegistryError> {
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if hex.len() != 64 {
        return Err(RegistryError::Corrupt(format!(
            "{field}: expected 32-byte hex (64 digits), got {} chars",
            hex.len()
        )));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        if chunk.len() != 2 {
            return Err(RegistryError::Corrupt(format!("{field}: odd hex length")));
        }
        let pair = std::str::from_utf8(chunk).map_err(|_| {
            RegistryError::Corrupt(format!("{field}: invalid utf-8 in hex"))
        })?;
        out[i] = u8::from_str_radix(pair, 16)
            .map_err(|_| RegistryError::Corrupt(format!("{field}: invalid hex")))?;
    }
    Ok(out)
}

fn unique_temp_registry_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cid-process-registry-test-{}-{}",
        name,
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_id::type_id_script_hash;

    fn sample_reg(state_args: [u8; 32], code_args: [u8; 32]) -> ProcessRegistration {
        ProcessRegistration {
            identity_type_id_args: state_args,
            identity_type_id: None,
            code_type_id_args: code_args,
            code_type_id: None,
        }
    }

    fn sample_reg_with_expected(
        state_args: [u8; 32],
        code_args: [u8; 32],
    ) -> ProcessRegistration {
        ProcessRegistration {
            identity_type_id_args: state_args,
            identity_type_id: Some(type_id_script_hash(&state_args)),
            code_type_id_args: code_args,
            code_type_id: Some(type_id_script_hash(&code_args)),
        }
    }

    #[test]
    fn round_trip_persist_and_reload() {
        let path = unique_temp_registry_path("round-trip");
        let _ = fs::remove_file(&path);
        let mut reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        let r = sample_reg_with_expected([0x01; 32], [0x02; 32]);
        assert_eq!(reg.register(r.clone()).unwrap(), RegisterOutcome::Created);
        drop(reg);

        let reg2 = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        let got = reg2.get(&r.identity_type_hash()).unwrap();
        assert_eq!(got, &r);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn register_idempotent_same_binding() {
        let path = unique_temp_registry_path("idempotent");
        let _ = fs::remove_file(&path);
        let mut reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        let r = sample_reg([0xaa; 32], [0xbb; 32]);
        assert_eq!(reg.register(r.clone()).unwrap(), RegisterOutcome::Created);
        assert_eq!(reg.register(r).unwrap(), RegisterOutcome::Updated);
        assert_eq!(reg.len(), 1);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn register_replace_code_binding() {
        let path = unique_temp_registry_path("replace");
        let _ = fs::remove_file(&path);
        let mut reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        let state = [0x10; 32];
        let r1 = sample_reg(state, [0x20; 32]);
        let r2 = sample_reg(state, [0x21; 32]);
        assert_eq!(reg.register(r1).unwrap(), RegisterOutcome::Created);
        assert_eq!(reg.register(r2.clone()).unwrap(), RegisterOutcome::Updated);
        assert_eq!(reg.get(&type_id_script_hash(&state)).unwrap().code_type_id_args, [0x21; 32]);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn delete_removes_entry() {
        let path = unique_temp_registry_path("delete");
        let _ = fs::remove_file(&path);
        let mut reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        let r = sample_reg([0x33; 32], [0x44; 32]);
        let key = r.identity_type_hash();
        reg.register(r).unwrap();
        assert!(reg.delete(&key).unwrap());
        assert!(reg.get(&key).is_none());
        assert!(!reg.delete(&key).unwrap());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn max_entries_full() {
        let path = unique_temp_registry_path("full");
        let _ = fs::remove_file(&path);
        let max = 3;
        let mut reg = ProcessRegistry::open(&path, max).unwrap();
        for i in 0..max {
            let mut state = [0u8; 32];
            state[0] = i as u8;
            assert_eq!(
                reg.register(sample_reg(state, [0x99; 32])).unwrap(),
                RegisterOutcome::Created
            );
        }
        let mut extra = [0u8; 32];
        extra[0] = 0xff;
        assert!(matches!(
            reg.register(sample_reg(extra, [0x99; 32])),
            Err(RegistryError::Full)
        ));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn validate_type_id_mismatch() {
        let reg = ProcessRegistration {
            identity_type_id_args: [0x11; 32],
            identity_type_id: Some([0x22; 32]),
            code_type_id_args: [0x33; 32],
            code_type_id: None,
        };
        assert!(matches!(reg.validate(), Err(RegistryError::Invalid(_))));
    }

    #[test]
    fn to_process_entry_ok() {
        let state = [0x55; 32];
        let code = [0x66; 32];
        let reg = sample_reg_with_expected(state, code);
        let entry = reg.to_process_entry().unwrap();
        assert_eq!(entry.identity_type_hash, type_id_script_hash(&state));
        assert_eq!(entry.code.args, code);
    }

    #[test]
    fn list_sorted_by_identity_args() {
        let path = unique_temp_registry_path("list");
        let _ = fs::remove_file(&path);
        let mut reg = ProcessRegistry::open(&path, DEFAULT_MAX_REGISTRY_ENTRIES).unwrap();
        reg.register(sample_reg([0x02; 32], [0x01; 32])).unwrap();
        reg.register(sample_reg([0x01; 32], [0x01; 32])).unwrap();
        let listed: Vec<_> = reg.list().iter().map(|r| r.identity_type_id_args[0]).collect();
        assert_eq!(listed, vec![0x01, 0x02]);
        let _ = fs::remove_file(&path);
    }
}