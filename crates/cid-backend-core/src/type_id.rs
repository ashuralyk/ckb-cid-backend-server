//! Built-in CKB TYPE_ID helpers for **code-cell** discovery.
//!
//! Distinctions (do not conflate):
//! - `type_id_args`: 32-byte args of the code cell's TYPE_ID **type script**.
//! - `type_id` (full hash): `calc_script_hash` of that TYPE_ID type script —
//!   the value used as `code_hash` when other cells reference the binary with
//!   `hash_type = type`. Optional config field for fail-closed verification.
//! - Live Process **state** cell type script / CID `process_type_hash`: a
//!   different script that usually points at the Process **code** via the
//!   derived `type_id` above; not the same as code-cell TYPE_ID args.

use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, Script};
use ckb_types::prelude::*;

/// Built-in TYPE_ID `code_hash` (ASCII `TYPE_ID` in the last 7 bytes).
pub const TYPE_ID_CODE_HASH: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x49, 0x44,
];

/// Configured identity of one Type-ID–deployed **code** cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeIdIdentity {
    /// TYPE_ID type-script args (exactly 32 bytes). Discovery key.
    pub args: [u8; 32],
    /// Optional full type-script hash (`calc_script_hash`). Never treated as args.
    pub expected_type_hash: Option<[u8; 32]>,
}

impl TypeIdIdentity {
    pub fn new(args: [u8; 32]) -> Self {
        Self {
            args,
            expected_type_hash: None,
        }
    }

    pub fn with_expected_hash(args: [u8; 32], expected: [u8; 32]) -> Result<Self, String> {
        let id = Self {
            args,
            expected_type_hash: Some(expected),
        };
        id.validate_expected()?;
        Ok(id)
    }

    /// TYPE_ID type script on the code cell.
    pub fn type_script(&self) -> Script {
        type_id_script(&self.args)
    }

    /// Full type-script hash — use as `code_hash` with `hash_type = type`.
    pub fn type_script_hash(&self) -> [u8; 32] {
        type_id_script_hash(&self.args)
    }

    pub fn validate_expected(&self) -> Result<(), String> {
        if let Some(expected) = self.expected_type_hash {
            let got = self.type_script_hash();
            if got != expected {
                return Err(format!(
                    "type_id (full type-script hash) mismatch: configured 0x{} derived 0x{} \
                     (type_id is not type_id_args; do not pass a full hash as args)",
                    hex32(&expected),
                    hex32(&got)
                ));
            }
        }
        Ok(())
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
    }
    s
}

pub fn type_id_script(args: &[u8; 32]) -> Script {
    Script::new_builder()
        .code_hash(Byte32::from_slice(&TYPE_ID_CODE_HASH).expect("TYPE_ID_CODE_HASH is 32 bytes"))
        .hash_type(ScriptHashType::Type.into())
        .args(args.to_vec().pack())
        .build()
}

pub fn type_id_script_hash(args: &[u8; 32]) -> [u8; 32] {
    type_id_script(args).calc_script_hash().unpack()
}

/// One code cell to discover via TYPE_ID and attach as a CellDep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeIdCodeDep {
    pub label: String,
    pub identity: TypeIdIdentity,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::script_hash32;

    #[test]
    fn type_id_code_hash_is_ascii_suffix() {
        assert_eq!(&TYPE_ID_CODE_HASH[25..], b"TYPE_ID");
    }

    #[test]
    fn type_script_hash_matches_calc_script_hash() {
        let args = [0xab; 32];
        let script = type_id_script(&args);
        assert_eq!(script_hash32(&script), type_id_script_hash(&args));
        let ht: ScriptHashType = script.hash_type().try_into().unwrap();
        assert_eq!(ht, ScriptHashType::Type);
        let code: [u8; 32] = script.code_hash().unpack();
        assert_eq!(code, TYPE_ID_CODE_HASH);
    }

    #[test]
    fn expected_type_hash_fail_closed_on_mismatch() {
        let args = [0x11; 32];
        let wrong = [0x22; 32];
        let err = TypeIdIdentity::with_expected_hash(args, wrong).unwrap_err();
        assert!(err.contains("type_id (full type-script hash) mismatch"));
        assert!(err.contains("not type_id_args"));
    }

    #[test]
    fn expected_type_hash_ok_when_matches_derived() {
        let args = [0x33; 32];
        let expected = type_id_script_hash(&args);
        let id = TypeIdIdentity::with_expected_hash(args, expected).unwrap();
        assert_eq!(id.type_script_hash(), expected);
    }

    #[test]
    fn different_args_yield_different_type_ids() {
        assert_ne!(type_id_script_hash(&[1; 32]), type_id_script_hash(&[2; 32]));
    }
}
