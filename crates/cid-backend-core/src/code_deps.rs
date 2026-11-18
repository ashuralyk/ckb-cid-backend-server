//! Resolve Type-ID–deployed code cells to CellDeps.
//!
//! Fail-closed: zero matches → Missing; two or more → Ambiguous; optional
//! `type_id` hash mismatch → IdentityMismatch. Transient indexer/RPC errors
//! surface as Unavailable and must not permanently reject Intentions.

use crate::rpc::{Rpc, RpcError};
use crate::type_id::{TypeIdCodeDep, TypeIdIdentity};
use ckb_types::core::DepType;
use ckb_types::packed::{CellDep, OutPoint};
use ckb_types::prelude::*;
use std::collections::BTreeSet;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeDepError {
    Missing { label: String },
    Ambiguous { label: String, count: usize },
    IdentityMismatch { label: String, detail: String },
    Unavailable(String),
}

impl fmt::Display for CodeDepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodeDepError::Missing { label } => {
                write!(f, "type-id code cell missing for {label}")
            }
            CodeDepError::Ambiguous { label, count } => {
                write!(
                    f,
                    "type-id code cell ambiguous for {label}: {count} live cells"
                )
            }
            CodeDepError::IdentityMismatch { label, detail } => {
                write!(f, "type-id identity mismatch for {label}: {detail}")
            }
            CodeDepError::Unavailable(m) => write!(f, "type-id resolve unavailable: {m}"),
        }
    }
}

impl std::error::Error for CodeDepError {}

impl From<RpcError> for CodeDepError {
    fn from(e: RpcError) -> Self {
        CodeDepError::Unavailable(e.to_string())
    }
}

/// Resolve one Type-ID code cell to a `DepType::Code` CellDep.
pub fn resolve_type_id_code_dep<R: Rpc>(
    rpc: &R,
    spec: &TypeIdCodeDep,
) -> Result<CellDep, CodeDepError> {
    if let Err(detail) = spec.identity.validate_expected() {
        return Err(CodeDepError::IdentityMismatch {
            label: spec.label.clone(),
            detail,
        });
    }
    let script = spec.identity.type_script();
    let cells = rpc.list_live_cells_by_type_script(&script)?;
    match cells.len() {
        0 => Err(CodeDepError::Missing {
            label: spec.label.clone(),
        }),
        1 => Ok(cell_dep_code(&cells[0].out_point)),
        n => Err(CodeDepError::Ambiguous {
            label: spec.label.clone(),
            count: n,
        }),
    }
}

/// Merge Type-ID–resolved deps with static extras. Dedupes by outpoint
/// (first wins: Type-ID order, then static). Snapshot is consistent for one
/// assemble/execute.
pub fn merge_cell_deps(resolved: &[CellDep], static_extra: &[CellDep]) -> Vec<CellDep> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for dep in resolved.iter().chain(static_extra.iter()) {
        let key = dep.out_point().as_slice().to_vec();
        if seen.insert(key) {
            out.push(dep.clone());
        }
    }
    out
}

/// Resolve all Type-ID specs then merge with static extras.
pub fn resolve_execution_cell_deps<R: Rpc>(
    rpc: &R,
    specs: &[TypeIdCodeDep],
    static_extra: &[CellDep],
) -> Result<Vec<CellDep>, CodeDepError> {
    let mut resolved = Vec::with_capacity(specs.len());
    for spec in specs {
        resolved.push(resolve_type_id_code_dep(rpc, spec)?);
    }
    Ok(merge_cell_deps(&resolved, static_extra))
}

fn cell_dep_code(op: &OutPoint) -> CellDep {
    CellDep::new_builder()
        .out_point(op.clone())
        .dep_type(DepType::Code.into())
        .build()
}

/// Convenience: validate identity and return the derived `code_hash` for
/// `hash_type = type` scanning.
pub fn derived_type_code_hash(identity: &TypeIdIdentity) -> Result<[u8; 32], CodeDepError> {
    identity
        .validate_expected()
        .map_err(|detail| CodeDepError::IdentityMismatch {
            label: "derived".into(),
            detail,
        })?;
    Ok(identity.type_script_hash())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{script_hash32, LiveCell, TxStatus};
    use crate::type_id::{type_id_script, TypeIdIdentity};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::{Byte32, CellOutput, Script};
    use std::cell::RefCell;

    struct MapRpc {
        cells: RefCell<Vec<LiveCell>>,
        fail: RefCell<bool>,
    }

    impl MapRpc {
        fn new(cells: Vec<LiveCell>) -> Self {
            Self {
                cells: RefCell::new(cells),
                fail: RefCell::new(false),
            }
        }
    }

    impl Rpc for MapRpc {
        fn list_live_intentions(
            &self,
            _: &[u8; 32],
            _: ScriptHashType,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(Vec::new())
        }

        fn get_live_process_by_type_hash(
            &self,
            _: &[u8; 32],
        ) -> Result<Option<LiveCell>, RpcError> {
            Ok(None)
        }

        fn list_live_cells_by_type_script(
            &self,
            type_script: &Script,
        ) -> Result<Vec<LiveCell>, RpcError> {
            if *self.fail.borrow() {
                return Err(RpcError::Unavailable("injected".into()));
            }
            let want = script_hash32(type_script);
            Ok(self
                .cells
                .borrow()
                .iter()
                .filter(|c| {
                    c.type_script()
                        .map(|ts| script_hash32(&ts) == want)
                        .unwrap_or(false)
                })
                .cloned()
                .collect())
        }

        fn submit_transaction(&self, _: &TransactionView) -> Result<Byte32, RpcError> {
            unreachable!()
        }

        fn get_transaction_status(&self, _: &Byte32) -> Result<TxStatus, RpcError> {
            unreachable!()
        }

        fn tip_block_number(&self) -> Result<u64, RpcError> {
            Ok(1)
        }
    }

    fn code_cell(tx_byte: u8, args: [u8; 32]) -> LiveCell {
        let type_script = type_id_script(&args);
        let lock = Script::new_builder()
            .code_hash(Byte32::from_slice(&[0x99; 32]).unwrap())
            .hash_type(ScriptHashType::Data1.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let output = CellOutput::new_builder()
            .capacity(1000u64.pack())
            .lock(lock)
            .type_(Some(type_script).pack())
            .build();
        crate::rpc::LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[tx_byte; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output,
            data: vec![0x7f, b'E', b'L', b'F'],
        }
    }

    #[test]
    fn resolve_success_exact_one_cell() {
        let args = [0xaa; 32];
        let cell = code_cell(1, args);
        let rpc = MapRpc::new(vec![cell.clone()]);
        let spec = TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: TypeIdIdentity::new(args),
        };
        let dep = resolve_type_id_code_dep(&rpc, &spec).unwrap();
        assert_eq!(dep.out_point(), cell.out_point);
        assert_eq!(dep.dep_type(), DepType::Code.into());
    }

    #[test]
    fn resolve_missing_fail_closed() {
        let rpc = MapRpc::new(vec![]);
        let spec = TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: TypeIdIdentity::new([0x01; 32]),
        };
        match resolve_type_id_code_dep(&rpc, &spec) {
            Err(CodeDepError::Missing { label }) => assert_eq!(label, "cid_lock"),
            other => panic!("expected Missing, got {other:?}"),
        }
    }

    #[test]
    fn resolve_ambiguous_fail_closed() {
        let args = [0xbb; 32];
        let rpc = MapRpc::new(vec![code_cell(1, args), code_cell(2, args)]);
        let spec = TypeIdCodeDep {
            label: "process_code".into(),
            identity: TypeIdIdentity::new(args),
        };
        match resolve_type_id_code_dep(&rpc, &spec) {
            Err(CodeDepError::Ambiguous { label, count }) => {
                assert_eq!(label, "process_code");
                assert_eq!(count, 2);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn resolve_wrong_identity_fail_closed() {
        let args = [0xcc; 32];
        let rpc = MapRpc::new(vec![code_cell(1, args)]);
        let spec = TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: TypeIdIdentity {
                args,
                expected_type_hash: Some([0xff; 32]),
            },
        };
        match resolve_type_id_code_dep(&rpc, &spec) {
            Err(CodeDepError::IdentityMismatch { .. }) => {}
            other => panic!("expected IdentityMismatch, got {other:?}"),
        }
    }

    #[test]
    fn resolve_transport_unavailable() {
        let rpc = MapRpc::new(vec![]);
        *rpc.fail.borrow_mut() = true;
        let spec = TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: TypeIdIdentity::new([0x01; 32]),
        };
        match resolve_type_id_code_dep(&rpc, &spec) {
            Err(CodeDepError::Unavailable(_)) => {}
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[test]
    fn outpoint_change_same_identity_resolves_new_dep() {
        let args = [0xdd; 32];
        let rpc = MapRpc::new(vec![code_cell(1, args)]);
        let spec = TypeIdCodeDep {
            label: "cid_lock".into(),
            identity: TypeIdIdentity::new(args),
        };
        let first = resolve_type_id_code_dep(&rpc, &spec).unwrap();
        *rpc.cells.borrow_mut() = vec![code_cell(9, args)];
        let second = resolve_type_id_code_dep(&rpc, &spec).unwrap();
        assert_ne!(first.out_point(), second.out_point());
        assert_eq!(second.out_point().tx_hash().as_slice()[0], 9);
    }

    #[test]
    fn merge_dedupes_and_keeps_static() {
        let a = cell_dep_code(
            &OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[1; 32]).unwrap())
                .index(0u32.pack())
                .build(),
        );
        let b = cell_dep_code(
            &OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[2; 32]).unwrap())
                .index(0u32.pack())
                .build(),
        );
        let merged = merge_cell_deps(std::slice::from_ref(&a), &[a.clone(), b.clone()]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0], a);
        assert_eq!(merged[1], b);
    }
}
