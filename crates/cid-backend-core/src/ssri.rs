use crate::process::ResolvedProcess;
use crate::scan::ScannedIntention;
use cid_process_executor::{
    build_draft_tx, AssembledTx, DraftCell, ExecutorError, MockHost, ProcessExecutor,
};
use ckb_types::packed::Script;
use ckb_types::prelude::*;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsriError {
    Incompatible(i8),
    Unusable(String),
}

impl fmt::Display for SsriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SsriError::Incompatible(c) => write!(f, "ssri incompatible exit {c}"),
            SsriError::Unusable(m) => write!(f, "ssri unusable: {m}"),
        }
    }
}

impl std::error::Error for SsriError {}

pub fn map_executor_error(err: ExecutorError) -> SsriError {
    match err {
        ExecutorError::Incompatible(code) => SsriError::Incompatible(code),
        ExecutorError::VmError(m) => SsriError::Unusable(format!("vm: {m}")),
        ExecutorError::CyclesExceeded => SsriError::Unusable("cycles exceeded".into()),
        ExecutorError::InvalidResponse(m) => SsriError::Unusable(format!("invalid response: {m}")),
    }
}

pub trait SsriHost {
    fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError>;
    fn assemble(
        &self,
        binary: &[u8],
        draft_tx: &[u8],
        host: MockHost,
    ) -> Result<AssembledTx, SsriError>;
}

pub struct EmbeddedSsri;

impl SsriHost for EmbeddedSsri {
    fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError> {
        ProcessExecutor::new(binary.to_vec())
            .get_batch_bounds()
            .map_err(map_executor_error)
    }

    fn assemble(
        &self,
        binary: &[u8],
        draft_tx: &[u8],
        host: MockHost,
    ) -> Result<AssembledTx, SsriError> {
        ProcessExecutor::new(binary.to_vec())
            .assemble(draft_tx, host)
            .map_err(map_executor_error)
    }
}

fn intention_draft_cell(intent: &ScannedIntention) -> DraftCell {
    let lock = Script::new_builder()
        .code_hash(intent.lock_code_hash.pack())
        .hash_type(intent.lock_hash_type.into())
        .args(intent.lock_args.pack())
        .build();
    let mut output = ckb_types::packed::CellOutput::new_builder()
        .capacity(intent.capacity.pack())
        .lock(lock);
    if let Some(ts) = &intent.type_script {
        output = output.type_(Some(ts.clone()).pack());
    }
    DraftCell {
        out_point: intent.out_point.clone(),
        output: output.build(),
        data: intent.data.clone(),
    }
}

/// Draft input cells for one batch: intentions (ASC) then the live Process state cell.
pub fn draft_cells_for_batch(
    intentions: &[ScannedIntention],
    process: &ResolvedProcess,
) -> Vec<DraftCell> {
    let mut cells: Vec<DraftCell> = intentions.iter().map(intention_draft_cell).collect();
    let live = &process.live_cell;
    cells.push(DraftCell {
        out_point: live.out_point.clone(),
        output: live.output.clone(),
        data: live.data.clone(),
    });
    cells
}

/// Molecule `Transaction` draft whose inputs are the batch draft cells (empty outputs/deps).
pub fn build_batch_draft(intentions: &[ScannedIntention], process: &ResolvedProcess) -> Vec<u8> {
    build_draft_tx(&draft_cells_for_batch(intentions, process))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid_process_executor::ExecutorError;

    #[test]
    fn maps_incompatible_vs_unusable() {
        assert!(matches!(
            map_executor_error(ExecutorError::Incompatible(21)),
            SsriError::Incompatible(21)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::VmError("x".into())),
            SsriError::Unusable(_)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::CyclesExceeded),
            SsriError::Unusable(_)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::InvalidResponse("bad".into())),
            SsriError::Unusable(_)
        ));
    }

    #[test]
    fn empty_binary_is_unusable_not_incompatible() {
        let err = EmbeddedSsri
            .get_batch_bounds(&[])
            .expect_err("empty binary");
        assert!(matches!(err, SsriError::Unusable(_)));
        let err = EmbeddedSsri
            .assemble(&[0xff; 8], &[], MockHost::new())
            .expect_err("garbage");
        assert!(matches!(err, SsriError::Unusable(_)));
    }

    /// Real RISC-V guest via ckb-vm. Ignored locally unless the pinned fixture is
    /// present *and* the test is selected (`--ignored` / `--include-ignored`).
    #[ignore = "requires pinned cid_process_example RISC-V guest; CI builds it; see tests/fixtures/README.md"]
    #[test]
    fn real_example_binary_batch_bounds_and_assemble() {
        use crate::host_query::seed_mock_host_from_live_cells;
        use crate::process::{ProcessEntry, ProcessResolver, ResolvedProcess};
        use crate::rpc::{script_hash32, LiveCell};
        use crate::type_id::{type_id_script, type_id_script_hash, TypeIdIdentity};
        use cid_protocol::{CidArgs, Payload, Script as CidScript};
        use ckb_types::core::ScriptHashType;
        use ckb_types::packed::{Byte32, CellOutput, OutPoint};

        let bin = load_example_binary().unwrap_or_else(|| {
            panic!(
                "cid_process_example missing while the real SSRI guest test was selected. \
                 See tests/fixtures/README.md."
            );
        });
        assert!(
            bin.len() >= 4 && bin[0] == 0x7f && &bin[1..4] == b"ELF",
            "cid_process_example must be a RISC-V ELF (got {} bytes)",
            bin.len()
        );

        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&bin)
            .expect("Process.batch via ckb-vm");
        assert_eq!((min, max), (1, 4));

        let state_args = [0xCDu8; 32];
        let code_args = [0xABu8; 32];
        let type_hash = type_id_script_hash(&state_args);
        let code_hash = type_id_script_hash(&code_args);
        let lock = Script::new_builder()
            .code_hash(code_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let state_type = type_id_script(&state_args);
        let state_out = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let state = LiveCell {
            out_point: state_out.clone(),
            output: CellOutput::new_builder()
                .capacity(500_0000_0000u64.pack())
                .lock(lock)
                .type_(Some(state_type).pack())
                .build(),
            data: vec![],
        };
        let code_out = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x42; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let code_cell = LiveCell {
            out_point: code_out,
            output: state.output.clone(),
            data: bin.clone(),
        };
        let entry = ProcessEntry {
            identity_type_hash: type_hash,
            identity: TypeIdIdentity::new(state_args),
            code: TypeIdIdentity::new(code_args),
            ssri_elf_path: None,
        };
        let resolved = ResolvedProcess {
            type_hash,
            live_cell: state,
            binary: bin.clone(),
            code_cell: Some(code_cell.clone()),
        };

        let user = CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0xEE],
        };
        let args = CidArgs::new(type_hash, user, Payload { data: vec![0xAA] });
        let lock_args = args.encode().unwrap();
        let intent_out = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x10; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let intent = ScannedIntention {
            out_point: intent_out.clone(),
            capacity: 100_0000_0000,
            type_script: None,
            data: vec![0xAA],
            lock_args,
            args,
            lock_code_hash: [0xC1; 32],
            lock_hash_type: ScriptHashType::Type,
        };

        let draft = build_batch_draft(std::slice::from_ref(&intent), &resolved);
        let tip = (0u64, [0xAB; 32]);
        let host = seed_mock_host_from_live_cells(
            &[
                (
                    intent_out,
                    intention_draft_cell(&intent).output,
                    intent.data.clone(),
                ),
                (
                    state_out,
                    resolved.live_cell.output.clone(),
                    resolved.live_cell.data.clone(),
                ),
            ],
            Some(tip),
        );

        let err = EmbeddedSsri
            .assemble(&bin, &[], host.clone())
            .expect_err("empty draft");
        assert!(matches!(err, SsriError::Incompatible(21)));

        let assembled = EmbeddedSsri
            .assemble(&bin, &draft, host)
            .expect("Process.assemble n=1 via ckb-vm");
        assert_eq!(assembled.outputs.len(), 2, "beneficiary + process recreate");
        let cap: u64 = assembled.outputs[0].capacity().unpack();
        assert_eq!(cap, 100_0000_0000);

        let _ = ProcessResolver::for_entry(entry);
        let _ = script_hash32;
    }

    /// SSRI ELF must come from the Process **code** cell, not divergent state payload.
    #[ignore = "requires pinned cid_process_example RISC-V guest; CI builds it; see tests/fixtures/README.md"]
    #[test]
    fn real_ssri_guest_binds_to_process_code_cell() {
        use crate::process::{ProcessEntry, ProcessResolver};
        use crate::rpc::{LiveCell, Rpc, RpcError, TxStatus};
        use crate::type_id::{type_id_script, type_id_script_hash, TypeIdIdentity};
        use ckb_types::core::{ScriptHashType, TransactionView};
        use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
        use ckb_types::prelude::*;

        let bin = load_example_binary().unwrap_or_else(|| {
            panic!(
                "cid_process_example missing while the Process-as-lock bind test was selected. \
                 See tests/fixtures/README.md."
            );
        });

        let state_args = [0xCDu8; 32];
        let code_args = [0xABu8; 32];
        let type_hash = type_id_script_hash(&state_args);
        let code_type_hash = type_id_script_hash(&code_args);
        let lock = Script::new_builder()
            .code_hash(code_type_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let state_type = type_id_script(&state_args);
        let state = LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output: CellOutput::new_builder()
                .capacity(500_0000_0000u64.pack())
                .lock(lock)
                .type_(Some(state_type).pack())
                .build(),
            data: vec![0x7f, b'E', b'L', b'F', 0xDE, 0xAD],
        };
        let code = LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[0x42; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output: state.output.clone(),
            data: bin.clone(),
        };

        struct OneProcess {
            cell: LiveCell,
        }
        impl Rpc for OneProcess {
            fn list_live_intentions(
                &self,
                _: &[u8; 32],
                _: ScriptHashType,
            ) -> Result<Vec<LiveCell>, RpcError> {
                Ok(vec![])
            }
            fn get_live_process_by_type_hash(
                &self,
                _: &[u8; 32],
            ) -> Result<Option<LiveCell>, RpcError> {
                Ok(Some(self.cell.clone()))
            }
            fn find_live_cells_by_type_script(
                &self,
                _: &Script,
                _: usize,
            ) -> Result<Vec<LiveCell>, RpcError> {
                Ok(vec![])
            }
            fn dry_run_transaction(&self, _: &TransactionView) -> Result<(), RpcError> {
                Ok(())
            }
            fn submit_transaction(&self, _: &TransactionView) -> Result<Byte32, RpcError> {
                unreachable!()
            }
            fn get_transaction_status(&self, _: &Byte32) -> Result<TxStatus, RpcError> {
                unreachable!()
            }
            fn tip_block_number(&self) -> Result<u64, RpcError> {
                Ok(0)
            }
        }

        let rpc = OneProcess { cell: state };
        let entry = ProcessEntry {
            identity_type_hash: type_hash,
            identity: TypeIdIdentity::new(state_args),
            code: TypeIdIdentity::new(code_args),
            ssri_elf_path: None,
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code));
        let resolved = resolver
            .resolve(&rpc, &type_hash)
            .unwrap()
            .expect("resolved");
        assert_eq!(resolved.binary, bin, "SSRI binary must be code cell ELF");
        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&resolved.binary)
            .expect("Process.batch via ckb-vm");
        assert_eq!((min, max), (1, 4));
    }

    fn load_example_binary() -> Option<Vec<u8>> {
        if let Ok(p) = std::env::var("CID_PROCESS_EXAMPLE_PATH") {
            return std::fs::read(p).ok();
        }
        let here = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let candidates = [
            here.join("../../tests/fixtures/cid_process_example"),
            here.join("../tests/fixtures/cid_process_example"),
        ];
        for p in candidates {
            if p.exists() {
                return std::fs::read(p).ok();
            }
        }
        None
    }
}
