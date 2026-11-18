use crate::scan::ScannedIntention;
use cid_process_executor::{ExecutorError, IntentionInput, PartialTx, ProcessExecutor};
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
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError>;
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
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError> {
        ProcessExecutor::new(binary.to_vec())
            .assemble(intentions)
            .map_err(map_executor_error)
    }
}

pub fn to_intention_input(scanned: &ScannedIntention) -> IntentionInput {
    IntentionInput {
        out_point: scanned.out_point.clone(),
        lock_args: scanned.lock_args.clone(),
        capacity: scanned.capacity,
        data: scanned.data.clone(),
        type_script: scanned.type_script.as_ref().map(|s| s.as_slice().to_vec()),
    }
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
        let err = EmbeddedSsri.assemble(&[0xff; 8], &[]).expect_err("garbage");
        assert!(matches!(err, SsriError::Unusable(_)));
    }

    /// Real RISC-V guest via ckb-vm. Ignored locally unless the pinned fixture is
    /// present *and* the test is selected (`--ignored` / `--include-ignored`).
    /// CI builds the ELF from pin `d9ee77805f7ac930ab149648bed70f484a9886de`
    /// and must run this test; missing fixture panics (job fails).
    #[ignore = "requires pinned cid_process_example RISC-V guest; CI builds it; see tests/fixtures/README.md"]
    #[test]
    fn real_example_binary_batch_bounds_and_assemble() {
        let bin = load_example_binary().unwrap_or_else(|| {
            panic!(
                "cid_process_example missing while the real SSRI guest test was selected. \
                 CI must build it from pin d9ee77805f7ac930ab149648bed70f484a9886de \
                 (CID_PROCESS_EXAMPLE_PATH or tests/fixtures/cid_process_example). \
                 Locally this test is #[ignore]; do not treat a skip as a real-SSRI pass. \
                 See tests/fixtures/README.md."
            );
        });
        assert!(
            bin.len() >= 4 && bin[0] == 0x7f && &bin[1..4] == b"ELF",
            "cid_process_example must be a RISC-V ELF (got {} bytes)",
            bin.len()
        );
        eprintln!(
            "real SSRI guest: {} bytes, executing Process.batch / Process.assemble via ckb-vm",
            bin.len()
        );

        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&bin)
            .expect("Process.batch via ckb-vm");
        assert_eq!(
            (min, max),
            (1, 4),
            "cid_process_example BATCH_MIN/BATCH_MAX"
        );

        let err = EmbeddedSsri
            .assemble(&bin, &[])
            .expect_err("n=0 is below BATCH_MIN");
        assert!(
            matches!(err, SsriError::Incompatible(21)),
            "empty assemble must be Incompatible(21), got {err:?}"
        );

        let one = cid_process_executor::IntentionInput {
            lock_args: vec![0x01, 0x02],
            capacity: 100_0000_0000,
            data: vec![0xAA],
            ..Default::default()
        };
        let partial = EmbeddedSsri
            .assemble(&bin, &[one])
            .expect("Process.assemble n=1 via ckb-vm");
        assert_eq!(partial.outputs.len(), 1, "one beneficiary output");
        assert_eq!(partial.outputs_data, vec![vec![0xAA]]);
        let cap: u64 = partial.outputs[0].capacity().unpack();
        assert_eq!(cap, 100_0000_0000);
        eprintln!(
            "real SSRI guest executed: Process.batch=({min},{max}) n=0 Incompatible(21) n=1 assemble ok"
        );
    }

    /// Type-ID Process code mode must execute the **code cell** ELF via ckb-vm,
    /// not a divergent state-cell payload. Uses the same pinned guest as
    /// `real_example_binary_batch_bounds_and_assemble`.
    #[ignore = "requires pinned cid_process_example RISC-V guest; CI builds it; see tests/fixtures/README.md"]
    #[test]
    fn real_ssri_guest_binds_to_type_id_process_code_cell() {
        use crate::process::ProcessResolver;
        use crate::rpc::{LiveCell, Rpc, RpcError, TxStatus};
        use crate::type_id::type_id_script_hash;
        use ckb_types::core::{ScriptHashType, TransactionView};
        use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
        use ckb_types::prelude::*;

        let bin = load_example_binary().unwrap_or_else(|| {
            panic!(
                "cid_process_example missing while the Type-ID SSRI bind test was selected. \
                 See tests/fixtures/README.md."
            );
        });

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

        let type_id_args = [0xABu8; 32];
        let expected = type_id_script_hash(&type_id_args);
        let ts = Script::new_builder()
            .code_hash(expected.pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0xCDu8; 32].to_vec().pack())
            .build();
        let hash = crate::rpc::script_hash32(&ts);
        // Divergent state payload that looks like an ELF but is not the guest.
        let state = LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output: CellOutput::new_builder()
                .capacity(500_0000_0000u64.pack())
                .lock(
                    Script::new_builder()
                        .code_hash([0u8; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args(Vec::<u8>::new().pack())
                        .build(),
                )
                .type_(Some(ts).pack())
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
        let rpc = OneProcess { cell: state };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
        resolver.set_tick_process_code(Some(&code));
        let resolved = resolver.resolve(&rpc, &hash).unwrap().expect("resolved");
        assert_eq!(
            resolved.binary, bin,
            "SSRI binary must be Type-ID code cell"
        );
        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&resolved.binary)
            .expect("Process.batch via ckb-vm on Type-ID code ELF");
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
