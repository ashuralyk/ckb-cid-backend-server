use crate::scan::ScannedIntention;
use ckb_types::prelude::*;
use cid_process_executor::{ExecutorError, IntentionInput, PartialTx, ProcessExecutor};
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
        type_script: scanned
            .type_script
            .as_ref()
            .map(|s| s.as_slice().to_vec()),
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
        let err = EmbeddedSsri
            .assemble(&[0xff; 8], &[])
            .expect_err("garbage");
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
