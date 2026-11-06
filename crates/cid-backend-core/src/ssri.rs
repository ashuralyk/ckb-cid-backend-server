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

    #[test]
    fn real_example_binary_batch_bounds_if_present() {
        let Some(bin) = load_example_binary() else {
            eprintln!(
                "skip: cid_process_example missing. Set CID_PROCESS_EXAMPLE_PATH or copy to tests/fixtures/cid_process_example (see tests/fixtures/README.md)"
            );
            return;
        };
        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&bin)
            .expect("Process.batch");
        assert!(min >= 1);
        assert!(max >= min);
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
