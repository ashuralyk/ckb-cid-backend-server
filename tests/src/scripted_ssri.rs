use ckb_types::core::ScriptHashType;
use ckb_types::packed::{CellOutput, RawTransaction, Script, Transaction};
use ckb_types::prelude::*;
use cid_backend_core::ssri::{SsriError, SsriHost};
use cid_process_executor::{IntentionInput, PartialTx};

pub struct ScriptedSsri {
    pub bounds: (u32, u32),
    pub mode: Mode,
}

pub enum Mode {
    MimicExample,
    AlwaysIncompatible(i8),
    Unusable,
}

impl ScriptedSsri {
    pub fn mimic(bounds: (u32, u32)) -> Self {
        Self {
            bounds,
            mode: Mode::MimicExample,
        }
    }

    pub fn incompatible(code: i8) -> Self {
        Self {
            bounds: (1, 4),
            mode: Mode::AlwaysIncompatible(code),
        }
    }

    pub fn unusable() -> Self {
        Self {
            bounds: (1, 4),
            mode: Mode::Unusable,
        }
    }
}

fn pack_partial(outputs: Vec<CellOutput>, data: Vec<Vec<u8>>) -> PartialTx {
    let bytes = Transaction::new_builder()
        .raw(
            RawTransaction::new_builder()
                .outputs(outputs.pack())
                .outputs_data(data.iter().map(|d| d.as_slice().pack()).pack())
                .build(),
        )
        .build()
        .as_slice()
        .to_vec();
    PartialTx::from_bytes(&bytes).unwrap()
}

impl SsriHost for ScriptedSsri {
    fn get_batch_bounds(&self, _binary: &[u8]) -> Result<(u32, u32), SsriError> {
        match self.mode {
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            _ => Ok(self.bounds),
        }
    }

    fn assemble(
        &self,
        _binary: &[u8],
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError> {
        match self.mode {
            Mode::AlwaysIncompatible(c) => Err(SsriError::Incompatible(c)),
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            Mode::MimicExample => {
                let mut outputs = Vec::new();
                let mut data = Vec::new();
                for it in intentions {
                    let lock = Script::new_builder()
                        .code_hash([0x11u8; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args(it.lock_args.clone().pack())
                        .build();
                    outputs.push(
                        CellOutput::new_builder()
                            .capacity(it.capacity.pack())
                            .lock(lock)
                            .build(),
                    );
                    data.push(it.data.clone());
                }
                Ok(pack_partial(outputs, data))
            }
        }
    }
}
