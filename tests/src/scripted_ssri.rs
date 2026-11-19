use std::sync::Mutex;

use cid_backend_core::ssri::{SsriError, SsriHost};
use cid_backend_core::type_id::TYPE_ID_CODE_HASH;
use cid_process_executor::{AssembledTx, HostCell, MockHost};
use cid_protocol::CidArgs;
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{CellOutput, RawTransaction, Script, Transaction};
use ckb_types::prelude::*;

pub struct ScriptedSsri {
    pub bounds: (u32, u32),
    pub mode: Mode,
    /// Binaries observed by `get_batch_bounds` / `assemble` (SSRI source tests).
    pub seen_binaries: Mutex<Vec<Vec<u8>>>,
}

pub enum Mode {
    MimicExample,
    AlwaysIncompatible(i8),
    Unusable,
    OkThenIncompatible {
        remaining_ok: Mutex<u32>,
        code: i8,
    },
    /// Digest_ok beneficiary output capacity (independent of Intention capacity).
    FixedBeneficiary(u64),
}

impl ScriptedSsri {
    fn base(bounds: (u32, u32), mode: Mode) -> Self {
        Self {
            bounds,
            mode,
            seen_binaries: Mutex::new(Vec::new()),
        }
    }

    pub fn mimic(bounds: (u32, u32)) -> Self {
        Self::base(bounds, Mode::MimicExample)
    }

    pub fn incompatible(code: i8) -> Self {
        Self::base((1, 4), Mode::AlwaysIncompatible(code))
    }

    pub fn unusable() -> Self {
        Self::base((1, 4), Mode::Unusable)
    }

    pub fn ok_then_incompatible(bounds: (u32, u32), ok_calls: u32, code: i8) -> Self {
        Self::base(
            bounds,
            Mode::OkThenIncompatible {
                remaining_ok: Mutex::new(ok_calls),
                code,
            },
        )
    }

    pub fn fixed_beneficiary(bounds: (u32, u32), capacity: u64) -> Self {
        Self::base(bounds, Mode::FixedBeneficiary(capacity))
    }

    fn note_binary(&self, binary: &[u8]) {
        self.seen_binaries.lock().unwrap().push(binary.to_vec());
    }
}

impl SsriHost for ScriptedSsri {
    fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError> {
        self.note_binary(binary);
        match &self.mode {
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            _ => Ok(self.bounds),
        }
    }

    fn assemble(
        &self,
        binary: &[u8],
        draft_tx: &[u8],
        host: MockHost,
    ) -> Result<AssembledTx, SsriError> {
        self.note_binary(binary);
        match &self.mode {
            Mode::AlwaysIncompatible(c) => Err(SsriError::Incompatible(*c)),
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            Mode::MimicExample => mimic_assemble(draft_tx, host, None),
            Mode::FixedBeneficiary(cap) => mimic_assemble(draft_tx, host, Some(*cap)),
            Mode::OkThenIncompatible { remaining_ok, code } => {
                let mut n = remaining_ok.lock().unwrap();
                if *n > 0 {
                    *n -= 1;
                    mimic_assemble(draft_tx, host, None)
                } else {
                    Err(SsriError::Incompatible(*code))
                }
            }
        }
    }
}

fn is_process_state(cell: &HostCell) -> bool {
    cell.output
        .type_()
        .to_opt()
        .map(|ts| {
            let code: [u8; 32] = ts.code_hash().unpack();
            code == TYPE_ID_CODE_HASH
                && matches!(
                    ScriptHashType::try_from(ts.hash_type()),
                    Ok(ScriptHashType::Type)
                )
        })
        .unwrap_or(false)
}

fn beneficiary_output(
    cell: &HostCell,
    fixed_cap: Option<u64>,
) -> Result<(CellOutput, Vec<u8>), SsriError> {
    let lock_args: Vec<u8> = cell.output.lock().args().unpack();
    let args =
        CidArgs::decode(&lock_args).map_err(|e| SsriError::Unusable(format!("cid args: {e:?}")))?;
    let user = args.entry.user_lock;
    let lock = Script::new_builder()
        .code_hash(user.code_hash.pack())
        .hash_type(user.hash_type.into())
        .args(user.args.pack())
        .build();
    let cap = fixed_cap.unwrap_or(cell.output.capacity().unpack());
    let mut builder = CellOutput::new_builder().capacity(cap.pack()).lock(lock);
    if let Some(ts) = cell.output.type_().to_opt() {
        builder = builder.type_(Some(ts).pack());
    }
    Ok((builder.build(), cell.data.clone()))
}

fn mimic_assemble(
    draft_tx: &[u8],
    host: MockHost,
    fixed_cap: Option<u64>,
) -> Result<AssembledTx, SsriError> {
    let draft = Transaction::from_slice(draft_tx)
        .map_err(|e| SsriError::Unusable(format!("draft: {e:?}")))?;
    let mut inputs = Vec::new();
    let mut beneficiary_outputs = Vec::new();
    let mut beneficiary_data = Vec::new();
    let mut process_out: Option<(CellOutput, Vec<u8>)> = None;

    for input in draft.raw().inputs().into_iter() {
        let op = input.previous_output();
        let cell = host
            .get_cell(op.as_slice())
            .ok_or_else(|| SsriError::Unusable("missing host cell".into()))?;
        // Preserve draft `since` (matches Process.assemble / WitnessEx::new_plain path).
        inputs.push(input);
        if is_process_state(&cell) {
            process_out = Some((cell.output.clone(), cell.data.clone()));
        } else {
            let (out, data) = beneficiary_output(&cell, fixed_cap)?;
            beneficiary_outputs.push(out);
            beneficiary_data.push(data);
        }
    }

    let (proc_out, proc_data) =
        process_out.ok_or_else(|| SsriError::Unusable("no process input in draft".into()))?;

    // Process recreate first (TYPE_ID continuity), then beneficiaries.
    let mut outputs = vec![proc_out];
    let mut outputs_data = vec![proc_data];
    outputs.extend(beneficiary_outputs);
    outputs_data.extend(beneficiary_data);

    let witnesses: Vec<ckb_types::packed::Bytes> =
        (0..inputs.len()).map(|_| Default::default()).collect();
    let bytes = Transaction::new_builder()
        .raw(
            RawTransaction::new_builder()
                .inputs(inputs.pack())
                .outputs(outputs.pack())
                .outputs_data(outputs_data.iter().map(|d| d.as_slice().pack()).pack())
                .build(),
        )
        .witnesses(witnesses.pack())
        .build()
        .as_slice()
        .to_vec();
    AssembledTx::from_bytes(&bytes).map_err(|e| SsriError::Unusable(format!("{e:?}")))
}
