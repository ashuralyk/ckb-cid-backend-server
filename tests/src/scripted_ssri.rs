use std::sync::Mutex;

use cid_backend_core::ssri::{SsriError, SsriHost};
use cid_backend_core::type_id::TYPE_ID_CODE_HASH;
use cid_process_executor::{AssembledTx, HostCell, MockHost};
use cid_protocol::CidArgs;
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{CellInput, CellOutput, OutPoint, RawTransaction, Script, Transaction};
use ckb_types::prelude::*;

type FilterScript = Box<dyn FnMut(usize) -> Result<Vec<i8>, SsriError> + Send>;
type AssemblyErrorScript = Box<dyn Fn(&[OutPoint], &MockHost) -> Option<i8> + Send + Sync>;

pub struct ScriptedSsri {
    filter_script: Mutex<Option<FilterScript>>,
    pub filter_requests: Mutex<Vec<Vec<u8>>>,
    intention_errors: Vec<(OutPoint, i8, Option<u64>)>,
    pair_error: Option<(OutPoint, i8)>,
    assembly_error: Option<AssemblyErrorScript>,
    additional_input: Mutex<Option<(OutPoint, Option<OutPoint>)>>,
    pub bounds: (u32, u32),
    pub mode: Mode,
    /// Binaries observed by `get_batch_bounds` / `assemble` (SSRI source tests).
    pub seen_binaries: Mutex<Vec<Vec<u8>>>,
}

pub enum Mode {
    MimicExample,
    AlwaysIncompatible(i8),
    Unusable,
    /// Digest_ok beneficiary output capacity (independent of Intention capacity).
    FixedBeneficiary(u64),
}

impl ScriptedSsri {
    fn base(bounds: (u32, u32), mode: Mode) -> Self {
        Self {
            filter_script: Mutex::new(None),
            filter_requests: Mutex::new(Vec::new()),
            intention_errors: Vec::new(),
            pair_error: None,
            assembly_error: None,
            additional_input: Mutex::new(None),
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

    pub fn fixed_beneficiary(bounds: (u32, u32), capacity: u64) -> Self {
        Self::base(bounds, Mode::FixedBeneficiary(capacity))
    }

    pub fn with_filter(
        self,
        filter: impl FnMut(usize) -> Result<Vec<i8>, SsriError> + Send + 'static,
    ) -> Self {
        *self.filter_script.lock().unwrap() = Some(Box::new(filter));
        self
    }

    pub fn with_intention_error(mut self, op: OutPoint, code: i8) -> Self {
        self.intention_errors.push((op, code, None));
        self
    }

    pub fn with_pair_error(mut self, op: OutPoint, code: i8) -> Self {
        self.pair_error = Some((op, code));
        self
    }

    pub fn with_assembly_error(
        mut self,
        error: impl Fn(&[OutPoint], &MockHost) -> Option<i8> + Send + Sync + 'static,
    ) -> Self {
        self.assembly_error = Some(Box::new(error));
        self
    }

    pub fn with_intention_error_at_state_capacity(
        mut self,
        op: OutPoint,
        code: i8,
        cap: u64,
    ) -> Self {
        self.intention_errors.push((op, code, Some(cap)));
        self
    }

    pub fn with_added_input(self, op: OutPoint) -> Self {
        self.set_added_input(op);
        self
    }

    pub fn set_added_input(&self, op: OutPoint) {
        *self.additional_input.lock().unwrap() = Some((op, None));
    }

    pub fn with_added_input_for(self, intention: OutPoint, input: OutPoint) -> Self {
        *self.additional_input.lock().unwrap() = Some((input, Some(intention)));
        self
    }

    fn note_binary(&self, binary: &[u8]) {
        self.seen_binaries.lock().unwrap().push(binary.to_vec());
    }
}

impl SsriHost for ScriptedSsri {
    fn filter(&self, _: &[u8], request: &[u8]) -> Result<Vec<i8>, SsriError> {
        self.filter_requests.lock().unwrap().push(request.to_vec());
        let count = cid_intention_filter::decode_filter_request(request)
            .unwrap()
            .cells
            .len();
        match self.filter_script.lock().unwrap().as_mut() {
            Some(script) => script(count),
            None => Ok(vec![0; count]),
        }
    }

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
        if self.assembly_error.is_some()
            || !self.intention_errors.is_empty()
            || self.pair_error.is_some()
        {
            let inputs: Vec<_> = Transaction::from_slice(draft_tx)
                .unwrap()
                .raw()
                .inputs()
                .into_iter()
                .map(|input| input.previous_output())
                .collect();
            if let Some(code) = self
                .assembly_error
                .as_ref()
                .and_then(|error| error(&inputs, &host))
            {
                return Err(SsriError::Incompatible(code));
            }
            if let Some((op, code)) = &self.pair_error {
                if inputs.len() == 3 && inputs.contains(op) {
                    return Err(SsriError::Incompatible(*code));
                }
            }
            for op in &inputs {
                for (bad, code, state_cap) in &self.intention_errors {
                    if bad == op
                        && state_cap.is_none_or(|cap| {
                            inputs.iter().any(|input| {
                                host.get_cell(input.as_slice()).is_some_and(|cell| {
                                    let capacity: u64 = cell.output.capacity().unpack();
                                    is_process_state(&cell) && capacity == cap
                                })
                            })
                        })
                    {
                        return Err(SsriError::Incompatible(*code));
                    }
                }
            }
        }
        match &self.mode {
            Mode::AlwaysIncompatible(c) => Err(SsriError::Incompatible(*c)),
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            Mode::MimicExample => {
                let assembled = mimic_assemble(draft_tx, host, None)?;
                let Some((op, intention)) = &*self.additional_input.lock().unwrap() else {
                    return Ok(assembled);
                };
                let tx = Transaction::from_slice(assembled.as_molecule_bytes()).unwrap();
                let mut inputs: Vec<_> = tx.raw().inputs().into_iter().collect();
                if intention.as_ref().is_some_and(|intention| {
                    !inputs
                        .iter()
                        .any(|input| &input.previous_output() == intention)
                }) {
                    return Ok(assembled);
                }
                inputs.push(CellInput::new_builder().previous_output(op.clone()).build());
                let mut witnesses: Vec<_> = tx.witnesses().into_iter().collect();
                witnesses.push(Default::default());
                let raw = tx.raw().as_builder().inputs(inputs.pack()).build();
                let tx = tx.as_builder().raw(raw).witnesses(witnesses.pack()).build();
                AssembledTx::from_bytes(tx.as_slice())
                    .map_err(|error| SsriError::Unusable(error.to_string()))
            }
            Mode::FixedBeneficiary(cap) => mimic_assemble(draft_tx, host, Some(*cap)),
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
    // Preserve draft cell_deps / header_deps (Process.assemble copies them).
    let bytes = Transaction::new_builder()
        .raw(
            RawTransaction::new_builder()
                .version(draft.raw().version())
                .cell_deps(draft.raw().cell_deps())
                .header_deps(draft.raw().header_deps())
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
