use crate::fee::{
    allocate_refund_fee_cuts, cell_occupied_capacity, refund_fee_budget, required_fee,
    tx_size_in_block, with_capacity, FeeGuard, RefundFeeMode,
};
use crate::process::ResolvedProcess;
use crate::rpc::script_hash32;
use crate::scan::ScannedIntention;
use cid_process_executor::AssembledTx;
use ckb_types::core::TransactionView;
use ckb_types::packed::{CellDep, CellInput, CellOutput, Script};
use ckb_types::prelude::*;
use std::fmt;

#[derive(Clone, Debug)]
pub struct ExecutionSkeleton {
    pub tx: TransactionView,
    /// Always `None` for passthrough `Process.assemble` (guest owns fee/capacity).
    pub fee_guard: Option<FeeGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    CapacityShortfall { inputs: u64, outputs: u64 },
    RefundUnbuildable(String),
    RefundFeeUnaffordable { fee: u64, budget: u64 },
    InvalidAssembled(String),
    SizeLimit { size: usize, max: usize },
}

impl fmt::Display for AssembleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssembleError::CapacityShortfall { inputs, outputs } => {
                write!(f, "capacity shortfall in={inputs} out={outputs}")
            }
            AssembleError::RefundUnbuildable(m) => write!(f, "refund unbuildable: {m}"),
            AssembleError::RefundFeeUnaffordable { fee, budget } => {
                write!(f, "refund fee {fee} exceeds capacity budget {budget}")
            }
            AssembleError::InvalidAssembled(m) => write!(f, "invalid assembled tx: {m}"),
            AssembleError::SizeLimit { size, max } => {
                write!(f, "assembled tx size {size} exceeds max {max}")
            }
        }
    }
}

impl std::error::Error for AssembleError {}

fn cell_input(op: &ckb_types::packed::OutPoint) -> CellInput {
    CellInput::new_builder()
        .previous_output(op.clone())
        .since(0u64.pack())
        .build()
}

fn sum_output_capacity(outputs: &[CellOutput]) -> u64 {
    outputs
        .iter()
        .map(|o| {
            let c: u64 = o.capacity().unpack();
            c
        })
        .sum()
}

/// Parse `Process.assemble` molecule bytes into a skeleton **without** rewriting
/// any field (byte-equal to [`AssembledTx::as_molecule_bytes`]).
pub fn execution_from_assembled(
    assembled: &AssembledTx,
) -> Result<ExecutionSkeleton, AssembleError> {
    let bytes = assembled.as_molecule_bytes();
    let packed = ckb_types::packed::Transaction::from_slice(bytes)
        .map_err(|e| AssembleError::InvalidAssembled(format!("{e:?}")))?;
    let tx: TransactionView = packed.into_view();
    if tx.data().as_slice() != bytes {
        return Err(AssembleError::InvalidAssembled(
            "transaction view bytes diverge from assembled molecule".into(),
        ));
    }
    Ok(ExecutionSkeleton {
        tx,
        fee_guard: None,
    })
}

fn input_out_points(assembled: &AssembledTx) -> Vec<ckb_types::packed::OutPoint> {
    assembled
        .inputs
        .iter()
        .map(|i| i.previous_output())
        .collect()
}

/// Structural checks before submit: every Intention and Process input spent,
/// TYPE_ID process recreate present, size within budget.
pub fn validate_assembled_tx(
    assembled: &AssembledTx,
    intentions: &[ScannedIntention],
    process: &ResolvedProcess,
    max_size: usize,
) -> Result<(), AssembleError> {
    let size = assembled.as_molecule_bytes().len();
    if size > max_size {
        return Err(AssembleError::SizeLimit {
            size,
            max: max_size,
        });
    }

    // Process.assemble emits one witness per input; backend fills nothing.
    if assembled.witnesses.len() != assembled.inputs.len() {
        return Err(AssembleError::InvalidAssembled(format!(
            "witness count {} != input count {} (backend must not fill witnesses)",
            assembled.witnesses.len(),
            assembled.inputs.len()
        )));
    }

    let inputs = input_out_points(assembled);
    for intent in intentions {
        if !inputs
            .iter()
            .any(|op| op.as_slice() == intent.out_point.as_slice())
        {
            return Err(AssembleError::InvalidAssembled(format!(
                "missing intention input {:?}",
                intent.out_point
            )));
        }
    }
    if !inputs
        .iter()
        .any(|op| op.as_slice() == process.live_cell.out_point.as_slice())
    {
        return Err(AssembleError::InvalidAssembled(
            "missing process state input".into(),
        ));
    }

    // Every draft input (intentions + process state) must appear. Extra inputs
    // are allowed at the backend validator layer for Processes that emit them.
    // The pinned `cid_process_example` guest rejects extra funding inputs with
    // UnsupportedFundingLock (38); do not claim the example accepts funding.
    let mut expected: Vec<ckb_types::packed::OutPoint> =
        intentions.iter().map(|i| i.out_point.clone()).collect();
    expected.push(process.live_cell.out_point.clone());
    for op in &expected {
        if !inputs.iter().any(|i| i.as_slice() == op.as_slice()) {
            return Err(AssembleError::InvalidAssembled(
                "assembled inputs missing a draft out_point".into(),
            ));
        }
    }

    let proc_lock = process.live_cell.lock();
    let has_process_recreate = assembled.outputs.iter().any(|out| {
        let type_ok = out
            .type_()
            .to_opt()
            .map(|t| script_hash32(&t) == process.type_hash)
            .unwrap_or(false);
        if !type_ok {
            return false;
        }
        // Lock + type continuity for TYPE_ID recreate (P1-8).
        out.lock().as_slice() == proc_lock.as_slice()
    });
    if !has_process_recreate {
        return Err(AssembleError::InvalidAssembled(
            "missing process recreate output with matching TYPE_ID type hash and lock".into(),
        ));
    }

    Ok(())
}

fn finish(
    inputs: Vec<CellInput>,
    input_capacity: u64,
    outputs: Vec<CellOutput>,
    outputs_data: Vec<Vec<u8>>,
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    let output_capacity = sum_output_capacity(&outputs);
    if input_capacity < output_capacity {
        return Err(AssembleError::CapacityShortfall {
            inputs: input_capacity,
            outputs: output_capacity,
        });
    }
    let tx = ckb_types::core::TransactionBuilder::default()
        .inputs(inputs)
        .outputs(outputs)
        .outputs_data(outputs_data.into_iter().map(|d| d.pack()))
        .cell_deps(extra_cell_deps.iter().cloned())
        .build();
    Ok(ExecutionSkeleton {
        tx,
        fee_guard: None,
    })
}

fn protocol_script_to_packed(script: &cid_protocol::Script) -> Script {
    Script::new_builder()
        .code_hash(script.code_hash.pack())
        .hash_type(script.hash_type.into())
        .args(script.args.clone().pack())
        .build()
}

/// User Lock refund for SSRI `Incompatible`. Does **not** spend Process.
/// Exact Intention capacity on every refund output (paid fee 0).
pub fn assemble_digest_incompatible(
    intentions: &[ScannedIntention],
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    if intentions.is_empty() {
        return Err(AssembleError::RefundUnbuildable(
            "no intentions to refund".into(),
        ));
    }
    let mut inputs = Vec::new();
    let mut input_capacity = 0u64;
    let mut outputs = Vec::new();
    let mut outputs_data = Vec::new();
    for intent in intentions {
        inputs.push(cell_input(&intent.out_point));
        input_capacity += intent.capacity;
        let (out, data) = refund_output(intent)?;
        outputs.push(out);
        outputs_data.push(data);
    }
    finish(
        inputs,
        input_capacity,
        outputs,
        outputs_data,
        extra_cell_deps,
    )
}

/// Build a refund for one lock identity using its configured fee mode.
pub(crate) fn assemble_refund_with_policy(
    intentions: &[ScannedIntention],
    cell_deps: &[CellDep],
    mode: RefundFeeMode,
    fee_rate: u64,
) -> Result<ExecutionSkeleton, AssembleError> {
    let mut skeleton = assemble_digest_incompatible(intentions, cell_deps)?;
    if mode == RefundFeeMode::Exact {
        return Ok(skeleton);
    }

    let sized_size = tx_size_in_block(&skeleton.tx);
    let fee = required_fee(&skeleton.tx, fee_rate);
    let outputs: Vec<_> = skeleton.tx.outputs().into_iter().collect();
    let budgets = outputs
        .iter()
        .zip(intentions)
        .map(|(out, intent)| {
            let occupied = cell_occupied_capacity(out, &intent.data)
                .filter(|&occupied| occupied <= intent.capacity)
                .ok_or_else(|| {
                    AssembleError::RefundUnbuildable("output below occupied capacity".into())
                })?;
            Ok(refund_fee_budget(intent.capacity, occupied))
        })
        .collect::<Result<Vec<_>, AssembleError>>()?;
    let cuts = allocate_refund_fee_cuts(&budgets, fee).map_err(|shortfall| {
        AssembleError::RefundFeeUnaffordable {
            fee,
            budget: fee - shortfall,
        }
    })?;
    let outputs = outputs
        .iter()
        .zip(intentions)
        .zip(cuts)
        .map(|((out, intent), cut)| with_capacity(out, intent.capacity - cut))
        .collect();
    skeleton.tx = skeleton
        .tx
        .as_advanced_builder()
        .set_outputs(outputs)
        .build();
    skeleton.fee_guard = Some(FeeGuard {
        sized_size,
        fee_rate,
        paid_fee: fee,
    });
    Ok(skeleton)
}

fn refund_output(intent: &ScannedIntention) -> Result<(CellOutput, Vec<u8>), AssembleError> {
    if !intent.has_supported_user_lock() {
        return Err(AssembleError::RefundUnbuildable(
            "unsupported CID user lock hash type".into(),
        ));
    }
    let user = protocol_script_to_packed(&intent.args.entry.user_lock);
    let mut builder = CellOutput::new_builder()
        .capacity(intent.capacity.pack())
        .lock(user);
    if let Some(ts) = &intent.type_script {
        let code: [u8; 32] = ts.code_hash().unpack();
        let ht: u8 = ts.hash_type().into();
        if !cid_protocol::is_standard_xudt(&code, ht) {
            return Err(AssembleError::RefundUnbuildable(
                "non-standard type script; cannot exact-refund".into(),
            ));
        }
        let amount = intent.data.get(..cid_protocol::XUDT_AMOUNT_LEN);
        if !amount.is_some_and(|bytes| bytes.iter().any(|&byte| byte != 0)) {
            return Err(AssembleError::RefundUnbuildable(
                "xUDT refund requires a nonzero 16-byte amount".into(),
            ));
        }
        builder = builder.type_(Some(ts.clone()).pack());
    } else {
        if !intent.data.is_empty() {
            return Err(AssembleError::RefundUnbuildable(
                "untyped Intention must have empty data".into(),
            ));
        }
        let input = crate::ssri::intention_draft_cell(intent).output;
        if !cell_occupied_capacity(&input, &[]).is_some_and(|occupied| intent.capacity > occupied) {
            return Err(AssembleError::RefundUnbuildable(
                "CKB Intention capacity must exceed occupied capacity".into(),
            ));
        }
    }
    let output = builder.build();
    if !cell_occupied_capacity(&output, &intent.data)
        .is_some_and(|occupied| intent.capacity >= occupied)
    {
        return Err(AssembleError::RefundUnbuildable(
            "output below occupied capacity".into(),
        ));
    }
    Ok((output, intent.data.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::ResolvedProcess;
    use crate::rpc::LiveCell;
    use crate::scan::ScannedIntention;
    use cid_protocol::{CidArgs, Payload, Script as CidScript};
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, Bytes, OutPoint, RawTransaction, Script, Transaction};

    fn pack_assembled(
        inputs: Vec<CellInput>,
        outputs: Vec<CellOutput>,
        outputs_data: Vec<Vec<u8>>,
    ) -> AssembledTx {
        let witnesses: Vec<ckb_types::packed::Bytes> =
            (0..inputs.len()).map(|_| Bytes::default()).collect();
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
        AssembledTx::from_bytes(&bytes).expect("assembled tx")
    }

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0xEE],
        }
    }

    fn intention(fill: u8, process_hash: [u8; 32], cap: u64) -> ScannedIntention {
        let args = CidArgs::new(process_hash, user_lock(), Payload { data: vec![0xAA] });
        let lock_args = args.encode().unwrap();
        ScannedIntention {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[fill; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            capacity: cap,
            type_script: None,
            data: vec![],
            lock_args,
            args,
            lock_code_hash: [0xC1; 32],
            lock_hash_type: ScriptHashType::Type,
        }
    }

    #[test]
    fn refund_preserves_xudt_trailing_data() {
        let mut intent = intention(1, [2; 32], 600_0000_0000);
        intent.type_script = Some(
            Script::new_builder()
                .code_hash(cid_protocol::XUDT_CODE_HASH.pack())
                .hash_type(ScriptHashType::Data1.into())
                .args([1u8; 32].to_vec().pack())
                .build(),
        );
        intent.data = [1u128.to_le_bytes().as_slice(), &[0xaa, 0xbb]].concat();
        let skeleton = assemble_digest_incompatible(std::slice::from_ref(&intent), &[]).unwrap();
        assert_eq!(
            skeleton.tx.outputs_data().get(0).unwrap().raw_data(),
            intent.data
        );
    }

    #[test]
    fn refund_rejects_untyped_data() {
        let mut intent = intention(1, [2; 32], 600_0000_0000);
        intent.data = vec![1];
        assert!(matches!(
            assemble_digest_incompatible(&[intent], &[]),
            Err(AssembleError::RefundUnbuildable(_))
        ));
    }

    #[test]
    fn refund_rejects_data2_user_lock() {
        let mut intent = intention(1, [2; 32], 600_0000_0000);
        intent.args.entry.user_lock.hash_type = ScriptHashType::Data2 as u8;
        intent.lock_args = intent.args.encode().unwrap();
        assert!(matches!(
            assemble_digest_incompatible(&[intent], &[]),
            Err(AssembleError::RefundUnbuildable(message)) if message.contains("user lock hash type")
        ));
    }

    fn process(hash: [u8; 32], cap: u64) -> ResolvedProcess {
        let ts = Script::new_builder()
            .code_hash([0x51u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(hash.to_vec().pack())
            .build();
        let lock = Script::new_builder()
            .code_hash([0u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let output = CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(lock)
            .type_(Some(ts).pack())
            .build();
        let live = LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[0x51; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output,
            data: vec![0x7f, b'E', b'L', b'F'],
        };
        ResolvedProcess {
            type_hash: crate::rpc::script_hash32(&live.type_script().unwrap()),
            live_cell: live,
            binary: vec![0x7f, b'E', b'L', b'F'],
            code_cell: None,
        }
    }

    fn beneficiary(cap: u64) -> CellOutput {
        CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(
                Script::new_builder()
                    .code_hash([0x11u8; 32].pack())
                    .hash_type(ScriptHashType::Type.into())
                    .args([0x01u8].pack())
                    .build(),
            )
            .build()
    }

    fn valid_assembled(intentions: &[ScannedIntention], proc: &ResolvedProcess) -> AssembledTx {
        let mut inputs: Vec<CellInput> = intentions
            .iter()
            .map(|i| cell_input(&i.out_point))
            .collect();
        inputs.push(cell_input(&proc.live_cell.out_point));
        let mut outputs = vec![beneficiary(intentions[0].capacity)];
        let mut outputs_data = vec![vec![]];
        outputs.push(proc.live_cell.output.clone());
        outputs_data.push(proc.live_cell.data.clone());
        pack_assembled(inputs, outputs, outputs_data)
    }

    #[test]
    fn execution_from_assembled_is_byte_equal_passthrough() {
        let proc = process([0xAB; 32], 200_000_000_000);
        let intent = intention(1, proc.type_hash, 100_000_000_000);
        let assembled = valid_assembled(std::slice::from_ref(&intent), &proc);
        let raw = assembled.as_molecule_bytes().to_vec();
        let skel = execution_from_assembled(&assembled).expect("passthrough");
        assert_eq!(skel.fee_guard, None);
        assert_eq!(skel.tx.data().as_slice(), raw.as_slice());
    }

    #[test]
    fn validate_rejects_missing_intention_input() {
        let proc = process([0xAB; 32], 200_000_000_000);
        let intent = intention(1, proc.type_hash, 100_000_000_000);
        let inputs = vec![cell_input(&proc.live_cell.out_point)];
        let outputs = vec![beneficiary(intent.capacity), proc.live_cell.output.clone()];
        let assembled = pack_assembled(inputs, outputs, vec![vec![], proc.live_cell.data.clone()]);
        let err = validate_assembled_tx(&assembled, &[intent], &proc, usize::MAX).unwrap_err();
        assert!(matches!(err, AssembleError::InvalidAssembled(_)));
    }

    #[test]
    fn validate_rejects_missing_process_input() {
        let proc = process([0xAB; 32], 200_000_000_000);
        let intent = intention(1, proc.type_hash, 100_000_000_000);
        let inputs = vec![cell_input(&intent.out_point)];
        let outputs = vec![beneficiary(intent.capacity), proc.live_cell.output.clone()];
        let assembled = pack_assembled(inputs, outputs, vec![vec![], proc.live_cell.data.clone()]);
        let err = validate_assembled_tx(&assembled, &[intent], &proc, usize::MAX).unwrap_err();
        assert!(matches!(err, AssembleError::InvalidAssembled(_)));
    }

    #[test]
    /// Backend validator permits Process-added extras; the example guest itself
    /// rejects funding inputs (38) — this does not claim the example accepts them.
    fn validate_allows_process_added_extra_inputs() {
        let proc = process([0xAB; 32], 200_000_000_000);
        let intent = intention(1, proc.type_hash, 100_000_000_000);
        let extra_op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0xFEu8; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let inputs = vec![
            cell_input(&intent.out_point),
            cell_input(&proc.live_cell.out_point),
            cell_input(&extra_op),
        ];
        let outputs = vec![
            beneficiary(intent.capacity),
            proc.live_cell.output.clone(),
            beneficiary(50_000_000_000),
        ];
        let assembled = pack_assembled(
            inputs,
            outputs,
            vec![vec![], proc.live_cell.data.clone(), vec![]],
        );
        validate_assembled_tx(&assembled, &[intent], &proc, usize::MAX).expect("extras ok");
    }

    #[test]
    fn validate_rejects_type_id_continuity_break() {
        let proc = process([0xAB; 32], 200_000_000_000);
        let intent = intention(1, proc.type_hash, 100_000_000_000);
        let wrong_type = Script::new_builder()
            .code_hash([0x99u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0x01].pack())
            .build();
        let bad_recreate = CellOutput::new_builder()
            .capacity(proc.live_cell.capacity().pack())
            .lock(proc.live_cell.lock())
            .type_(Some(wrong_type).pack())
            .build();
        let inputs = vec![
            cell_input(&intent.out_point),
            cell_input(&proc.live_cell.out_point),
        ];
        let outputs = vec![beneficiary(intent.capacity), bad_recreate];
        let assembled = pack_assembled(inputs, outputs, vec![vec![], vec![]]);
        let err = validate_assembled_tx(&assembled, &[intent], &proc, usize::MAX).unwrap_err();
        assert!(matches!(err, AssembleError::InvalidAssembled(_)));
    }

    #[test]
    fn digest_incompatible_exact_refund_no_process() {
        let intent = intention(1, [0xAB; 32], 100_000_000_000);
        let skel = assemble_digest_incompatible(std::slice::from_ref(&intent), &[]).unwrap();
        let tx = skel.tx;
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(
            tx.inputs().get(0).unwrap().previous_output(),
            intent.out_point
        );
        assert_eq!(tx.outputs().len(), 1);
        let out = tx.outputs().get(0).unwrap();
        let cap: u64 = out.capacity().unpack();
        assert_eq!(cap, intent.capacity);
        let code: [u8; 32] = out.lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(out.type_().to_opt().is_none());
        assert!(tx.header_deps().is_empty());
    }
}
