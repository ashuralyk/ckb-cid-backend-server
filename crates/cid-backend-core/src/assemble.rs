use crate::process::ResolvedProcess;
use crate::rpc::script_hash32;
use crate::scan::ScannedIntention;
use ckb_types::core::{TransactionBuilder, TransactionView};
use ckb_types::packed::{CellDep, CellInput, CellOutput, Script};
use ckb_types::prelude::*;
use cid_process_executor::PartialTx;
use std::fmt;

#[derive(Clone, Debug)]
pub struct ExecutionSkeleton {
    pub tx: TransactionView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    CapacityShortfall { inputs: u64, outputs: u64 },
    RefundUnbuildable(String),
}

impl fmt::Display for AssembleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssembleError::CapacityShortfall { inputs, outputs } => {
                write!(f, "capacity shortfall in={inputs} out={outputs}")
            }
            AssembleError::RefundUnbuildable(m) => write!(f, "refund unbuildable: {m}"),
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
    let tx = TransactionBuilder::default()
        .inputs(inputs)
        .outputs(outputs)
        .outputs_data(outputs_data.into_iter().map(|d| d.pack()))
        .cell_deps(extra_cell_deps.iter().cloned())
        .build();
    Ok(ExecutionSkeleton { tx })
}

pub fn assemble_digest_ok(
    intentions: &[ScannedIntention],
    process: &ResolvedProcess,
    partial: &PartialTx,
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    let mut inputs: Vec<CellInput> = intentions
        .iter()
        .map(|i| cell_input(&i.out_point))
        .collect();
    inputs.push(cell_input(&process.live_cell.out_point));

    let input_capacity = intentions.iter().map(|i| i.capacity).sum::<u64>()
        + process.live_cell.capacity();

    let mut outputs = Vec::new();
    let mut outputs_data = Vec::new();
    let mut recreate: Option<(CellOutput, Vec<u8>)> = None;
    for (i, out) in partial.outputs.iter().enumerate() {
        let is_proc = out
            .type_()
            .to_opt()
            .map(|t| script_hash32(&t) == process.type_hash)
            .unwrap_or(false);
        if is_proc && recreate.is_none() {
            recreate = Some((out.clone(), partial.outputs_data[i].clone()));
        } else {
            outputs.push(out.clone());
            outputs_data.push(partial.outputs_data[i].clone());
        }
    }
    let (proc_out, proc_data) = recreate.unwrap_or_else(|| {
        (
            process.live_cell.output.clone(),
            process.live_cell.data.clone(),
        )
    });
    outputs.push(proc_out);
    outputs_data.push(proc_data);

    finish(inputs, input_capacity, outputs, outputs_data, extra_cell_deps)
}

fn protocol_script_to_packed(script: &cid_protocol::Script) -> Script {
    Script::new_builder()
        .code_hash(script.code_hash.pack())
        .hash_type(script.hash_type.into())
        .args(script.args.clone().pack())
        .build()
}

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
    finish(inputs, input_capacity, outputs, outputs_data, extra_cell_deps)
}

fn refund_output(
    intent: &ScannedIntention,
) -> Result<(CellOutput, Vec<u8>), AssembleError> {
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
        if intent.data.len() != cid_protocol::XUDT_AMOUNT_LEN {
            return Err(AssembleError::RefundUnbuildable(
                "xUDT amount is not 16 bytes".into(),
            ));
        }
        builder = builder.type_(Some(ts.clone()).pack());
    }
    Ok((builder.build(), intent.data.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::ResolvedProcess;
    use crate::rpc::LiveCell;
    use crate::scan::ScannedIntention;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{
        Byte32, CellOutput, OutPoint, RawTransaction, Script, Transaction,
    };
    use ckb_types::prelude::*;
    use cid_protocol::{CidArgs, Payload, Script as CidScript};
    use cid_process_executor::PartialTx;

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

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0xEE],
        }
    }

    fn intention(fill: u8, process_hash: [u8; 32], cap: u64) -> ScannedIntention {
        let args = CidArgs::new(
            process_hash,
            user_lock(),
            Payload { data: vec![0xAA] },
        );
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
        }
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
        }
    }

    fn beneficiary(cap: u64) -> CellOutput {
        CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(
                Script::new_builder()
                    .code_hash([0x11u8; 32].pack())
                    .hash_type(ScriptHashType::Type.into())
                    .args(vec![0x01u8].pack())
                    .build(),
            )
            .build()
    }

    #[test]
    fn digest_ok_destroys_intention_adds_process_pair_no_fee_no_user_lock_input() {
        let proc = process([0xAB; 32], 200_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 100_0000_0000);
        let partial = pack_partial(vec![beneficiary(100_0000_0000)], vec![vec![]]);
        let skel = assemble_digest_ok(&[intent.clone()], &proc, &partial, &[])
            .expect("assemble");
        let tx = skel.tx;
        assert_eq!(tx.inputs().len(), 2, "intention + process only; no fee input");
        let in_ops: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert_eq!(in_ops[0], intent.out_point);
        assert_eq!(in_ops[1], proc.live_cell.out_point);
        assert_eq!(tx.outputs().len(), 2, "beneficiary + process recreate");
        let proc_out = tx.outputs().get(1).unwrap();
        assert_eq!(
            proc_out.type_().to_opt().map(|t| crate::rpc::script_hash32(&t)),
            Some(hash)
        );
        assert_eq!(proc_out.capacity(), proc.live_cell.output.capacity());
        assert_eq!(proc_out.lock(), proc.live_cell.lock());
        assert_eq!(
            in_ops.len(),
            2,
            "User Lock is not an input; only intention + process outpoints"
        );
        assert!(
            tx.header_deps().is_empty(),
            "timeout_burn uses header deps; digest_ok must not"
        );
    }

    #[test]
    fn digest_ok_skips_on_capacity_shortfall() {
        let proc = process([0xAB; 32], 50_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 10_0000_0000);
        let partial = pack_partial(vec![beneficiary(500_0000_0000)], vec![vec![]]);
        let err = assemble_digest_ok(&[intent], &proc, &partial, &[]).unwrap_err();
        assert!(matches!(
            err,
            AssembleError::CapacityShortfall { .. }
        ));
    }

    #[test]
    fn digest_ok_uses_partial_process_recreate_when_present() {
        let proc = process([0xAB; 32], 200_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 100_0000_0000);
        let recreate = CellOutput::new_builder()
            .capacity(180_0000_0000u64.pack())
            .lock(proc.live_cell.lock())
            .type_(proc.live_cell.type_script().pack())
            .build();
        let partial = pack_partial(
            vec![beneficiary(100_0000_0000), recreate.clone()],
            vec![vec![], vec![0x01]],
        );
        let skel = assemble_digest_ok(&[intent], &proc, &partial, &[]).unwrap();
        assert_eq!(skel.tx.outputs().len(), 2);
        let proc_out = skel.tx.outputs().get(1).unwrap();
        let cap: u64 = proc_out.capacity().unpack();
        assert_eq!(cap, 180_0000_0000);
        let data: Vec<u8> = skel.tx.outputs_data().get(1).unwrap().unpack();
        assert_eq!(data, vec![0x01]);
    }

    #[test]
    fn digest_incompatible_refunds_user_lock_without_process_pair() {
        let intent = intention(1, [0xAB; 32], 100_0000_0000);
        let skel = assemble_digest_incompatible(&[intent.clone()], &[]).unwrap();
        let tx = skel.tx;
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.inputs().get(0).unwrap().previous_output(), intent.out_point);
        assert_eq!(tx.outputs().len(), 1);
        let out = tx.outputs().get(0).unwrap();
        let cap: u64 = out.capacity().unpack();
        assert_eq!(cap, 100_0000_0000);
        let lock = out.lock();
        let code: [u8; 32] = lock.code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(out.type_().to_opt().is_none());
        assert!(tx.header_deps().is_empty());
    }

    #[test]
    fn digest_incompatible_preserves_standard_xudt() {
        use cid_protocol::{XUDT_AMOUNT_LEN, XUDT_CODE_HASH, SCRIPT_HASH_TYPE_DATA1};
        let mut intent = intention(1, [0xAB; 32], 100_0000_0000);
        let ts = Script::new_builder()
            .code_hash(XUDT_CODE_HASH.pack())
            .hash_type(SCRIPT_HASH_TYPE_DATA1.into())
            .args([0x33u8; 32].to_vec().pack())
            .build();
        intent.type_script = Some(ts.clone());
        intent.data = 1_000u128.to_le_bytes()[..XUDT_AMOUNT_LEN].to_vec();
        let skel = assemble_digest_incompatible(&[intent], &[]).unwrap();
        let out = skel.tx.outputs().get(0).unwrap();
        let got = out.type_().to_opt().expect("xudt type");
        assert_eq!(got.as_slice(), ts.as_slice());
        let data: Vec<u8> = skel.tx.outputs_data().get(0).unwrap().unpack();
        assert_eq!(data, 1_000u128.to_le_bytes()[..XUDT_AMOUNT_LEN].to_vec());
    }

    #[test]
    fn digest_incompatible_rejects_non_standard_type() {
        let mut intent = intention(1, [0xAB; 32], 100_0000_0000);
        intent.type_script = Some(
            Script::new_builder()
                .code_hash([0xFFu8; 32].pack())
                .hash_type(ScriptHashType::Type.into())
                .args(Vec::<u8>::new().pack())
                .build(),
        );
        intent.data = vec![1, 2, 3];
        let err = assemble_digest_incompatible(&[intent], &[]).unwrap_err();
        assert!(matches!(err, AssembleError::RefundUnbuildable(_)));
    }
}
