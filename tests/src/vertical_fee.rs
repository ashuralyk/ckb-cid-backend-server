//! FakeRpc vertical slices for Execution fee deducted from Process recreate.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::script_hash32;
    use cid_backend_core::{
        fee_for_size, process_min_capacity, tx_size_in_block, DEFAULT_FEE_RATE,
    };
    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{intention_cell, process_cell, process_type_script, user_lock, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg(fee_rate: u64) -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            fee_rate,
        }
    }

    fn output_caps(tx: &ckb_types::core::TransactionView) -> Vec<u64> {
        tx.outputs()
            .into_iter()
            .map(|o| {
                let c: u64 = o.capacity().unpack();
                c
            })
            .collect()
    }

    #[test]
    fn n1_deducts_fee_from_process_not_beneficiary() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc_cap = 200_0000_0000u64;
        let intent = intention_cell(1, hash, intent_cap);
        let proc = process_cell(1, proc_cap, vec![0x7f, b'E', b'L', b'F', 1]);
        rpc.insert_cell(intent);
        rpc.insert_cell(proc);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 2, "no fee input");
        let caps = output_caps(tx);
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0], intent_cap, "beneficiary untouched");
        assert!(caps[1] < proc_cap, "Process recreate pays fee");
        let paid = proc_cap + intent_cap - caps[0] - caps[1];
        let need = fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE);
        assert!(paid >= need, "paid={paid} need={need}");
        assert_eq!(paid, proc_cap - caps[1]);
    }

    #[test]
    fn batch_deducts_fee_from_process_only() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        let proc_cap = 400_0000_0000u64;
        rpc.insert_cell(process_cell(2, proc_cap, vec![0x7f, b'E', b'L', b'F', 2]));
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3);
        let caps = output_caps(tx);
        assert_eq!(caps.len(), 3);
        assert_eq!(caps[0], 100_0000_0000);
        assert_eq!(caps[1], 100_0000_0000);
        assert!(caps[2] < proc_cap);
        let paid = proc_cap - caps[2];
        assert!(paid >= fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE));
    }

    #[test]
    fn incompatible_refund_keeps_exact_capacity_no_process_fee() {
        // Incompatible path cannot spend Process for fee without flipping the
        // Process example contract into DigestOk (beneficiary) verification.
        // Refund outputs therefore stay exact; fee paid is 0.
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.outputs().len(), 1);
        let cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
        assert_eq!(cap, intent_cap, "refund not reduced for fee");
        let code: [u8; 32] = tx.outputs().get(0).unwrap().lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == proc_op),
            "Process stays live on refund path"
        );
    }

    #[test]
    fn fee_unaffordable_skips_without_submit() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(4);
        let hash = script_hash32(&ts);
        let data = vec![0x7fu8; 128];
        let probe = process_cell(4, 0, data.clone());
        let min = process_min_capacity(&probe.output, &data).expect("occupied");
        let intent = intention_cell(1, hash, 100_0000_0000);
        // Only 10 shannons above occupied — far below any fee at rate 1000.
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(4, min + 10, data));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == intent.out_point),
            "Intention must remain live (non-permanent skip)"
        );
        assert!(loop_.state().submit.rejected_combos.is_empty());
    }

    #[test]
    fn assembled_fee_satisfies_size_times_rate_over_1000() {
        let ts = process_type_script(5);
        let hash = script_hash32(&ts);

        for rate in [1u64, 1000, 2500, 50_000] {
            let rpc = FakeRpc::new();
            rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
            rpc.insert_cell(process_cell(
                5,
                200_0000_0000,
                vec![0x7f, b'E', b'L', b'F', 5],
            ));
            let mut loop_ = DigestLoop::new(
                rpc,
                NopClock,
                ScriptedSsri::mimic((1, 4)),
                cfg(rate),
                ProcessResolver::new(),
            );
            assert_eq!(loop_.tick().submitted, 1, "rate={rate}");
            let tx = &loop_.rpc().submitted()[0];
            let caps = output_caps(tx);
            let paid = 200_0000_0000u64 + 100_0000_0000 - caps[0] - caps[1];
            let need = fee_for_size(tx_size_in_block(tx), rate);
            assert!(paid >= need, "rate={rate} paid={paid} need={need}");
        }
    }
}
