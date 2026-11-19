//! The guest pays DigestOk fees; the backend handles incompatible refunds.

#[cfg(test)]
mod tests {
    use cid_backend_core::rpc::script_hash32;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, intention_cell, loop_with_resolver, process_cell, process_type_script,
        user_lock,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn incompatible_refund_keeps_exact_capacity_no_process_fee() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::incompatible(21),
            default_loop_config(),
            3,
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.outputs().len(), 1);
        let cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
        assert_eq!(cap, intent_cap, "exact refund only");
        let code: [u8; 32] = tx.outputs().get(0).unwrap().lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(tx.header_deps().is_empty());
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == proc_op),
            "Process stays live on incompatible refund"
        );
    }

    #[test]
    fn digest_ok_fee_guard_is_none_passthrough() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(
            1,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 1],
        ));

        let mut loop_ =
            loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        let caps: Vec<u64> = tx
            .outputs()
            .into_iter()
            .map(|o| o.capacity().unpack())
            .collect();
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[1], 100_0000_0000, "beneficiary capacity preserved");
        assert_eq!(
            caps[0], 200_0000_0000,
            "process recreate unchanged (guest owns fee)"
        );
    }

    #[test]
    fn legacy_scan_and_bounded_refund_preserve_per_lock_fee_modes() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{fee_for_size, tx_size_in_block, CidLockIdentity};
        use ckb_types::core::ScriptHashType;

        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let capacity = 100_0000_0000;
        let legacy = CidLockIdentity::new([0xd1; 32], ScriptHashType::Data1);
        let legacy_cell =
            intention_cell_with_lock(2, hash, capacity, legacy.code_hash, legacy.hash_type);
        let primary = intention_cell(1, hash, capacity);
        rpc.insert_cell(primary.clone());
        rpc.insert_cell(legacy_cell.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.extra_scan_locks = vec![legacy];
        config.refund_policy.bounded_locks = vec![legacy];
        config.refund_policy.fee_rate = 2500;
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::incompatible(21), config, 3);

        let report = loop_.tick();
        assert_eq!(report.scanned, 2);
        assert_eq!(report.submitted, 2, "{:?}", report.last_error);
        for tx in loop_.rpc().submitted() {
            assert_eq!(tx.inputs().len(), 1);
            let input = tx.inputs().get(0).unwrap().previous_output();
            let returned: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
            if input == legacy_cell.out_point {
                let fee = fee_for_size(tx_size_in_block(&tx), 2500);
                assert!(fee > 0);
                assert_eq!(capacity - returned, fee);
            } else {
                assert_eq!(input, primary.out_point);
                assert_eq!(capacity, returned);
            }
        }
    }
    #[test]
    fn unaffordable_bounded_group_does_not_suppress_exact_refund() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{CidLockIdentity, MAX_FEE_RATE};
        use ckb_types::core::ScriptHashType;

        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let capacity = 100_0000_0000;
        let legacy = CidLockIdentity::new([0xd1; 32], ScriptHashType::Data1);
        let bounded = intention_cell(1, hash, capacity);
        let exact = intention_cell_with_lock(2, hash, capacity, legacy.code_hash, legacy.hash_type);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(exact.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        config.extra_scan_locks = vec![legacy];
        let ssri = ScriptedSsri::ok_then_incompatible((2, 2), 0, 21);
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(
            tx.inputs().get(0).unwrap().previous_output(),
            exact.out_point
        );
        assert!(loop_.rpc().has_cell(&bounded.out_point));
    }
}
