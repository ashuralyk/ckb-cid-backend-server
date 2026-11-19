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
        use cid_backend_core::{out_point_key, CidLockIdentity, DEFAULT_FEE_RATE, MAX_FEE_RATE};
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
        let pending = loop_.workers().get(&hash).unwrap().with_state(|s| {
            s.pending_incompatible_refund
                .contains(&out_point_key(&bounded.out_point))
        });
        assert!(
            pending,
            "unaffordable identity must be marked for below-min refund retry"
        );
        let expanded = loop_
            .workers()
            .get(&hash)
            .unwrap()
            .with_state(|s| s.isolate_expanded.len());
        assert_eq!(
            expanded, 0,
            "RefundFeeUnaffordable must not trigger isolate splits"
        );

        // P2: singleton leftover (len < min=2) must still refund once fee is affordable.
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        let report2 = loop_.tick();
        assert_eq!(
            report2.submitted, 1,
            "below-min incompatible leftover must refund after fee becomes affordable; {:?}",
            report2.last_error
        );
        assert!(!loop_.rpc().has_cell(&bounded.out_point));
    }

    #[test]
    fn refund_fee_unaffordable_does_not_isolate_or_cache_reject() {
        use cid_backend_core::{out_point_key, DEFAULT_FEE_RATE, MAX_FEE_RATE};

        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let capacity = 100_0000_0000;
        let bounded = intention_cell(1, hash, capacity);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::incompatible(21), config, 3);

        let report = loop_.tick();
        assert_eq!(report.submitted, 0, "{:?}", report.last_error);
        assert!(loop_.rpc().has_cell(&bounded.out_point));
        loop_.workers().get(&hash).unwrap().with_state(|s| {
            assert!(s
                .pending_incompatible_refund
                .contains(&out_point_key(&bounded.out_point)));
            assert!(
                s.isolate_expanded.is_empty(),
                "FeeUnaffordable must not isolate-split"
            );
            assert!(
                s.submit.rejected_combos.is_empty(),
                "FeeUnaffordable must not cache rejected_combos"
            );
        });

        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 1, "{:?}", report2.last_error);
        assert!(!loop_.rpc().has_cell(&bounded.out_point));
    }

    #[test]
    fn below_min_refund_drop_restores_pending_and_retries() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{out_point_key, CidLockIdentity, DEFAULT_FEE_RATE, MAX_FEE_RATE};
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

        assert_eq!(loop_.tick().submitted, 1);
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        loop_.rpc().defer_commit();
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 1, "{:?}", report2.last_error);
        assert!(
            loop_.workers().get(&hash).unwrap().with_state(|s| {
                s.pending_incompatible_refund
                    .contains(&out_point_key(&bounded.out_point))
            }),
            "mark must survive refund submit while tx is pending"
        );
        let tx_hash = loop_.rpc().submitted().last().unwrap().hash();
        loop_.rpc().simulate_drop(&tx_hash);
        assert!(loop_.rpc().has_cell(&bounded.out_point));
        assert!(
            loop_.workers().get(&hash).unwrap().with_state(|s| {
                s.pending_incompatible_refund
                    .contains(&out_point_key(&bounded.out_point))
            }),
            "mark must remain after drop so below-min refund_only can retry"
        );

        let report3 = loop_.tick();
        assert_eq!(
            report3.submitted, 1,
            "dropped below-min refund must retry; {:?}",
            report3.last_error
        );
        assert_eq!(loop_.rpc().submitted().len(), 3);
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .in_flight
                .contains(&out_point_key(&bounded.out_point)),
            "retry submit must re-occupy the restored Intention"
        );
    }

    #[test]
    fn below_min_mixed_frame_refunds_pending_subset_only() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{CidLockIdentity, DEFAULT_FEE_RATE, MAX_FEE_RATE};
        use ckb_types::core::ScriptHashType;

        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let capacity = 100_0000_0000;
        let legacy = CidLockIdentity::new([0xd1; 32], ScriptHashType::Data1);
        let bounded = intention_cell(1, hash, capacity);
        let exact_a =
            intention_cell_with_lock(2, hash, capacity, legacy.code_hash, legacy.hash_type);
        let exact_b =
            intention_cell_with_lock(3, hash, capacity, legacy.code_hash, legacy.hash_type);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(exact_a);
        rpc.insert_cell(exact_b);
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        config.extra_scan_locks = vec![legacy];
        let ssri = ScriptedSsri::ok_then_incompatible((3, 3), 0, 21);
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        let report = loop_.tick();
        assert_eq!(
            report.submitted, 1,
            "exact identity group refunds; {:?}",
            report.last_error
        );
        assert!(loop_.rpc().has_cell(&bounded.out_point));

        // Non-pending peer joins the below-min bucket (2 < min=3).
        let peer = intention_cell(4, hash, capacity);
        loop_.rpc().insert_cell(peer.clone());
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        let report2 = loop_.tick();
        assert_eq!(
            report2.submitted, 1,
            "pending subset must refund despite non-pending peer; {:?}",
            report2.last_error
        );
        assert!(!loop_.rpc().has_cell(&bounded.out_point));
        assert!(
            loop_.rpc().has_cell(&peer.out_point),
            "non-pending peer must remain for a legal [min,max] window"
        );
    }

    #[test]
    fn refund_only_dry_run_reject_does_not_isolate_or_burn_attempts() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{CidLockIdentity, DEFAULT_FEE_RATE, MAX_FEE_RATE};
        use ckb_types::core::ScriptHashType;

        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let capacity = 100_0000_0000;
        let legacy = CidLockIdentity::new([0xd1; 32], ScriptHashType::Data1);
        let bounded = intention_cell(1, hash, capacity);
        let exact = intention_cell_with_lock(2, hash, capacity, legacy.code_hash, legacy.hash_type);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(exact);
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        config.extra_scan_locks = vec![legacy];
        let ssri = ScriptedSsri::ok_then_incompatible((2, 2), 0, 21);
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        assert_eq!(loop_.tick().submitted, 1);
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        loop_.rpc().inject_dry_run_reject_once();
        let dry_before = loop_.rpc().dry_run_attempts();
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 0, "{:?}", report2.last_error);
        assert!(loop_.rpc().has_cell(&bounded.out_point));
        assert_eq!(
            loop_.rpc().dry_run_attempts(),
            dry_before + 1,
            "first refund_only attempt should dry-run once"
        );
        loop_.workers().get(&hash).unwrap().with_state(|s| {
            assert!(
                s.isolate_expanded.is_empty(),
                "refund_only permanent reject must not isolate-split"
            );
            assert!(
                !s.submit.rejected_combos.is_empty(),
                "combo must be cached to avoid re-burn"
            );
        });

        // Later ticks: soft-skip via process=None refund fingerprint — no
        // isolate_windows, no further RunBatch/dry-run, no continue_round burn.
        let dry_cached = loop_.rpc().dry_run_attempts();
        for _ in 0..3 {
            let r = loop_.tick();
            assert_eq!(r.submitted, 0);
            assert_eq!(
                r.isolate_windows, 0,
                "cached refund_only must soft-skip before isolate_windows"
            );
            assert!(loop_.rpc().has_cell(&bounded.out_point));
        }
        assert_eq!(
            loop_.rpc().dry_run_attempts(),
            dry_cached,
            "cached refund_only must not dispatch RunBatch / dry-run again"
        );
        assert!(loop_
            .workers()
            .get(&hash)
            .unwrap()
            .with_state(|s| s.isolate_expanded.is_empty()));
    }
}
