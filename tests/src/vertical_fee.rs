//! The guest pays DigestOk fees; the backend applies the configured fee policy to authorized refunds.

#[cfg(test)]
mod tests {
    use cid_backend_core::rpc::script_hash32;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        code_cell, default_loop_config, intention_cell, loop_with_resolver, process_cell,
        process_type_script, user_lock, FIXTURE_ELF, PROC_CODE_ARGS,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn filter_refund_keeps_exact_capacity_no_process_fee() {
        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent_cap = 200_0000_0000u64;
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![5; n])),
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
    fn filter_refunds_use_the_configured_cid_fee_mode() {
        use crate::helpers::intention_cell_with_lock;
        use cid_backend_core::{fee_for_size, tx_size_in_block, CidLockIdentity};
        use ckb_types::core::ScriptHashType;

        for bounded in [false, true] {
            let rpc = FakeRpc::new();
            rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
            let hash = script_hash32(&process_type_script(3));
            let capacity = 200_0000_0000;
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
            if bounded {
                config
                    .refund_policy
                    .bounded_locks
                    .push(config.scan_lock_identities()[0]);
            }
            config.refund_policy.fee_rate = 2500;
            let mut loop_ = loop_with_resolver(
                rpc,
                ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![5; n])),
                config,
                3,
            );

            let report = loop_.tick();
            assert_eq!(report.scanned, 2);
            assert_eq!(report.submitted, 1, "{:?}", report.last_error);
            assert!(loop_.rpc().has_cell(&legacy_cell.out_point));
            let tx = &loop_.rpc().submitted()[0];
            assert_eq!(tx.inputs().len(), 1);
            assert_eq!(
                tx.inputs().get(0).unwrap().previous_output(),
                primary.out_point
            );
            let returned: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
            let expected_fee = if bounded {
                fee_for_size(tx_size_in_block(tx), 2500)
            } else {
                0
            };
            assert_eq!(capacity - returned, expected_fee);
        }
    }

    #[test]
    fn unaffordable_filter_refund_does_not_suppress_an_affordable_refund() {
        use cid_backend_core::DEFAULT_FEE_RATE;
        use cid_protocol::XUDT_CODE_HASH;
        use ckb_types::{core::ScriptHashType, packed::Script};

        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let hash = script_hash32(&process_type_script(3));
        let mut expensive = intention_cell(1, hash, 2000_0000_0000);
        expensive.data = vec![0; 1024];
        expensive.data[0] = 1;
        let xudt = Script::new_builder()
            .code_hash(XUDT_CODE_HASH.pack())
            .hash_type(ScriptHashType::Data1.into())
            .args([1u8; 32].as_slice().pack())
            .build();
        expensive.output = expensive
            .output
            .as_builder()
            .type_(Some(xudt).pack())
            .build();
        let affordable = intention_cell(2, hash, 200_0000_0000);
        rpc.insert_cell(expensive.clone());
        rpc.insert_cell(affordable.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = 1_000_000;
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|count| Ok(vec![6; count]));
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(
            tx.inputs().get(0).unwrap().previous_output(),
            affordable.out_point
        );
        assert!(loop_.rpc().has_cell(&expensive.out_point));
        loop_.workers().get(&hash).unwrap().with_state(|s| {
            assert!(s.isolate_expanded.is_empty());
        });

        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        assert!(!loop_.rpc().has_cell(&expensive.out_point));
    }

    #[test]
    fn refund_fee_unaffordable_does_not_isolate_or_cache_reject() {
        use cid_backend_core::{DEFAULT_FEE_RATE, MAX_FEE_RATE};

        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let hash = script_hash32(&process_type_script(3));
        let capacity = 200_0000_0000;
        let bounded = intention_cell(1, hash, capacity);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![5; n])),
            config,
            3,
        );

        let report = loop_.tick();
        assert_eq!(report.submitted, 0, "{:?}", report.last_error);
        assert!(loop_.rpc().has_cell(&bounded.out_point));
        loop_.workers().get(&hash).unwrap().with_state(|s| {
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
    fn below_min_filter_refund_refreshes_its_verdict_after_drop() {
        use cid_backend_core::{DEFAULT_FEE_RATE, MAX_FEE_RATE};

        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let hash = script_hash32(&process_type_script(3));
        let capacity = 200_0000_0000;
        let bounded = intention_cell(1, hash, capacity);
        let companion = intention_cell(2, hash, capacity);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(companion.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok([5, 0][..n].to_vec()));
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        assert_eq!(loop_.tick().submitted, 0);
        // Another actor spends the companion before the next scan.
        loop_.rpc().consume(&companion.out_point);
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        loop_.rpc().defer_commit();
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 1, "{:?}", report2.last_error);
        assert!(loop_
            .state()
            .submit
            .unwrap()
            .pending_occupies(&bounded.out_point));
        let tx_hash = loop_.rpc().submitted().last().unwrap().hash();
        loop_.rpc().simulate_drop(&tx_hash);
        assert!(loop_.rpc().has_cell(&bounded.out_point));

        let report3 = loop_.tick();
        assert_eq!(
            report3.submitted, 1,
            "dropped below-min refund must retry; {:?}",
            report3.last_error
        );
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .in_flight
                .contains(&cid_backend_core::out_point_key(&bounded.out_point)),
            "retry submit must re-occupy the restored Intention"
        );
    }

    #[test]
    fn below_min_filter_refunds_only_the_rejected_cell() {
        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let hash = script_hash32(&process_type_script(3));
        let rejected = intention_cell(1, hash, 200_0000_0000);
        let peer = intention_cell(2, hash, 200_0000_0000);
        rpc.insert_cell(rejected.clone());
        rpc.insert_cell(peer.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let ssri = ScriptedSsri::mimic((3, 3)).with_filter(|_| Ok(vec![5, 0]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 3);

        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{report:?}");
        assert!(!loop_.rpc().has_cell(&rejected.out_point));
        assert!(loop_.rpc().has_cell(&peer.out_point));
    }

    #[test]
    fn filter_refund_dry_run_reject_does_not_isolate_or_repeat_dry_run() {
        use cid_backend_core::{DEFAULT_FEE_RATE, MAX_FEE_RATE};

        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let hash = script_hash32(&process_type_script(3));
        let capacity = 200_0000_0000;
        let bounded = intention_cell(1, hash, capacity);
        let companion = intention_cell(2, hash, capacity);
        rpc.insert_cell(bounded.clone());
        rpc.insert_cell(companion.clone());
        rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = vec![config.scan_lock_identities()[0]];
        config.refund_policy.fee_rate = MAX_FEE_RATE;
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok([5, 0][..n].to_vec()));
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 3);

        assert_eq!(loop_.tick().submitted, 0);
        // Another actor spends the companion before the next scan.
        loop_.rpc().consume(&companion.out_point);
        loop_.config_mut().refund_policy.fee_rate = DEFAULT_FEE_RATE;
        loop_.rpc().inject_dry_run_reject_once();
        let dry_before = loop_.rpc().dry_run_attempts();
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 0, "{:?}", report2.last_error);
        assert!(loop_.rpc().has_cell(&bounded.out_point));
        assert_eq!(
            loop_.rpc().dry_run_attempts(),
            dry_before + 1,
            "first filter refund attempt should dry-run once"
        );
        loop_.workers().get(&hash).unwrap().with_state(|s| {
            assert!(
                s.isolate_expanded.is_empty(),
                "filter refund permanent reject must not isolate-split"
            );
            assert!(
                !s.submit.rejected_combos.is_empty(),
                "combo must be cached to avoid re-burn"
            );
        });

        // A repeated verdict for the same state does not repeat a rejected dry-run.
        let dry_cached = loop_.rpc().dry_run_attempts();
        for _ in 0..3 {
            let r = loop_.tick();
            assert_eq!(r.submitted, 0);
            assert_eq!(
                r.isolate_windows, 0,
                "cached filter refund must soft-skip before isolate_windows"
            );
            assert!(loop_.rpc().has_cell(&bounded.out_point));
        }
        assert_eq!(
            loop_.rpc().dry_run_attempts(),
            dry_cached,
            "cached filter refund must not dry-run again"
        );
        assert!(loop_
            .workers()
            .get(&hash)
            .unwrap()
            .with_state(|s| s.isolate_expanded.is_empty()));
    }
}
