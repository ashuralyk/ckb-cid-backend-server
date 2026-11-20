#[cfg(test)]
mod tests {
    use cid_backend_core::digest_loop::LoopConfig;
    use cid_backend_core::rpc::{out_point_key, script_hash32, LiveCell};
    use ckb_types::core::ScriptHashType;
    use std::time::Duration;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        cid_lock_args, intention_cell, live_cell, lock_script, loop_with_resolver, out_point,
        process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![crate::helpers::placeholder_code_dep()],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            extra_scan_locks: vec![],
            refund_policy: Default::default(),
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    #[test]
    fn transient_unavailable_submit_retries_next_tick() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(26);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(26, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 26]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        rpc.inject_unavailable_submit_once();
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(), 26);

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(first.conflicts, 0);
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 1, "transient Unavailable must retry");
        assert_eq!(loop_.rpc().submitted().len(), 1);
    }

    #[test]
    fn dry_run_reject_prevents_send_transaction() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(28);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(28, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 28]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        rpc.inject_dry_run_reject_once();
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(), 28);

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));

        let second = loop_.tick();
        assert_eq!(
            second.submitted, 0,
            "combo blacklisted after dry-run reject"
        );
        assert_eq!(loop_.rpc().submit_attempts(), 0);
    }

    #[test]
    fn permanent_reject_does_not_retry_or_leak_occupancy() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(27);
        let hash = script_hash32(&ts);
        let bad = intention_cell(1, hash, 100_0000_0000);
        let good = intention_cell(2, hash, 110_0000_0000);
        let proc = process_cell(27, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 27]);
        rpc.insert_cell(bad.clone());
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        rpc.inject_reject_once();
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 1)), cfg(), 27);

        let first = loop_.tick();
        assert_eq!(
            first.submitted, 1,
            "later Intention in the bucket still submits"
        );
        assert_eq!(first.conflicts, 0);
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .rejected
                .contains(&out_point_key(&bad.out_point)),
            "transaction reject must not blacklist an Intention outpoint"
        );
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .rejected
                .contains(&out_point_key(&proc.out_point)),
            "Process outpoint must not enter rejected"
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .in_flight
            .contains(&out_point_key(&bad.out_point)));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&bad.out_point));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == bad.out_point));
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            good.out_point
        );

        let attempts_after_first = loop_.rpc().submit_attempts();
        let second = loop_.tick();
        assert_eq!(
            second.submitted, 1,
            "Process recreate is a new combo; singleton reject of {{bad, P1}} must not blacklist bad"
        );
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert!(
            loop_.rpc().submit_attempts() > attempts_after_first,
            "new Process outpoint may retry leftover Intention"
        );
        assert!(loop_.state().submit.as_ref().unwrap().rejected.is_empty());
    }

    #[test]
    fn rejected_multi_intention_batch_isolates_valid_sibling() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(28);
        let hash = script_hash32(&ts);
        let bad = intention_cell(1, hash, 100_0000_0000);
        let good = intention_cell(2, hash, 110_0000_0000);
        let proc = process_cell(28, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 28]);
        rpc.insert_cell(bad.clone());
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        rpc.reject_when_consuming(&bad.out_point);
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(), 28);

        let first = loop_.tick();
        assert_eq!(
            first.submitted, 1,
            "B must submit after [A,B] and [A] are rejected"
        );
        assert!(
            loop_.rpc().submit_attempts() <= 4,
            "isolate must be bounded within a tick, got {}",
            loop_.rpc().submit_attempts()
        );
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            good.out_point
        );
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .rejected
                .contains(&out_point_key(&good.out_point)),
            "valid B must not be blacklisted"
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&bad.out_point)));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&proc.out_point)));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&bad.out_point));
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));
    }

    /// Async Rejected must queue sync drop_first+shrink frames (isolate_pending),
    /// not rely on `already`. min=1, [A,B,C], C poison → shrink [A,B] submits.
    #[test]
    fn async_rejected_min1_shrinks_abc_to_ab() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(36);
        let hash = script_hash32(&ts);
        let a = intention_cell(1, hash, 100_0000_0000);
        let b = intention_cell(2, hash, 110_0000_0000);
        let c = intention_cell(3, hash, 120_0000_0000);
        let proc = process_cell(36, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 36]);
        rpc.insert_cell(a.clone());
        rpc.insert_cell(b.clone());
        rpc.insert_cell(c.clone());
        rpc.insert_cell(proc.clone());
        rpc.reject_when_consuming(&c.out_point);
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 3)), cfg(), 36);

        let first = loop_.tick();
        assert_eq!(
            first.submitted, 1,
            "after [A,B,C] reject, shrink [A,B] must submit via isolate_pending"
        );
        let submitted = &loop_.rpc().submitted()[0];
        let inputs: Vec<_> = submitted
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert!(inputs.contains(&a.out_point), "submitted tx must spend A");
        assert!(inputs.contains(&b.out_point), "submitted tx must spend B");
        assert!(
            !inputs.contains(&c.out_point),
            "poison C must not be in the successful tx"
        );
        assert!(
            loop_.rpc().submit_attempts() >= 2,
            "must have attempted the rejected [A,B,C] window plus shrink"
        );
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&a.out_point));
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&b.out_point));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&c.out_point));
    }

    /// Stronger P0 case: min=2, max=3, [A,B,C,D], C poison. Sync rejects [A,B,C]
    /// (rest=[D]) and pushes shrink [A,B] + drop_first [B,C,D]. Async that only
    /// marks expanded then `already` with rest=[D] would push only [D] (<min).
    #[test]
    fn async_rejected_min2_abcd_shrinks_to_ab() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(37);
        let hash = script_hash32(&ts);
        let a = intention_cell(1, hash, 100_0000_0000);
        let b = intention_cell(2, hash, 110_0000_0000);
        let c = intention_cell(3, hash, 120_0000_0000);
        let d = intention_cell(4, hash, 130_0000_0000);
        let proc = process_cell(37, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 37]);
        rpc.insert_cell(a.clone());
        rpc.insert_cell(b.clone());
        rpc.insert_cell(c.clone());
        rpc.insert_cell(d.clone());
        rpc.insert_cell(proc.clone());
        rpc.reject_when_consuming(&c.out_point);
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((2, 3)), cfg(), 37);

        let first = loop_.tick();
        assert_eq!(
            first.submitted, 1,
            "after [A,B,C] reject (rest D), shrink [A,B] must submit (not stall on D)"
        );
        let submitted = &loop_.rpc().submitted()[0];
        let inputs: Vec<_> = submitted
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert!(inputs.contains(&a.out_point), "submitted tx must spend A");
        assert!(inputs.contains(&b.out_point), "submitted tx must spend B");
        assert!(
            !inputs.contains(&c.out_point),
            "poison C must not be in the successful tx"
        );
        assert!(
            !inputs.contains(&d.out_point),
            "D is rest, not part of shrink [A,B]"
        );
        assert!(
            loop_.rpc().submit_attempts() >= 2,
            "must attempt rejected [A,B,C] plus shrink [A,B]"
        );
    }

    #[test]
    fn rejected_min_gt_1_window_does_not_blacklist_members() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(29);
        let hash = script_hash32(&ts);
        let bad = intention_cell(1, hash, 100_0000_0000);
        let good = intention_cell(2, hash, 110_0000_0000);
        let proc = process_cell(29, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 29]);
        rpc.insert_cell(bad.clone());
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        rpc.reject_when_consuming(&bad.out_point);
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((2, 2)), cfg(), 29);

        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "min=2 cannot isolate B from A");
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(
            loop_.state().submit.as_ref().unwrap().rejected.is_empty(),
            "min>1 reject must not write the window into rejected, got {:?}",
            loop_.state().submit.as_ref().unwrap().rejected
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&bad.out_point));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == good.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 0);
        assert_eq!(
            loop_.rpc().submit_attempts(),
            1,
            "same min>1 combo must not resubmit unboundedly"
        );
        assert!(loop_.state().submit.as_ref().unwrap().rejected.is_empty());
    }

    #[test]
    fn always_failing_combo_is_bounded_across_ticks() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(30);
        let hash = script_hash32(&ts);
        let bad = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(30, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 30]);
        rpc.insert_cell(bad.clone());
        rpc.insert_cell(proc.clone());
        rpc.reject_when_consuming(&bad.out_point);
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 1)), cfg(), 30);

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_.state().submit.as_ref().unwrap().rejected.is_empty());
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&bad.out_point));

        let second = loop_.tick();
        let third = loop_.tick();
        assert_eq!(second.submitted, 0);
        assert_eq!(third.submitted, 0);
        assert_eq!(
            loop_.rpc().submit_attempts(),
            1,
            "always-failing combo is tried once, then skipped"
        );
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == bad.out_point));
    }

    #[test]
    fn cached_reject_prefix_does_not_starve_later_valid_intention() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(32);
        let hash = script_hash32(&ts);
        const POISON: u8 = 64;
        let mut poison = Vec::new();
        for fill in 1..=POISON {
            let cell = intention_cell(fill, hash, 100_0000_0000);
            rpc.reject_when_consuming(&cell.out_point);
            rpc.insert_cell(cell.clone());
            poison.push(cell);
        }
        let good = intention_cell(POISON + 1, hash, 110_0000_0000);
        let proc = process_cell(32, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 32]);
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        // Hold occupancy on the same Process so a recreate cannot mint a new
        // combo and retry the poison prefix (that path is covered elsewhere).
        rpc.defer_commit();
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 1)), cfg(), 32);

        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "budget stops at 64 poison combos");
        assert_eq!(loop_.rpc().submit_attempts(), 64);

        let second = loop_.tick();
        assert_eq!(
            second.submitted, 1,
            "cached poison must not consume the next tick's new-attempt budget"
        );
        assert_eq!(loop_.rpc().submit_attempts(), 65);
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            good.out_point
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&good.out_point)));
        for cell in &poison {
            assert!(!loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending_occupies(&cell.out_point));
            assert!(!loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .rejected
                .contains(&out_point_key(&cell.out_point)));
        }
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));

        let attempts = loop_.rpc().submit_attempts();
        let third = loop_.tick();
        assert_eq!(third.submitted, 0);
        assert_eq!(
            loop_.rpc().submit_attempts(),
            attempts,
            "cached combos must not be resent"
        );
    }

    #[test]
    fn cached_reject_prefix_multi_bounds_reaches_valid_intention() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(33);
        let hash = script_hash32(&ts);
        const POISON: u8 = 64;
        for fill in 1..=POISON {
            let cell = intention_cell(fill, hash, 100_0000_0000);
            rpc.reject_when_consuming(&cell.out_point);
            rpc.insert_cell(cell);
        }
        let good = intention_cell(POISON + 1, hash, 110_0000_0000);
        let proc = process_cell(33, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 33]);
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc);
        rpc.defer_commit();
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(), 33);

        let mut saw_good = false;
        for _ in 0..16 {
            let _ = loop_.tick();
            if loop_
                .rpc()
                .submitted()
                .iter()
                .any(|tx| tx.inputs().get(0).unwrap().previous_output() == good.out_point)
            {
                saw_good = true;
                break;
            }
        }
        assert!(
            saw_good,
            "valid B after a long (1,4) poison prefix must submit"
        );
        let attempts = loop_.rpc().submit_attempts();
        assert!(
            attempts > 64,
            "isolation should try poison windows before B, got {attempts}"
        );
        let _ = loop_.tick();
        let _ = loop_.tick();
        assert_eq!(
            loop_.rpc().submit_attempts(),
            attempts,
            "cached split-tree combos must not be resent"
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&good.out_point)));
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));
    }

    fn unbuildable_intention(fill: u8, process_type_hash: [u8; 32]) -> LiveCell {
        live_cell(
            out_point(fill, 0),
            lock_script(
                CID_CODE,
                ScriptHashType::Type,
                cid_lock_args(process_type_hash),
            ),
            Some(lock_script([0xFFu8; 32], ScriptHashType::Type, vec![])),
            100_0000_0000,
            vec![1, 2, 3],
        )
    }

    #[test]
    fn unbuildable_refund_prefix_does_not_starve_later_refundable_intention() {
        let rpc = FakeRpc::new();
        rpc.insert_cell(crate::helpers::code_cell(
            0xA0,
            crate::helpers::PROC_CODE_ARGS,
            crate::helpers::FIXTURE_ELF.to_vec(),
        ));
        let ts = process_type_script(34);
        let hash = script_hash32(&ts);
        const POISON: u8 = 16;
        let mut poison = Vec::new();
        for fill in 1..=POISON {
            let cell = unbuildable_intention(fill, hash);
            rpc.insert_cell(cell.clone());
            poison.push(cell);
        }
        let good = intention_cell(POISON + 1, hash, 200_0000_0000);
        let proc = process_cell(34, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 34]);
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((1, 1)).with_filter(|n| Ok(vec![6; n])),
            cfg(),
            34,
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "budget stops at 16 unbuildable refunds");
        assert_eq!(
            loop_.rpc().submit_attempts(),
            0,
            "assemble skip must not send"
        );

        let second = loop_.tick();
        assert_eq!(
            second.submitted, 1,
            "later refundable B must be reached after an unbuildable prefix"
        );
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            good.out_point
        );
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending_occupies(&good.out_point),
            "refund occupies B until the next tick settles"
        );
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending_occupies(&proc.out_point),
            "User Lock refund must not occupy Process"
        );
        for cell in &poison {
            assert!(!loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending_occupies(&cell.out_point));
            assert!(!loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .rejected
                .contains(&out_point_key(&cell.out_point)));
        }

        let attempts = loop_.rpc().submit_attempts();
        let third = loop_.tick();
        let fourth = loop_.tick();
        assert_eq!(third.submitted, 0);
        assert_eq!(fourth.submitted, 0);
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));
        assert_eq!(
            loop_.rpc().submit_attempts(),
            attempts,
            "unbuildable skips must not send on later ticks"
        );
    }

    /// Unique contiguous windows are O(n²). Without a visited-set the isolate
    /// tree revisits diamonds exponentially (`2^n - 1` for min=1, max=n).
    fn isolate_expansion_bound(n: usize) -> usize {
        n * n + 256 + 64
    }

    fn all_rejected_max_window_other_process_progresses(n: u8, poison_unique: u8, good_unique: u8) {
        let rpc = FakeRpc::new();
        let poison_ts = process_type_script(poison_unique);
        let poison_hash = script_hash32(&poison_ts);
        for fill in 1..=n {
            let cell = intention_cell(fill, poison_hash, 100_0000_0000);
            rpc.reject_when_consuming(&cell.out_point);
            rpc.insert_cell(cell);
        }
        let later = intention_cell(n.saturating_add(1), poison_hash, 110_0000_0000);
        rpc.insert_cell(later.clone());
        rpc.insert_cell(process_cell(
            poison_unique,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', poison_unique],
        ));

        let good_ts = process_type_script(good_unique);
        let good_hash = script_hash32(&good_ts);
        let good = intention_cell(n.saturating_add(2), good_hash, 110_0000_0000);
        let good_proc = process_cell(
            good_unique,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', good_unique],
        );
        rpc.insert_cell(good.clone());
        rpc.insert_cell(good_proc.clone());
        rpc.defer_commit();

        let mut loop_ =
            crate::helpers::digest_loop_empty(rpc, ScriptedSsri::mimic((1, n as u32)), cfg());
        crate::helpers::register_fixture(&mut loop_, poison_unique);
        crate::helpers::register_fixture(&mut loop_, good_unique);

        let bound = isolate_expansion_bound(n as usize);
        let mut saw_other = false;
        let mut saw_later = false;
        for tick in 0..8 {
            let before = loop_.rpc().submit_attempts();
            let report = loop_.tick();
            assert!(
                report.isolate_windows <= bound,
                "tick {tick}: isolate expansions {} exceeded polynomial/budget bound {bound} (n={n})",
                report.isolate_windows
            );
            let sent = loop_.rpc().submit_attempts() - before;
            assert!(
                sent <= 66,
                "tick {tick}: poison send cap is 64; +2 are later/other valid Intentions, sent {sent}"
            );
            for tx in loop_.rpc().submitted() {
                let first = tx.inputs().get(0).unwrap().previous_output();
                if first == good.out_point {
                    saw_other = true;
                }
                if first == later.out_point {
                    saw_later = true;
                }
            }
            if saw_other && saw_later {
                break;
            }
        }
        assert!(
            saw_other,
            "valid Intention on another Process must still progress under max={n} all-rejected live cells"
        );
        assert!(
            saw_later,
            "later valid Intention in the poison bucket must still progress under max={n}"
        );
        let good_worker = loop_.workers().get(&good_hash).expect("good worker");
        assert!(good_worker.with_state(|s| s.submit.pending_occupies(&good.out_point)));
        let poison_worker = loop_.workers().get(&poison_hash).expect("poison worker");
        assert!(poison_worker.with_state(|s| s.submit.pending_occupies(&later.out_point)));
        assert!(!good_worker
            .with_state(|s| { s.submit.rejected.contains(&out_point_key(&good.out_point)) }));
        assert!(!poison_worker
            .with_state(|s| { s.submit.rejected.contains(&out_point_key(&later.out_point)) }));
    }

    #[test]
    fn cached_reject_max16_expansions_are_bounded_and_other_process_progresses() {
        all_rejected_max_window_other_process_progresses(16, 36, 37);
    }

    #[test]
    fn cached_reject_max20_expansions_are_bounded_and_other_process_progresses() {
        all_rejected_max_window_other_process_progresses(20, 38, 39);
    }

    #[test]
    #[ignore = "guest-assembled passthrough no longer applies backend capacity shortfall during isolate"]
    fn capacity_shortfall_prefix_does_not_starve_later_valid_intention() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(35);
        let hash = script_hash32(&ts);
        const POISON: u8 = 64;
        for fill in 1..=POISON {
            rpc.insert_cell(intention_cell(fill, hash, 10_0000_0000));
        }
        let good = intention_cell(POISON + 1, hash, 500_0000_0000);
        let proc = process_cell(35, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 35]);
        rpc.insert_cell(good.clone());
        rpc.insert_cell(proc.clone());
        rpc.defer_commit();
        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::fixed_beneficiary((1, 1), 400_0000_0000),
            cfg(),
            35,
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "budget stops at 64 capacity shortfalls");
        assert_eq!(loop_.rpc().submit_attempts(), 0);

        let second = loop_.tick();
        assert_eq!(
            second.submitted, 1,
            "later valid B must be reached after a capacity-shortfall prefix"
        );
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            good.out_point
        );
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&good.out_point));
        let attempts = loop_.rpc().submit_attempts();
        let _ = loop_.tick();
        let _ = loop_.tick();
        assert_eq!(
            loop_.rpc().submit_attempts(),
            attempts,
            "capacity-shortfall skips must not send on later ticks"
        );
    }
}
