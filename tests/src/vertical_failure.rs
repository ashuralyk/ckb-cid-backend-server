#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};
    use ckb_types::core::ScriptHashType;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            confirmation_depth: 1,
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(first.conflicts, 0);
        assert!(!loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(!loop_
            .state()
            .submit
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 1)),
            cfg(),
            ProcessResolver::new(),
        );

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
                .rejected
                .contains(&out_point_key(&bad.out_point)),
            "transaction reject must not blacklist an Intention outpoint"
        );
        assert!(
            !loop_
                .state()
                .submit
                .rejected
                .contains(&out_point_key(&proc.out_point)),
            "Process outpoint must not enter rejected"
        );
        assert!(!loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&bad.out_point)));
        assert!(!loop_.state().submit.pending_occupies(&bad.out_point));
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
        assert!(loop_.state().submit.rejected.is_empty());
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(),
            ProcessResolver::new(),
        );

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
                .rejected
                .contains(&out_point_key(&good.out_point)),
            "valid B must not be blacklisted"
        );
        assert!(!loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&bad.out_point)));
        assert!(!loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&proc.out_point)));
        assert!(!loop_.state().submit.pending_occupies(&bad.out_point));
        assert!(loop_.state().submit.pending_occupies(&good.out_point));
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0, "min=2 cannot isolate B from A");
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(
            loop_.state().submit.rejected.is_empty(),
            "min>1 reject must not write the window into rejected, got {:?}",
            loop_.state().submit.rejected
        );
        assert!(!loop_.state().submit.pending_occupies(&bad.out_point));
        assert!(!loop_.state().submit.pending_occupies(&good.out_point));
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
        assert!(loop_.state().submit.rejected.is_empty());
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 1)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_.state().submit.rejected.is_empty());
        assert!(!loop_.state().submit.pending_occupies(&bad.out_point));

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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 1)),
            cfg(),
            ProcessResolver::new(),
        );

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
            .rejected
            .contains(&out_point_key(&good.out_point)));
        for cell in &poison {
            assert!(!loop_.state().submit.pending_occupies(&cell.out_point));
            assert!(!loop_
                .state()
                .submit
                .rejected
                .contains(&out_point_key(&cell.out_point)));
        }
        assert!(loop_.state().submit.pending_occupies(&good.out_point));

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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(),
            ProcessResolver::new(),
        );

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
            .rejected
            .contains(&out_point_key(&good.out_point)));
        assert!(loop_.state().submit.pending_occupies(&good.out_point));
    }
}
