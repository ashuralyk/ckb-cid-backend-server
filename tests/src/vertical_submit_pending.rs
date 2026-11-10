#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32, Rpc, TxStatus};
    use ckb_types::core::ScriptHashType;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn pending_digest_ok_does_not_respend_process_on_later_ticks() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        let proc = process_cell(2, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 2]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
                confirmation_depth: 1,
            },
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        assert_eq!(first.conflicts, 0);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        assert_eq!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Pending
        );
        let live_proc = loop_
            .rpc()
            .get_live_process_by_type_hash(&hash)
            .unwrap()
            .expect("old Process still live while Pending");
        assert_eq!(live_proc.out_point, proc.out_point);
        assert!(
            loop_
                .state()
                .submit
                .in_flight
                .contains(&out_point_key(&proc.out_point)),
            "Process occupancy must survive Intention-only live_keys"
        );

        let second = loop_.tick();
        assert_eq!(second.submitted, 0, "must not assemble another digest_ok");
        assert_eq!(second.conflicts, 0);
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert_eq!(
            loop_
                .rpc()
                .get_live_process_by_type_hash(&hash)
                .unwrap()
                .unwrap()
                .out_point,
            proc.out_point
        );
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&proc.out_point)));

        loop_.rpc().commit_pending(&tx_hash);
        let third = loop_.tick();
        assert_eq!(third.submitted, 1);
        assert_eq!(third.conflicts, 0);
        assert_eq!(loop_.rpc().submitted().len(), 2);
        let tx2_process = loop_.rpc().submitted()[1]
            .inputs()
            .get(loop_.rpc().submitted()[1].inputs().len() - 1)
            .unwrap()
            .previous_output();
        assert_eq!(
            tx2_process.tx_hash(),
            tx_hash,
            "after commit, next digest_ok spends Process recreate"
        );
    }

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
    fn pending_occupancy_holds_across_multiple_ticks() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(6);
        let hash = script_hash32(&ts);
        let proc = process_cell(6, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 6]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        assert_eq!(first.conflicts, 0);
        for i in 0..3 {
            let report = loop_.tick();
            assert_eq!(report.submitted, 0, "tick {i}: no second digest_ok");
            assert_eq!(report.conflicts, 0, "tick {i}: no conflict");
            assert_eq!(loop_.rpc().submitted().len(), 1);
            assert!(
                loop_
                    .state()
                    .submit
                    .in_flight
                    .contains(&out_point_key(&proc.out_point)),
                "Process occupancy must hold across waiting ticks"
            );
            assert_eq!(
                loop_
                    .rpc()
                    .get_live_process_by_type_hash(&hash)
                    .unwrap()
                    .unwrap()
                    .out_point,
                proc.out_point
            );
        }
    }

    #[test]
    fn pending_occupancy_held_when_status_query_unavailable() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(7);
        let hash = script_hash32(&ts);
        let proc = process_cell(7, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 7]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        loop_.rpc().fail_status_queries();
        let second = loop_.tick();
        assert_eq!(second.submitted, 0);
        assert_eq!(second.conflicts, 0);
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert!(
            loop_
                .state()
                .submit
                .in_flight
                .contains(&out_point_key(&proc.out_point)),
            "Unavailable status must not release pending occupancy"
        );
        assert!(loop_.state().submit.pending_occupies(&proc.out_point));
    }

    #[test]
    fn defer_commit_then_drop_retries_next_tick() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(8);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(8, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 8]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        assert_eq!(first.conflicts, 0);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        assert_eq!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Pending
        );
        loop_.rpc().simulate_drop(&tx_hash);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == proc.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 1, "drop of pending must allow retry");
        assert_eq!(second.conflicts, 0);
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert_eq!(
            loop_.rpc().submitted()[1]
                .inputs()
                .get(0)
                .unwrap()
                .previous_output(),
            intent.out_point
        );
    }

    #[test]
    fn pending_digest_ok_still_allows_incompatible_refund() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(9);
        let hash = script_hash32(&ts);
        let digest_intent = intention_cell(1, hash, 100_0000_0000);
        let refund_intent = intention_cell(2, hash, 110_0000_0000);
        let proc = process_cell(9, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 9]);
        rpc.insert_cell(digest_intent.clone());
        rpc.insert_cell(refund_intent.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::ok_then_incompatible((1, 1), 1, 21),
            cfg(),
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        assert_eq!(first.conflicts, 0);
        let digest_tx = &loop_.rpc().submitted()[0];
        assert!(digest_tx
            .inputs()
            .into_iter()
            .any(|i| i.previous_output() == proc.out_point));
        assert!(loop_.state().submit.pending_occupies(&proc.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 1, "refund must still submit");
        assert_eq!(second.conflicts, 0);
        assert_eq!(loop_.rpc().submitted().len(), 2);
        let refund_tx = &loop_.rpc().submitted()[1];
        assert_eq!(refund_tx.inputs().len(), 1, "refund does not spend Process");
        assert_eq!(
            refund_tx.inputs().get(0).unwrap().previous_output(),
            refund_intent.out_point
        );
        assert_eq!(
            loop_
                .rpc()
                .get_live_process_by_type_hash(&hash)
                .unwrap()
                .unwrap()
                .out_point,
            proc.out_point,
            "Process stays live under pending digest_ok"
        );
        assert!(loop_.state().submit.pending_occupies(&proc.out_point));
    }
}
