#[cfg(test)]
mod tests {
    use cid_backend_core::digest_loop::LoopConfig;
    use cid_backend_core::rpc::{out_point_key, script_hash32, Rpc, TxStatus};
    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use std::time::Duration;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, loop_with_resolver, process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg(depth: u64) -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![crate::helpers::placeholder_code_dep()],
            type_id_code_deps: vec![],
            confirmation_depth: depth,
            extra_scan_locks: vec![],
            refund_policy: Default::default(),
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    #[test]
    fn confirmation_depth_holds_occupancy_until_depth_met() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(21);
        let hash = script_hash32(&ts);
        let proc = process_cell(21, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 21]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((2, 2)), cfg(3), 21);

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&tx_hash);
        assert!(matches!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Committed { block_number: 0 }
        ));

        for i in 0..2 {
            let report = loop_.tick();
            assert_eq!(report.submitted, 0, "tick {i}: depth 3 not met");
            assert_eq!(loop_.rpc().submitted().len(), 1);
            assert!(
                loop_
                    .state()
                    .submit
                    .as_ref()
                    .unwrap()
                    .pending_occupies(&proc.out_point),
                "Process occupancy must hold until confirmation depth"
            );
            assert!(loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .in_flight
                .contains(&out_point_key(&proc.out_point)));
            loop_.rpc().advance_tip(1);
        }

        let settled = loop_.tick();
        assert_eq!(
            settled.submitted, 1,
            "depth met: next digest_ok may proceed"
        );
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending
            .contains_key(tx_hash.as_slice()));
        let tx2_process = loop_.rpc().submitted()[1]
            .inputs()
            .get(loop_.rpc().submitted()[1].inputs().len() - 1)
            .unwrap()
            .previous_output();
        assert_eq!(
            tx2_process.tx_hash(),
            tx_hash,
            "after settlement, next digest_ok spends Process recreate"
        );
    }

    #[test]
    fn stale_tip_after_status_does_not_settle_committed_tx() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(22);
        let hash = script_hash32(&ts);
        let proc = process_cell(22, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 22]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((2, 2)), cfg(10), 22);

        let first = loop_.tick();
        assert_eq!(first.submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().set_tip(89);
        loop_.rpc().commit_pending(&tx_hash);
        assert!(matches!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Committed { block_number: 89 }
        ));
        loop_.rpc().set_tip(100);
        loop_.rpc().rewind_tip_on_next_status(90);

        let attempts_before = loop_.rpc().submit_attempts();
        let orig_key = tx_hash.as_slice().to_vec();
        let rewind_tick = loop_.tick();
        assert_eq!(
            rewind_tick.submitted, 0,
            "stale tip 100 vs rewind-to-90 must not release occupancy for another digest"
        );
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending
                .contains_key(&orig_key),
            "Committed(89) with tip 90 and depth 10 must stay pending"
        );
        assert!(
            loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending_occupies(&proc.out_point),
            "Process occupancy must be held (Unavailable-style) across a tip rewind"
        );
        assert_eq!(
            loop_.rpc().submit_attempts(),
            attempts_before,
            "no extra digest may be submitted on the rewind tick"
        );
        assert_eq!(loop_.rpc().tip(), 90);

        loop_.rpc().set_tip(98);
        let settled = loop_.tick();
        assert_eq!(
            settled.submitted, 1,
            "once confirmations reach depth, occupancy releases and leftover work submits"
        );
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending
            .contains_key(&orig_key));
        assert_eq!(loop_.rpc().submitted().len(), 2);
    }

    #[test]
    fn delete_drains_pending_without_starting_another_batch() {
        use crate::helpers::{fixture_process_entry, register_fixture};
        use cid_backend_core::digest_loop::{DigestLoop, NopClock, WorkerCommand, WorkerCommands};
        use cid_backend_core::workers::WorkerPool;
        use std::sync::Arc;

        let rpc = Arc::new(FakeRpc::new());
        let ssri = Arc::new(ScriptedSsri::mimic((1, 1)));
        let cmds = Arc::new(WorkerCommands::default());
        let workers = WorkerPool::new(rpc.clone(), ssri.clone());
        let hash = fixture_process_entry(21).identity_type_hash;
        rpc.insert_cell(process_cell(21, 400_0000_0000, vec![]));
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));
        let mut loop_ =
            DigestLoop::with_workers(rpc, NopClock, ssri, cfg(1), workers, Some(cmds.clone()));
        register_fixture(&mut loop_, 21);
        assert_eq!(loop_.tick().submitted, 1);
        cmds.enqueue(WorkerCommand::delete(hash));
        assert_eq!(
            loop_.tick().submitted,
            0,
            "deleting worker must stop scheduling new work"
        );
        assert_eq!(loop_.tick().submitted, 0);
        assert!(!loop_.workers().contains(&hash));
        assert_eq!(loop_.rpc().submitted().len(), 1);
    }
}
