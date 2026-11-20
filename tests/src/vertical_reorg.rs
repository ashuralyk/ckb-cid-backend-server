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
    fn reorg_of_confirming_tx_releases_occupancy_and_retries() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(22);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(22, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 22]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(3), 22);

        assert_eq!(loop_.tick().submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&tx_hash);
        loop_.rpc().advance_tip(1);
        let confirming = loop_.tick();
        assert_eq!(confirming.submitted, 0);
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&proc.out_point));
        assert_eq!(loop_.rpc().submitted().len(), 1);

        loop_.rpc().simulate_drop(&tx_hash);
        assert!(matches!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Dropped
        ));
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

        let retried = loop_.tick();
        assert_eq!(retried.submitted, 1, "reorg of confirming tx must retry");
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .in_flight
            .contains(&out_point_key(&proc.out_point)));
    }

    #[test]
    fn reorg_of_committed_tx_after_settlement_retries() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(23);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(23, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 23]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(1), 23);

        assert_eq!(loop_.tick().submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        let settled = loop_.tick();
        assert_eq!(settled.submitted, 0);
        assert!(
            !loop_
                .state()
                .submit
                .as_ref()
                .unwrap()
                .pending
                .contains_key(tx_hash.as_slice()),
            "depth 1 settles on first committed sight at tip"
        );

        loop_.rpc().simulate_drop(&tx_hash);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));

        let retried = loop_.tick();
        assert_eq!(
            retried.submitted, 1,
            "deep reorg after settle must rescan-retry"
        );
        assert_eq!(loop_.rpc().submitted().len(), 2);
    }

    #[test]
    fn tip_rewind_delays_settlement_without_double_spend() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        rpc.set_tip(10);
        let ts = process_type_script(24);
        let hash = script_hash32(&ts);
        let proc = process_cell(24, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 24]);
        rpc.insert_cell(proc.clone());
        for i in 1..=4 {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((2, 2)), cfg(4), 24);

        assert_eq!(loop_.tick().submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&tx_hash);
        loop_.rpc().advance_tip(2);
        assert_eq!(loop_.rpc().tip(), 12);
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&proc.out_point));

        loop_.rpc().set_tip(10);
        let rewound = loop_.tick();
        assert_eq!(rewound.submitted, 0, "tip rewind must not settle");
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert!(loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .pending_occupies(&proc.out_point));
        assert_eq!(loop_.state().last_tip, Some(10));

        loop_.rpc().advance_tip(3);
        let settled = loop_.tick();
        assert_eq!(settled.submitted, 1);
        assert_eq!(loop_.rpc().submitted().len(), 2);
    }

    #[test]
    fn simulate_reorg_drops_txs_above_new_tip() {
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        rpc.set_tip(5);
        let ts = process_type_script(25);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(25, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 25]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), cfg(4), 25);

        assert_eq!(loop_.tick().submitted, 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&tx_hash);
        loop_.rpc().advance_tip(1);
        assert_eq!(loop_.tick().submitted, 0);

        loop_.rpc().simulate_reorg(4);
        assert_eq!(loop_.rpc().tip(), 4);
        assert!(matches!(
            loop_.rpc().get_transaction_status(&tx_hash).unwrap(),
            TxStatus::Dropped
        ));
        let retried = loop_.tick();
        assert_eq!(retried.submitted, 1);
        assert_eq!(loop_.rpc().submitted().len(), 2);
    }
}
