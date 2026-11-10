#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32, Rpc, TxStatus};
    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg(depth: u64) -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            confirmation_depth: depth,
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
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(3),
            ProcessResolver::new(),
        );

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
                loop_.state().submit.pending_occupies(&proc.out_point),
                "Process occupancy must hold until confirmation depth"
            );
            assert!(loop_
                .state()
                .submit
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
}
