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
}
