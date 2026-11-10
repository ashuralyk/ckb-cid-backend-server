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
        assert!(loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&bad.out_point)));
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

        let second = loop_.tick();
        assert_eq!(
            second.submitted, 0,
            "rejected Intention must not retry forever"
        );
        assert_eq!(loop_.rpc().submitted().len(), 1);
        assert!(loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&bad.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == bad.out_point));
    }
}
