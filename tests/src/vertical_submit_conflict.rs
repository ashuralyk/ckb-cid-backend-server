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

    #[test]
    fn conflict_then_retry_next_tick_succeeds() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(4);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(
            4,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 4],
        ));
        rpc.inject_conflict_once();

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
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
        assert_eq!(first.submitted, 0);
        assert_eq!(first.conflicts, 1);
        assert!(loop_
            .state()
            .submit
            .failed
            .contains(&out_point_key(&intent.out_point)));
        assert!(!loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 1);
        assert_eq!(second.conflicts, 0);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .all(|c| c.out_point != intent.out_point));
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
    }
}
