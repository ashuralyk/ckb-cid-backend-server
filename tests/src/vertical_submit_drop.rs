#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn submit_then_drop_releases_in_flight_and_retries() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(5);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(
            5,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 5],
        ));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
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
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().simulate_drop(&tx_hash);
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == intent.out_point),
            "dropped tx restores the Intention cell"
        );

        let second = loop_.tick();
        assert_eq!(second.submitted, 1, "dropped pending must be retryable");
        assert_eq!(second.conflicts, 0);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .all(|c| c.out_point != intent.out_point));
        assert_eq!(loop_.rpc().submitted().len(), 2);
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
    }
}
