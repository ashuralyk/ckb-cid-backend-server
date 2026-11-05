#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};

    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;
    use crate::fake_rpc::FakeRpc;

    #[test]
    fn tick_n1_digest_ok_shape_accepted_by_fakerpc() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(1, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 1]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());

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
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        assert_eq!(report.conflicts, 0);

        let txs = loop_.rpc().submitted();
        assert_eq!(txs.len(), 1);
        let tx = &txs[0];
        assert_eq!(tx.inputs().len(), 2, "intention + process; no fee input");
        let ops: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert_eq!(ops[0], intent.out_point);
        assert_eq!(ops[1], proc.out_point);
        assert_eq!(tx.outputs().len(), 2, "beneficiary + process recreate");
        let proc_out = tx.outputs().get(1).unwrap();
        assert_eq!(
            proc_out.type_().to_opt().map(|t| script_hash32(&t)),
            Some(hash)
        );
        assert!(tx.header_deps().is_empty(), "must not build timeout_burn");
        assert_eq!(
            ops.len(),
            2,
            "User Lock is not an input; only intention + process outpoints"
        );
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .all(|c| c.out_point != intent.out_point));
    }

    #[test]
    fn tick_unusable_ssri_skips_without_refund_or_submit() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(1, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 1]));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::unusable(),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == intent.out_point),
            "Unusable must skip, not refund/destroy"
        );
    }
}
