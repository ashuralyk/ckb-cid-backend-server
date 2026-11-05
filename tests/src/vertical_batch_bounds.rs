#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::script_hash32;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn setup(n_intentions: u8) -> (FakeRpc, [u8; 32]) {
        let rpc = FakeRpc::new();
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(2, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 2]));
        for i in 1..=n_intentions {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        (rpc, hash)
    }

    fn cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
        }
    }

    #[test]
    fn below_min_does_not_submit() {
        let (rpc, _) = setup(1);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.rpc().submitted().len(), 0);
        assert_eq!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .filter(|c| c.lock().code_hash().as_slice() == CID_CODE)
                .count(),
            1
        );
    }

    #[test]
    fn at_max_submits_exactly_max() {
        let (rpc, _) = setup(2);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3, "2 intentions + 1 process");
    }

    #[test]
    fn above_max_takes_first_max_asc_leaves_remainder_below_min() {
        let (rpc, _) = setup(3);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3, "first max=2 intentions + process");
        let remaining_intentions = loop_
            .rpc()
            .live_cells()
            .iter()
            .filter(|c| c.lock().code_hash().as_slice() == CID_CODE)
            .count();
        assert_eq!(remaining_intentions, 1);
        let first_intent_fill = tx.inputs().get(0).unwrap().previous_output();
        assert_eq!(
            first_intent_fill.tx_hash().as_slice(),
            &[1u8; 32],
            "asc bucket head"
        );
    }
}
