//! Fee handling moved to guest-assembled txs; backend only passthrough-validates.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::LoopConfig;
    use cid_backend_core::rpc::script_hash32;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, intention_cell, loop_with_resolver, process_cell, process_type_script,
        user_lock,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn incompatible_refund_keeps_exact_capacity_no_process_fee() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::incompatible(21),
            default_loop_config(),
            3,
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.outputs().len(), 1);
        let cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
        assert_eq!(cap, intent_cap, "exact refund only");
        let code: [u8; 32] = tx.outputs().get(0).unwrap().lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(tx.header_deps().is_empty());
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == proc_op),
            "Process stays live on incompatible refund"
        );
    }

    #[test]
    fn digest_ok_fee_guard_is_none_passthrough() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(1, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 1]));

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((1, 4)),
            default_loop_config(),
            1,
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        let caps: Vec<u64> = tx
            .outputs()
            .into_iter()
            .map(|o| o.capacity().unpack())
            .collect();
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[1], 100_0000_0000, "beneficiary capacity preserved");
        assert_eq!(caps[0], 200_0000_0000, "process recreate unchanged (guest owns fee)");
    }

    #[test]
    #[ignore = "legacy backend fee deduction removed; guest assembles complete tx"]
    fn legacy_backend_fee_tests_removed() {}
}