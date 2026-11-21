#[cfg(test)]
mod tests {
    use cid_backend_core::rpc::script_hash32;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, intention_cell, loop_with_resolver, process_cell, process_type_script,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn initial_exit_21_leaves_the_intention_live() {
        let rpc = FakeRpc::new();
        let hash = script_hash32(&process_type_script(3));
        let intent = intention_cell(1, hash, 200_0000_0000);
        let proc = process_cell(3, 200_0000_0000, vec![]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::incompatible(21),
            default_loop_config(),
            3,
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report.last_error.unwrap().contains("exit 21"));
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        assert!(loop_.rpc().has_cell(&intent.out_point));
        assert!(loop_.rpc().has_cell(&proc.out_point));
    }

    #[test]
    fn assemble_host_and_state_errors_do_not_refund() {
        let mut refunded = Vec::new();
        for code in [36, 37] {
            let rpc = FakeRpc::new();
            let hash = script_hash32(&process_type_script(3));
            rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
            rpc.insert_cell(process_cell(3, 200_0000_0000, vec![]));
            let mut loop_ = loop_with_resolver(
                rpc,
                ScriptedSsri::incompatible(code),
                crate::helpers::default_loop_config(),
                3,
            );
            let _ = loop_.tick();
            if !loop_.rpc().submitted().is_empty() {
                refunded.push(code);
            }
        }
        assert!(
            refunded.is_empty(),
            "guest exits triggered refunds: {refunded:?}"
        );
    }
}
