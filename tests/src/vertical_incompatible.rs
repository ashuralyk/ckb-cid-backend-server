#[cfg(test)]
mod tests {
    use cid_backend_core::digest_loop::LoopConfig;
    use cid_backend_core::rpc::script_hash32;
    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use std::time::Duration;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, loop_with_resolver, process_cell, process_type_script, user_lock, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn incompatible_refunds_user_lock_without_process_pair() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::incompatible(21),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![crate::helpers::placeholder_code_dep()],
                type_id_code_deps: vec![],
                confirmation_depth: 1,
                max_assembled_tx_bytes: 512 * 1024,
            },
            3,
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);

        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(
            tx.inputs().get(0).unwrap().previous_output(),
            intent.out_point
        );
        assert_eq!(tx.outputs().len(), 1);
        let out = tx.outputs().get(0).unwrap();
        let cap: u64 = out.capacity().unpack();
        assert_eq!(cap, 100_0000_0000);
        let code: [u8; 32] = out.lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(out.type_().to_opt().is_none());
        assert!(tx.header_deps().is_empty());

        let live = loop_.rpc().live_cells();
        assert!(
            live.iter().any(|c| c.out_point == proc.out_point),
            "process cell must remain live"
        );
        assert!(live.iter().all(|c| c.out_point != intent.out_point));
        assert!(live.iter().any(|c| {
            let code: [u8; 32] = c.lock().code_hash().unpack();
            code == user_lock().code_hash && c.capacity() == 100_0000_0000
        }));
    }
}
