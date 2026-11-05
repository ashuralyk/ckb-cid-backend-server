#[cfg(test)]
mod tests {
    use ckb_types::core::ScriptHashType;
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{script_hash32, Rpc};

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{live_cell, lock_script, out_point};

    fn process_type(args_fill: u8) -> ckb_types::packed::Script {
        lock_script([0x51; 32], ScriptHashType::Type, vec![args_fill; 32])
    }

    #[test]
    fn fakerpc_resolve_skips_missing_process() {
        let rpc = FakeRpc::new();
        let mut resolver = ProcessResolver::new();
        assert!(resolver
            .resolve(&rpc, &[0xAB; 32])
            .unwrap()
            .is_none());
    }

    #[test]
    fn fakerpc_resolve_reads_live_data() {
        let rpc = FakeRpc::new();
        let ts = process_type(0x01);
        let hash = script_hash32(&ts);
        rpc.insert_cell(live_cell(
            out_point(0x71, 0),
            lock_script([0u8; 32], ScriptHashType::Type, vec![]),
            Some(ts),
            500_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 0x99],
        ));
        let mut resolver = ProcessResolver::new();
        let got = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(got.binary.last().copied(), Some(0x99));
    }
}
