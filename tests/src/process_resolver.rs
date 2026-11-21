#[cfg(test)]
mod tests {
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::type_id::TYPE_ID_CODE_HASH;
    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        fixture_process_entry, live_cell, lock_script, out_point, process_cell, process_type_hash,
        FIXTURE_ELF,
    };

    #[test]
    fn fakerpc_resolve_skips_missing_process() {
        let rpc = FakeRpc::new();
        let entry = fixture_process_entry(0x01);
        let mut resolver = ProcessResolver::for_entry(entry);
        assert!(resolver
            .resolve(
                &rpc,
                &[0xAB; 32],
                cid_backend_core::CidLockIdentity::new(
                    crate::helpers::CID_CODE,
                    ckb_types::core::ScriptHashType::Type
                )
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn fakerpc_resolve_reads_live_data_via_fallback() {
        let rpc = FakeRpc::new();
        let unique = 0x01u8;
        let hash = process_type_hash(unique);
        let proc = process_cell(unique, 500_0000_0000, vec![]);
        assert_eq!(
            proc.type_script().map(|t| {
                let code: [u8; 32] = t.code_hash().unpack();
                code
            }),
            Some(TYPE_ID_CODE_HASH)
        );
        rpc.insert_cell(proc);
        let entry = fixture_process_entry(unique);
        let mut resolver =
            ProcessResolver::for_entry(entry).with_fallback_binary(FIXTURE_ELF.to_vec());
        let got = resolver
            .resolve(
                &rpc,
                &hash,
                cid_backend_core::CidLockIdentity::new(
                    crate::helpers::CID_CODE,
                    ckb_types::core::ScriptHashType::Type,
                ),
            )
            .unwrap()
            .expect("resolved");
        assert_eq!(got.binary, FIXTURE_ELF);
        assert_eq!(got.type_hash, hash);
    }

    #[test]
    fn fakerpc_resolve_wrong_lock_rejected() {
        let rpc = FakeRpc::new();
        let unique = 0x02u8;
        let hash = process_type_hash(unique);
        rpc.insert_cell(live_cell(
            out_point(0x71, 0),
            lock_script([0u8; 32], ScriptHashType::Type, vec![]),
            Some(crate::helpers::process_type_script(unique)),
            500_0000_0000,
            vec![],
        ));
        let entry = fixture_process_entry(unique);
        let mut resolver =
            ProcessResolver::for_entry(entry).with_fallback_binary(FIXTURE_ELF.to_vec());
        assert!(resolver
            .resolve(
                &rpc,
                &hash,
                cid_backend_core::CidLockIdentity::new(
                    crate::helpers::CID_CODE,
                    ckb_types::core::ScriptHashType::Type
                )
            )
            .unwrap()
            .is_none());
    }
}
