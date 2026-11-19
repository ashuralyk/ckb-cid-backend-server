//! FakeRpc → [`RpcHostQuery`] / seeded [`MockHost`] coverage for all 7 host-query
//! syscalls, exercised through the executor's [`HostQuery`] trait (the same
//! surface `ProcessExecutor` serves to the guest).

#[cfg(test)]
mod tests {
    use cid_backend_core::host_query::{
        seed_host_for_batch, RpcHostQuery, FIND_CELL_BY_OUT_POINT, FIND_CELL_DATA_BY_OUT_POINT,
        GET_BLOCK_HASH, GET_CELLS, GET_HEADER, GET_HEADER_BY_NUMBER, GET_LIVE_CELL,
    };
    use cid_process_executor::host_query::{
        LiveCell as ExecLiveCell, Script as HqScript, ScriptType, SearchKeyFilter, SearchMode,
    };
    use cid_process_executor::{
        build_draft_tx, AssembledTx, DraftCell, HostQuery, MockHost, Pagination, ProcessExecutor,
        SearchKey,
    };
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, Header, OutPoint, RawHeader, Script};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{live_cell, lock_script, out_point, FIXTURE_ELF};

    fn sample_header(number: u64) -> Header {
        Header::new_builder()
            .raw(RawHeader::new_builder().number(number.pack()).build())
            .build()
    }

    fn load_example_binary() -> Option<Vec<u8>> {
        if let Ok(path) = std::env::var("CID_PROCESS_EXAMPLE_PATH") {
            if let Ok(bytes) = std::fs::read(&path) {
                if bytes.len() >= 4 && bytes[0] == 0x7f && &bytes[1..4] == b"ELF" {
                    return Some(bytes);
                }
            }
        }
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/cid_process_example");
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() >= 4 && bytes[0] == 0x7f && &bytes[1..4] == b"ELF" {
            Some(bytes)
        } else {
            None
        }
    }

    #[test]
    fn syscall_numbers_match_catalog() {
        assert_eq!(FIND_CELL_BY_OUT_POINT, 2287);
        assert_eq!(FIND_CELL_DATA_BY_OUT_POINT, 2297);
        assert_eq!(GET_LIVE_CELL, 2317);
        assert_eq!(GET_HEADER, 2327);
        assert_eq!(GET_HEADER_BY_NUMBER, 2337);
        assert_eq!(GET_BLOCK_HASH, 2347);
        assert_eq!(GET_CELLS, 2367);
    }

    #[test]
    fn fakerpc_find_cell_by_out_point_via_host_query_and_mock_host() {
        let rpc = FakeRpc::new();
        let cell = live_cell(
            out_point(1, 0),
            lock_script([0x11; 32], ScriptHashType::Type, vec![]),
            None,
            100,
            vec![0xAA],
        );
        rpc.insert_cell(cell.clone());
        let hq = RpcHostQuery::new(&rpc);
        let via_rpc = HostQuery::find_cell_by_out_point(&hq, cell.out_point.as_slice()).unwrap();
        assert_eq!(via_rpc, cell.output.as_slice());

        let draft = DraftCell {
            out_point: cell.out_point.clone(),
            output: cell.output.clone(),
            data: cell.data.clone(),
        };
        rpc.set_tip(0);
        let host = seed_host_for_batch(&rpc, &[draft], 0).unwrap();
        let via_mock = HostQuery::find_cell_by_out_point(&host, cell.out_point.as_slice()).unwrap();
        assert_eq!(via_mock, via_rpc);
    }

    #[test]
    fn fakerpc_find_cell_data_by_out_point_via_host_query_and_mock_host() {
        let rpc = FakeRpc::new();
        let cell = live_cell(
            out_point(2, 0),
            lock_script([0x22; 32], ScriptHashType::Type, vec![]),
            None,
            100,
            vec![0xDE, 0xAD],
        );
        rpc.insert_cell(cell.clone());
        let hq = RpcHostQuery::new(&rpc);
        let via_rpc =
            HostQuery::find_cell_data_by_out_point(&hq, cell.out_point.as_slice()).unwrap();
        assert_eq!(via_rpc, cell.data);

        let draft = DraftCell {
            out_point: cell.out_point.clone(),
            output: cell.output.clone(),
            data: cell.data.clone(),
        };
        rpc.set_tip(0);
        let host = seed_host_for_batch(&rpc, &[draft], 0).unwrap();
        let via_mock =
            HostQuery::find_cell_data_by_out_point(&host, cell.out_point.as_slice()).unwrap();
        assert_eq!(via_mock, via_rpc);
    }

    #[test]
    fn fakerpc_get_live_cell_via_host_query_and_mock_host() {
        let rpc = FakeRpc::new();
        let cell = live_cell(
            out_point(3, 0),
            lock_script([0x33; 32], ScriptHashType::Type, vec![]),
            None,
            100,
            vec![0x01],
        );
        rpc.insert_cell(cell.clone());
        let hq = RpcHostQuery::new(&rpc);
        let via_rpc = HostQuery::get_live_cell(&hq, cell.out_point.as_slice(), true).unwrap();
        let decoded: ExecLiveCell = serde_molecule::from_slice(&via_rpc, false).unwrap();
        assert!(decoded.output.is_some());
        assert_eq!(decoded.data.as_deref(), Some(cell.data.as_slice()));

        let draft = DraftCell {
            out_point: cell.out_point.clone(),
            output: cell.output.clone(),
            data: cell.data.clone(),
        };
        rpc.set_tip(0);
        let host = seed_host_for_batch(&rpc, &[draft], 0).unwrap();
        let via_mock = HostQuery::get_live_cell(&host, cell.out_point.as_slice(), true).unwrap();
        let decoded_mock: ExecLiveCell = serde_molecule::from_slice(&via_mock, false).unwrap();
        assert_eq!(decoded_mock.output, decoded.output);
        assert_eq!(decoded_mock.data, decoded.data);
    }

    #[test]
    fn fakerpc_get_header_via_host_query_and_mock_host() {
        let rpc = FakeRpc::new();
        let header = sample_header(9);
        rpc.insert_header(9, header.clone());
        let hq = RpcHostQuery::new(&rpc);
        let by_number = HostQuery::get_header_by_number(&hq, 9).unwrap();
        assert_eq!(by_number, header.as_slice());
        let hash = header.calc_header_hash();
        let by_hash = HostQuery::get_header(&hq, hash.as_slice()).unwrap();
        assert_eq!(by_hash, header.as_slice());

        // Seed tip header onto MockHost through the same RPC surface.
        let cell = live_cell(
            out_point(9, 0),
            lock_script([0x99; 32], ScriptHashType::Type, vec![]),
            None,
            1,
            vec![],
        );
        rpc.insert_cell(cell.clone());
        let draft = DraftCell {
            out_point: cell.out_point.clone(),
            output: cell.output.clone(),
            data: cell.data.clone(),
        };
        let host = seed_host_for_batch(&rpc, &[draft], 9).unwrap();
        assert_eq!(
            HostQuery::get_header_by_number(&host, 9).as_deref(),
            Some(header.as_slice())
        );
        assert_eq!(
            HostQuery::get_header(&host, hash.as_slice()).as_deref(),
            Some(header.as_slice())
        );
    }

    #[test]
    fn fakerpc_get_block_hash_via_host_query_and_mock_host() {
        let rpc = FakeRpc::new();
        rpc.insert_block_hash(4, [0x44; 32]);
        let hq = RpcHostQuery::new(&rpc);
        assert_eq!(HostQuery::get_block_hash(&hq, 4), Some([0x44; 32]));

        let cell = live_cell(
            out_point(4, 0),
            lock_script([0x44; 32], ScriptHashType::Type, vec![]),
            None,
            1,
            vec![],
        );
        rpc.insert_cell(cell.clone());
        let draft = DraftCell {
            out_point: cell.out_point,
            output: cell.output,
            data: cell.data,
        };
        let host = seed_host_for_batch(&rpc, &[draft], 4).unwrap();
        assert_eq!(HostQuery::get_block_hash(&host, 4), Some([0x44; 32]));
    }

    #[test]
    fn fakerpc_get_cells_pagination_and_script_len_range_via_host_query() {
        let rpc = FakeRpc::new();
        let lock_code = [0xBBu8; 32];
        let with_type = {
            live_cell(
                out_point(0x10, 0),
                lock_script(lock_code, ScriptHashType::Type, vec![0x01]),
                Some(
                    Script::new_builder()
                        .code_hash([0x11; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args([0xAA; 8].pack())
                        .build(),
                ),
                100,
                vec![],
            )
        };
        let without_type = live_cell(
            out_point(0x11, 0),
            lock_script(lock_code, ScriptHashType::Type, vec![0x02]),
            None,
            100,
            vec![],
        );
        rpc.insert_cell(with_type.clone());
        rpc.insert_cell(without_type);

        let key = SearchKey {
            script: HqScript {
                code_hash: lock_code,
                hash_type: 1,
                args: vec![],
            },
            script_type: ScriptType::Lock,
            script_search_mode: Some(SearchMode::Prefix),
            filter: Some(SearchKeyFilter {
                script: None,
                script_len_range: Some([1, 1000]),
                output_data: None,
                output_data_filter_mode: None,
                output_data_len_range: None,
                output_capacity_range: None,
                block_range: None,
            }),
            with_data: Some(true),
            group_by_transaction: None,
        };
        let key_bytes = serde_molecule::to_vec(&key, false).unwrap();
        let hq = RpcHostQuery::new(&rpc);
        let page = HostQuery::get_cells(&hq, &key_bytes, 0, 10, &[]).unwrap();
        let pag: Pagination = serde_molecule::from_slice(&page, false).unwrap();
        assert_eq!(pag.objects.len(), 1);
        let expected: [u8; 32] = with_type.out_point.tx_hash().unpack();
        assert_eq!(pag.objects[0].out_point.tx_hash, expected);

        // Also through MockHost seeded from FakeRpc cells (executor host path).
        let host = MockHost::new();
        for cell in rpc.live_cells() {
            host.insert_cell(cell.out_point, cell.output, cell.data);
        }
        let via_mock = HostQuery::get_cells(&host, &key_bytes, 0, 10, &[]).unwrap();
        let pag_mock: Pagination = serde_molecule::from_slice(&via_mock, false).unwrap();
        assert_eq!(pag_mock.objects.len(), 1);
    }

    /// Drive the real `ProcessExecutor` guest so find_cell / find_cell_data /
    /// get_block_hash syscalls run inside ckb-vm (example Process assemble path).
    #[test]
    fn real_executor_assemble_exercises_host_queries_from_fakerpc_seed() {
        let Some(bin) = load_example_binary() else {
            // Soft skip when fixture absent (CI builds it; local default ignores).
            eprintln!("cid_process_example missing; skipping real executor host-query test");
            return;
        };
        assert!(
            bin.len() >= 4 && bin[0] == 0x7f && &bin[1..4] == b"ELF",
            "fixture must be ELF"
        );
        let _ = FIXTURE_ELF;

        use cid_backend_core::type_id::{type_id_script, type_id_script_hash};
        use cid_protocol::{CidArgs, Payload, Script as CidScript};

        let rpc = FakeRpc::new();
        let state_args = [0xCDu8; 32];
        let type_hash = type_id_script_hash(&state_args);
        let code_hash = type_id_script_hash(&[0xABu8; 32]);
        let lock = Script::new_builder()
            .code_hash(code_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let state_type = type_id_script(&state_args);
        let state_op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let state = live_cell(
            state_op.clone(),
            lock,
            Some(state_type),
            500_0000_0000,
            vec![],
        );
        let user = CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0xEE],
        };
        let args = CidArgs::new(type_hash, user, Payload { data: vec![0xAA] });
        let lock_args = args.encode().unwrap();
        let intent_op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x10; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let intent = live_cell(
            intent_op.clone(),
            Script::new_builder()
                .code_hash([0xC1; 32].pack())
                .hash_type(ScriptHashType::Type.into())
                .args(lock_args.pack())
                .build(),
            None,
            100_0000_0000,
            vec![0xAA],
        );
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(state.clone());
        rpc.insert_block_hash(0, [0xAB; 32]);

        let cells = vec![
            DraftCell {
                out_point: intent.out_point.clone(),
                output: intent.output.clone(),
                data: intent.data.clone(),
            },
            DraftCell {
                out_point: state.out_point.clone(),
                output: state.output.clone(),
                data: state.data.clone(),
            },
        ];
        let host = seed_host_for_batch(&rpc, &cells, 0).expect("seed from FakeRpc");
        // Probe HostQuery surface before VM (same MockHost the executor uses).
        assert!(HostQuery::find_cell_by_out_point(&host, intent_op.as_slice()).is_some());
        assert!(HostQuery::find_cell_data_by_out_point(&host, intent_op.as_slice()).is_some());
        assert_eq!(HostQuery::get_block_hash(&host, 0), Some([0xAB; 32]));

        let draft = build_draft_tx(&cells);
        let assembled = ProcessExecutor::new(bin)
            .assemble(&draft, host)
            .expect("Process.assemble via real executor + FakeRpc-seeded MockHost");
        assert_eq!(assembled.inputs.len(), assembled.witnesses.len());
        assert_eq!(assembled.outputs.len(), 2);
        let _ = AssembledTx::from_bytes(assembled.as_molecule_bytes()).unwrap();
    }
}
