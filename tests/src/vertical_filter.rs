#[cfg(test)]
mod tests {
    use cid_backend_core::rpc::LiveCell;
    use cid_backend_core::SsriError;
    use cid_intention_filter::decode_filter_request;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::OutPoint;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, intention_cell, intention_cell_with_lock, loop_with_resolver,
        process_cell, process_type_hash,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn chain(count: u8) -> (FakeRpc, LiveCell, Vec<OutPoint>) {
        let rpc = FakeRpc::new();
        let state = process_cell(1, 1000_0000_0000, vec![]);
        rpc.insert_cell(state.clone());
        let ops = (1..=count)
            .map(|tag| {
                let cell = intention_cell(tag, process_type_hash(1), 600_0000_0000);
                let op = cell.out_point.clone();
                rpc.insert_cell(cell);
                op
            })
            .collect();
        (rpc, state, ops)
    }

    fn input_sets(rpc: &FakeRpc) -> Vec<Vec<OutPoint>> {
        rpc.submitted()
            .iter()
            .map(|tx| {
                tx.inputs()
                    .into_iter()
                    .map(|i| i.previous_output())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn mixed_reasons_filter_before_batch_selection() {
        let (rpc, state, ops) = chain(9);
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|_| Ok(vec![0, 5, 4, 2, 0, 1, 3, 6, 7]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        let inputs = input_sets(loop_.rpc());
        assert_eq!(report.submitted, 6, "{report:?}");
        assert!(inputs.contains(&vec![ops[0].clone(), ops[4].clone(), state.out_point]));
        for index in [1, 2, 5, 7, 8] {
            assert!(inputs.contains(&vec![ops[index].clone()]));
        }
        for index in [3, 6] {
            assert!(inputs.iter().flatten().all(|op| op != &ops[index]));
        }
    }

    #[test]
    fn filters_more_than_64_candidates_in_order() {
        let (rpc, _, ops) = chain(65);
        let ssri = ScriptedSsri::mimic((1, 100));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        let calls = loop_.ssri().filter_requests.lock().unwrap();
        let mut observed = Vec::new();
        let sizes: Vec<_> = calls
            .iter()
            .map(|bytes| {
                let request = decode_filter_request(bytes).unwrap();
                observed.extend(
                    request
                        .cells
                        .iter()
                        .map(|cell| OutPoint::from_slice(&cell.out_point).unwrap()),
                );
                request.cells.len()
            })
            .collect();
        assert_eq!(sizes, [64, 1]);
        assert_eq!(observed, ops);
    }

    #[test]
    fn filter_respects_stdin_size_and_leaves_an_oversized_cell_live() {
        let (rpc, _, _) = chain(0);
        let mut ops = Vec::new();
        for (tag, size) in [(1, 36 * 1024), (2, 64 * 1024), (3, 36 * 1024)] {
            let mut cell = intention_cell(tag, process_type_hash(1), 7_000_000_000_000);
            cell.data = vec![0; size];
            ops.push(cell.out_point.clone());
            rpc.insert_cell(cell);
        }
        let mut loop_ =
            loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        assert_eq!(report.skipped, 1);
        assert!(loop_.rpc().has_cell(&ops[1]));
        let calls = loop_.ssri().filter_requests.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (request, op) in calls.iter().zip([&ops[0], &ops[2]]) {
            assert!(request.len() <= cid_intention_filter::FILTER_STDIN_MAX);
            let parsed = decode_filter_request(request).unwrap();
            assert_eq!(parsed.cells.len(), 1);
            assert_eq!(parsed.cells[0].out_point, op.as_slice());
        }
    }

    #[test]
    fn assembly_cannot_add_an_unselected_intention_as_funding() {
        let (rpc, _, ops) = chain(2);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(|_| Ok(vec![0, 5]))
            .with_added_input(ops[1].clone());
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[1].clone()]]);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn assembly_can_add_an_ordinary_funding_input() {
        use crate::helpers::{live_cell, lock_script, out_point};

        let (rpc, state, ops) = chain(1);
        let funding_op = out_point(2, 0);
        rpc.insert_cell(live_cell(
            funding_op.clone(),
            lock_script([0x44; 32], ScriptHashType::Type, vec![0xAA]),
            None,
            100_0000_0000,
            vec![],
        ));
        let ssri = ScriptedSsri::mimic((1, 4)).with_added_input(funding_op.clone());
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), state.out_point, funding_op]]
        );
    }

    #[test]
    fn fee_retry_requires_a_fresh_filter_rejection() {
        let (rpc, _, ops) = chain(1);
        let mut reason = 5;
        let ssri = ScriptedSsri::mimic((2, 4)).with_filter(move |_| {
            let previous = reason;
            reason = 0;
            Ok(vec![previous])
        });
        let mut config = default_loop_config();
        config.refund_policy.bounded_locks = config.scan_lock_identities();
        config.refund_policy.fee_rate = cid_backend_core::MAX_FEE_RATE;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert_eq!(loop_.tick().submitted, 0);
        loop_.config_mut().refund_policy.fee_rate = cid_backend_core::DEFAULT_FEE_RATE;
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn method_missing_falls_back_only_with_opt_in() {
        for allow in [false, true] {
            let (rpc, _, _) = chain(1);
            let ssri = ScriptedSsri::mimic((1, 4)).with_filter(|_| Err(SsriError::MethodNotFound));
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1)
                .with_missing_filter_fallback(allow);
            assert_eq!(loop_.tick().submitted, usize::from(allow));
        }
        let (rpc, _, _) = chain(1);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(|_| Err(SsriError::Unusable("guest crash".into())));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1)
            .with_missing_filter_fallback(true);
        assert_eq!(loop_.tick().submitted, 0);
    }

    #[test]
    fn invalid_filter_response_retries_the_window() {
        for invalid in [vec![], vec![99]] {
            let (rpc, _, _) = chain(65);
            let mut calls = 0;
            let ssri = ScriptedSsri::mimic((1, 100)).with_filter(move |count| {
                calls += 1;
                match calls {
                    1 => Ok(vec![5; count]),
                    2 => Ok(invalid.clone()),
                    _ => Ok(vec![0; count]),
                }
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);
            assert!(loop_.rpc().submitted().is_empty());
            assert_eq!(loop_.tick().submitted, 1);
        }
    }

    #[test]
    fn isolates_and_refunds_intention_exits_39_and_40() {
        for (code, min) in [(39, 1), (40, 1), (40, 2)] {
            let (rpc, state, ops) = chain(3);
            let ssri = ScriptedSsri::mimic((min, 4)).with_intention_error(ops[1].clone(), code);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            let report = loop_.tick();
            let inputs = input_sets(loop_.rpc());
            assert_eq!(report.submitted, 2, "exit {code}: {report:?}");
            assert!(inputs.contains(&vec![ops[1].clone()]));
            assert!(inputs.contains(&vec![ops[0].clone(), ops[2].clone(), state.out_point]));
        }
    }

    #[test]
    fn isolation_can_use_accepted_peers_across_a_window_boundary() {
        let (rpc, state, ops) = chain(4);
        let ssri = ScriptedSsri::mimic((2, 2))
            .with_intention_error(ops[0].clone(), 39)
            .with_intention_error(ops[2].clone(), 40);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let first = loop_.tick();
        assert_eq!(first.submitted, 3, "{first:?}");
        let inputs = input_sets(loop_.rpc());
        assert!(inputs.contains(&vec![ops[0].clone()]));
        assert!(inputs.contains(&vec![ops[1].clone(), ops[3].clone(), state.out_point]));
        assert!(inputs.contains(&vec![ops[2].clone()]));
    }

    #[test]
    fn invalid_beneficiary_does_not_stall_other_intentions() {
        let (rpc, state, ops) = chain(2);
        let ssri = ScriptedSsri::mimic((1, 4)).with_intention_error(ops[0].clone(), 32);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{report:?}");
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn process_fault_suppresses_filter_refunds_and_stops_retries_for_state() {
        let (rpc, state, ops) = chain(2);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(|_| Ok(vec![0, 5]))
            .with_intention_error(ops[0].clone(), 41);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert!(report.last_error.unwrap().contains("41"));
        assert_eq!(
            loop_
                .workers()
                .get(&process_type_hash(1))
                .unwrap()
                .with_state(|worker| worker.unhealthy_state.clone()),
            Some(state.out_point)
        );
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn routes_only_well_formed_intentions_for_configured_cid_and_registered_process() {
        let (rpc, _, ops) = chain(1);
        let unregistered = intention_cell(2, process_type_hash(2), 600_0000_0000);
        let wrong_cid = intention_cell_with_lock(
            3,
            process_type_hash(1),
            600_0000_0000,
            [0x33; 32],
            ScriptHashType::Type,
        );
        let mut malformed = intention_cell(4, process_type_hash(1), 600_0000_0000);
        let lock = malformed.lock().as_builder().args([1u8].pack()).build();
        malformed.output = malformed.output.as_builder().lock(lock).build();
        rpc.insert_cell(unregistered);
        rpc.insert_cell(wrong_cid.clone());
        rpc.insert_cell(malformed);
        let mut config = default_loop_config();
        config
            .extra_scan_locks
            .push(cid_backend_core::CidLockIdentity::new(
                [0x33; 32],
                ScriptHashType::Type,
            ));
        let mut loop_ = loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), config, 1);
        assert_eq!(loop_.tick().submitted, 1);
        let calls = loop_.ssri().filter_requests.lock().unwrap();
        let req = decode_filter_request(&calls[0]).unwrap();
        assert_eq!(req.cells.len(), 1);
        assert_eq!(req.cells[0].out_point, ops[0].as_slice());
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == wrong_cid.out_point));
    }
}
