#[cfg(test)]
mod tests {
    use cid_backend_core::rpc::LiveCell;
    use cid_backend_core::SsriError;
    use cid_intention_filter::decode_filter_request;
    use cid_protocol::{CidArgs, Payload};
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::OutPoint;
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        code_cell, default_loop_config, intention_cell, intention_cell_with_lock, live_cell,
        lock_script, loop_with_resolver, out_point, process_cell, process_type_hash, FIXTURE_ELF,
        PROC_CODE_ARGS,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn chain(count: u8) -> (FakeRpc, LiveCell, Vec<OutPoint>) {
        let rpc = FakeRpc::new();
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let state = process_cell(1, 1000_0000_0000, vec![]);
        rpc.insert_cell(state.clone());
        let ops = (1..=count)
            .map(|tag| {
                let mut cell = intention_cell(tag, process_type_hash(1), 600_0000_0000);
                let user = crate::helpers::user_lock();
                let args = CidArgs::new(
                    process_type_hash(1),
                    user.clone(),
                    Payload::from_script(&user).unwrap(),
                )
                .encode()
                .unwrap();
                let lock = cell.lock().as_builder().args(args.pack()).build();
                cell.output = cell.output.as_builder().lock(lock).build();
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
    fn deleting_during_a_refund_sweep_preserves_only_the_admitted_submission() {
        use cid_backend_core::{DigestLoop, NopClock, WorkerCommand, WorkerCommands, WorkerPool};
        use std::sync::Arc;

        let (rpc, _, ops) = chain(3);
        let rpc = Arc::new(rpc);
        rpc.defer_commit();
        let ssri = Arc::new(ScriptedSsri::mimic((1, 4)).with_filter(|n| Ok(vec![5; n])));
        let workers = WorkerPool::new(rpc.clone(), ssri.clone());
        let commands = Arc::new(WorkerCommands::default());
        let mut loop_ = DigestLoop::with_workers(
            rpc.clone(),
            NopClock,
            ssri,
            default_loop_config(),
            workers,
            Some(commands.clone()),
        );
        crate::helpers::register_fixture(&mut loop_, 1);
        let hash = process_type_hash(1);
        rpc.after_next_submit(move || assert!(commands.enqueue(WorkerCommand::delete(hash))));

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(&rpc), [vec![ops[0].clone()]]);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.state().submit.unwrap().pending.len(), 1);
        rpc.commit_pending(&rpc.submitted()[0].hash());
        loop_.tick();
        loop_.tick();
        assert!(!loop_.workers().contains(&hash));
        assert!(rpc.has_cell(&ops[1]));
        assert!(rpc.has_cell(&ops[2]));
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
    fn assembly_cannot_add_a_legacy_data1_cid_input() {
        let (rpc, _, ops) = chain(1);
        let hash = [0x33; 32];
        let legacy = intention_cell_with_lock(
            2,
            process_type_hash(1),
            600_0000_0000,
            hash,
            ScriptHashType::Data1,
        );
        let legacy_op = legacy.out_point.clone();
        rpc.insert_cell(legacy);
        let mut config = default_loop_config();
        config
            .extra_scan_locks
            .push(cid_backend_core::CidLockIdentity::new(
                hash,
                ScriptHashType::Data1,
            ));
        let ssri = ScriptedSsri::mimic((1, 4)).with_added_input(legacy_op.clone());
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        assert!(loop_.rpc().has_cell(&legacy_op));
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn assembly_checks_the_resolved_cid_binary_data_hash_alias() {
        use cid_backend_core::{TypeIdCodeDep, TypeIdIdentity};
        use ckb_types::packed::CellOutput;

        let (rpc, state, _) = chain(0);
        let identity = TypeIdIdentity::new([0xCC; 32]);
        let cid_hash = identity.type_script_hash();
        let data = vec![0xCA, 0xFE];
        let alias = CellOutput::calc_data_hash(&data).unpack();
        rpc.insert_cell(code_cell(0xC0, identity.args, data));
        let state_lock = state
            .lock()
            .as_builder()
            .args([cid_hash.as_slice(), &[1]].concat().pack())
            .build();
        rpc.insert_cell(LiveCell {
            output: state.output.as_builder().lock(state_lock).build(),
            ..state
        });
        rpc.insert_cell(intention_cell_with_lock(
            1,
            process_type_hash(1),
            600_0000_0000,
            cid_hash,
            ScriptHashType::Type,
        ));
        let alias_op = out_point(2, 0);
        rpc.insert_cell(live_cell(
            alias_op.clone(),
            lock_script(alias, ScriptHashType::Data1, vec![0xAA]),
            None,
            100_0000_0000,
            vec![],
        ));
        let mut config = default_loop_config();
        config.cid_code_hash = cid_hash;
        config.type_id_code_deps = vec![TypeIdCodeDep {
            label: "cid_lock".into(),
            identity,
        }];
        let ssri = ScriptedSsri::mimic((1, 4)).with_added_input(alias_op);
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
    }

    #[test]
    fn stale_filter_refund_cannot_submit_and_upgrade_refilters() {
        let (rpc, state, ops) = chain(1);
        let stale = out_point(0xA0, 0);
        rpc.mark_dep_spent(stale.clone());
        let mut reason = 5;
        let ssri = ScriptedSsri::mimic((1, 4)).with_filter(move |_| {
            let previous = reason;
            reason = 0;
            Ok(vec![previous])
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_.rpc().has_cell(&ops[0]));
        loop_.rpc().consume(&stale);
        loop_.rpc().insert_cell(code_cell(
            0xA1,
            PROC_CODE_ARGS,
            [FIXTURE_ELF, &[2]].concat(),
        ));
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), state.out_point]]
        );
    }

    #[test]
    fn stale_assemble_verdict_cannot_submit_a_refund() {
        let (rpc, _, ops) = chain(1);
        rpc.mark_dep_spent(out_point(0xA0, 0));
        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::incompatible(21),
            default_loop_config(),
            1,
        );
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn filter_refunds_require_the_live_elf_that_supplied_the_verdict() {
        let (rpc, _, ops) = chain(1);
        rpc.consume(&out_point(0xA0, 0));
        rpc.insert_cell(code_cell(0xA1, PROC_CODE_ARGS, vec![0]));
        let ssri = ScriptedSsri::mimic((2, 4)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn filter_refund_sweep_is_bounded_and_resumes_after_unavailability() {
        let (rpc, _, ops) = chain(33);
        rpc.inject_unavailable_dry_run_once();
        let ssri = ScriptedSsri::mimic((2, 4)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        loop_.rpc().inject_unavailable_refund_once(ops[1].clone());
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 2);
        // Both failed cells stay live, and the sweep resumes past them.
        assert_eq!(loop_.tick().submitted, 16);
        assert!(loop_.rpc().has_cell(&ops[0]));
        assert_eq!(loop_.tick().submitted, 16);
        assert!(loop_.rpc().has_cell(&ops[1]));
        assert_eq!(loop_.tick().submitted, 1);
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
            rpc.defer_commit();
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
    fn isolation_keeps_exit_39_when_an_unrelated_cell_returns_exit_32() {
        for min in [1, 2] {
            let (rpc, state, ops) = chain(4);
            rpc.defer_commit();
            let ssri = ScriptedSsri::mimic((min, 4))
                .with_intention_error(ops[0].clone(), 39)
                .with_intention_error(ops[1].clone(), 32);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 2);
            let inputs = input_sets(loop_.rpc());
            assert!(inputs.contains(&vec![ops[0].clone()]));
            assert!(inputs.contains(&vec![ops[2].clone(), ops[3].clone(), state.out_point]));
            assert!(inputs.iter().flatten().all(|op| op != &ops[1]));
        }
    }

    #[test]
    fn replacement_state_invalidates_an_unaudited_exit_39() {
        let (rpc, state, ops) = chain(1);
        rpc.inject_unavailable_refund_once(ops[0].clone());
        let ssri = ScriptedSsri::mimic((1, 4)).with_intention_error_at_state_capacity(
            ops[0].clone(),
            39,
            state.capacity(),
        );
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);

        let mut replacement = state.clone();
        replacement.out_point = out_point(0x51, 0);
        replacement.output = replacement
            .output
            .as_builder()
            .capacity(900_0000_0000u64.pack())
            .build();
        loop_.rpc().consume(&state.out_point);
        loop_.rpc().insert_cell(replacement.clone());

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), replacement.out_point]]
        );
    }

    #[test]
    fn isolated_refund_checks_that_authorizing_state_is_still_live() {
        let (rpc, state, ops) = chain(2);
        let ssri = ScriptedSsri::mimic((1, 4)).with_intention_error(ops[0].clone(), 39);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn isolation_can_use_accepted_peers_across_a_window_boundary() {
        let (rpc, state, ops) = chain(4);
        rpc.defer_commit();
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
    fn isolated_refund_retries_after_peer_commit_and_refund_unavailability() {
        let (rpc, state, ops) = chain(3);
        rpc.defer_commit();
        rpc.inject_unavailable_refund_once(ops[1].clone());
        let ssri = ScriptedSsri::mimic((2, 4)).with_intention_error(ops[1].clone(), 39);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1)
            .with_example_policy_data_hashes(vec![ckb_types::packed::CellOutput::calc_data_hash(
                FIXTURE_ELF,
            )
            .unpack()]);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), ops[2].clone(), state.out_point]]
        );
        loop_
            .rpc()
            .commit_pending(&loop_.rpc().submitted()[0].hash());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc())[1], [ops[1].clone()]);
    }

    #[test]
    fn dropped_isolated_refund_retries_only_under_the_authorizing_code() {
        for upgraded in [false, true] {
            let (rpc, _, ops) = chain(3);
            rpc.defer_commit();
            let ssri = ScriptedSsri::mimic((2, 4)).with_intention_error(ops[1].clone(), 40);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1)
                .with_example_policy_data_hashes(vec![
                    ckb_types::packed::CellOutput::calc_data_hash(FIXTURE_ELF).unpack(),
                ]);
            assert_eq!(loop_.tick().submitted, 2);
            for tx in loop_.rpc().submitted() {
                if tx.inputs().len() == 1 {
                    loop_.rpc().simulate_drop(&tx.hash());
                } else {
                    loop_.rpc().commit_pending(&tx.hash());
                }
            }
            if upgraded {
                loop_.rpc().consume(&out_point(0xA0, 0));
                loop_.rpc().insert_cell(code_cell(
                    0xA1,
                    PROC_CODE_ARGS,
                    [FIXTURE_ELF, &[2]].concat(),
                ));
            }
            assert_eq!(loop_.tick().submitted, usize::from(!upgraded));
            if !upgraded {
                assert_eq!(input_sets(loop_.rpc())[2], [ops[1].clone()]);
            }
        }
    }

    #[test]
    fn isolation_resumes_after_64_comparison_probes() {
        let (rpc, state, ops) = chain(13);
        rpc.defer_commit();
        let mut ssri = ScriptedSsri::mimic((2, 13));
        for op in &ops[..11] {
            ssri = ssri.with_intention_error(op.clone(), 39);
        }
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.ssri().seen_binaries.lock().unwrap().len(), 66);
        let arrival = intention_cell(14, process_type_hash(1), 600_0000_0000);
        let arrival_op = arrival.out_point.clone();
        loop_.rpc().insert_cell(arrival);
        assert_eq!(loop_.tick().submitted, 12);
        assert!(loop_.rpc().has_cell(&arrival_op));
        assert!(input_sets(loop_.rpc()).contains(&vec![
            ops[11].clone(),
            ops[12].clone(),
            state.out_point
        ]));
    }

    #[test]
    fn isolation_resumes_when_its_initial_failure_disappears() {
        let (rpc, state, ops) = chain(14);
        rpc.defer_commit();
        let mut ssri = ScriptedSsri::mimic((2, 14));
        for (i, op) in ops[..12].iter().enumerate() {
            ssri = ssri.with_intention_error(op.clone(), if i == 1 { 32 } else { 39 });
        }
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        loop_.rpc().consume(&ops[0]);
        let arrival = intention_cell(15, process_type_hash(1), 600_0000_0000);
        let arrival_op = arrival.out_point.clone();
        loop_.rpc().insert_cell(arrival);

        assert_eq!(loop_.tick().submitted, 11);
        assert!(input_sets(loop_.rpc()).contains(&vec![
            ops[12].clone(),
            ops[13].clone(),
            state.out_point
        ]));
        assert!(loop_.rpc().has_cell(&arrival_op));
    }

    #[test]
    fn comparison_probe_cannot_use_an_indexed_but_dead_cell() {
        let (rpc, _, ops) = chain(3);
        rpc.defer_commit();
        rpc.mark_indexer_cell_stale(ops[1].clone());
        let ssri = ScriptedSsri::mimic((2, 3)).with_intention_error(ops[0].clone(), 39);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
    }

    #[test]
    fn exit_41_discards_earlier_isolated_verdicts() {
        let (rpc, _, ops) = chain(3);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_intention_error(ops[0].clone(), 39)
            .with_intention_error(ops[1].clone(), 32)
            .with_intention_error(ops[2].clone(), 41);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report.last_error.unwrap().contains("exit 41"));
    }

    #[test]
    fn example_policy_refunds_a_process_lock_beneficiary_before_assembly() {
        use cid_protocol::{CidArgs, Payload, Script};
        use ckb_types::packed::CellOutput;

        for enabled in [false, true] {
            let (rpc, state, _) = chain(0);
            let mut item = intention_cell(1, process_type_hash(1), 600_0000_0000);
            let beneficiary = Script {
                code_hash: state.lock().code_hash().unpack(),
                hash_type: 1,
                args: vec![],
            };
            let user = crate::helpers::user_lock();
            let args = CidArgs::new(
                process_type_hash(1),
                user.clone(),
                Payload::from_script(&beneficiary).unwrap(),
            )
            .encode()
            .unwrap();
            let lock = item.lock().as_builder().args(args.pack()).build();
            item.output = item.output.as_builder().lock(lock).build();
            let op = item.out_point.clone();
            rpc.insert_cell(item);
            let config = default_loop_config();
            let ssri = ScriptedSsri::mimic((1, 4)).with_intention_error(op.clone(), 32);
            let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
            if enabled {
                loop_ = loop_
                    .with_example_policy_data_hashes(vec![
                        CellOutput::calc_data_hash(FIXTURE_ELF).unpack()
                    ]);
            }
            assert_eq!(loop_.tick().submitted, usize::from(enabled));
            if enabled {
                assert_eq!(input_sets(loop_.rpc()), [vec![op]]);
                let output = loop_.rpc().submitted()[0].outputs().get(0).unwrap();
                assert_eq!(
                    output.lock(),
                    lock_script(user.code_hash, ScriptHashType::Type, user.args)
                );
                assert!(loop_.ssri().seen_binaries.lock().unwrap().is_empty());
            }
        }
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
        let ssri = ScriptedSsri::mimic((1, 4)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[0].clone()]]);
        assert!(!loop_.rpc().has_cell(&ops[0]));
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
