#[cfg(test)]
mod tests {
    use cid_backend_core::host_query::HostQueryRpc;
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
        let config = default_loop_config();
        rpc.enforce_single_input_cid_lock_groups(config.cid_code_hash, config.hash_type);
        rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, FIXTURE_ELF.to_vec()));
        let state = process_cell(1, 1000_0000_0000, vec![]);
        rpc.insert_cell(state.clone());
        let ops = (1..=count)
            .map(|tag| {
                let cell = filter_intention(tag, 600_0000_0000);
                let op = cell.out_point.clone();
                rpc.insert_cell(cell);
                op
            })
            .collect();
        (rpc, state, ops)
    }

    // CID v0.1.4 allows only one Intention per complete lock script.
    fn filter_intention(tag: u8, capacity: u64) -> LiveCell {
        let mut cell = intention_cell(tag, process_type_hash(1), capacity);
        let mut user = crate::helpers::user_lock();
        user.args[0] = tag;
        let args = CidArgs::new(
            process_type_hash(1),
            user.clone(),
            Payload::from_script(&user).unwrap(),
        )
        .encode()
        .unwrap();
        let lock = cell.lock().as_builder().args(args.pack()).build();
        cell.output = cell.output.as_builder().lock(lock).build();
        cell
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

    fn set_below_minimum_deposit(rpc: &FakeRpc, op: &OutPoint) {
        let mut cell = rpc.get_live_cell(op).unwrap().unwrap();
        cell.output = cell
            .output
            .as_builder()
            .capacity(599_0000_0000u64.pack())
            .build();
        rpc.insert_cell(cell);
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
        let (rpc, state, ops) = chain(7);
        rpc.defer_commit();
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| {
            Ok(if n == 7 {
                vec![0, 5, 4, 0, 1, 6, 7]
            } else {
                vec![5, 4, 1, 6, 7]
            })
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{report:?}");
        let digest = &loop_.rpc().submitted()[0];
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![
                ops[0].clone(),
                ops[3].clone(),
                state.out_point.clone()
            ]]
        );
        loop_.rpc().commit_pending(&digest.hash());
        let report = loop_.tick();
        assert_eq!(report.submitted, 4, "{report:?}");
        let txs = loop_.rpc().submitted();
        let inputs = input_sets(loop_.rpc());
        for intention_index in [1, 2, 5, 6] {
            let tx_index = inputs
                .iter()
                .position(|input| input == &[ops[intention_index].clone()])
                .expect("filter refund");
            let deps: Vec<_> = txs[tx_index]
                .cell_deps()
                .into_iter()
                .map(|dep| dep.out_point())
                .collect();
            assert!(deps.contains(&out_point(0xA0, 0)));
            assert!(deps.iter().any(|dep| dep.tx_hash() == digest.hash()));
        }
        assert!(inputs.iter().flatten().all(|op| op != &ops[4]));
        assert!(loop_.rpc().has_cell(&ops[4]));
    }

    #[test]
    fn duplicate_cid_lock_groups_never_reach_dry_run() {
        let (rpc, state, ops) = chain(3);
        let duplicate_lock = rpc.get_live_cell(&ops[0]).unwrap().unwrap().lock();
        let mut duplicate = rpc.get_live_cell(&ops[1]).unwrap().unwrap();
        duplicate.output = duplicate.output.as_builder().lock(duplicate_lock).build();
        rpc.insert_cell(duplicate);
        let ssri = ScriptedSsri::mimic((2, 2));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), ops[2].clone(), state.out_point]]
        );
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        assert!(loop_.rpc().has_cell(&ops[1]));
    }

    #[test]
    fn isolation_skips_duplicate_cid_lock_groups() {
        let (rpc, state, ops) = chain(4);
        let duplicate_lock = rpc.get_live_cell(&ops[0]).unwrap().unwrap().lock();
        let mut duplicate = rpc.get_live_cell(&ops[2]).unwrap().unwrap();
        duplicate.output = duplicate.output.as_builder().lock(duplicate_lock).build();
        rpc.insert_cell(duplicate);
        let ssri = ScriptedSsri::mimic((2, 2)).with_intention_error(ops[1].clone(), 32);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), ops[3].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[2]));
    }

    #[test]
    fn assembly_isolation_moves_past_a_conflict_at_either_rpc_gate() {
        for dry_run in [true, false] {
            let (rpc, state, ops) = chain(3);
            rpc.conflict_on_input(Some(ops[1].clone()), dry_run);
            let ssri = ScriptedSsri::mimic((1, 3)).with_intention_error(ops[0].clone(), 39);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

            let report = loop_.tick();
            assert_eq!(report.conflicts, 1);
            assert_eq!(report.submitted, 1);
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[2].clone(), state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[0]));
            assert!(loop_.rpc().has_cell(&ops[1]));
        }
    }

    #[test]
    fn first_assembly_moves_past_a_conflict_at_either_rpc_gate() {
        for dry_run in [true, false] {
            let (rpc, state, ops) = chain(2);
            rpc.conflict_on_input(Some(ops[0].clone()), dry_run);
            let mut loop_ =
                loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), default_loop_config(), 1);

            let first = loop_.tick();
            assert_eq!(first.submitted, 1);
            assert_eq!(first.conflicts, 2);
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[1].clone(), state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[0]));
        }
    }

    #[test]
    fn assembly_isolation_retries_conflicts_next_tick() {
        let (rpc, state, ops) = chain(2);
        rpc.conflict_on_input(Some(ops[1].clone()), false);
        let ssri = ScriptedSsri::mimic((1, 2)).with_intention_error(ops[0].clone(), 39);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        loop_.rpc().conflict_on_input(None, false);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), state.out_point]]
        );
    }

    #[test]
    fn filter_refund_requires_matching_node_intention() {
        let (rpc, _, ops) = chain(2);
        let mut snapshot = rpc.get_live_cell(&ops[0]).unwrap().unwrap();
        let mut args = CidArgs::decode(&snapshot.lock().args().raw_data()).unwrap();
        args.entry.payload = Payload { data: vec![0] };
        let lock = snapshot
            .lock()
            .as_builder()
            .args(args.encode().unwrap().pack())
            .build();
        snapshot.output = snapshot.output.as_builder().lock(lock).build();
        rpc.set_indexer_snapshot(ops[0].clone(), Some(snapshot));
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![6; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[1].clone()]]);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn resolution_defers_spent_or_mismatched_indexer_state_before_filter() {
        for spent in [true, false] {
            let (rpc, state, _) = chain(1);
            if spent {
                rpc.mark_indexer_cell_stale(state.out_point.clone());
            } else {
                let mut snapshot = state.clone();
                snapshot.data.push(1);
                rpc.set_indexer_snapshot(state.out_point.clone(), Some(snapshot));
            }
            let mut loop_ =
                loop_with_resolver(rpc, ScriptedSsri::mimic((1, 1)), default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);
            assert!(loop_.ssri().filter_requests.lock().unwrap().is_empty());
            assert_eq!(loop_.rpc().dry_run_attempts(), 0);

            loop_.rpc().restore_indexer_cell_liveness(&state.out_point);
            loop_.rpc().clear_indexer_snapshot(&state.out_point);
            assert_eq!(loop_.tick().submitted, 1);
        }
    }

    #[test]
    fn filter_refund_requires_matching_live_node_process_state() {
        for spent in [true, false] {
            let (rpc, mut state, ops) = chain(1);
            if spent {
                rpc.mark_indexer_cell_stale(state.out_point);
            } else {
                state.data.push(1);
                rpc.set_indexer_snapshot(state.out_point.clone(), Some(state));
            }
            let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![6; n]));
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);
            assert_eq!(loop_.rpc().submit_attempts(), 0);
            assert!(loop_.rpc().has_cell(&ops[0]));
        }
    }

    #[test]
    fn filter_refund_requires_valid_process_lock_args_for_the_configured_cid() {
        let (rpc, state, ops) = chain(1);
        let mut invalid_binding = state.lock().args().raw_data().to_vec();
        invalid_binding[0] ^= 1;
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        for args in [Vec::new(), invalid_binding] {
            let lock = state.lock().as_builder().args(args.pack()).build();
            let mut invalid_state = state.clone();
            invalid_state.output = invalid_state.output.as_builder().lock(lock).build();
            loop_.rpc().insert_cell(invalid_state);
            assert_eq!(loop_.tick().submitted, 0);
            assert!(loop_.ssri().filter_requests.lock().unwrap().is_empty());
            assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        }

        loop_.rpc().insert_cell(state);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[0].clone()]]);
    }

    #[test]
    fn filter_refund_requires_matching_node_process_code() {
        let (rpc, _, ops) = chain(1);
        let snapshot = code_cell(0xA0, PROC_CODE_ARGS, [FIXTURE_ELF, &[3]].concat());
        rpc.set_indexer_snapshot(snapshot.out_point.clone(), Some(snapshot));
        let ssri = ScriptedSsri::mimic((2, 2)).with_filter(|n| Ok(vec![6; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn data2_user_lock_is_skipped_before_batch_selection() {
        let (rpc, state, _) = chain(0);
        let mut cell = intention_cell(1, process_type_hash(1), 600_0000_0000);
        let mut user = crate::helpers::user_lock();
        let payload = Payload::from_script(&user).unwrap();
        user.hash_type = ScriptHashType::Data2 as u8;
        let args = CidArgs::new(process_type_hash(1), user, payload)
            .encode()
            .unwrap();
        let lock = cell.lock().as_builder().args(args.pack()).build();
        cell.output = cell.output.as_builder().lock(lock).build();
        let bad = cell.out_point.clone();
        rpc.insert_cell(cell);
        let good = intention_cell(2, process_type_hash(1), 600_0000_0000);
        let good_op = good.out_point.clone();
        rpc.insert_cell(good);
        let ssri = ScriptedSsri::mimic((1, 1));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![good_op, state.out_point]]);
        assert!(loop_.rpc().has_cell(&bad));
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
            let mut cell = filter_intention(tag, 7_000_000_000_000);
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
    fn rejected_guest_funding_can_change_on_the_next_tick() {
        let (rpc, state, ops) = chain(2);
        let funding = out_point(3, 0);
        rpc.insert_cell(live_cell(
            funding.clone(),
            lock_script([0x44; 32], ScriptHashType::Type, vec![0xAA]),
            None,
            100_0000_0000,
            vec![],
        ));
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_filter(|_| Ok(vec![0, 1]))
            .with_added_input(ops[1].clone());
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 0);
        loop_.ssri().set_added_input(funding.clone());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), state.out_point, funding]]
        );
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
    fn isolation_skips_a_subset_with_spent_guest_funding() {
        let (rpc, state, ops) = chain(3);
        let funding_op = out_point(4, 0);
        rpc.insert_cell(live_cell(
            funding_op.clone(),
            lock_script([0x44; 32], ScriptHashType::Type, vec![0xAA]),
            None,
            100_0000_0000,
            vec![],
        ));
        rpc.consume(&funding_op);
        let ssri = ScriptedSsri::mimic((1, 3))
            .with_intention_error(ops[0].clone(), 32)
            .with_added_input_for(ops[1].clone(), funding_op);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[2].clone(), state.out_point]]
        );
        assert!(ops[..2].iter().all(|op| loop_.rpc().has_cell(op)));
    }

    #[test]
    fn unavailable_guest_funding_lookup_is_retried_next_tick() {
        let (rpc, state, ops) = chain(3);
        let funding_op = out_point(4, 0);
        rpc.insert_cell(live_cell(
            funding_op.clone(),
            lock_script([0x44; 32], ScriptHashType::Type, vec![0xAA]),
            None,
            100_0000_0000,
            vec![],
        ));
        rpc.fail_live_cell_once(funding_op.clone());
        let ssri = ScriptedSsri::mimic((1, 3))
            .with_intention_error(ops[0].clone(), 32)
            .with_added_input_for(ops[1].clone(), funding_op.clone());
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), state.out_point, funding_op]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
        assert!(loop_.rpc().has_cell(&ops[2]));
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
    fn filter_refund_cannot_submit_if_process_state_is_replaced_at_submission() {
        let (rpc, state, ops) = chain(1);
        let mut replacement = state.clone();
        replacement.out_point = out_point(0x51, 0);
        let lock = state
            .lock()
            .as_builder()
            .code_hash([0x52; 32].pack())
            .build();
        replacement.output = replacement.output.as_builder().lock(lock).build();
        rpc.replace_cell_on_next_submit(state.out_point.clone(), replacement.clone());
        let ssri = ScriptedSsri::mimic((1, 4)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        let report = loop_.tick();
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_.rpc().has_cell(&out_point(0xA0, 0)));
        assert!(loop_
            .rpc()
            .get_live_cell(&state.out_point)
            .unwrap()
            .is_none());
        assert_eq!(
            loop_.rpc().get_live_cell(&replacement.out_point).unwrap(),
            Some(replacement)
        );
        assert_eq!(report.submitted, 0);
        assert_eq!(report.conflicts, 1);
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
    fn digest_and_filter_refund_rejections_do_not_share_a_cache_entry() {
        for (first, second) in [(0, 5), (5, 0)] {
            let (rpc, state, ops) = chain(1);
            rpc.inject_dry_run_reject_once();
            let mut reason = first;
            let ssri = ScriptedSsri::mimic((1, 1)).with_filter(move |n| {
                let previous = reason;
                reason = second;
                Ok(vec![previous; n])
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);
            assert!(loop_.rpc().has_cell(&ops[0]));

            let report = loop_.tick();
            assert_eq!(report.submitted, 1, "{first} → {second}: {report:?}");
            let mut expected = vec![ops[0].clone()];
            if second == 0 {
                expected.push(state.out_point);
            }
            assert_eq!(input_sets(loop_.rpc()), [expected]);
        }
    }

    #[test]
    fn rejected_filter_refund_is_reconsidered_on_replacement_state() {
        let (rpc, mut state, ops) = chain(1);
        rpc.inject_dry_run_reject_once();
        let ssri = ScriptedSsri::mimic((1, 1)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);

        loop_.rpc().consume(&state.out_point);
        state.out_point = out_point(0xA1, 0);
        loop_.rpc().insert_cell(state.clone());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[0].clone()]]);
        assert!(loop_.rpc().submitted()[0]
            .cell_deps()
            .into_iter()
            .any(|dep| dep.out_point() == state.out_point));
    }

    #[test]
    fn committed_refund_returning_to_pool_blocks_digest_again() {
        let (rpc, state, ops) = chain(2);
        rpc.defer_commit();
        let second = rpc.get_live_cell(&ops[1]).unwrap().unwrap();
        rpc.consume(&ops[1]);
        let mut reason = 5;
        let ssri = ScriptedSsri::mimic((1, 1)).with_filter(move |n| {
            let current = reason;
            reason = 0;
            Ok(vec![current; n])
        });
        let mut config = default_loop_config();
        config.confirmation_depth = 3;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);

        assert_eq!(loop_.tick().submitted, 1);
        let refund = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&refund);
        assert_eq!(loop_.tick().submitted, 0);
        let pending = loop_.state().submit.unwrap().pending;
        assert!(pending[refund.as_slice()].seen_committed);
        assert!(pending[refund.as_slice()].currently_committed);

        loop_.rpc().return_to_pool(&refund);
        loop_.rpc().insert_cell(second);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[0].clone()]]);
        assert!(loop_.rpc().has_cell(&state.out_point));
        assert!(loop_.rpc().has_cell(&ops[1]));
        let pending = loop_.state().submit.unwrap().pending;
        assert!(pending[refund.as_slice()].seen_committed);
        assert!(!pending[refund.as_slice()].currently_committed);

        loop_.rpc().commit_pending(&refund);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone()], vec![ops[1].clone(), state.out_point]]
        );
    }

    #[test]
    fn same_height_reorg_between_refund_status_and_tip_keeps_digest_blocked() {
        let (rpc, state, ops) = chain(2);
        rpc.defer_commit();
        rpc.set_tip(9);
        let accepted = rpc.get_live_cell(&ops[1]).unwrap().unwrap();
        rpc.consume(&ops[1]);
        let mut reason = 5;
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_filter(move |n| Ok(vec![std::mem::take(&mut reason); n]));
        let mut config = default_loop_config();
        config.confirmation_depth = 10;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);

        assert_eq!(loop_.tick().submitted, 1);
        let refund = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&refund);
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.state().submit.unwrap().pending[refund.as_slice()].currently_committed);
        loop_.rpc().insert_cell(accepted);
        loop_.rpc().replace_commit_block_on_next_status();

        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        assert!(!loop_.state().submit.unwrap().pending[refund.as_slice()].currently_committed);
        assert!(loop_.rpc().has_cell(&state.out_point));
        assert!(loop_.rpc().has_cell(&ops[1]));

        loop_.rpc().commit_pending(&refund);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone()], vec![ops[1].clone(), state.out_point],]
        );
    }

    #[test]
    fn filter_refund_proceeds_after_peer_settlement_despite_a_capped_search() {
        let (rpc, _, ops) = chain(68);
        rpc.defer_commit();
        let refundable = ops[67].clone();
        let digest_input = ops[0].clone();
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_filter(|n| {
                let mut reasons = vec![0; n];
                // The refundable item is the last cell in the short second chunk.
                if n < 64 {
                    reasons[n - 1] = 5;
                }
                Ok(reasons)
            })
            .with_assembly_error(move |inputs, _| (!inputs.contains(&digest_input)).then_some(32));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        let digest = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&digest);

        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{report:?}");
        assert_eq!(input_sets(loop_.rpc())[1], [refundable]);
        assert!(ops[1..67].iter().all(|op| loop_.rpc().has_cell(op)));
    }

    #[test]
    fn continuous_arrivals_leave_probes_for_a_larger_batch() {
        let (rpc, state, ops) = chain(80);
        let arrival = filter_intention(83, 600_0000_0000);
        let target = [ops[0].clone(), arrival.out_point.clone()];
        let expected = [target[0].clone(), target[1].clone(), state.out_point];
        let ssri = ScriptedSsri::mimic((1, 3)).with_assembly_error(move |inputs, _| {
            (inputs[..inputs.len() - 1] != target).then_some(32)
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        for tag in 81..=83 {
            loop_
                .rpc()
                .insert_cell(filter_intention(tag, 600_0000_0000));
            assert_eq!(loop_.tick().submitted, usize::from(tag == 83));
        }
        assert_eq!(input_sets(loop_.rpc()), [expected]);
    }

    #[test]
    fn priority_probe_pairs_an_arrival_with_a_distinct_lock_group() {
        let (rpc, state, ops) = chain(80);
        let mut arrival = rpc.get_live_cell(&ops[0]).unwrap().unwrap();
        arrival.out_point = out_point(161, 0);
        let target = [ops[1].clone(), arrival.out_point.clone()];
        let expected = [target[0].clone(), target[1].clone(), state.out_point];
        let ssri = ScriptedSsri::mimic((2, 2))
            .with_assembly_error(move |inputs, _| (inputs[..2] != target).then_some(32));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);

        for tag in 81..160 {
            loop_
                .rpc()
                .insert_cell(filter_intention(tag, 600_0000_0000));
        }
        loop_.rpc().insert_cell(arrival);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [expected]);
    }

    #[test]
    fn continuous_arrivals_do_not_starve_a_filter_refund() {
        let (rpc, _, ops) = chain(80);
        // Keep the reason-5 cell first so filter chunk boundaries do not matter.
        let mut reject_first_chunk = true;
        let ssri = ScriptedSsri::mimic((1, 3))
            .with_filter(move |n| {
                let mut reasons = vec![0; n];
                if std::mem::take(&mut reject_first_chunk) {
                    reasons[0] = 5;
                }
                Ok(reasons)
            })
            .with_assembly_error(|_, _| Some(32));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        for tag in 81..=82 {
            loop_
                .rpc()
                .insert_cell(filter_intention(tag, 600_0000_0000));
            assert_eq!(loop_.tick().submitted, usize::from(tag == 81));
        }
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[0].clone()]]);
        assert!(ops[1..].iter().all(|op| loop_.rpc().has_cell(op)));
    }

    #[test]
    fn unchanged_tip_and_live_set_are_scanned_again_each_tick() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        for (count, max) in [(1, 1), (4, 3), (80, 1)] {
            let (rpc, _, _) = chain(count);
            let attempts = Arc::new(AtomicUsize::new(0));
            let calls = attempts.clone();
            let ssri = ScriptedSsri::mimic((1, max)).with_assembly_error(move |_, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(32)
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);
            let first = attempts.load(Ordering::Relaxed);
            assert_eq!(loop_.tick().submitted, 0);
            assert_eq!(attempts.load(Ordering::Relaxed), 2 * first);
        }
    }

    #[test]
    fn fresh_scan_includes_a_revived_intention() {
        let (rpc, state, ops) = chain(2);
        rpc.mark_indexer_cell_stale(ops[1].clone());
        let ssri = ScriptedSsri::mimic((1, 1)).with_intention_error(ops[0].clone(), 32);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);

        loop_.rpc().restore_indexer_cell_liveness(&ops[1]);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), state.out_point]]
        );
    }

    #[test]
    fn unaudited_exit_37_retries_an_unchanged_candidate_on_the_next_tick() {
        let (rpc, _, _) = chain(1);
        use std::sync::atomic::{AtomicBool, Ordering};

        let fail_once = AtomicBool::new(true);
        let ssri = ScriptedSsri::mimic((1, 1)).with_assembly_error(move |_, _| {
            fail_once.swap(false, Ordering::Relaxed).then_some(37)
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.tick().submitted, 1);
    }

    #[test]
    fn large_search_only_checks_liveness_for_reached_candidates() {
        let (rpc, _, ops) = chain(150);
        let ssri = ScriptedSsri::mimic((1, 1)).with_assembly_error(|_, _| Some(32));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        for _ in 0..2 {
            assert_eq!(loop_.tick().submitted, 0);
            let reads = loop_.rpc().take_live_cell_reads();
            let intention_reads: Vec<_> = ops.iter().filter_map(|op| reads.get(op)).collect();
            assert!(intention_reads.len() <= 65);
            assert!(intention_reads.into_iter().sum::<usize>() <= 130);
        }
    }

    #[test]
    fn failing_singletons_leave_room_for_a_larger_batch_within_one_tick() {
        for (count, (a, b)) in [(10, (8, 9)), (80, (1, 2))] {
            let (rpc, state, ops) = chain(count);
            let viable = [ops[a].clone(), ops[b].clone()];
            let expected = [viable[0].clone(), viable[1].clone(), state.out_point];
            let ssri = ScriptedSsri::mimic((1, 2)).with_assembly_error(move |inputs, _| {
                (inputs[..inputs.len() - 1] != viable).then_some(39)
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 1);
            assert_eq!(input_sets(loop_.rpc()), [expected]);
        }
    }

    #[test]
    fn fresh_probes_recheck_node_liveness_for_an_unchanged_indexer_set() {
        let (rpc, state, ops) = chain(80);
        let revived = ops[1].clone();
        rpc.mark_indexer_cell_stale(revived.clone());
        let viable = [revived.clone(), ops[2].clone()];
        let expected = [viable[0].clone(), viable[1].clone(), state.out_point];
        let ssri = ScriptedSsri::mimic((2, 2))
            .with_assembly_error(move |inputs, _| (inputs[..2] != viable).then_some(39));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        loop_.rpc().restore_indexer_cell_liveness(&revived);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc()), [expected]);
    }

    #[test]
    fn fresh_search_uses_the_current_candidates_and_process_state() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        for replace_state in [false, true] {
            let (rpc, mut state, ops) = chain(80);
            let viable = [ops[0].clone(), ops[1].clone()];
            let changed = Arc::new(AtomicBool::new(false));
            let observe_change = changed.clone();
            let ssri = ScriptedSsri::mimic((2, 2)).with_assembly_error(move |inputs, _| {
                (!observe_change.load(Ordering::Relaxed) || inputs[..2] != viable).then_some(39)
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 0);

            if replace_state {
                loop_.rpc().consume(&state.out_point);
                state.out_point = out_point(0x51, 1);
                loop_.rpc().insert_cell(state.clone());
            } else {
                // Keep the candidate count and every node liveness bit the same.
                loop_.rpc().consume(&ops[79]);
                loop_.rpc().insert_cell(filter_intention(81, 600_0000_0000));
            }
            changed.store(true, Ordering::Relaxed);
            assert_eq!(loop_.tick().submitted, 1);
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[0].clone(), ops[1].clone(), state.out_point]]
            );
        }
    }

    #[test]
    fn rejected_pairs_still_allow_noncontiguous_pairs() {
        for stage in ["dry-run", "submit", "cached"] {
            let (rpc, state, ops) = chain(3);
            let mut loop_ =
                loop_with_resolver(rpc, ScriptedSsri::mimic((2, 2)), default_loop_config(), 1);
            match stage {
                "dry-run" => loop_.rpc().reject_dry_run_when_consuming(ops[1].clone()),
                "submit" => loop_.rpc().reject_when_consuming(&ops[1]),
                _ => {
                    // Let code discovery initialize the rejection cache first.
                    loop_.rpc().inject_unavailable_dry_run_once();
                    assert_eq!(loop_.tick().submitted, 0);
                    let mut code_key = loop_.state().execution_code_key;
                    code_key.extend_from_slice(out_point(0xA0, 0).as_slice());
                    let worker = loop_.workers().get(&process_type_hash(1)).unwrap();
                    worker.with_state_mut(|worker| {
                        for pair in [&ops[..2], &ops[1..]] {
                            worker.submit.rejected_combos.insert(
                                cid_backend_core::submit::combo_fingerprint(
                                    pair,
                                    Some(&state.out_point),
                                    &code_key,
                                ),
                            );
                        }
                    });
                }
            }
            let before = loop_.rpc().dry_run_attempts();
            assert_eq!(loop_.tick().submitted, 1, "{stage}");
            assert_eq!(
                loop_.rpc().dry_run_attempts() - before,
                if stage == "cached" { 1 } else { 3 }
            );
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[0].clone(), ops[2].clone(), state.out_point]]
            );
        }
    }

    #[test]
    fn rewind_between_refund_status_and_tip_keeps_digest_blocked() {
        for (start_tip, rewound_tip) in [(12, 8), (12, 11), (8, 8)] {
            let (rpc, state, ops) = chain(2);
            rpc.defer_commit();
            rpc.set_tip(9);
            let second = rpc.get_live_cell(&ops[1]).unwrap().unwrap();
            rpc.consume(&ops[1]);
            let mut reason = 5;
            let ssri = ScriptedSsri::mimic((1, 1)).with_filter(move |n| {
                let current = reason;
                reason = 0;
                Ok(vec![current; n])
            });
            let mut config = default_loop_config();
            config.confirmation_depth = 10;
            let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);

            assert_eq!(loop_.tick().submitted, 1);
            let refund = loop_.rpc().submitted()[0].hash();
            loop_.rpc().commit_pending(&refund);
            assert_eq!(loop_.tick().submitted, 0);
            assert!(loop_.state().submit.unwrap().pending[refund.as_slice()].currently_committed);
            loop_.rpc().insert_cell(second);
            loop_.rpc().set_tip(start_tip);
            loop_.rpc().rewind_tip_on_next_status(rewound_tip);

            assert_eq!(loop_.tick().submitted, 0, "rewound to {rewound_tip}");
            assert!(!loop_.state().submit.unwrap().pending[refund.as_slice()].currently_committed);
            assert!(loop_.rpc().has_cell(&state.out_point));
            assert!(loop_.rpc().has_cell(&ops[1]));
            loop_.rpc().set_tip(12);
            assert_eq!(loop_.tick().submitted, 1);
        }
    }

    #[test]
    fn filter_refund_sweep_is_bounded_and_retries_after_unavailability() {
        let (rpc, _, ops) = chain(33);
        rpc.inject_unavailable_dry_run_once();
        let ssri = ScriptedSsri::mimic((2, 4)).with_filter(|n| Ok(vec![5; n]));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 1);
        loop_.rpc().inject_unavailable_refund_once(ops[1].clone());
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().dry_run_attempts(), 2);
        // A fresh sweep varies its start and considers at most 16 cells.
        assert_eq!(loop_.tick().submitted, 16);
        assert!(loop_.rpc().has_cell(&ops[0]));
        assert_eq!(loop_.tick().submitted, 16);
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
        for invalid in [
            Ok(vec![]),
            Ok(vec![2]),
            Ok(vec![3]),
            Ok(vec![99]),
            Err(SsriError::Unusable("invalid response".into())),
        ] {
            let (rpc, _, _) = chain(65);
            let mut calls = 0;
            let ssri = ScriptedSsri::mimic((1, 100)).with_filter(move |count| {
                calls += 1;
                match calls {
                    1 => Ok(vec![5; count]),
                    2 => invalid.clone(),
                    _ => Ok(vec![0; count]),
                }
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            let report = loop_.tick();
            assert_eq!(report.submitted, 0);
            assert!(report.last_error.is_some());
            assert_eq!(loop_.rpc().submit_attempts(), 0);
            assert_eq!(loop_.rpc().dry_run_attempts(), 0);
            assert_eq!(loop_.tick().submitted, 1);
        }
    }

    #[test]
    fn first_assembly_errors_alert_and_make_progress_without_refunds() {
        for code in [21, 32, 37, 39, 40, 42] {
            let (rpc, state, ops) = chain(3);
            set_below_minimum_deposit(&rpc, &ops[0]);
            let ssri = ScriptedSsri::mimic((2, 2)).with_intention_error(ops[0].clone(), code);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            let report = loop_.tick();
            assert_eq!(report.submitted, 1, "exit {code}: {report:?}");
            assert!(
                report
                    .last_error
                    .as_deref()
                    .is_some_and(|message| message.contains(&format!("exit {code}"))),
                "exit {code}: {report:?}"
            );
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[1].clone(), ops[2].clone(), state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[0]), "exit {code}");
        }
    }

    #[test]
    fn exhausted_shrink_keeps_queued_sibling_eligible() {
        let (rpc, state, ops) = chain(3);
        rpc.inject_dry_run_reject_once();
        let shrink = ops[..2].to_vec();
        let ssri = ScriptedSsri::mimic((2, 3)).with_assembly_error(move |inputs, _| {
            (inputs[..inputs.len() - 1] == shrink).then_some(39)
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), ops[2].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn combination_failure_excludes_the_cell_without_refunding_it() {
        for code in [39, 40] {
            let (rpc, state, ops) = chain(4);
            rpc.defer_commit();
            let ssri = ScriptedSsri::mimic((2, 3))
                .with_pair_error(ops[0].clone(), code)
                .with_intention_error(ops[1].clone(), 39);
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 1);
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[2].clone(), ops[3].clone(), state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[0]));
        }
    }

    #[test]
    fn batch_size_dependent_errors_can_submit_a_smaller_digest() {
        for code in [21, 37, 39, 40] {
            let (rpc, state, ops) = chain(3);
            let ssri = ScriptedSsri::mimic((2, 4))
                .with_assembly_error(move |inputs, _| (inputs.len() == 4).then_some(code));
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            let report = loop_.tick();
            assert_eq!(report.submitted, 1, "exit {code}: {report:?}");
            assert!(
                report
                    .last_error
                    .as_deref()
                    .is_some_and(|message| message.contains(&format!("exit {code}"))),
                "exit {code}: {report:?}"
            );
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![ops[0].clone(), ops[1].clone(), state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[2]));
        }
    }

    #[test]
    fn isolation_can_find_a_working_batch_above_the_minimum_size() {
        for code in [21, 37] {
            let (rpc, state, ops) = chain(3);
            let compatible = [ops[0].clone(), ops[2].clone()];
            let pair = compatible.clone();
            let ssri = ScriptedSsri::mimic((1, 3)).with_assembly_error(move |inputs, _| {
                (inputs.len() != 3 || !pair.iter().all(|op| inputs.contains(op))).then_some(code)
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
            assert_eq!(loop_.tick().submitted, 1, "exit {code}");
            let [first, last] = compatible;
            assert_eq!(
                input_sets(loop_.rpc()),
                [vec![first, last, state.out_point]]
            );
            assert!(loop_.rpc().has_cell(&ops[1]));
        }
    }

    #[test]
    fn singleton_is_reconsidered_when_external_policy_changes() {
        let (rpc, state, ops) = chain(1);
        let tip = rpc.tip();
        rpc.insert_block_hash(tip, [0x91; 32]);
        let ssri = ScriptedSsri::mimic((1, 4)).with_assembly_error(move |_, host| {
            (host.get_block_hash(tip) == Some([0x91; 32])).then_some(39)
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 0);
        loop_.rpc().insert_block_hash(tip, [0x92; 32]);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), state.out_point]]
        );
        assert_eq!(loop_.rpc().submit_attempts(), 1);
    }

    #[test]
    fn isolation_advances_after_a_failed_host_query() {
        let (rpc, state, ops) = chain(4);
        let ssri = ScriptedSsri::mimic((2, 4))
            .with_pair_error(ops[0].clone(), 36)
            .with_intention_error(ops[1].clone(), 39);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[2].clone(), ops[3].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
        assert!(loop_.rpc().has_cell(&ops[1]));
    }

    #[test]
    fn replacement_state_reconsiders_a_cell_excluded_on_exit_39() {
        let (rpc, state, ops) = chain(1);
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
    fn isolation_can_use_accepted_peers_across_a_window_boundary() {
        let (rpc, state, ops) = chain(4);
        let ssri = ScriptedSsri::mimic((2, 2))
            .with_intention_error(ops[0].clone(), 39)
            .with_intention_error(ops[2].clone(), 32);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let first = loop_.tick();
        assert_eq!(first.submitted, 1, "{first:?}");
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[1].clone(), ops[3].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
        assert!(loop_.rpc().has_cell(&ops[2]));
    }

    #[test]
    fn filter_refund_retries_after_peer_commit_and_refund_unavailability() {
        let (rpc, state, ops) = chain(3);
        rpc.defer_commit();
        rpc.inject_unavailable_refund_once(ops[1].clone());
        let ssri = ScriptedSsri::mimic((2, 4))
            .with_filter(|n| Ok(if n == 3 { vec![0, 5, 0] } else { vec![5; n] }));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![
                ops[0].clone(),
                ops[2].clone(),
                state.out_point.clone()
            ]]
        );
        let digest = &loop_.rpc().submitted()[0];
        loop_.rpc().commit_pending(&digest.hash());
        assert_eq!(
            loop_.tick().submitted,
            0,
            "first refund submit is unavailable"
        );
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc())[1], [ops[1].clone()]);
        let deps: Vec<_> = loop_.rpc().submitted()[1]
            .cell_deps()
            .into_iter()
            .map(|dep| dep.out_point())
            .collect();
        assert!(!deps.contains(&state.out_point));
        assert!(deps.iter().any(|dep| dep.tx_hash() == digest.hash()));
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn timed_out_digest_send_blocks_its_filter_refund_in_the_same_tick() {
        let (rpc, _, ops) = chain(2);
        rpc.defer_commit();
        rpc.lose_next_submit_response();
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_filter(|n| Ok(if n == 2 { vec![0, 5] } else { vec![5] }));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        let hash = loop_.rpc().submitted()[0].hash();
        assert!(loop_
            .state()
            .submit
            .unwrap()
            .pending
            .contains_key(hash.as_slice()));
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);

        loop_.rpc().commit_pending(&hash);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc())[1], [ops[1].clone()]);
    }

    #[test]
    fn timed_out_refund_send_blocks_digest_until_status_reconciles() {
        let (rpc, state, ops) = chain(2);
        rpc.defer_commit();
        rpc.lose_next_submit_response();
        let mut first_filter = true;
        let ssri = ScriptedSsri::mimic((1, 1)).with_filter(move |n| {
            Ok(if std::mem::take(&mut first_filter) {
                vec![5, 1]
            } else if n == 2 {
                vec![1, 0]
            } else {
                vec![0]
            })
        });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        let hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().fail_status_queries();
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_
            .state()
            .submit
            .unwrap()
            .pending
            .contains_key(hash.as_slice()));

        loop_.rpc().restore_status_queries();
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);

        loop_.rpc().simulate_drop(&hash);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc())[1],
            [ops[1].clone(), state.out_point]
        );
    }

    #[test]
    fn deferred_filter_refund_gets_a_turn_with_more_digests_ready() {
        let (rpc, state, ops) = chain(3);
        rpc.defer_commit();
        let ssri = ScriptedSsri::mimic((1, 1)).with_filter(|n| {
            Ok(match n {
                3 => vec![0, 0, 5],
                2 => vec![0, 5],
                _ => vec![0; n],
            })
        });
        let mut config = default_loop_config();
        config.confirmation_depth = 24;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![ops[0].clone(), state.out_point]]
        );
        loop_
            .rpc()
            .commit_pending(&loop_.rpc().submitted()[0].hash());

        assert_eq!(loop_.tick().submitted, 0, "digest still confirming");
        loop_.rpc().advance_tip(23);
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc())[1], [ops[2].clone()]);
        assert_eq!(loop_.tick().submitted, 0, "refund still needs this state");
        loop_
            .rpc()
            .commit_pending(&loop_.rpc().submitted()[1].hash());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(input_sets(loop_.rpc())[2][0], ops[1]);
    }

    #[test]
    fn dropped_filter_refund_requires_a_fresh_verdict_when_the_indexer_catches_up() {
        for still_rejected in [false, true] {
            let (rpc, _, ops) = chain(1);
            rpc.defer_commit();
            let mut reason = 5;
            let ssri = ScriptedSsri::mimic((2, 4)).with_filter(move |n| {
                let previous = reason;
                if !still_rejected {
                    reason = 0;
                }
                Ok(vec![previous; n])
            });
            let mut config = default_loop_config();
            config.confirmation_depth = 2;
            let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
            assert_eq!(loop_.tick().submitted, 1);
            let refund = &loop_.rpc().submitted()[0];
            loop_.rpc().commit_pending(&refund.hash());
            assert_eq!(loop_.tick().scanned, 0);
            loop_.rpc().simulate_drop(&refund.hash());
            loop_.rpc().set_indexer_snapshot(ops[0].clone(), None);
            let lagging = loop_.tick();
            assert_eq!(lagging.scanned, 0);
            assert_eq!(lagging.submitted, 0);
            assert!(loop_.rpc().has_cell(&ops[0]));

            loop_.rpc().clear_indexer_snapshot(&ops[0]);
            assert_eq!(loop_.tick().submitted, usize::from(still_rejected));
            if still_rejected {
                assert_eq!(
                    input_sets(loop_.rpc()),
                    [vec![ops[0].clone()], vec![ops[0].clone()]]
                );
            } else {
                assert!(loop_.rpc().has_cell(&ops[0]));
            }
            assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 2);
        }
    }

    #[test]
    fn other_guest_can_assemble_a_later_self_funded_batch_after_exit_37() {
        let (rpc, mut state, ops) = chain(1);
        let occupied =
            cid_backend_core::cell_occupied_capacity(&state.output, &state.data).unwrap();
        state.output = state.output.as_builder().capacity(occupied.pack()).build();
        rpc.insert_cell(state.clone());

        let ssri = ScriptedSsri::fixed_beneficiary((1, 1), 599_0000_0000)
            .with_intention_error(ops[0].clone(), 37);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert!(first.last_error.as_deref().unwrap().contains("exit 37"));

        let funded = filter_intention(2, 600_0000_0000);
        loop_.rpc().insert_cell(funded.clone());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![funded.out_point, state.out_point]]
        );
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.outputs().get(0).unwrap(), state.output);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn revived_intention_is_retried_while_failing_arrivals_keep_coming() {
        let (rpc, state, ops) = chain(80);
        let revived = ops[1].clone();
        rpc.mark_indexer_cell_stale(revived.clone());
        let target = vec![revived.clone(), ops[2].clone()];
        let ssri = ScriptedSsri::mimic((2, 2))
            .with_assembly_error(move |inputs, _| (inputs[..2] != target).then_some(39));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);
        for tag in 81..=83 {
            loop_
                .rpc()
                .insert_cell(filter_intention(tag, 600_0000_0000));
            assert_eq!(loop_.tick().submitted, 0);
        }

        loop_.rpc().restore_indexer_cell_liveness(&revived);
        loop_.rpc().insert_cell(filter_intention(84, 600_0000_0000));
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![revived, ops[2].clone(), state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn fresh_probes_reach_a_revived_intention_beyond_the_initial_budget() {
        let (rpc, state, ops) = chain(80);
        let revived = ops[79].clone();
        rpc.mark_indexer_cell_stale(revived.clone());
        let target = revived.clone();
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_assembly_error(move |inputs, _| (!inputs.contains(&target)).then_some(39));
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        assert_eq!(loop_.tick().submitted, 0);

        loop_.rpc().restore_indexer_cell_liveness(&revived);
        for tag in 81..=96 {
            loop_
                .rpc()
                .insert_cell(filter_intention(tag, 600_0000_0000));
            if loop_.tick().submitted > 0 {
                break;
            }
        }
        assert_eq!(input_sets(loop_.rpc()), [vec![revived, state.out_point]]);
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    #[ignore = "requires the exact deployed v0.1.4 guest; see tests/fixtures/README.md"]
    fn audited_guest_with_zero_fee_headroom_waits_for_state_or_code_change() {
        let path = std::env::var_os("CID_PROCESS_EXAMPLE_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/cid_process_example")
            });
        let audited = std::fs::read(path).expect("exact deployed v0.1.4 guest");
        for replace_state in [true, false] {
            let (rpc, mut state, ops) = chain(100);
            rpc.insert_cell(code_cell(0xA0, PROC_CODE_ARGS, audited.clone()));
            let occupied =
                cid_backend_core::cell_occupied_capacity(&state.output, &state.data).unwrap();
            state.output = state.output.as_builder().capacity(occupied.pack()).build();
            rpc.insert_cell(state.clone());
            let poor_state = state.out_point.clone();
            let upgraded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let guest_upgraded = upgraded.clone();
            let ssri = ScriptedSsri::mimic((1, 4)).with_assembly_error(move |inputs, _| {
                (inputs.contains(&poor_state)
                    && !guest_upgraded.load(std::sync::atomic::Ordering::Relaxed))
                .then_some(37)
            });
            let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

            let first = loop_.tick();
            assert!(first.last_error.as_deref().unwrap().contains("exit 37"));
            assert_eq!(loop_.ssri().seen_binaries.lock().unwrap().len(), 2);
            loop_
                .rpc()
                .insert_cell(filter_intention(101, 600_0000_0000));
            for _ in 0..5 {
                assert_eq!(loop_.tick().submitted, 0);
            }
            assert_eq!(loop_.ssri().seen_binaries.lock().unwrap().len(), 2);
            assert_eq!(loop_.rpc().submit_attempts(), 0);
            assert!(ops.iter().all(|op| loop_.rpc().has_cell(op)));

            if replace_state {
                loop_.rpc().consume(&state.out_point);
                let mut funded = process_cell(1, 1000_0000_0000, vec![]);
                funded.out_point = out_point(0x51, 1);
                loop_.rpc().insert_cell(funded);
            } else {
                loop_.rpc().consume(&out_point(0xA0, 0));
                let mut updated = FIXTURE_ELF.to_vec();
                updated.push(0);
                loop_
                    .rpc()
                    .insert_cell(code_cell(0xA1, PROC_CODE_ARGS, updated));
                upgraded.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            assert_eq!(loop_.tick().submitted, 1);
        }
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
    fn new_candidate_is_included_in_the_next_tick() {
        let (rpc, state, ops) = chain(1);
        let ssri = ScriptedSsri::mimic((1, 1)).with_intention_error(ops[0].clone(), 32);
        let arrival = intention_cell(2, process_type_hash(1), 600_0000_0000);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 0);
        loop_.rpc().insert_cell(arrival.clone());
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(
            input_sets(loop_.rpc()),
            [vec![arrival.out_point, state.out_point]]
        );
        assert!(loop_.rpc().has_cell(&ops[0]));
    }

    #[test]
    fn filter_refund_does_not_wait_for_a_capped_assembly_scan_to_finish() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let (rpc, _, ops) = chain(67);
        let attempts = Arc::new(AtomicUsize::new(0));
        let calls = attempts.clone();
        let ssri = ScriptedSsri::mimic((1, 1))
            .with_filter(|n| {
                let mut reasons = vec![0; n];
                if n == 3 {
                    reasons[2] = 5;
                }
                Ok(reasons)
            })
            .with_assembly_error(move |_, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                Some(32)
            });
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);

        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(attempts.load(Ordering::Relaxed), 64);
        assert_eq!(input_sets(loop_.rpc()), [vec![ops[66].clone()]]);
        assert!(ops[..66].iter().all(|op| loop_.rpc().has_cell(op)));
    }

    #[test]
    fn exit_41_during_a_probe_quarantines_the_state_and_suppresses_filter_refunds() {
        let (rpc, _, ops) = chain(4);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(|_| Ok(vec![0, 0, 0, 5]))
            .with_intention_error(ops[0].clone(), 39)
            .with_intention_error(ops[1].clone(), 32)
            .with_intention_error(ops[2].clone(), 41);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report.last_error.unwrap().contains("exit 41"));
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 0);
        assert!(ops.iter().all(|op| loop_.rpc().has_cell(op)));
    }

    #[test]
    fn process_lock_beneficiary_rejection_leaves_the_intention_live() {
        use cid_protocol::{CidArgs, Payload, Script};

        for rejects in [false, true] {
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
            let state_op = state.out_point.clone();
            rpc.insert_cell(item);
            let config = default_loop_config();
            let ssri = ScriptedSsri::mimic((1, 4));
            let ssri = if rejects {
                ssri.with_intention_error(op.clone(), 32)
            } else {
                ssri
            };
            let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
            let report = loop_.tick();
            assert_eq!(report.submitted, usize::from(!rejects));
            if rejects {
                assert!(report.last_error.unwrap().contains("exit 32"));
                assert_eq!(loop_.rpc().submit_attempts(), 0);
                assert!(loop_.rpc().has_cell(&op));
            } else {
                assert_eq!(input_sets(loop_.rpc()), [vec![op, state_op]]);
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
        let (rpc, _, ops) = chain(2);
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(|_| Ok(vec![0, 5]))
            .with_intention_error(ops[0].clone(), 41);
        let mut loop_ = loop_with_resolver(rpc, ssri, default_loop_config(), 1);
        let report = loop_.tick();
        assert!(report.last_error.unwrap().contains("41"));
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.tick().submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn process_fault_still_blocks_refunds_when_the_faulted_code_returns() {
        let (rpc, _, ops) = chain(2);
        rpc.set_tip(100);
        let faulted_code = rpc.get_live_cell(&out_point(0xA0, 0)).unwrap().unwrap();
        let upgraded_code = code_cell(0xA1, PROC_CODE_ARGS, [FIXTURE_ELF, &[2]].concat());
        let mut passes = [vec![0, 5], vec![1], vec![5]].into_iter();
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(move |_| Ok(passes.next().unwrap()))
            .with_intention_error(ops[0].clone(), 41);
        let mut config = default_loop_config();
        config.confirmation_depth = 24;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        let report = loop_.tick();
        assert!(report.last_error.unwrap().contains("exit 41"));

        loop_.rpc().consume(&ops[0]);
        loop_.rpc().consume(&faulted_code.out_point);
        loop_.rpc().insert_cell(upgraded_code.clone());
        loop_.rpc().set_tip(150);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 2);

        loop_.rpc().consume(&upgraded_code.out_point);
        loop_.rpc().insert_cell(faulted_code);
        loop_.rpc().set_tip(149);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 2);
        assert!(loop_.rpc().has_cell(&ops[1]));
    }

    #[test]
    fn unrelated_replacements_do_not_expire_a_faulted_state_code_pair() {
        let (rpc, _, ops) = chain(2);
        rpc.set_tip(100);
        let faulted = rpc.get_live_cell(&out_point(0xA0, 0)).unwrap().unwrap();
        let replacement_a = code_cell(0xA1, PROC_CODE_ARGS, [FIXTURE_ELF, &[1]].concat());
        let replacement_b = code_cell(0xA2, PROC_CODE_ARGS, [FIXTURE_ELF, &[2]].concat());
        let mut passes = [vec![0, 5], vec![1], vec![1], vec![5]].into_iter();
        let ssri = ScriptedSsri::mimic((1, 4))
            .with_filter(move |_| Ok(passes.next().unwrap()))
            .with_intention_error(ops[0].clone(), 41);
        let mut config = default_loop_config();
        config.confirmation_depth = 24;
        let mut loop_ = loop_with_resolver(rpc, ssri, config, 1);
        assert!(loop_.tick().last_error.unwrap().contains("exit 41"));

        loop_.rpc().consume(&ops[0]);
        loop_.rpc().consume(&faulted.out_point);
        loop_.rpc().insert_cell(replacement_a.clone());
        loop_.rpc().set_tip(101);
        assert_eq!(loop_.tick().submitted, 0);

        loop_.rpc().consume(&replacement_a.out_point);
        loop_.rpc().insert_cell(replacement_b.clone());
        loop_.rpc().set_tip(125);
        assert_eq!(loop_.tick().submitted, 0);

        loop_.rpc().consume(&replacement_b.out_point);
        loop_.rpc().insert_cell(faulted);
        loop_.rpc().set_tip(124);
        assert_eq!(loop_.tick().submitted, 0);
        assert_eq!(loop_.ssri().filter_requests.lock().unwrap().len(), 3);
        assert!(loop_.rpc().has_cell(&ops[1]));
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
