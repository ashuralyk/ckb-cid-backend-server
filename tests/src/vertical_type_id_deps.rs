#[cfg(test)]
mod tests {
    #![allow(clippy::inconsistent_digit_grouping)]

    use std::time::Duration;

    use cid_backend_core::digest_loop::LoopConfig;
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::out_point_key;
    use cid_backend_core::type_id::{TypeIdCodeDep, TypeIdIdentity};
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, CellDep, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, fixture_process_entry_with_code, intention_cell, loop_with_resolver,
        loop_with_resolver_and_code, process_cell, process_cell_for_code, process_type_hash,
        FIXTURE_ELF, PROC_CODE_ARGS,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn code_cell(tx_byte: u8, args: [u8; 32], data: Vec<u8>) -> cid_backend_core::rpc::LiveCell {
        let type_script = cid_backend_core::type_id::type_id_script(&args);
        let lock = Script::new_builder()
            .code_hash(Byte32::from_slice(&[0x99; 32]).unwrap())
            .hash_type(ScriptHashType::Data1.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let output = CellOutput::new_builder()
            .capacity(1_000_0000_0000u64.pack())
            .lock(lock)
            .type_(Some(type_script).pack())
            .build();
        cid_backend_core::rpc::LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[tx_byte; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output,
            data,
        }
    }

    fn cfg(type_id_code_deps: Vec<TypeIdCodeDep>, extra: Vec<CellDep>) -> LoopConfig {
        LoopConfig {
            cid_code_hash: crate::helpers::CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: extra,
            type_id_code_deps,
            confirmation_depth: 1,
            max_assembled_tx_bytes: 512 * 1024,
        }
    }

    /// Worker Process code is resolved from `ProcessEntry.code`, not LoopConfig.
    fn worker_code_args() -> [u8; 32] {
        [0xb2; 32]
    }

    #[test]
    fn type_id_cid_lock_dep_resolves_before_digest_ok() {
        let cid_args = [0xa1; 32];
        let code = code_cell(0x10, cid_args, vec![0x7f, b'E', b'L', b'F']);
        let rpc = FakeRpc::new();
        let hash = process_type_hash(1);
        let code_args = worker_code_args();
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(process_cell_for_code(1, 200_0000_0000, vec![], code_args));
        rpc.insert_cell(code_cell(0x11, code_args, FIXTURE_ELF.to_vec()));
        rpc.insert_cell(code.clone());

        let mut loop_ = loop_with_resolver_and_code(
            rpc,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(cid_args),
                }],
                vec![],
            ),
            1,
            code_args,
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
    }

    #[test]
    fn missing_type_id_cid_lock_dep_fails_tick_without_rejecting_intention() {
        let cid_args = [0xa3; 32];
        let rpc = FakeRpc::new();
        let hash = process_type_hash(4);
        let code_args = PROC_CODE_ARGS;
        let intent = intention_cell(4, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(4, 200_0000_0000, vec![]));
        rpc.insert_cell(code_cell(0x30, code_args, FIXTURE_ELF.to_vec()));

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(cid_args),
                }],
                vec![],
            ),
            4,
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(!report.scan_ok);
        assert!(report
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("missing"));
        assert!(loop_.rpc().has_cell(&intent.out_point));
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .unwrap()
            .rejected
            .contains(&out_point_key(&intent.out_point)));

        loop_.rpc().insert_cell(code_cell(0x31, cid_args, vec![1]));
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 1, "{:?}", report2.last_error);
    }

    #[test]
    fn ambiguous_type_id_cells_fail_closed_without_submit() {
        let cid_args = [0xa4; 32];
        let rpc = FakeRpc::new();
        let hash = process_type_hash(5);
        rpc.insert_cell(intention_cell(5, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(5, 200_0000_0000, vec![]));
        rpc.insert_cell(code_cell(0x41, cid_args, vec![1]));
        rpc.insert_cell(code_cell(0x42, cid_args, vec![2]));

        let mut loop_ = loop_with_resolver(
            rpc,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(cid_args),
                }],
                vec![],
            ),
            5,
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("ambiguous"));
    }

    #[test]
    fn process_ssri_uses_worker_code_cell_not_state_data() {
        let code_args = worker_code_args();
        let state_elf = vec![0x7f, b'E', b'L', b'F', 0xAA];
        let code_elf = vec![0x7f, b'E', b'L', b'F', 0xBB];
        let rpc = FakeRpc::new();
        let hash = process_type_hash(13);
        rpc.insert_cell(intention_cell(13, hash, 100_0000_0000));
        rpc.insert_cell(process_cell_for_code(
            13,
            200_0000_0000,
            state_elf.clone(),
            code_args,
        ));
        rpc.insert_cell(code_cell(0x72, code_args, code_elf.clone()));

        let ssri = ScriptedSsri::mimic((1, 4));
        let mut loop_ =
            loop_with_resolver_and_code(rpc, ssri, default_loop_config(), 13, code_args);
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        let seen = loop_.ssri().seen_binaries.lock().unwrap().clone();
        assert!(seen.iter().any(|b| b == &code_elf));
        assert!(seen.iter().all(|b| b != &state_elf));
    }

    /// Type-ID execution_code_key rotation must clear isolate_pending and
    /// isolate_resume (same as rejected_combos / isolate_expanded).
    #[test]
    fn type_id_execution_code_key_rotation_clears_isolate_pending_and_resume() {
        let cid_args = [0xa9; 32];
        let code_args = worker_code_args();
        let rpc = FakeRpc::new();
        let hash = process_type_hash(40);
        rpc.insert_cell(process_cell_for_code(40, 200_0000_0000, vec![], code_args));
        rpc.insert_cell(code_cell(0x11, code_args, FIXTURE_ELF.to_vec()));
        let cid_v1 = code_cell(0x10, cid_args, vec![0x7f, b'E', b'L', b'F']);
        rpc.insert_cell(cid_v1.clone());

        let mut loop_ = loop_with_resolver_and_code(
            rpc,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(cid_args),
                }],
                vec![],
            ),
            40,
            code_args,
        );
        let _ = loop_.tick();

        loop_.workers().get(&hash).unwrap().with_state_mut(|s| {
            s.submit.rejected_combos.insert(vec![0xAA]);
            s.isolate_expanded.insert(vec![0xBB]);
            s.isolate_pending.push(Vec::new());
            s.isolate_resume = Some(vec![0xCC]);
        });

        loop_.rpc().consume(&cid_v1.out_point);
        loop_
            .rpc()
            .insert_cell(code_cell(0x20, cid_args, vec![0x7f, b'E', b'L', b'F']));
        let _ = loop_.tick();

        loop_.workers().get(&hash).unwrap().with_state(|s| {
            assert!(
                s.submit.rejected_combos.is_empty(),
                "rejected_combos must clear on Type-ID code rotation"
            );
            assert!(
                s.isolate_expanded.is_empty(),
                "isolate_expanded must clear on Type-ID code rotation"
            );
            assert!(
                s.isolate_pending.is_empty(),
                "isolate_pending must clear on Type-ID code rotation"
            );
            assert!(
                s.isolate_resume.is_none(),
                "isolate_resume must clear on Type-ID code rotation"
            );
        });
    }

    #[test]
    fn process_code_upgrade_refreshes_ssri_binary() {
        let code_args = [0xb3; 32];
        let rpc = FakeRpc::new();
        let hash = process_type_hash(14);
        rpc.insert_cell(intention_cell(14, hash, 100_0000_0000));
        rpc.insert_cell(process_cell_for_code(
            14,
            200_0000_0000,
            vec![0x00],
            code_args,
        ));
        let code_v1 = code_cell(0x73, code_args, vec![0x7f, b'E', b'L', b'F', 0x01]);
        rpc.insert_cell(code_v1.clone());

        let ssri = ScriptedSsri::mimic((1, 4));
        let mut loop_ =
            loop_with_resolver_and_code(rpc, ssri, default_loop_config(), 14, code_args);
        assert_eq!(loop_.tick().submitted, 1);
        assert!(loop_
            .ssri()
            .seen_binaries
            .lock()
            .unwrap()
            .iter()
            .any(|b| b.last() == Some(&0x01)));

        loop_.rpc().consume(&code_v1.out_point);
        let code_v2 = code_cell(0x74, code_args, vec![0x7f, b'E', b'L', b'F', 0x02]);
        loop_.rpc().insert_cell(code_v2.clone());
        let entry15 = fixture_process_entry_with_code(15, code_args);
        loop_.register_process(entry15.clone());
        if let Some(w) = loop_.workers().get(&entry15.identity_type_hash) {
            w.with_state_mut(|state| {
                state.resolver =
                    ProcessResolver::for_entry(entry15).with_fallback_binary(FIXTURE_ELF.to_vec());
            });
        }
        let hash2 = process_type_hash(15);
        loop_
            .rpc()
            .insert_cell(intention_cell(15, hash2, 100_0000_0000));
        loop_.rpc().insert_cell(process_cell_for_code(
            15,
            200_0000_0000,
            vec![0x00],
            code_args,
        ));
        loop_.ssri().seen_binaries.lock().unwrap().clear();

        assert_eq!(loop_.tick().submitted, 1);
        assert!(loop_
            .ssri()
            .seen_binaries
            .lock()
            .unwrap()
            .iter()
            .any(|b| b.last() == Some(&0x02)));
    }

    #[test]
    fn process_state_wrong_lock_skips_without_poison() {
        let code_args = PROC_CODE_ARGS;
        let rpc = FakeRpc::new();
        let hash = process_type_hash(20);
        let intent = intention_cell(20, hash, 100_0000_0000);
        let mut bad = process_cell(20, 200_0000_0000, vec![]);
        let wrong_lock = Script::new_builder()
            .code_hash([0xEE; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        bad.output = CellOutput::new_builder()
            .capacity(bad.output.capacity())
            .lock(wrong_lock)
            .type_(bad.output.type_())
            .build();
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(bad);
        rpc.insert_cell(code_cell(0x80, code_args, FIXTURE_ELF.to_vec()));

        let mut loop_ =
            loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), default_loop_config(), 20);
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(loop_.rpc().has_cell(&intent.out_point));
    }

    #[test]
    fn unregistered_process_type_hash_intention_is_skipped() {
        let rpc = FakeRpc::new();
        let unknown = process_type_hash(99);
        let intent = intention_cell(1, unknown, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(99, 200_0000_0000, vec![]));

        let mut loop_ =
            loop_with_resolver(rpc, ScriptedSsri::mimic((1, 4)), default_loop_config(), 1);
        let report = loop_.tick();
        assert_eq!(report.scanned, 1);
        assert_eq!(report.submitted, 0, "{:?}", report.last_error);
        assert!(report.skipped >= 1);
        assert!(loop_.rpc().submitted().is_empty());
        assert!(!loop_
            .state()
            .submit
            .as_ref()
            .map(|s| s.rejected.contains(&out_point_key(&intent.out_point)))
            .unwrap_or(false));
    }

    #[test]
    #[ignore = "legacy extra_scan_locks / bounded_refund scan removed from LoopConfig"]
    fn legacy_scan_and_bounded_refund_tests_removed() {}
}
