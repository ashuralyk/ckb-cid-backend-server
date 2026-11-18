#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::code_deps::PROCESS_CODE_LABEL;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};
    use cid_backend_core::type_id::{
        type_id_script, type_id_script_hash, TypeIdCodeDep, TypeIdIdentity,
    };
    use ckb_types::core::{DepType, ScriptHashType};
    use ckb_types::packed::{Byte32, CellDep, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_cell_for_code, process_type_script,
        process_type_script_for_code, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn code_cell(tx_byte: u8, args: [u8; 32], data: Vec<u8>) -> cid_backend_core::rpc::LiveCell {
        let type_script = type_id_script(&args);
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
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: extra,
            type_id_code_deps,
            confirmation_depth: 1,
            fee_rate: 1000,
            bounded_refund_fee: false,
        }
    }

    fn process_code_resolver(args: [u8; 32]) -> ProcessResolver {
        ProcessResolver::with_type_id_process_code(type_id_script_hash(&args))
    }

    fn process_for_type_id(
        unique: u8,
        capacity: u64,
        data: Vec<u8>,
        code_args: [u8; 32],
    ) -> ([u8; 32], cid_backend_core::rpc::LiveCell) {
        let code_hash = type_id_script_hash(&code_args);
        let ts = process_type_script_for_code(code_hash, unique);
        let hash = script_hash32(&ts);
        let cell = process_cell_for_code(unique, capacity, data, code_hash);
        (hash, cell)
    }

    #[test]
    fn type_id_discovery_attaches_resolved_cell_deps_to_digest_tx() {
        let args = [0xa1; 32];
        let code = code_cell(0x10, args, vec![0x7f, b'E', b'L', b'F']);
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(
            1,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 1],
        ));
        rpc.insert_cell(code.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        let tx = &loop_.rpc().submitted()[0];
        let deps: Vec<_> = tx.cell_deps().into_iter().collect();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].out_point(), code.out_point);
        assert_eq!(deps[0].dep_type(), DepType::Code.into());
    }

    #[test]
    fn type_id_outpoint_change_between_ticks_uses_new_dep() {
        let args = [0xa2; 32];
        let rpc = FakeRpc::new();
        let (hash, proc) = process_for_type_id(2, 200_0000_0000, vec![0x00], args);
        let intent = intention_cell(2, hash, 100_0000_0000);
        let code_v1 = code_cell(0x21, args, vec![0x7f, b'E', b'L', b'F', 1]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc);
        rpc.insert_cell(code_v1.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: PROCESS_CODE_LABEL.into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            process_code_resolver(args),
        );
        assert_eq!(
            loop_.tick().submitted,
            1,
            "v1 must submit with ELF code cell"
        );
        let first_dep = loop_.rpc().submitted()[0].cell_deps().get(0).unwrap();
        assert_eq!(first_dep.out_point(), code_v1.out_point);

        loop_.rpc().consume(&code_v1.out_point);
        let code_v2 = code_cell(0x22, args, vec![0x7f, b'E', b'L', b'F', 2]);
        loop_.rpc().insert_cell(code_v2.clone());
        let (hash2, proc2) = process_for_type_id(3, 200_0000_0000, vec![0x00], args);
        loop_
            .rpc()
            .insert_cell(intention_cell(3, hash2, 100_0000_0000));
        loop_.rpc().insert_cell(proc2);

        assert_eq!(loop_.tick().submitted, 1);
        let second = &loop_.rpc().submitted()[1];
        let dep = second.cell_deps().get(0).unwrap();
        assert_eq!(dep.out_point(), code_v2.out_point);
        assert_ne!(dep.out_point(), code_v1.out_point);
    }

    #[test]
    fn missing_type_id_cell_skips_tick_without_rejecting_intention() {
        let args = [0xa3; 32];
        let rpc = FakeRpc::new();
        let ts = process_type_script(4);
        let hash = script_hash32(&ts);
        let intent = intention_cell(4, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(
            4,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 4],
        ));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(!report.scan_ok);
        assert!(report
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("missing"));
        assert!(
            loop_.rpc().has_cell(&intent.out_point),
            "intention must remain live"
        );
        assert!(!loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&intent.out_point)));

        loop_
            .rpc()
            .insert_cell(code_cell(0x30, args, vec![0x7f, b'E', b'L', b'F']));
        let report2 = loop_.tick();
        assert_eq!(report2.submitted, 1, "{:?}", report2.last_error);
    }

    #[test]
    fn ambiguous_type_id_cells_fail_closed_without_submit() {
        let args = [0xa4; 32];
        let rpc = FakeRpc::new();
        let ts = process_type_script(5);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(5, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(
            5,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 5],
        ));
        rpc.insert_cell(code_cell(0x41, args, vec![1]));
        rpc.insert_cell(code_cell(0x42, args, vec![2]));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
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
    fn type_script_transport_failure_does_not_poison_intention() {
        let args = [0xa5; 32];
        let rpc = FakeRpc::new();
        let ts = process_type_script(6);
        let hash = script_hash32(&ts);
        let intent = intention_cell(6, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(
            6,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 6],
        ));
        rpc.insert_cell(code_cell(0x50, args, vec![1]));
        rpc.fail_type_script_queries();

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("unavailable"));
        assert!(!loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&intent.out_point)));

        loop_.rpc().restore_type_script_queries();
        assert_eq!(loop_.tick().submitted, 1);
    }

    #[test]
    fn static_extra_deps_merge_with_type_id_and_dedupe() {
        let args = [0xa6; 32];
        let code = code_cell(0x60, args, vec![1]);
        let static_only = CellDep::new_builder()
            .out_point(
                OutPoint::new_builder()
                    .tx_hash(Byte32::from_slice(&[0xee; 32]).unwrap())
                    .index(7u32.pack())
                    .build(),
            )
            .dep_type(DepType::Code.into())
            .build();
        let duplicate = CellDep::new_builder()
            .out_point(code.out_point.clone())
            .dep_type(DepType::Code.into())
            .build();

        let rpc = FakeRpc::new();
        let ts = process_type_script(7);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(7, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(
            7,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 7],
        ));
        rpc.insert_cell(code.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![duplicate, static_only.clone()],
            ),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let deps: Vec<_> = loop_.rpc().submitted()[0].cell_deps().into_iter().collect();
        assert_eq!(deps.len(), 2, "resolved + unique static");
        assert_eq!(deps[0].out_point(), code.out_point);
        assert_eq!(deps[1].out_point(), static_only.out_point());
    }

    /// Regression #1: code-deps refresh failure must not stall pending settlement.
    #[test]
    fn pending_settles_while_type_id_refresh_unavailable() {
        let args = [0xb1; 32];
        let rpc = FakeRpc::new();
        rpc.defer_commit();
        let ts = process_type_script(11);
        let hash = script_hash32(&ts);
        let proc = process_cell(11, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 11]);
        rpc.insert_cell(proc.clone());
        rpc.insert_cell(intention_cell(11, hash, 100_0000_0000));
        let code = code_cell(0x71, args, vec![0x7f, b'E', b'L', b'F']);
        rpc.insert_cell(code);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 1)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        assert_eq!(loop_.state().submit.pending.len(), 1);
        let tx_hash = loop_.rpc().submitted()[0].hash();
        loop_.rpc().commit_pending(&tx_hash);
        loop_.rpc().advance_tip(1);

        // Second Intention appears while a prior digest_ok is still confirming.
        loop_
            .rpc()
            .insert_cell(intention_cell(12, hash, 100_0000_0000));

        loop_.rpc().fail_type_script_queries();
        let blocked = loop_.tick();
        assert_eq!(blocked.submitted, 0);
        assert!(blocked
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("unavailable"));
        assert!(
            loop_.state().submit.pending.is_empty(),
            "pending must settle even when Type-ID refresh fails"
        );

        loop_.rpc().restore_type_script_queries();
        let after = loop_.tick();
        assert_eq!(
            after.submitted, 1,
            "new assembly resumes after deps restore; err={:?}",
            after.last_error
        );
    }

    /// Regression #2: SSRI must use Type-ID Process CODE bytes, not divergent state ELF.
    #[test]
    fn process_type_id_ssri_uses_code_cell_not_state_elf() {
        let args = [0xb2; 32];
        let state_elf = vec![0x7f, b'E', b'L', b'F', 0xAA];
        let code_elf = vec![0x7f, b'E', b'L', b'F', 0xBB];
        let rpc = FakeRpc::new();
        let (hash, proc) = process_for_type_id(13, 200_0000_0000, state_elf.clone(), args);
        rpc.insert_cell(intention_cell(13, hash, 100_0000_0000));
        rpc.insert_cell(proc);
        let code = code_cell(0x72, args, code_elf.clone());
        rpc.insert_cell(code.clone());

        let ssri = ScriptedSsri::mimic((1, 4));
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ssri,
            cfg(
                vec![TypeIdCodeDep {
                    label: PROCESS_CODE_LABEL.into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            process_code_resolver(args),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "{:?}", report.last_error);
        let seen = loop_.ssri().seen_binaries.borrow().clone();
        assert!(
            seen.iter().any(|b| b == &code_elf),
            "SSRI must see code-cell ELF, got {seen:?}"
        );
        assert!(
            seen.iter().all(|b| b != &state_elf),
            "SSRI must not use divergent state ELF"
        );
        let dep = loop_.rpc().submitted()[0].cell_deps().get(0).unwrap();
        assert_eq!(dep.out_point(), code.out_point);
    }

    /// Regression #2 upgrade path: code outpoint change invalidates SSRI source.
    #[test]
    fn process_code_upgrade_changes_ssri_binary_and_cell_dep() {
        let args = [0xb3; 32];
        let rpc = FakeRpc::new();
        let (hash, proc) = process_for_type_id(14, 200_0000_0000, vec![0x00], args);
        rpc.insert_cell(intention_cell(14, hash, 100_0000_0000));
        rpc.insert_cell(proc);
        let code_v1 = code_cell(0x73, args, vec![0x7f, b'E', b'L', b'F', 0x01]);
        rpc.insert_cell(code_v1.clone());

        let ssri = ScriptedSsri::mimic((1, 4));
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ssri,
            cfg(
                vec![TypeIdCodeDep {
                    label: PROCESS_CODE_LABEL.into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            process_code_resolver(args),
        );
        assert_eq!(loop_.tick().submitted, 1);
        assert!(loop_
            .ssri()
            .seen_binaries
            .borrow()
            .iter()
            .any(|b| b.last() == Some(&0x01)));

        loop_.rpc().consume(&code_v1.out_point);
        let code_v2 = code_cell(0x74, args, vec![0x7f, b'E', b'L', b'F', 0x02]);
        loop_.rpc().insert_cell(code_v2.clone());
        let (hash2, proc2) = process_for_type_id(15, 200_0000_0000, vec![0x00], args);
        loop_
            .rpc()
            .insert_cell(intention_cell(15, hash2, 100_0000_0000));
        loop_.rpc().insert_cell(proc2);
        loop_.ssri().seen_binaries.borrow_mut().clear();

        assert_eq!(loop_.tick().submitted, 1);
        assert!(loop_
            .ssri()
            .seen_binaries
            .borrow()
            .iter()
            .any(|b| b.last() == Some(&0x02)));
        assert_eq!(
            loop_.rpc().submitted()[1]
                .cell_deps()
                .get(0)
                .unwrap()
                .out_point(),
            code_v2.out_point
        );
    }

    /// Rejected combo under old Process code must retry after Type-ID code upgrade;
    /// unchanged code must keep the combo cached (no duplicate submit).
    #[test]
    fn rejected_combo_retries_after_process_code_upgrade_not_under_same_code() {
        let args = [0xb5; 32];
        let rpc = FakeRpc::new();
        let (hash, proc) = process_for_type_id(17, 200_0000_0000, vec![0x00], args);
        let intent = intention_cell(17, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc);
        let code_v1 = code_cell(0x76, args, vec![0x7f, b'E', b'L', b'F', 0x01]);
        rpc.insert_cell(code_v1.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 1)),
            cfg(
                vec![TypeIdCodeDep {
                    label: PROCESS_CODE_LABEL.into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            process_code_resolver(args),
        );
        loop_.rpc().inject_reject_once();
        let rejected = loop_.tick();
        assert_eq!(rejected.submitted, 0);
        assert_eq!(loop_.state().submit.rejected_combos.len(), 1);
        assert_eq!(loop_.rpc().submit_attempts(), 1);

        // Same code snapshot: cached reject, no new send.
        let cached = loop_.tick();
        assert_eq!(cached.submitted, 0);
        assert_eq!(loop_.rpc().submit_attempts(), 1);
        assert!(loop_.rpc().has_cell(&intent.out_point));

        // Upgrade Process code outpoint/ELF; same state + Intention.
        loop_.rpc().consume(&code_v1.out_point);
        let code_v2 = code_cell(0x77, args, vec![0x7f, b'E', b'L', b'F', 0x02]);
        loop_.rpc().insert_cell(code_v2.clone());
        let after_upgrade = loop_.tick();
        assert_eq!(
            after_upgrade.submitted, 1,
            "new code identity must allow retry; err={:?}",
            after_upgrade.last_error
        );
        assert_eq!(loop_.rpc().submit_attempts(), 2);
        assert_eq!(
            loop_.rpc().submitted()[0]
                .cell_deps()
                .get(0)
                .unwrap()
                .out_point(),
            code_v2.out_point
        );
    }

    /// Process state type script must bind to derived Process code TYPE_ID.
    #[test]
    fn process_state_wrong_code_hash_skips_without_poison() {
        let args = [0xb6; 32];
        let rpc = FakeRpc::new();
        // Legacy helper: type code_hash = [0x51;32] ≠ derived Type-ID.
        let ts = process_type_script(18);
        let hash = script_hash32(&ts);
        let intent = intention_cell(18, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(18, 200_0000_0000, vec![0x00]));
        rpc.insert_cell(code_cell(0x78, args, vec![0x7f, b'E', b'L', b'F', 1]));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: PROCESS_CODE_LABEL.into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            process_code_resolver(args),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report.skipped >= 1 || report.scanned >= 1);
        assert!(loop_.state().submit.rejected_combos.is_empty());
        assert!(!loop_
            .state()
            .submit
            .rejected
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_.rpc().has_cell(&intent.out_point));
    }

    /// Regression #3: same outpoint, conflicting dep_type fails closed (no submit).
    #[test]
    fn conflicting_dep_type_on_same_outpoint_skips_assembly() {
        let args = [0xb4; 32];
        let code = code_cell(0x75, args, vec![0x7f, b'E', b'L', b'F']);
        let conflict = CellDep::new_builder()
            .out_point(code.out_point.clone())
            .dep_type(DepType::DepGroup.into())
            .build();
        let rpc = FakeRpc::new();
        let ts = process_type_script(16);
        let hash = script_hash32(&ts);
        rpc.insert_cell(intention_cell(16, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(
            16,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 16],
        ));
        rpc.insert_cell(code);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "cid_lock".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![conflict],
            ),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(report
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("dep_type"));
    }
}
