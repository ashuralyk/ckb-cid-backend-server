#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};
    use cid_backend_core::type_id::{type_id_script, TypeIdCodeDep, TypeIdIdentity};
    use ckb_types::core::{DepType, ScriptHashType};
    use ckb_types::packed::{Byte32, CellDep, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
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
        }
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
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        let intent = intention_cell(2, hash, 100_0000_0000);
        let proc = process_cell(2, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 2]);
        let code_v1 = code_cell(0x21, args, vec![1]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());
        rpc.insert_cell(code_v1.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(
                vec![TypeIdCodeDep {
                    label: "process_code".into(),
                    identity: TypeIdIdentity::new(args),
                }],
                vec![],
            ),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let first_dep = loop_.rpc().submitted()[0].cell_deps().get(0).unwrap();
        assert_eq!(first_dep.out_point(), code_v1.out_point);

        // Upgrade: same TYPE_ID args, new code-cell outpoint. Intention still live
        // because first tick consumed it — re-insert a new intention + process.
        loop_.rpc().consume(&code_v1.out_point);
        let code_v2 = code_cell(0x22, args, vec![2]);
        loop_.rpc().insert_cell(code_v2.clone());
        let intent2 = intention_cell(3, hash, 100_0000_0000);
        let proc2 = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        loop_.rpc().insert_cell(intent2);
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

        // Recovery when the code cell appears.
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
        let ts = process_type_script(4);
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
}
