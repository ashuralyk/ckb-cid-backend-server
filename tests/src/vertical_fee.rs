//! FakeRpc vertical slices for Execution fee deducted from Process recreate.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::{ProcessResolver, ResolvedProcess};
    use cid_backend_core::rpc::{script_hash32, LiveCell};
    use cid_backend_core::scan::ScannedIntention;
    use cid_backend_core::{
        assemble_digest_ok, fee_for_size, process_min_capacity, tx_size_in_block, CidLockIdentity,
        DEFAULT_FEE_RATE,
    };
    use cid_process_executor::PartialTx;
    use cid_protocol::CidArgs;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{CellOutput, RawTransaction, Script, Transaction};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, intention_cell_with_lock, process_cell, process_type_script, user_lock,
        CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn cfg(fee_rate: u64) -> LoopConfig {
        cfg_allowlist(fee_rate, vec![])
    }

    fn cfg_allowlist(fee_rate: u64, bounded_refund_locks: Vec<CidLockIdentity>) -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
            type_id_code_deps: vec![],
            confirmation_depth: 1,
            fee_rate,
            bounded_refund_locks,
            extra_scan_locks: vec![],
        }
    }

    fn allow_cid() -> Vec<CidLockIdentity> {
        vec![CidLockIdentity::new(CID_CODE, ScriptHashType::Type)]
    }

    fn output_caps(tx: &ckb_types::core::TransactionView) -> Vec<u64> {
        tx.outputs()
            .into_iter()
            .map(|o| {
                let c: u64 = o.capacity().unpack();
                c
            })
            .collect()
    }

    #[test]
    fn n1_deducts_fee_from_process_not_beneficiary() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc_cap = 200_0000_0000u64;
        let intent = intention_cell(1, hash, intent_cap);
        let proc = process_cell(1, proc_cap, vec![0x7f, b'E', b'L', b'F', 1]);
        rpc.insert_cell(intent);
        rpc.insert_cell(proc);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 2, "no fee input");
        let caps = output_caps(tx);
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0], intent_cap, "beneficiary untouched");
        assert!(caps[1] < proc_cap, "Process recreate pays fee");
        let paid = proc_cap + intent_cap - caps[0] - caps[1];
        let need = fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE);
        assert!(paid >= need, "paid={paid} need={need}");
        assert_eq!(paid, proc_cap - caps[1]);
    }

    #[test]
    fn batch_deducts_fee_from_process_only() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        let proc_cap = 400_0000_0000u64;
        rpc.insert_cell(process_cell(2, proc_cap, vec![0x7f, b'E', b'L', b'F', 2]));
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3);
        let caps = output_caps(tx);
        assert_eq!(caps.len(), 3);
        assert_eq!(caps[0], 100_0000_0000);
        assert_eq!(caps[1], 100_0000_0000);
        assert!(caps[2] < proc_cap);
        let paid = proc_cap - caps[2];
        assert!(paid >= fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE));
    }

    #[test]
    fn incompatible_refund_keeps_exact_capacity_no_process_fee() {
        // Legacy default: exact User Lock capacity ⇒ fee 0. Never auto-haircut
        // without allowlisted lock identity (existing deployments).
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.outputs().len(), 1);
        let cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
        assert_eq!(cap, intent_cap, "refund not reduced for fee");
        let code: [u8; 32] = tx.outputs().get(0).unwrap().lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(tx.header_deps().is_empty(), "not TimeoutBurn");
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == proc_op),
            "Process stays live on refund path"
        );
    }

    #[test]
    fn incompatible_bounded_refund_pays_fee_without_process() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(13);
        let hash = script_hash32(&ts);
        let intent_cap = 100_0000_0000u64;
        let proc = process_cell(13, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 13]);
        let proc_op = proc.out_point.clone();
        rpc.insert_cell(intention_cell(1, hash, intent_cap));
        rpc.insert_cell(proc);

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg_allowlist(DEFAULT_FEE_RATE, allow_cid()),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1, "no Process; no DigestOk co-bundle");
        assert_eq!(tx.outputs().len(), 1, "no siphon output");
        assert!(tx.header_deps().is_empty(), "TimeoutBurn stays out of loop");
        let cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
        let paid = intent_cap - cap;
        let need = fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE);
        assert_eq!(paid, need);
        assert!(paid > 0);
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == proc_op),
            "Process stays live"
        );
    }

    #[test]
    fn incompatible_bounded_batch_fee_equals_sum_of_cuts() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(14);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(
            14,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 14],
        ));
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg_allowlist(DEFAULT_FEE_RATE, allow_cid()),
            ProcessResolver::new(),
        );
        assert_eq!(loop_.tick().submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 2);
        assert_eq!(tx.outputs().len(), 2);
        let caps = output_caps(tx);
        let paid = 200_0000_0000u64 - caps[0] - caps[1];
        assert_eq!(paid, fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE));
    }

    #[test]
    fn mixed_old_new_locks_partition_exact_and_bounded() {
        // NEW lock is allowlisted; OLD is only scanned as legacy. Incompatible
        // batch must not haircut the legacy Intention.
        const OLD: [u8; 32] = [0xC0; 32];
        let rpc = FakeRpc::new();
        let ts = process_type_script(20);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(
            20,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 20],
        ));
        rpc.insert_cell(intention_cell_with_lock(
            1,
            hash,
            100_0000_0000,
            OLD,
            ScriptHashType::Type,
        ));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));

        let mut cfg = cfg_allowlist(DEFAULT_FEE_RATE, allow_cid());
        cfg.extra_scan_locks = vec![CidLockIdentity::new(OLD, ScriptHashType::Type)];
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg,
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 2, "exact + bounded as separate txs");
        let txs = loop_.rpc().submitted();
        assert_eq!(txs.len(), 2);
        let mut saw_exact = false;
        let mut saw_bounded = false;
        for tx in &txs {
            assert_eq!(tx.inputs().len(), 1);
            assert_eq!(tx.outputs().len(), 1);
            let in_cap = 100_0000_0000u64;
            let out_cap: u64 = tx.outputs().get(0).unwrap().capacity().unpack();
            if out_cap == in_cap {
                saw_exact = true;
            } else {
                saw_bounded = true;
                assert_eq!(
                    in_cap - out_cap,
                    fee_for_size(tx_size_in_block(tx), DEFAULT_FEE_RATE)
                );
            }
        }
        assert!(saw_exact && saw_bounded);
    }

    #[test]
    fn mixed_exact_submit_plus_bounded_unaffordable_still_isolates() {
        // Exact submits; Bounded n=2 unaffordable but n=1 OK. Must return
        // FeeUnaffordable (not Submitted) so isolate shrinks Bounded. Exact
        // input stays in_flight — never double-spent.
        const OLD: [u8; 32] = [0xC0; 32];
        let rate = 100_000u64;
        let rpc = FakeRpc::new();
        let ts = process_type_script(23);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(
            23,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 23],
        ));
        let occupied = 61_0000_0000u64;
        let legacy = intention_cell_with_lock(1, hash, 100_0000_0000, OLD, ScriptHashType::Type);
        let tight = intention_cell(2, hash, occupied + 1_000);
        let ample = intention_cell(3, hash, occupied + 50_000);
        rpc.insert_cell(legacy.clone());
        rpc.insert_cell(tight.clone());
        rpc.insert_cell(ample.clone());

        let mut cfg = cfg_allowlist(rate, allow_cid());
        cfg.extra_scan_locks = vec![CidLockIdentity::new(OLD, ScriptHashType::Type)];
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg,
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert!(
            report.submitted >= 2,
            "exact + viable bounded singleton; got submitted={}",
            report.submitted
        );
        let txs = loop_.rpc().submitted();
        let legacy_spends = txs
            .iter()
            .filter(|tx| {
                tx.inputs()
                    .into_iter()
                    .any(|i| i.previous_output() == legacy.out_point)
            })
            .count();
        assert_eq!(legacy_spends, 1, "legacy Exact must not be double-spent");
        assert!(
            txs.iter().any(|tx| tx
                .inputs()
                .into_iter()
                .any(|i| i.previous_output() == ample.out_point)),
            "viable Bounded subset must submit after isolate"
        );
        // Legacy refund is exact (fee 0); bounded pays fee.
        let legacy_tx = txs
            .iter()
            .find(|tx| {
                tx.inputs()
                    .into_iter()
                    .any(|i| i.previous_output() == legacy.out_point)
            })
            .expect("legacy tx");
        let legacy_out: u64 = legacy_tx.outputs().get(0).unwrap().capacity().unpack();
        assert_eq!(legacy_out, 100_0000_0000);
    }

    #[test]
    fn bounded_refund_unaffordable_isolates_viable_subset() {
        // High fee_rate so n=2 needs more headroom than tight+ample combined,
        // while n=1 fits in ample alone → isolate/shrink must submit the subset.
        let rate = 100_000u64;
        let rpc = FakeRpc::new();
        let ts = process_type_script(21);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(
            21,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 21],
        ));
        let occupied = 61_0000_0000u64;
        let tight = intention_cell(1, hash, occupied + 1_000);
        let ample = intention_cell(2, hash, occupied + 50_000);
        rpc.insert_cell(tight.clone());
        rpc.insert_cell(ample.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            cfg_allowlist(rate, allow_cid()),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert!(
            report.submitted >= 1,
            "viable singleton must submit after isolate"
        );
        assert!(
            loop_.rpc().submitted().iter().any(|tx| tx
                .inputs()
                .into_iter()
                .any(|i| i.previous_output() == ample.out_point)),
            "ample intention must be consumed"
        );
    }

    #[test]
    fn fee_guard_present_on_digest_ok_and_bounded_refund() {
        use cid_backend_core::{assemble_digest_incompatible, RefundFeeMode};
        use cid_process_executor::PartialTx;
        use ckb_types::packed::{CellOutput, RawTransaction, Transaction};

        let ts = process_type_script(22);
        let hash = script_hash32(&ts);
        let intent = scanned(&intention_cell(1, hash, 100_0000_0000));
        let proc = resolved(&process_cell(
            22,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 22],
        ));
        let outputs = vec![CellOutput::new_builder()
            .capacity(100_0000_0000u64.pack())
            .lock(
                ckb_types::packed::Script::new_builder()
                    .code_hash([0x11u8; 32].pack())
                    .hash_type(ScriptHashType::Type.into())
                    .args([0x01u8].pack())
                    .build(),
            )
            .build()];
        let data: Vec<Vec<u8>> = vec![vec![]];
        let bytes = Transaction::new_builder()
            .raw(
                RawTransaction::new_builder()
                    .outputs(outputs.pack())
                    .outputs_data(data.iter().map(|d| d.as_slice().pack()).pack())
                    .build(),
            )
            .build()
            .as_slice()
            .to_vec();
        let partial = PartialTx::from_bytes(&bytes).unwrap();
        let ok = assemble_digest_ok(
            std::slice::from_ref(&intent),
            &proc,
            &partial,
            &[],
            DEFAULT_FEE_RATE,
        )
        .unwrap();
        let g = ok.fee_guard.expect("digest_ok fee_guard");
        g.verify(&ok.tx).expect("digest_ok guard");

        let refund = assemble_digest_incompatible(
            std::slice::from_ref(&intent),
            &[],
            RefundFeeMode::Bounded,
            DEFAULT_FEE_RATE,
        )
        .unwrap();
        let g = refund.fee_guard.expect("bounded refund fee_guard");
        g.verify(&refund.tx).expect("bounded guard");

        let exact = assemble_digest_incompatible(
            std::slice::from_ref(&intent),
            &[],
            RefundFeeMode::Exact,
            DEFAULT_FEE_RATE,
        )
        .unwrap();
        assert!(exact.fee_guard.is_none(), "exact refunds have no fee_guard");
    }

    #[test]
    fn fee_unaffordable_skips_without_submit() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(4);
        let hash = script_hash32(&ts);
        let data = vec![0x7fu8; 128];
        let probe = process_cell(4, 0, data.clone());
        let min = process_min_capacity(&probe.output, &data).expect("occupied");
        let intent = intention_cell(1, hash, 100_0000_0000);
        // Only 10 shannons above occupied — far below any fee at rate 1000.
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(4, min + 10, data));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert!(loop_.rpc().submitted().is_empty());
        assert!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .any(|c| c.out_point == intent.out_point),
            "Intention must remain live (non-permanent skip)"
        );
        assert!(loop_.state().submit.rejected_combos.is_empty());
    }

    #[test]
    fn assembled_fee_satisfies_size_times_rate_over_1000() {
        let ts = process_type_script(5);
        let hash = script_hash32(&ts);

        for rate in [1u64, 1000, 2500, 50_000] {
            let rpc = FakeRpc::new();
            rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
            rpc.insert_cell(process_cell(
                5,
                200_0000_0000,
                vec![0x7f, b'E', b'L', b'F', 5],
            ));
            let mut loop_ = DigestLoop::new(
                rpc,
                NopClock,
                ScriptedSsri::mimic((1, 4)),
                cfg(rate),
                ProcessResolver::new(),
            );
            assert_eq!(loop_.tick().submitted, 1, "rate={rate}");
            let tx = &loop_.rpc().submitted()[0];
            let caps = output_caps(tx);
            let paid = 200_0000_0000u64 + 100_0000_0000 - caps[0] - caps[1];
            let need = fee_for_size(tx_size_in_block(tx), rate);
            assert!(paid >= need, "rate={rate} paid={paid} need={need}");
        }
    }

    fn scanned(cell: &LiveCell) -> ScannedIntention {
        let lock = cell.lock();
        let lock_args: Vec<u8> = lock.args().unpack();
        let args = CidArgs::decode(&lock_args).expect("cid args");
        let lock_code_hash: [u8; 32] = lock.code_hash().unpack();
        let lock_hash_type = ScriptHashType::try_from(lock.hash_type()).expect("hash_type");
        ScannedIntention {
            out_point: cell.out_point.clone(),
            capacity: cell.capacity(),
            type_script: cell.type_script(),
            data: cell.data.clone(),
            lock_args,
            args,
            lock_code_hash,
            lock_hash_type,
        }
    }

    fn resolved(cell: &LiveCell) -> ResolvedProcess {
        ResolvedProcess {
            type_hash: script_hash32(&cell.type_script().expect("process type")),
            live_cell: cell.clone(),
            binary: cell.data.clone(),
        }
    }

    fn mimic_partial(intentions: &[ScannedIntention]) -> PartialTx {
        let outputs: Vec<_> = intentions
            .iter()
            .map(|i| {
                CellOutput::new_builder()
                    .capacity(i.capacity.pack())
                    .lock(
                        Script::new_builder()
                            .code_hash([0x11u8; 32].pack())
                            .hash_type(ScriptHashType::Type.into())
                            .args(i.lock_args.clone().pack())
                            .build(),
                    )
                    .build()
            })
            .collect();
        let data: Vec<Vec<u8>> = intentions.iter().map(|i| i.data.clone()).collect();
        let bytes = Transaction::new_builder()
            .raw(
                RawTransaction::new_builder()
                    .outputs(outputs.pack())
                    .outputs_data(data.iter().map(|d| d.as_slice().pack()).pack())
                    .build(),
            )
            .build()
            .as_slice()
            .to_vec();
        PartialTx::from_bytes(&bytes).unwrap()
    }

    fn paid_process_fee(
        intentions: &[ScannedIntention],
        process: &ResolvedProcess,
        fee_rate: u64,
    ) -> u64 {
        let partial = mimic_partial(intentions);
        let skel = assemble_digest_ok(intentions, process, &partial, &[], fee_rate).unwrap();
        let proc_out: u64 = skel
            .tx
            .outputs()
            .get(skel.tx.outputs().len() - 1)
            .unwrap()
            .capacity()
            .unpack();
        process.live_cell.capacity() - proc_out
    }

    #[test]
    fn fee_unaffordable_at_max_shrinks_to_affordable_smaller_batch() {
        // Fee scales with tx size: max window may be unaffordable while a
        // shrunk window clears. Isolate like Rejected (non-permanent).
        let ts = process_type_script(6);
        let hash = script_hash32(&ts);
        let data = vec![0x7f, b'E', b'L', b'F', 6];
        let ample = process_cell(6, 500_0000_0000, data.clone());
        let i1 = intention_cell(1, hash, 100_0000_0000);
        let i2 = intention_cell(2, hash, 100_0000_0000);
        let s1 = scanned(&i1);
        let s2 = scanned(&i2);
        let proc = resolved(&ample);
        let fee1 = paid_process_fee(std::slice::from_ref(&s1), &proc, DEFAULT_FEE_RATE);
        let fee2 = paid_process_fee(&[s1, s2], &proc, DEFAULT_FEE_RATE);
        assert!(
            fee2 > fee1,
            "larger batch must need more fee (fee1={fee1} fee2={fee2})"
        );

        let min = process_min_capacity(&ample.output, &data).expect("occupied");
        // Surplus clears n=1 but not n=2.
        let surplus = fee1 + (fee2 - fee1) / 2;
        assert!(surplus >= fee1 && surplus < fee2);

        let rpc = FakeRpc::new();
        rpc.insert_cell(intention_cell(1, hash, 100_0000_0000));
        rpc.insert_cell(intention_cell(2, hash, 100_0000_0000));
        rpc.insert_cell(process_cell(6, min + surplus, data));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 2)),
            cfg(DEFAULT_FEE_RATE),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1, "shrunk window must submit");
        assert!(loop_.state().submit.rejected_combos.is_empty());
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(
            tx.inputs().len(),
            2,
            "expected n=1 intention + process after shrink from max=2"
        );
        let remaining = loop_
            .rpc()
            .live_cells()
            .iter()
            .filter(|c| c.lock().code_hash().as_slice() == CID_CODE)
            .count();
        assert_eq!(remaining, 1, "one Intention left for a later tick");
    }
}
