#[cfg(test)]
mod tests {
    use ckb_types::core::{ScriptHashType, TransactionBuilder};
    use ckb_types::packed::CellInput;
    use ckb_types::prelude::*;
    use cid_backend_core::assemble::ExecutionSkeleton;
    use cid_backend_core::rpc::{out_point_key, Rpc};
    use cid_backend_core::submit::{submit_execution, SubmitState};

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{live_cell, lock_script, out_point};

    #[test]
    fn submit_success_marks_in_flight_conflict_marks_failed() {
        let rpc = FakeRpc::new();
        let op = out_point(1, 0);
        rpc.insert_cell(live_cell(
            op.clone(),
            lock_script([0xC1; 32], ScriptHashType::Type, vec![]),
            None,
            100_0000_0000,
            vec![],
        ));
        let tx = TransactionBuilder::default()
            .input(
                CellInput::new_builder()
                    .previous_output(op.clone())
                    .since(0u64.pack())
                    .build(),
            )
            .build();
        let skeleton = ExecutionSkeleton { tx };
        let mut state = SubmitState::default();
        let hash = submit_execution(&rpc, &skeleton, &mut state, &[op.clone()]).unwrap();
        assert!(!hash.as_slice().is_empty() || hash.as_slice().len() == 32);
        assert!(state.in_flight.contains(&out_point_key(&op)));
        assert!(state.failed.is_empty());
        assert!(rpc.live_cells().is_empty());

        let op2 = out_point(2, 0);
        rpc.insert_cell(live_cell(
            op2.clone(),
            lock_script([0xC1; 32], ScriptHashType::Type, vec![]),
            None,
            100_0000_0000,
            vec![],
        ));
        rpc.inject_conflict_once();
        let tx2 = TransactionBuilder::default()
            .input(
                CellInput::new_builder()
                    .previous_output(op2.clone())
                    .since(0u64.pack())
                    .build(),
            )
            .build();
        let mut state2 = SubmitState::default();
        let err = submit_execution(
            &rpc,
            &ExecutionSkeleton { tx: tx2 },
            &mut state2,
            &[op2.clone()],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            cid_backend_core::rpc::RpcError::Conflict(_)
        ));
        assert!(state2.failed.contains(&out_point_key(&op2)));
        assert!(!state2.in_flight.contains(&out_point_key(&op2)));
        assert_eq!(rpc.live_cells().len(), 1);
    }
}
