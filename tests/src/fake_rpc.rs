use std::cell::RefCell;
use std::collections::HashMap;

use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, OutPoint};
use ckb_types::prelude::*;
use cid_backend_core::rpc::{out_point_key, script_hash32, LiveCell, Rpc, RpcError};

#[derive(Default)]
struct Inner {
    cells: HashMap<Vec<u8>, LiveCell>,
    order: Vec<OutPoint>,
    fail_next_submit: bool,
    submitted: Vec<TransactionView>,
}

pub struct FakeRpc {
    inner: RefCell<Inner>,
}

impl FakeRpc {
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(Inner::default()),
        }
    }

    pub fn insert_cell(&self, cell: LiveCell) {
        let mut inner = self.inner.borrow_mut();
        let key = out_point_key(&cell.out_point);
        if !inner.cells.contains_key(&key) {
            inner.order.push(cell.out_point.clone());
        }
        inner.cells.insert(key, cell);
    }

    pub fn consume(&self, op: &OutPoint) {
        let mut inner = self.inner.borrow_mut();
        inner.cells.remove(&out_point_key(op));
        inner.order.retain(|o| o.as_slice() != op.as_slice());
    }

    pub fn inject_conflict_once(&self) {
        self.inner.borrow_mut().fail_next_submit = true;
    }

    pub fn live_cells(&self) -> Vec<LiveCell> {
        let inner = self.inner.borrow();
        inner
            .order
            .iter()
            .filter_map(|op| inner.cells.get(&out_point_key(op)).cloned())
            .collect()
    }

    pub fn submitted(&self) -> Vec<TransactionView> {
        self.inner.borrow().submitted.clone()
    }
}

impl Default for FakeRpc {
    fn default() -> Self {
        Self::new()
    }
}

impl Rpc for FakeRpc {
    fn list_live_intentions(
        &self,
        cid_code_hash: &[u8; 32],
        hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError> {
        let expected_ht: ckb_types::packed::Byte = hash_type.into();
        let inner = self.inner.borrow();
        let mut out = Vec::new();
        for op in &inner.order {
            let Some(cell) = inner.cells.get(&out_point_key(op)) else {
                continue;
            };
            let lock = cell.lock();
            let code: [u8; 32] = lock.code_hash().unpack();
            if &code == cid_code_hash && lock.hash_type() == expected_ht {
                out.push(cell.clone());
            }
        }
        Ok(out)
    }

    fn get_live_process_by_type_hash(
        &self,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError> {
        let inner = self.inner.borrow();
        for op in &inner.order {
            let Some(cell) = inner.cells.get(&out_point_key(op)) else {
                continue;
            };
            if let Some(ts) = cell.type_script() {
                if &script_hash32(&ts) == process_type_hash {
                    return Ok(Some(cell.clone()));
                }
            }
        }
        Ok(None)
    }

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError> {
        let mut inner = self.inner.borrow_mut();
        if inner.fail_next_submit {
            inner.fail_next_submit = false;
            return Err(RpcError::Conflict("injected conflict".into()));
        }
        for input in tx.inputs() {
            let op = input.previous_output();
            if !inner.cells.contains_key(&out_point_key(&op)) {
                return Err(RpcError::Conflict("input cell missing".into()));
            }
        }
        for input in tx.inputs() {
            let op = input.previous_output();
            inner.cells.remove(&out_point_key(&op));
            inner.order.retain(|o| o.as_slice() != op.as_slice());
        }
        let tx_hash = tx.hash();
        for (i, (output, data)) in tx
            .outputs()
            .into_iter()
            .zip(tx.outputs_data().into_iter())
            .enumerate()
        {
            let op = OutPoint::new_builder()
                .tx_hash(tx_hash.clone())
                .index((i as u32).pack())
                .build();
            let bytes: Vec<u8> = data.unpack();
            let cell = LiveCell {
                out_point: op.clone(),
                output,
                data: bytes,
            };
            inner.order.push(op.clone());
            inner.cells.insert(out_point_key(&op), cell);
        }
        inner.submitted.push(tx.clone());
        Ok(tx_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{live_cell, lock_script, out_point};
    use ckb_types::core::ScriptHashType;

    const CID_CODE: [u8; 32] = [0xC1; 32];
    const OTHER_CODE: [u8; 32] = [0x99; 32];

    #[test]
    fn fakerpc_stores_and_lists_intention_cells_in_insert_order() {
        let rpc = FakeRpc::new();
        let a = live_cell(
            out_point(1, 0),
            lock_script(CID_CODE, ScriptHashType::Type, vec![0x01]),
            None,
            100_0000_0000,
            vec![],
        );
        let b = live_cell(
            out_point(2, 0),
            lock_script(CID_CODE, ScriptHashType::Type, vec![0x02]),
            None,
            200_0000_0000,
            vec![],
        );
        let other = live_cell(
            out_point(3, 0),
            lock_script(OTHER_CODE, ScriptHashType::Type, vec![]),
            None,
            150_0000_0000,
            vec![],
        );
        rpc.insert_cell(a.clone());
        rpc.insert_cell(b.clone());
        rpc.insert_cell(other);

        let listed = rpc
            .list_live_intentions(&CID_CODE, ScriptHashType::Type)
            .expect("list");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].out_point, a.out_point);
        assert_eq!(listed[1].out_point, b.out_point);
        assert_eq!(listed[0].capacity(), 100_0000_0000);
        assert_eq!(rpc.live_cells().len(), 3);
    }
}
