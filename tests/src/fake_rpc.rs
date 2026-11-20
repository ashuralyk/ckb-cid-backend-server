use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use cid_backend_core::host_query::{HostQueryRpc, IndexedLiveCell};
use cid_backend_core::rpc::{out_point_key, script_hash32, LiveCell, Rpc, RpcError, TxStatus};
use cid_process_executor::{HostCell, HostQuery, MockHost, Pagination};
use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, Header, OutPoint, Script};
use ckb_types::prelude::*;

#[derive(Default)]
struct Inner {
    cells: HashMap<Vec<u8>, LiveCell>,
    order: Vec<OutPoint>,
    fail_next_submit: bool,
    reject_next_submit: bool,
    reject_next_dry_run: bool,
    spent_deps: HashSet<OutPoint>,
    unavailable_submit_inputs: HashSet<OutPoint>,
    unavailable_next_dry_run: bool,
    reject_inputs: HashSet<Vec<u8>>,
    submit_attempts: usize,
    dry_run_attempts: usize,
    unavailable_next_submit: bool,
    defer_commit: bool,
    fail_status_queries: bool,
    fail_tip_queries: bool,
    fail_type_script_queries: bool,
    rewind_tip_on_status: Option<u64>,
    submitted: Vec<TransactionView>,
    consumed_by_tx: HashMap<Vec<u8>, Vec<LiveCell>>,
    tx_status: HashMap<Vec<u8>, TxStatus>,
    tip: u64,
    headers_by_number: HashMap<u64, Vec<u8>>,
    headers_by_hash: HashMap<[u8; 32], Vec<u8>>,
    block_hashes: HashMap<u64, [u8; 32]>,
    /// Optional indexer order metadata keyed by out_point bytes.
    cell_index_meta: HashMap<Vec<u8>, (u64, u32)>,
}

pub struct FakeRpc {
    inner: Mutex<Inner>,
}

impl FakeRpc {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn insert_cell(&self, cell: LiveCell) {
        let mut inner = self.inner.lock().unwrap();
        let key = out_point_key(&cell.out_point);
        if !inner.cells.contains_key(&key) {
            inner.order.push(cell.out_point.clone());
        }
        inner.cells.insert(key, cell);
    }

    /// Store a packed molecule `Header` for host-query syscalls 2327/2337/2347.
    pub fn insert_header(&self, number: u64, header: Header) {
        let bytes = header.as_slice().to_vec();
        let hash_b32 = header.calc_header_hash();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(hash_b32.as_slice());
        let mut inner = self.inner.lock().unwrap();
        inner.block_hashes.insert(number, hash);
        inner.headers_by_number.insert(number, bytes.clone());
        inner.headers_by_hash.insert(hash, bytes);
    }

    /// Override the synthetic tip hash used when no header was inserted.
    pub fn insert_block_hash(&self, number: u64, hash: [u8; 32]) {
        self.inner.lock().unwrap().block_hashes.insert(number, hash);
    }

    /// Set indexer `(block_number, tx_index)` for `get_cells` order / metadata.
    pub fn set_cell_index_meta(&self, out_point: &OutPoint, block_number: u64, tx_index: u32) {
        self.inner
            .lock()
            .unwrap()
            .cell_index_meta
            .insert(out_point_key(out_point), (block_number, tx_index));
    }

    fn index_host(inner: &Inner) -> MockHost {
        let host = MockHost::new();
        for cell in inner.cells.values() {
            let key = out_point_key(&cell.out_point);
            let (block_number, tx_index) =
                inner.cell_index_meta.get(&key).copied().unwrap_or((0, 0));
            host.insert_cell_ex(
                cell.out_point.clone(),
                HostCell {
                    output: cell.output.clone(),
                    data: cell.data.clone(),
                    block_hash: None,
                    block_number: Some(block_number),
                    tx_index: Some(tx_index),
                },
            );
        }
        for (&number, bytes) in &inner.headers_by_number {
            if let Ok(header) = Header::from_slice(bytes) {
                host.insert_header(number, header);
            }
        }
        for (&number, hash) in &inner.block_hashes {
            if !inner.headers_by_number.contains_key(&number) {
                host.insert_block_hash(number, *hash);
            }
        }
        host
    }

    pub fn consume(&self, op: &OutPoint) {
        let mut inner = self.inner.lock().unwrap();
        inner.cells.remove(&out_point_key(op));
        inner.order.retain(|o| o.as_slice() != op.as_slice());
    }

    pub fn inject_conflict_once(&self) {
        self.inner.lock().unwrap().fail_next_submit = true;
    }

    /// Submit succeeds but leaves inputs live and does not apply outputs (indexer lag / mempool).
    pub fn defer_commit(&self) {
        self.inner.lock().unwrap().defer_commit = true;
    }

    /// `get_transaction_status` returns Unavailable until restored (occupancy must be held).
    pub fn fail_status_queries(&self) {
        self.inner.lock().unwrap().fail_status_queries = true;
    }

    pub fn restore_status_queries(&self) {
        self.inner.lock().unwrap().fail_status_queries = false;
    }

    /// Next `submit_transaction` returns `SubmitRejected` (permanent for those cells).
    pub fn inject_reject_once(&self) {
        self.inner.lock().unwrap().reject_next_submit = true;
    }

    /// Next `dry_run_transaction` returns `SubmitRejected` (combo blacklist path).
    pub fn inject_dry_run_reject_once(&self) {
        self.inner.lock().unwrap().reject_next_dry_run = true;
    }

    /// The node rejects this dependency while the indexer still lists it.
    pub fn mark_dep_spent(&self, op: OutPoint) {
        self.inner.lock().unwrap().spent_deps.insert(op);
    }

    pub fn inject_unavailable_refund_once(&self, op: OutPoint) {
        self.inner
            .lock()
            .unwrap()
            .unavailable_submit_inputs
            .insert(op);
    }

    pub fn inject_unavailable_dry_run_once(&self) {
        self.inner.lock().unwrap().unavailable_next_dry_run = true;
    }

    /// Every submit that spends `op` returns `SubmitRejected` (A-fails / B-succeeds).
    pub fn reject_when_consuming(&self, op: &OutPoint) {
        self.inner
            .lock()
            .unwrap()
            .reject_inputs
            .insert(out_point_key(op));
    }

    /// Count of `submit_transaction` calls, including injected failures.
    pub fn submit_attempts(&self) -> usize {
        self.inner.lock().unwrap().submit_attempts
    }

    pub fn dry_run_attempts(&self) -> usize {
        self.inner.lock().unwrap().dry_run_attempts
    }

    /// Next `submit_transaction` returns `Unavailable` (transient).
    pub fn inject_unavailable_submit_once(&self) {
        self.inner.lock().unwrap().unavailable_next_submit = true;
    }

    pub fn tip(&self) -> u64 {
        self.inner.lock().unwrap().tip
    }

    pub fn set_tip(&self, n: u64) {
        self.inner.lock().unwrap().tip = n;
    }

    pub fn advance_tip(&self, delta: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.tip = inner.tip.saturating_add(delta);
    }

    pub fn fail_tip_queries(&self) {
        self.inner.lock().unwrap().fail_tip_queries = true;
    }

    pub fn restore_tip_queries(&self) {
        self.inner.lock().unwrap().fail_tip_queries = false;
    }

    /// Next `list_live_cells_by_type_script` returns Unavailable.
    pub fn fail_type_script_queries(&self) {
        self.inner.lock().unwrap().fail_type_script_queries = true;
    }

    pub fn restore_type_script_queries(&self) {
        self.inner.lock().unwrap().fail_type_script_queries = false;
    }

    /// Next `get_transaction_status` rewinds the tip first (stale-tip settle).
    pub fn rewind_tip_on_next_status(&self, new_tip: u64) {
        self.inner.lock().unwrap().rewind_tip_on_status = Some(new_tip);
    }

    pub fn live_cells(&self) -> Vec<LiveCell> {
        let inner = self.inner.lock().unwrap();
        inner
            .order
            .iter()
            .filter_map(|op| inner.cells.get(&out_point_key(op)).cloned())
            .collect()
    }

    pub fn has_cell(&self, op: &OutPoint) -> bool {
        self.inner
            .lock()
            .unwrap()
            .cells
            .contains_key(&out_point_key(op))
    }

    pub fn submitted(&self) -> Vec<TransactionView> {
        self.inner.lock().unwrap().submitted.clone()
    }

    /// Restore consumed inputs of `tx_hash`, drop its outputs, and mark the tx Dropped.
    /// Models a pool eviction after `submit_transaction` succeeded.
    pub fn simulate_drop(&self, tx_hash: &Byte32) {
        let mut inner = self.inner.lock().unwrap();
        let key = tx_hash.as_slice().to_vec();
        inner.order.retain(|op| op.tx_hash() != *tx_hash);
        inner
            .cells
            .retain(|_, cell| cell.out_point.tx_hash() != *tx_hash);
        if let Some(cells) = inner.consumed_by_tx.get(&key).cloned() {
            for cell in cells {
                let cell_key = out_point_key(&cell.out_point);
                if !inner.cells.contains_key(&cell_key) {
                    inner.order.push(cell.out_point.clone());
                }
                inner.cells.insert(cell_key, cell);
            }
        }
        inner.tx_status.insert(key, TxStatus::Dropped);
    }

    /// Rewind the tip. Transactions committed above `new_tip` are dropped and
    /// their inputs restored (reorg of confirming/committed txs).
    pub fn simulate_reorg(&self, new_tip: u64) {
        let to_drop: Vec<Byte32> = {
            let inner = self.inner.lock().unwrap();
            inner
                .tx_status
                .iter()
                .filter_map(|(k, st)| match st {
                    TxStatus::Committed { block_number } if *block_number > new_tip => {
                        Byte32::from_slice(k).ok()
                    }
                    _ => None,
                })
                .collect()
        };
        self.inner.lock().unwrap().tip = new_tip;
        for hash in to_drop {
            self.simulate_drop(&hash);
        }
    }

    /// Apply a previously deferred submit: consume inputs and create outputs.
    pub fn commit_pending(&self, tx_hash: &Byte32) {
        let mut inner = self.inner.lock().unwrap();
        let key = tx_hash.as_slice().to_vec();
        if inner.tx_status.get(&key) != Some(&TxStatus::Pending) {
            return;
        }
        apply_commit(&mut inner, tx_hash);
        let block_number = inner.tip;
        inner
            .tx_status
            .insert(key, TxStatus::Committed { block_number });
    }
}

fn apply_commit(inner: &mut Inner, tx_hash: &Byte32) {
    let key = tx_hash.as_slice().to_vec();
    if let Some(cells) = inner.consumed_by_tx.get(&key).cloned() {
        for cell in cells {
            inner.cells.remove(&out_point_key(&cell.out_point));
            inner
                .order
                .retain(|o| o.as_slice() != cell.out_point.as_slice());
        }
    }
    let Some(tx) = inner
        .submitted
        .iter()
        .find(|t| t.hash() == *tx_hash)
        .cloned()
    else {
        return;
    };
    for (i, (output, data)) in tx.outputs().into_iter().zip(tx.outputs_data()).enumerate() {
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
        let inner = self.inner.lock().unwrap();
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
        let inner = self.inner.lock().unwrap();
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

    fn find_live_cells_by_type_script(
        &self,
        type_script: &Script,
        max_results: usize,
    ) -> Result<Vec<LiveCell>, RpcError> {
        let inner = self.inner.lock().unwrap();
        if inner.fail_type_script_queries {
            return Err(RpcError::Unavailable(
                "injected type-script query unavailable".into(),
            ));
        }
        let want = script_hash32(type_script);
        let mut out = Vec::new();
        for op in &inner.order {
            if out.len() >= max_results {
                break;
            }
            let Some(cell) = inner.cells.get(&out_point_key(op)) else {
                continue;
            };
            if let Some(ts) = cell.type_script() {
                if script_hash32(&ts) == want {
                    out.push(cell.clone());
                }
            }
        }
        Ok(out)
    }

    fn dry_run_transaction(&self, _tx: &TransactionView) -> Result<(), RpcError> {
        let mut inner = self.inner.lock().unwrap();
        inner.dry_run_attempts += 1;
        if std::mem::take(&mut inner.unavailable_next_dry_run) {
            return Err(RpcError::Unavailable("injected dry-run unavailable".into()));
        }
        if inner.reject_next_dry_run {
            inner.reject_next_dry_run = false;
            return Err(RpcError::SubmitRejected("injected dry-run reject".into()));
        }
        Ok(())
    }

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError> {
        let mut inner = self.inner.lock().unwrap();
        inner.submit_attempts += 1;
        if tx
            .cell_deps()
            .into_iter()
            .any(|dep| inner.spent_deps.contains(&dep.out_point()))
        {
            return Err(RpcError::Conflict("dependency is spent".into()));
        }
        if tx.inputs().len() == 1
            && inner
                .unavailable_submit_inputs
                .remove(&tx.inputs().get(0).unwrap().previous_output())
        {
            return Err(RpcError::Unavailable("injected refund unavailable".into()));
        }
        if inner.unavailable_next_submit {
            inner.unavailable_next_submit = false;
            return Err(RpcError::Unavailable("injected submit unavailable".into()));
        }
        if inner.reject_next_submit {
            inner.reject_next_submit = false;
            return Err(RpcError::SubmitRejected("injected reject".into()));
        }
        let hits_poison = tx.inputs().into_iter().any(|input| {
            inner
                .reject_inputs
                .contains(&out_point_key(&input.previous_output()))
        });
        if hits_poison {
            return Err(RpcError::SubmitRejected("injected input reject".into()));
        }
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
        let mut consumed_cells = Vec::new();
        for input in tx.inputs() {
            let op = input.previous_output();
            let key = out_point_key(&op);
            let Some(cell) = inner.cells.get(&key).cloned() else {
                return Err(RpcError::Conflict("input cell missing".into()));
            };
            consumed_cells.push(cell);
        }
        let tx_hash = tx.hash();
        let hash_key = tx_hash.as_slice().to_vec();
        inner
            .consumed_by_tx
            .insert(hash_key.clone(), consumed_cells);
        inner.submitted.push(tx.clone());
        if inner.defer_commit {
            inner.tx_status.insert(hash_key, TxStatus::Pending);
        } else {
            apply_commit(&mut inner, &tx_hash);
            let block_number = inner.tip;
            inner
                .tx_status
                .insert(hash_key, TxStatus::Committed { block_number });
        }
        Ok(tx_hash)
    }

    fn get_transaction_status(&self, tx_hash: &Byte32) -> Result<TxStatus, RpcError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(t) = inner.rewind_tip_on_status.take() {
            inner.tip = t;
        }
        if inner.fail_status_queries {
            return Err(RpcError::Unavailable("injected status unavailable".into()));
        }
        Ok(inner
            .tx_status
            .get(tx_hash.as_slice())
            .copied()
            .unwrap_or(TxStatus::Dropped))
    }

    fn tip_block_number(&self) -> Result<u64, RpcError> {
        let inner = self.inner.lock().unwrap();
        if inner.fail_tip_queries {
            return Err(RpcError::Unavailable("injected tip unavailable".into()));
        }
        Ok(inner.tip)
    }
}

impl HostQueryRpc for FakeRpc {
    fn get_live_cell(&self, out_point: &OutPoint) -> Result<Option<LiveCell>, RpcError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.cells.get(&out_point_key(out_point)).cloned())
    }

    fn get_block_hash(&self, number: u64) -> Result<Option<[u8; 32]>, RpcError> {
        let inner = self.inner.lock().unwrap();
        if inner.fail_tip_queries {
            return Err(RpcError::Unavailable("injected tip unavailable".into()));
        }
        if let Some(h) = inner.block_hashes.get(&number) {
            return Ok(Some(*h));
        }
        // Deterministic synthetic tip hash when callers only set_tip.
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&number.to_le_bytes());
        Ok(Some(h))
    }

    fn get_header_by_number(&self, number: u64) -> Result<Option<Vec<u8>>, RpcError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.headers_by_number.get(&number).cloned())
    }

    fn get_header(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, RpcError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.headers_by_hash.get(hash).cloned())
    }

    fn get_cells_page(
        &self,
        search_key_molecule: &[u8],
        order: u64,
        limit: u32,
        after: &[u8],
    ) -> Result<(Vec<IndexedLiveCell>, Vec<u8>), RpcError> {
        let inner = self.inner.lock().unwrap();
        let host = Self::index_host(&inner);
        let page = HostQuery::get_cells(&host, search_key_molecule, order, u64::from(limit), after)
            .map_err(|_| RpcError::Unavailable("get_cells_page: invalid SearchKey".into()))?;
        let pag: Pagination = serde_molecule::from_slice(&page, false)
            .map_err(|e| RpcError::Unavailable(format!("get_cells_page pagination: {e}")))?;
        let mut cells = Vec::with_capacity(pag.objects.len());
        for obj in &pag.objects {
            let op = OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&obj.out_point.tx_hash).unwrap())
                .index(obj.out_point.index.pack())
                .build();
            if let Some(live) = inner.cells.get(&out_point_key(&op)) {
                cells.push(IndexedLiveCell {
                    cell: live.clone(),
                    block_number: obj.block_number,
                    tx_index: obj.tx_index,
                });
            }
        }
        Ok((cells, pag.last_cursor))
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
    fn fakerpc_host_query_resolves_inserted_cells_and_tip() {
        let rpc = FakeRpc::new();
        rpc.set_tip(12);
        let cell = live_cell(
            out_point(9, 0),
            lock_script(CID_CODE, ScriptHashType::Type, vec![]),
            None,
            100,
            vec![0xAA],
        );
        rpc.insert_cell(cell.clone());
        let got = HostQueryRpc::get_live_cell(&rpc, &cell.out_point)
            .unwrap()
            .expect("live");
        assert_eq!(got.data, cell.data);
        let mut tip_hash = [0u8; 32];
        tip_hash[..8].copy_from_slice(&12u64.to_le_bytes());
        assert_eq!(
            HostQueryRpc::get_block_hash(&rpc, 12).unwrap(),
            Some(tip_hash)
        );
    }

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
