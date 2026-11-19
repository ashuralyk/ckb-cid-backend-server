//! RPC-backed SSRI host-query: [`HostQueryRpc`] plus [`RpcHostQuery`] which
//! implements the executor [`cid_process_executor::HostQuery`] trait for all
//! seven syscalls. Assemble still consumes [`MockHost`]; seed it from RPC
//! (or from [`RpcHostQuery`]) so the offline VM serves the same catalog.
//!
//! ## Syscall → [`HostQueryRpc`] mapping
//!
//! | Syscall | Number | Trait surface |
//! |---------|--------|---------------|
//! | `find_cell_by_out_point` | 2287 | [`HostQueryRpc::get_live_cell`] |
//! | `find_cell_data_by_out_point` | 2297 | [`HostQueryRpc::get_live_cell`] |
//! | `get_live_cell` | 2317 | [`HostQueryRpc::get_live_cell`] |
//! | `get_header` | 2327 | [`HostQueryRpc::get_header`] |
//! | `get_header_by_number` | 2337 | [`HostQueryRpc::get_header_by_number`] |
//! | `get_block_hash` | 2347 | [`HostQueryRpc::get_block_hash`] |
//! | `get_cells` | 2367 | [`HostQueryRpc::get_cells_page`] |

use crate::rpc::{LiveCell, RpcError};
use cid_process_executor::host_query::{
    IndexerCell, IndexerCellOutput, IndexerOutPoint, LiveCell as ExecLiveCell, Script as HqScript,
    ScriptType, SearchMode,
};
use cid_process_executor::{
    DraftCell, HostCell, HostQuery, HostQueryError as ExecHostQueryError, MockHost, Pagination,
    SearchKey,
};
use ckb_types::packed::{CellOutput, Header, OutPoint};
use ckb_types::prelude::*;
use std::fmt;

/// Syscall numbers (ckb-ssri-std native injection catalog).
pub const FIND_CELL_BY_OUT_POINT: u64 = 2287;
pub const FIND_CELL_DATA_BY_OUT_POINT: u64 = 2297;
pub const GET_LIVE_CELL: u64 = 2317;
pub const GET_HEADER: u64 = 2327;
pub const GET_HEADER_BY_NUMBER: u64 = 2337;
pub const GET_BLOCK_HASH: u64 = 2347;
pub const GET_CELLS: u64 = 2367;

/// Bounded page size for host `get_cells` pagination (documentation default).
pub const DEFAULT_HOST_QUERY_PAGE_LIMIT: u32 = 50;

/// Documented upper bound for RPC host-query round-trips (seconds).
pub const DEFAULT_HOST_QUERY_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostQueryError {
    Rpc(RpcError),
    MissingLiveCell(OutPoint),
    MissingBlockHash { number: u64 },
}

impl fmt::Display for HostQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostQueryError::Rpc(e) => write!(f, "{e}"),
            HostQueryError::MissingLiveCell(_) => {
                write!(f, "live cell missing for draft out_point")
            }
            HostQueryError::MissingBlockHash { number } => {
                write!(f, "block hash missing for height {number}")
            }
        }
    }
}

impl std::error::Error for HostQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HostQueryError::Rpc(e) => Some(e),
            _ => None,
        }
    }
}

/// One indexer live cell with ckb-indexer order metadata.
///
/// Order key is `(block_number, tx_index, output_index)` — same as MockHost /
/// ckb-indexer — so [`RpcHostQuery::get_cells`] can emit matching `tx_index`.
#[derive(Clone, Debug)]
pub struct IndexedLiveCell {
    pub cell: LiveCell,
    pub block_number: u64,
    pub tx_index: u32,
}

/// Minimal RPC surface for host-query seeding and indexer pagination.
/// Implement on the live CKB adapter without extending the core [`crate::rpc::Rpc`] trait.
///
/// Header methods must return **packed molecule `Header` bytes** (not JSON).
/// `get_cells_page` takes a molecule [`SearchKey`] and returns a page of live
/// cells plus the next cursor (empty when exhausted). Pages must be ordered by
/// `(block_number, tx_index, output_index)` ascending when `order=0`.
pub trait HostQueryRpc {
    fn get_live_cell(&self, out_point: &OutPoint) -> Result<Option<LiveCell>, RpcError>;

    fn get_block_hash(&self, number: u64) -> Result<Option<[u8; 32]>, RpcError>;

    fn get_header_by_number(&self, number: u64) -> Result<Option<Vec<u8>>, RpcError>;

    fn get_header(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, RpcError>;

    /// Bounded `get_cells` page: molecule-encoded `search_key`, cursor `after`.
    fn get_cells_page(
        &self,
        search_key_molecule: &[u8],
        limit: u32,
        after: &[u8],
    ) -> Result<(Vec<IndexedLiveCell>, Vec<u8>), RpcError>;
}

/// Adapter: [`HostQueryRpc`] → executor [`HostQuery`] (all 7 syscalls).
#[derive(Clone, Copy, Debug)]
pub struct RpcHostQuery<'a, R: ?Sized> {
    pub rpc: &'a R,
}

impl<'a, R: ?Sized> RpcHostQuery<'a, R> {
    pub fn new(rpc: &'a R) -> Self {
        Self { rpc }
    }
}

impl<R: HostQueryRpc + ?Sized> HostQuery for RpcHostQuery<'_, R> {
    fn find_cell_by_out_point(&self, out_point: &[u8]) -> Option<Vec<u8>> {
        let op = OutPoint::from_slice(out_point).ok()?;
        let live = self.rpc.get_live_cell(&op).ok().flatten()?;
        Some(encode_cell_output(&live.output))
    }

    fn find_cell_data_by_out_point(&self, out_point: &[u8]) -> Option<Vec<u8>> {
        let op = OutPoint::from_slice(out_point).ok()?;
        let live = self.rpc.get_live_cell(&op).ok().flatten()?;
        Some(encode_cell_data(&live.data))
    }

    fn get_live_cell(
        &self,
        out_point: &[u8],
        with_data: bool,
    ) -> Result<Vec<u8>, ExecHostQueryError> {
        let live = match OutPoint::from_slice(out_point) {
            Ok(op) => self
                .rpc
                .get_live_cell(&op)
                .map_err(|_| ExecHostQueryError::Encoding)?,
            Err(_) => return Err(ExecHostQueryError::InvalidArgs),
        };
        let encoded = match live {
            Some(cell) => ExecLiveCell {
                output: Some(IndexerCellOutput::from(&cell.output)),
                data: with_data.then_some(cell.data),
                block_hash: None,
            },
            None => ExecLiveCell {
                output: None,
                data: None,
                block_hash: None,
            },
        };
        serde_molecule::to_vec(&encoded, false).map_err(|_| ExecHostQueryError::Encoding)
    }

    fn get_header(&self, block_hash: &[u8]) -> Option<Vec<u8>> {
        if block_hash.len() != 32 {
            return None;
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(block_hash);
        self.rpc.get_header(&key).ok().flatten()
    }

    fn get_header_by_number(&self, number: u64) -> Option<Vec<u8>> {
        self.rpc.get_header_by_number(number).ok().flatten()
    }

    fn get_block_hash(&self, number: u64) -> Option<[u8; 32]> {
        self.rpc.get_block_hash(number).ok().flatten()
    }

    fn get_cells(
        &self,
        search_key: &[u8],
        order: u64,
        limit: u64,
        last_cursor: &[u8],
    ) -> Result<Vec<u8>, ExecHostQueryError> {
        // Validate molecule SearchKey + order up front (match MockHost errno).
        let _: SearchKey = serde_molecule::from_slice(search_key, false)
            .map_err(|_| ExecHostQueryError::InvalidArgs)?;
        if order > 1 {
            return Err(ExecHostQueryError::InvalidArgs);
        }
        if !last_cursor.is_empty() && last_cursor.len() != 8 {
            return Err(ExecHostQueryError::InvalidArgs);
        }
        let page_limit = u32::try_from(limit.min(u64::from(u32::MAX))).unwrap_or(u32::MAX);
        let (cells, next) = self
            .rpc
            .get_cells_page(search_key, page_limit, last_cursor)
            .map_err(|_| ExecHostQueryError::Encoding)?;
        let with_data = serde_molecule::from_slice::<SearchKey>(search_key, false)
            .ok()
            .and_then(|k| k.with_data)
            .unwrap_or(true);
        let objects: Vec<IndexerCell> = cells
            .iter()
            .map(|indexed| IndexerCell {
                output: IndexerCellOutput::from(&indexed.cell.output),
                output_data: with_data.then(|| indexed.cell.data.clone()),
                out_point: IndexerOutPoint::from(&indexed.cell.out_point),
                block_number: indexed.block_number,
                tx_index: indexed.tx_index,
            })
            .collect();
        let pagination = Pagination {
            objects,
            last_cursor: next,
        };
        serde_molecule::to_vec(&pagination, false).map_err(|_| ExecHostQueryError::Encoding)
    }
}

/// Populate [`MockHost`] from chain live cells referenced by `draft_cells`, plus
/// tip block hash / header when available.
pub fn seed_host_for_batch<R: HostQueryRpc>(
    rpc: &R,
    draft_cells: &[DraftCell],
    tip_number: u64,
) -> Result<MockHost, HostQueryError> {
    seed_mock_host_for_draft(rpc, draft_cells, Some(tip_number))
}

pub fn seed_mock_host_for_draft<R: HostQueryRpc>(
    rpc: &R,
    draft_cells: &[DraftCell],
    tip_number: Option<u64>,
) -> Result<MockHost, HostQueryError> {
    let host = MockHost::new();
    for draft in draft_cells {
        let live = rpc
            .get_live_cell(&draft.out_point)
            .map_err(HostQueryError::Rpc)?
            .ok_or_else(|| HostQueryError::MissingLiveCell(draft.out_point.clone()))?;
        let mut cell = HostCell::new(live.output, live.data);
        if let Some(number) = tip_number {
            if let Ok(Some(hash)) = rpc.get_block_hash(number) {
                cell.block_hash = Some(hash);
                cell.block_number = Some(number);
            }
        }
        host.insert_cell_ex(live.out_point, cell);
    }
    if let Some(number) = tip_number {
        let hash = rpc
            .get_block_hash(number)
            .map_err(HostQueryError::Rpc)?
            .ok_or(HostQueryError::MissingBlockHash { number })?;
        // Headers are optional for tip seeding: Process.assemble only probes
        // `get_block_hash`; incomplete/missing header RPC must not fail the tick.
        match rpc.get_header_by_number(number) {
            Ok(Some(bytes)) => match Header::from_slice(&bytes) {
                Ok(header) => host.insert_header(number, header),
                Err(_) => host.insert_block_hash(number, hash),
            },
            _ => host.insert_block_hash(number, hash),
        }
    }
    Ok(host)
}

/// In-memory seed from caller-provided live cells (no RPC).
pub fn seed_mock_host_from_live_cells(
    cells: &[(OutPoint, CellOutput, Vec<u8>)],
    tip: Option<(u64, [u8; 32])>,
) -> MockHost {
    let host = MockHost::new();
    for (op, output, data) in cells {
        host.insert_cell(op.clone(), output.clone(), data.clone());
    }
    if let Some((number, hash)) = tip {
        host.insert_block_hash(number, hash);
    }
    host
}

/// Molecule bytes for `find_cell_by_out_point` / `get_live_cell` output field.
pub fn encode_cell_output(output: &CellOutput) -> Vec<u8> {
    output.as_slice().to_vec()
}

/// Raw cell data for `find_cell_data_by_out_point`.
pub fn encode_cell_data(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

/// 32-byte block hash for `get_block_hash` syscall load.
pub fn encode_block_hash(hash: &[u8; 32]) -> Vec<u8> {
    hash.to_vec()
}

/// Convert molecule [`SearchKey`] bytes into a CKB indexer JSON search key.
pub fn search_key_molecule_to_json(bytes: &[u8]) -> Result<serde_json::Value, RpcError> {
    let key: SearchKey = serde_molecule::from_slice(bytes, false)
        .map_err(|_| RpcError::Unavailable("get_cells_page: invalid molecule SearchKey".into()))?;
    Ok(search_key_to_json(&key))
}

fn search_key_to_json(key: &SearchKey) -> serde_json::Value {
    let script_type = match key.script_type {
        ScriptType::Lock => "lock",
        ScriptType::Type => "type",
    };
    let mode = match key.script_search_mode.unwrap_or(SearchMode::Prefix) {
        SearchMode::Prefix => "prefix",
        SearchMode::Exact => "exact",
        SearchMode::Partial => "partial",
    };
    let mut obj = serde_json::json!({
        "script": hq_script_to_json(&key.script),
        "script_type": script_type,
        "script_search_mode": mode,
        "with_data": key.with_data.unwrap_or(true),
    });
    if let Some(filter) = &key.filter {
        let mut f = serde_json::Map::new();
        if let Some(script) = &filter.script {
            f.insert("script".into(), hq_script_to_json(script));
        }
        if let Some([start, end]) = filter.script_len_range {
            f.insert(
                "script_len_range".into(),
                serde_json::json!([hex_u64(start), hex_u64(end)]),
            );
        }
        if let Some([start, end]) = filter.output_data_len_range {
            f.insert(
                "output_data_len_range".into(),
                serde_json::json!([hex_u64(start), hex_u64(end)]),
            );
        }
        if let Some([start, end]) = filter.output_capacity_range {
            f.insert(
                "output_capacity_range".into(),
                serde_json::json!([hex_u64(start), hex_u64(end)]),
            );
        }
        if let Some([start, end]) = filter.block_range {
            f.insert(
                "block_range".into(),
                serde_json::json!([hex_u64(start), hex_u64(end)]),
            );
        }
        if let Some(data) = &filter.output_data {
            f.insert(
                "output_data".into(),
                serde_json::Value::String(hex_bytes(data)),
            );
            let mode = filter.output_data_filter_mode.unwrap_or(SearchMode::Prefix);
            let mode = match mode {
                SearchMode::Prefix => "prefix",
                SearchMode::Exact => "exact",
                SearchMode::Partial => "partial",
            };
            f.insert(
                "output_data_filter_mode".into(),
                serde_json::Value::String(mode.into()),
            );
        }
        if !f.is_empty() {
            obj["filter"] = serde_json::Value::Object(f);
        }
    }
    obj
}

fn hq_script_to_json(script: &HqScript) -> serde_json::Value {
    serde_json::json!({
        "code_hash": hex_bytes(&script.code_hash),
        "hash_type": hash_type_str(script.hash_type),
        "args": hex_bytes(&script.args),
    })
}

fn hash_type_str(ht: u8) -> &'static str {
    match ht {
        0 => "data",
        1 => "type",
        2 => "data1",
        4 => "data2",
        _ => "type",
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hex_u64(v: u64) -> String {
    format!("0x{v:x}")
}

/// Stub [`HostQueryRpc`] for in-memory `Rpc` test doubles (empty / `None` responses).
#[macro_export]
macro_rules! impl_host_query_rpc_stubs {
    ($ty:ty) => {
        impl $crate::HostQueryRpc for $ty {
            fn get_live_cell(
                &self,
                _: &ckb_types::packed::OutPoint,
            ) -> Result<Option<$crate::rpc::LiveCell>, $crate::rpc::RpcError> {
                Ok(None)
            }

            fn get_block_hash(&self, _: u64) -> Result<Option<[u8; 32]>, $crate::rpc::RpcError> {
                Ok(None)
            }

            fn get_header_by_number(
                &self,
                _: u64,
            ) -> Result<Option<Vec<u8>>, $crate::rpc::RpcError> {
                Ok(None)
            }

            fn get_header(&self, _: &[u8; 32]) -> Result<Option<Vec<u8>>, $crate::rpc::RpcError> {
                Ok(None)
            }

            fn get_cells_page(
                &self,
                _: &[u8],
                _: u32,
                _: &[u8],
            ) -> Result<(Vec<$crate::IndexedLiveCell>, Vec<u8>), $crate::rpc::RpcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid_process_executor::host_query::SearchKeyFilter;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, Header, RawHeader, Script};
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct StubHostRpc {
        cells: HashMap<Vec<u8>, LiveCell>,
        block_hashes: HashMap<u64, [u8; 32]>,
        headers_by_number: HashMap<u64, Vec<u8>>,
        headers_by_hash: HashMap<[u8; 32], Vec<u8>>,
        /// All cells for get_cells_page (filtered via MockHost).
        index: Mutex<MockHost>,
    }

    impl StubHostRpc {
        fn new() -> Self {
            Self {
                cells: HashMap::new(),
                block_hashes: HashMap::new(),
                headers_by_number: HashMap::new(),
                headers_by_hash: HashMap::new(),
                index: Mutex::new(MockHost::new()),
            }
        }

        fn insert(&mut self, cell: LiveCell) {
            let index = self.index.lock().unwrap();
            index.insert_cell(
                cell.out_point.clone(),
                cell.output.clone(),
                cell.data.clone(),
            );
            drop(index);
            self.cells.insert(cell.out_point.as_slice().to_vec(), cell);
        }

        fn insert_hash(&mut self, number: u64, hash: [u8; 32]) {
            self.block_hashes.insert(number, hash);
        }

        fn insert_header(&mut self, number: u64, header: Header) {
            let bytes = header.as_slice().to_vec();
            let hash_b32 = header.calc_header_hash();
            let mut hash = [0u8; 32];
            hash.copy_from_slice(hash_b32.as_slice());
            self.block_hashes.insert(number, hash);
            self.headers_by_number.insert(number, bytes.clone());
            self.headers_by_hash.insert(hash, bytes);
            self.index.lock().unwrap().insert_header(number, header);
        }
    }

    impl HostQueryRpc for StubHostRpc {
        fn get_live_cell(&self, out_point: &OutPoint) -> Result<Option<LiveCell>, RpcError> {
            Ok(self.cells.get(out_point.as_slice()).cloned())
        }

        fn get_block_hash(&self, number: u64) -> Result<Option<[u8; 32]>, RpcError> {
            Ok(self.block_hashes.get(&number).copied())
        }

        fn get_header_by_number(&self, number: u64) -> Result<Option<Vec<u8>>, RpcError> {
            Ok(self.headers_by_number.get(&number).cloned())
        }

        fn get_header(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, RpcError> {
            Ok(self.headers_by_hash.get(hash).cloned())
        }

        fn get_cells_page(
            &self,
            search_key_molecule: &[u8],
            limit: u32,
            after: &[u8],
        ) -> Result<(Vec<IndexedLiveCell>, Vec<u8>), RpcError> {
            let host = self.index.lock().unwrap().clone();
            let page = HostQuery::get_cells(&host, search_key_molecule, 0, u64::from(limit), after)
                .map_err(|_| RpcError::Unavailable("invalid search_key".into()))?;
            let pag: Pagination = serde_molecule::from_slice(&page, false)
                .map_err(|e| RpcError::Unavailable(format!("pagination: {e}")))?;
            let mut cells = Vec::with_capacity(pag.objects.len());
            for obj in &pag.objects {
                let op = OutPoint::new_builder()
                    .tx_hash(Byte32::from_slice(&obj.out_point.tx_hash).unwrap())
                    .index(obj.out_point.index.pack())
                    .build();
                let Some(live) = self.cells.get(op.as_slice()) else {
                    continue;
                };
                cells.push(IndexedLiveCell {
                    cell: live.clone(),
                    block_number: obj.block_number,
                    tx_index: obj.tx_index,
                });
            }
            Ok((cells, pag.last_cursor))
        }
    }

    fn live_at(fill: u8, cap: u64) -> LiveCell {
        let out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[fill; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let output = CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(
                Script::new_builder()
                    .code_hash([fill; 32].pack())
                    .hash_type(ScriptHashType::Type.into())
                    .build(),
            )
            .build();
        LiveCell {
            out_point,
            output,
            data: vec![fill],
        }
    }

    fn sample_header(number: u64) -> Header {
        Header::new_builder()
            .raw(RawHeader::new_builder().number(number.pack()).build())
            .build()
    }

    #[test]
    fn seed_from_live_cells_serves_mock_host() {
        let cell = live_at(0x11, 100_000_000_000);
        let op = cell.out_point.clone();
        let host = seed_mock_host_from_live_cells(
            &[(
                cell.out_point.clone(),
                cell.output.clone(),
                cell.data.clone(),
            )],
            Some((99, [0xAB; 32])),
        );
        let got = host.get_cell(op.as_slice()).expect("cell");
        assert_eq!(got.output.as_slice(), cell.output.as_slice());
        assert_eq!(got.data, cell.data);
        assert_eq!(host.get_block_hash(99), Some([0xAB; 32]));
    }

    #[test]
    fn seed_host_for_batch_matches_seed_for_draft() {
        let live = live_at(0x22, 50_000_000_000);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        rpc.insert_hash(7, [0x77; 32]);
        let draft = DraftCell {
            out_point: live.out_point.clone(),
            output: live.output.clone(),
            data: live.data.clone(),
        };
        let host = seed_host_for_batch(&rpc, &[draft], 7).expect("seed");
        assert_eq!(host.get_block_hash(7), Some([0x77; 32]));
    }

    #[test]
    fn seed_for_draft_resolves_rpc_cells_and_tip() {
        let live = live_at(0x22, 50_000_000_000);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        rpc.insert_hash(7, [0x77; 32]);
        let draft = DraftCell {
            out_point: live.out_point.clone(),
            output: live.output.clone(),
            data: live.data.clone(),
        };
        let host = seed_mock_host_for_draft(&rpc, &[draft], Some(7)).expect("seed");
        let got = host.get_cell(live.out_point.as_slice()).unwrap();
        assert_eq!(got.data, live.data);
        assert_eq!(host.get_block_hash(7), Some([0x77; 32]));
    }

    #[test]
    fn seed_for_draft_missing_cell_errors() {
        let out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x33; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let draft = DraftCell {
            out_point: out_point.clone(),
            output: CellOutput::new_builder().capacity(1u64.pack()).build(),
            data: vec![],
        };
        let rpc = StubHostRpc::new();
        let err = match seed_mock_host_for_draft(&rpc, &[draft], None) {
            Err(e) => e,
            Ok(_) => panic!("expected missing live cell"),
        };
        assert!(matches!(err, HostQueryError::MissingLiveCell(_)));
    }

    #[test]
    fn seed_for_draft_missing_tip_hash_errors() {
        let live = live_at(0x44, 1);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        let draft = DraftCell {
            out_point: live.out_point,
            output: live.output,
            data: live.data,
        };
        let err = match seed_mock_host_for_draft(&rpc, &[draft], Some(5)) {
            Err(e) => e,
            Ok(_) => panic!("expected missing block hash"),
        };
        assert!(matches!(
            err,
            HostQueryError::MissingBlockHash { number: 5 }
        ));
    }

    #[test]
    fn encoding_helpers_match_molecule_slices() {
        let output = CellOutput::new_builder().capacity(42u64.pack()).build();
        assert_eq!(encode_cell_output(&output), output.as_slice());
        assert_eq!(encode_cell_data(b"data"), b"data".to_vec());
        assert_eq!(encode_block_hash(&[0x55; 32]), [0x55; 32].to_vec());
    }

    #[test]
    fn rpc_host_query_find_cell_by_out_point() {
        let live = live_at(0xA1, 10);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        let hq = RpcHostQuery::new(&rpc);
        let got = HostQuery::find_cell_by_out_point(&hq, live.out_point.as_slice()).unwrap();
        assert_eq!(got, live.output.as_slice());
        assert!(HostQuery::find_cell_by_out_point(&hq, &[0u8; 36]).is_none());
    }

    #[test]
    fn rpc_host_query_find_cell_data_by_out_point() {
        let live = live_at(0xA2, 10);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        let hq = RpcHostQuery::new(&rpc);
        let got = HostQuery::find_cell_data_by_out_point(&hq, live.out_point.as_slice()).unwrap();
        assert_eq!(got, live.data);
    }

    #[test]
    fn rpc_host_query_get_live_cell_ok_and_missing() {
        let live = live_at(0xA3, 10);
        let mut rpc = StubHostRpc::new();
        rpc.insert(live.clone());
        let hq = RpcHostQuery::new(&rpc);
        let with = HostQuery::get_live_cell(&hq, live.out_point.as_slice(), true).unwrap();
        let decoded: ExecLiveCell = serde_molecule::from_slice(&with, false).unwrap();
        assert!(decoded.output.is_some());
        assert_eq!(decoded.data.as_deref(), Some(live.data.as_slice()));
        let missing = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x00; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let empty = HostQuery::get_live_cell(&hq, missing.as_slice(), true).unwrap();
        let decoded: ExecLiveCell = serde_molecule::from_slice(&empty, false).unwrap();
        assert!(decoded.output.is_none());
    }

    #[test]
    fn rpc_host_query_get_header_and_by_number() {
        let header = sample_header(7);
        let mut rpc = StubHostRpc::new();
        rpc.insert_header(7, header.clone());
        let hq = RpcHostQuery::new(&rpc);
        let by_number = HostQuery::get_header_by_number(&hq, 7).unwrap();
        assert_eq!(by_number, header.as_slice());
        let hash = header.calc_header_hash();
        let by_hash = HostQuery::get_header(&hq, hash.as_slice()).unwrap();
        assert_eq!(by_hash, header.as_slice());
        assert!(HostQuery::get_header_by_number(&hq, 8).is_none());
    }

    #[test]
    fn rpc_host_query_get_block_hash() {
        let mut rpc = StubHostRpc::new();
        rpc.insert_hash(3, [0x33; 32]);
        let hq = RpcHostQuery::new(&rpc);
        assert_eq!(HostQuery::get_block_hash(&hq, 3), Some([0x33; 32]));
        assert!(HostQuery::get_block_hash(&hq, 4).is_none());
    }

    #[test]
    fn rpc_host_query_get_cells_bounded_pagination_and_script_len_range() {
        let mut rpc = StubHostRpc::new();
        // Two lock-matched cells; one with type (script_len > 0), one without.
        let lock_code = [0xBBu8; 32];
        let a = {
            let mut c = live_at(0x01, 100);
            c.output = c
                .output
                .as_builder()
                .lock(
                    Script::new_builder()
                        .code_hash(lock_code.pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args([0x01].pack())
                        .build(),
                )
                .type_(
                    Some(
                        Script::new_builder()
                            .code_hash([0x11; 32].pack())
                            .hash_type(ScriptHashType::Type.into())
                            .args([0xAA; 8].pack())
                            .build(),
                    )
                    .pack(),
                )
                .build();
            c
        };
        let b = {
            let mut c = live_at(0x02, 100);
            c.output = c
                .output
                .as_builder()
                .lock(
                    Script::new_builder()
                        .code_hash(lock_code.pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args([0x02].pack())
                        .build(),
                )
                .build();
            c
        };
        rpc.insert(a.clone());
        rpc.insert(b.clone());

        let key = SearchKey {
            script: HqScript {
                code_hash: lock_code,
                hash_type: 1,
                args: vec![],
            },
            script_type: ScriptType::Lock,
            script_search_mode: Some(SearchMode::Prefix),
            filter: Some(SearchKeyFilter {
                script: None,
                // Require secondary (type) script length in [1, 1000)
                script_len_range: Some([1, 1000]),
                output_data: None,
                output_data_filter_mode: None,
                output_data_len_range: None,
                output_capacity_range: None,
                block_range: None,
            }),
            with_data: Some(true),
            group_by_transaction: None,
        };
        let key_bytes = serde_molecule::to_vec(&key, false).unwrap();
        let hq = RpcHostQuery::new(&rpc);
        let page0 = HostQuery::get_cells(&hq, &key_bytes, 0, 10, &[]).unwrap();
        let pag: Pagination = serde_molecule::from_slice(&page0, false).unwrap();
        assert_eq!(
            pag.objects.len(),
            1,
            "script_len_range must drop typeless cell"
        );
        let expected: [u8; 32] = a.out_point.tx_hash().unpack();
        assert_eq!(pag.objects[0].out_point.tx_hash, expected);

        // Pagination: limit 1 over unfiltered lock prefix.
        let key_all = SearchKey {
            script: HqScript {
                code_hash: lock_code,
                hash_type: 1,
                args: vec![],
            },
            script_type: ScriptType::Lock,
            script_search_mode: Some(SearchMode::Prefix),
            filter: None,
            with_data: Some(false),
            group_by_transaction: None,
        };
        let key_all_bytes = serde_molecule::to_vec(&key_all, false).unwrap();
        let p0 = HostQuery::get_cells(&hq, &key_all_bytes, 0, 1, &[]).unwrap();
        let pag0: Pagination = serde_molecule::from_slice(&p0, false).unwrap();
        assert_eq!(pag0.objects.len(), 1);
        assert!(!pag0.last_cursor.is_empty());
        let p1 = HostQuery::get_cells(&hq, &key_all_bytes, 0, 1, &pag0.last_cursor).unwrap();
        let pag1: Pagination = serde_molecule::from_slice(&p1, false).unwrap();
        assert_eq!(pag1.objects.len(), 1);
        assert_ne!(
            pag0.objects[0].out_point.tx_hash,
            pag1.objects[0].out_point.tx_hash
        );
    }

    #[test]
    fn search_key_molecule_to_json_roundtrip_fields() {
        let key = SearchKey {
            script: HqScript {
                code_hash: [0xAB; 32],
                hash_type: 1,
                args: vec![0x01, 0x02],
            },
            script_type: ScriptType::Type,
            script_search_mode: Some(SearchMode::Exact),
            filter: Some(SearchKeyFilter {
                script: None,
                script_len_range: Some([0, 64]),
                output_data: None,
                output_data_filter_mode: None,
                output_data_len_range: None,
                output_capacity_range: None,
                block_range: None,
            }),
            with_data: Some(true),
            group_by_transaction: None,
        };
        let bytes = serde_molecule::to_vec(&key, false).unwrap();
        let json = search_key_molecule_to_json(&bytes).unwrap();
        assert_eq!(json["script_type"], "type");
        assert_eq!(json["script_search_mode"], "exact");
        assert_eq!(json["filter"]["script_len_range"][0], "0x0");
        assert_eq!(json["filter"]["script_len_range"][1], "0x40");
    }
}
