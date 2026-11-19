//! RPC-backed SSRI host-query helpers for seeding [`MockHost`] in offline tests
//! and live digest execution. Syscall numbers match ckb-ssri-std / the pinned
//! `cid-process-executor` VM; see upstream `docs/ssri-backend-interface.md`.
//!
//! ## Syscall → [`HostQueryRpc`] mapping
//!
//! | Syscall | Number | Trait surface |
//! |---------|--------|---------------|
//! | `find_cell_by_out_point` | 2287 | [`get_live_cell`] + [`encode_cell_output`] |
//! | `find_cell_data_by_out_point` | 2297 | [`get_live_cell`] + [`encode_cell_data`] |
//! | `get_live_cell` | 2317 | [`get_live_cell`] |
//! | `get_header` | 2327 | [`get_header`] |
//! | `get_header_by_number` | 2337 | [`get_header_by_number`] |
//! | `get_block_hash` | 2347 | [`get_block_hash`] |
//! | `get_cells` | 2367 | [`get_cells_page`] |

use crate::rpc::{LiveCell, RpcError};
use cid_process_executor::{DraftCell, MockHost};
use ckb_types::packed::{CellOutput, OutPoint};
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

/// Minimal RPC surface for host-query seeding and indexer pagination.
/// Implement on the live CKB adapter without extending the core [`crate::rpc::Rpc`] trait.
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
    ) -> Result<(Vec<LiveCell>, Vec<u8>), RpcError>;
}

/// Populate [`MockHost`] from chain live cells referenced by `draft_cells`, plus
/// optional tip block hash.
/// Seed [`MockHost`] for one assemble batch using live RPC at `tip_number`.
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
        host.insert_cell(live.out_point, live.output, live.data);
    }
    if let Some(number) = tip_number {
        let hash = rpc
            .get_block_hash(number)
            .map_err(HostQueryError::Rpc)?
            .ok_or(HostQueryError::MissingBlockHash { number })?;
        host.insert_block_hash(number, hash);
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

            fn get_block_hash(
                &self,
                _: u64,
            ) -> Result<Option<[u8; 32]>, $crate::rpc::RpcError> {
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
            ) -> Result<(Vec<$crate::rpc::LiveCell>, Vec<u8>), $crate::rpc::RpcError> {
                Ok((Vec::new(), Vec::new()))
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, Script};
    use std::collections::HashMap;

    struct StubHostRpc {
        cells: HashMap<Vec<u8>, LiveCell>,
        block_hashes: HashMap<u64, [u8; 32]>,
    }

    impl StubHostRpc {
        fn new() -> Self {
            Self {
                cells: HashMap::new(),
                block_hashes: HashMap::new(),
            }
        }

        fn insert(&mut self, cell: LiveCell) {
            self.cells.insert(cell.out_point.as_slice().to_vec(), cell);
        }

        fn insert_hash(&mut self, number: u64, hash: [u8; 32]) {
            self.block_hashes.insert(number, hash);
        }
    }

    impl HostQueryRpc for StubHostRpc {
        fn get_live_cell(&self, out_point: &OutPoint) -> Result<Option<LiveCell>, RpcError> {
            Ok(self.cells.get(out_point.as_slice()).cloned())
        }

        fn get_block_hash(&self, number: u64) -> Result<Option<[u8; 32]>, RpcError> {
            Ok(self.block_hashes.get(&number).copied())
        }

        fn get_header_by_number(&self, _number: u64) -> Result<Option<Vec<u8>>, RpcError> {
            Ok(None)
        }

        fn get_header(&self, _hash: &[u8; 32]) -> Result<Option<Vec<u8>>, RpcError> {
            Ok(None)
        }

        fn get_cells_page(
            &self,
            _search_key_molecule: &[u8],
            _limit: u32,
            _after: &[u8],
        ) -> Result<(Vec<LiveCell>, Vec<u8>), RpcError> {
            Ok((Vec::new(), Vec::new()))
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
}
