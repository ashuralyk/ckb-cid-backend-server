use crate::rpc::{LiveCell, Rpc, RpcError};
use std::collections::HashMap;

pub type ProcessFallback =
    Box<dyn Fn(&[u8; 32], &LiveCell) -> Option<Vec<u8>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ResolvedProcess {
    pub type_hash: [u8; 32],
    pub live_cell: LiveCell,
    pub binary: Vec<u8>,
}

pub struct ProcessResolver {
    fallback: Option<ProcessFallback>,
    binary_cache: HashMap<[u8; 32], Vec<u8>>,
}

impl Default for ProcessResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessResolver {
    pub fn new() -> Self {
        Self {
            fallback: None,
            binary_cache: HashMap::new(),
        }
    }

    pub fn with_fallback(fallback: ProcessFallback) -> Self {
        Self {
            fallback: Some(fallback),
            binary_cache: HashMap::new(),
        }
    }

    pub fn with_fallback_binary(binary: Vec<u8>) -> Self {
        Self::with_fallback(Box::new(move |_hash, cell| {
            if looks_like_elf(&cell.data) {
                None
            } else {
                Some(binary.clone())
            }
        }))
    }

    pub fn resolve<R: Rpc>(
        &mut self,
        rpc: &R,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<ResolvedProcess>, RpcError> {
        let Some(cell) = rpc.get_live_process_by_type_hash(process_type_hash)? else {
            log::warn!(
                "no live Process cell for type hash {:02x?}",
                process_type_hash
            );
            return Ok(None);
        };
        let binary = if let Some(cached) = self.binary_cache.get(process_type_hash) {
            cached.clone()
        } else if looks_like_elf(&cell.data) {
            cell.data.clone()
        } else if let Some(fb) = &self.fallback {
            match fb(process_type_hash, &cell) {
                Some(b) if looks_like_elf(&b) => b,
                _ => {
                    log::warn!("Process cell data empty/unusable and fallback missing");
                    return Ok(None);
                }
            }
        } else {
            log::warn!("Process cell data empty/unusable; no fallback installed");
            return Ok(None);
        };
        self.binary_cache
            .insert(*process_type_hash, binary.clone());
        Ok(Some(ResolvedProcess {
            type_hash: *process_type_hash,
            live_cell: cell,
            binary,
        }))
    }
}

fn looks_like_elf(data: &[u8]) -> bool {
    data.len() >= 4 && data[0] == 0x7f && data[1] == b'E' && data[2] == b'L' && data[3] == b'F'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{LiveCell, Rpc, RpcError};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;
    use std::cell::RefCell;

    struct TestRpc {
        cell: RefCell<Option<LiveCell>>,
    }

    impl Rpc for TestRpc {
        fn list_live_intentions(
            &self,
            _c: &[u8; 32],
            _h: ScriptHashType,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(vec![])
        }
        fn get_live_process_by_type_hash(
            &self,
            _h: &[u8; 32],
        ) -> Result<Option<LiveCell>, RpcError> {
            Ok(self.cell.borrow().clone())
        }
        fn submit_transaction(&self, _tx: &TransactionView) -> Result<Byte32, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
    }

    fn process_cell(data: Vec<u8>) -> ( [u8; 32], LiveCell ) {
        let ts = Script::new_builder()
            .code_hash([0x51u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(vec![0xABu8; 32].pack())
            .build();
        let hash = crate::rpc::script_hash32(&ts);
        let op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let cell = LiveCell {
            out_point: op,
            output: CellOutput::new_builder()
                .capacity(500_0000_0000u64.pack())
                .lock(
                    Script::new_builder()
                        .code_hash([0u8; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args(Vec::<u8>::new().pack())
                        .build(),
                )
                .type_(Some(ts).pack())
                .build(),
            data,
        };
        (hash, cell)
    }

    #[test]
    fn resolve_uses_live_cell_data_when_non_empty() {
        let (hash, cell) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver = ProcessResolver::new();
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("resolved");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F', 0x01]);
        assert_eq!(got.type_hash, hash);
    }

    #[test]
    fn resolve_skips_missing_and_empty_without_fallback() {
        let rpc = TestRpc {
            cell: RefCell::new(None),
        };
        let mut resolver = ProcessResolver::new();
        assert!(resolver.resolve(&rpc, &[0x11; 32]).unwrap().is_none());

        let (hash, cell) = process_cell(vec![]);
        rpc.cell.replace(Some(cell));
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn fallback_used_when_live_data_not_elf() {
        let (hash, cell) = process_cell(b"not-an-elf".to_vec());
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver = ProcessResolver::with_fallback_binary(vec![0x7f, b'E', b'L', b'F']);
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("fallback");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F']);
        assert_eq!(got.live_cell.data, b"not-an-elf");
    }

    #[test]
    fn resolve_refetches_live_cell_out_point_after_recreate() {
        let (hash, cell1) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell1.clone())),
        };
        let mut resolver = ProcessResolver::new();
        let first = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(first.live_cell.out_point, cell1.out_point);

        let mut cell2 = cell1.clone();
        cell2.out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x88; 32]).unwrap())
            .index(0u32.pack())
            .build();
        rpc.cell.replace(Some(cell2.clone()));
        let second = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(second.live_cell.out_point, cell2.out_point);
        assert_eq!(second.binary, first.binary);
    }
}
