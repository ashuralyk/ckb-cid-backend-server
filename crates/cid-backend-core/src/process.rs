use crate::rpc::{out_point_key, script_hash32, LiveCell, Rpc, RpcError};
use crate::type_id::{TypeIdIdentity, TYPE_ID_CODE_HASH};
use ckb_types::core::ScriptHashType;
use ckb_types::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

pub type ProcessFallback = Box<dyn Fn(&[u8; 32], &LiveCell) -> Option<Vec<u8>> + Send + Sync>;

/// Static configuration for one Process (Process-as-lock on chain).
#[derive(Clone, Debug)]
pub struct ProcessEntry {
    /// `calc_script_hash` of the state cell built-in TYPE_ID type script (= Intention `process_type_hash`).
    pub identity_type_hash: [u8; 32],
    /// TYPE_ID args on the Process **state** cell type script.
    pub identity: TypeIdIdentity,
    /// TYPE_ID identity of the Process **code** cell (SSRI ELF).
    pub code: TypeIdIdentity,
    /// Optional local ELF fallback when the live code cell is empty/unusable.
    pub ssri_elf_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ResolvedProcess {
    pub type_hash: [u8; 32],
    pub live_cell: LiveCell,
    pub binary: Vec<u8>,
    pub code_cell: Option<LiveCell>,
}

struct CachedBinary {
    source_key: Vec<u8>,
    binary: Vec<u8>,
}

pub struct ProcessResolver {
    entry: ProcessEntry,
    fallback: Option<ProcessFallback>,
    binary_cache: HashMap<[u8; 32], CachedBinary>,
    tick_process_code: Option<LiveCell>,
}

impl ProcessResolver {
    pub fn for_entry(entry: ProcessEntry) -> Self {
        if entry.identity_type_hash != entry.identity.type_script_hash() {
            log::warn!("ProcessEntry identity_type_hash does not match derived state TYPE_ID hash");
        }
        Self {
            entry,
            fallback: None,
            binary_cache: HashMap::new(),
            tick_process_code: None,
        }
    }

    pub fn with_fallback(mut self, fallback: ProcessFallback) -> Self {
        self.fallback = Some(fallback);
        self
    }

    /// Test / fixture helper: use `binary` when no Process code cell or
    /// `ssri_elf_path` is available. Process-as-lock state cell data is never
    /// treated as the SSRI ELF, so this always returns the provided bytes.
    pub fn with_fallback_binary(mut self, binary: Vec<u8>) -> Self {
        self.fallback = Some(Box::new(move |_hash, _cell| Some(binary.clone())));
        self
    }

    pub fn entry(&self) -> &ProcessEntry {
        &self.entry
    }

    pub fn set_tick_process_code(&mut self, code: Option<&LiveCell>) {
        let next_key = code.map(|c| out_point_key(&c.out_point));
        let prev_key = self
            .tick_process_code
            .as_ref()
            .map(|c| out_point_key(&c.out_point));
        if next_key != prev_key {
            self.binary_cache.clear();
        }
        self.tick_process_code = code.cloned();
    }

    pub fn resolve<R: Rpc>(
        &mut self,
        rpc: &R,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<ResolvedProcess>, RpcError> {
        if process_type_hash != &self.entry.identity_type_hash {
            return Ok(None);
        }
        // Process-as-lock: state cell type is built-in TYPE_ID; discover by exact
        // type script (args), not by treating process_type_hash as a type code_hash.
        let matches = rpc.find_live_cells_by_type_script(&self.entry.identity.type_script(), 2)?;
        let Some(cell) = matches.into_iter().next() else {
            log::warn!(
                "no live Process cell for type hash {:02x?}",
                process_type_hash
            );
            return Ok(None);
        };
        if !state_is_process_lock(&self.entry, &cell) {
            log::warn!("live Process cell failed Process-as-lock validation");
            return Ok(None);
        }
        let source_key = self
            .tick_process_code
            .as_ref()
            .map(|c| out_point_key(&c.out_point))
            .unwrap_or_default();
        let binary = match self.binary_cache.get(process_type_hash) {
            Some(cached) if cached.source_key == source_key => cached.binary.clone(),
            _ => match self.derive_binary(process_type_hash, &cell) {
                Some(binary) => {
                    self.binary_cache.insert(
                        *process_type_hash,
                        CachedBinary {
                            source_key,
                            binary: binary.clone(),
                        },
                    );
                    binary
                }
                None => {
                    self.binary_cache.remove(process_type_hash);
                    return Ok(None);
                }
            },
        };
        Ok(Some(ResolvedProcess {
            type_hash: *process_type_hash,
            live_cell: cell,
            binary,
            code_cell: self.tick_process_code.clone(),
        }))
    }

    fn derive_binary(&self, process_type_hash: &[u8; 32], cell: &LiveCell) -> Option<Vec<u8>> {
        if let Some(code) = &self.tick_process_code {
            if looks_like_elf(&code.data) {
                return Some(code.data.clone());
            }
            log::warn!(
                "Process code cell data is not a usable ELF ({} bytes)",
                code.data.len()
            );
        } else {
            log::warn!("no tick Process code snapshot (code deps refresh missing process_code?)");
        }
        if let Some(path) = &self.entry.ssri_elf_path {
            match std::fs::read(path) {
                Ok(b) if looks_like_elf(&b) => return Some(b),
                Ok(b) => {
                    log::warn!("ssri_elf_path {:?} is not ELF ({} bytes)", path, b.len());
                }
                Err(e) => {
                    log::warn!("ssri_elf_path {:?} read failed: {e}", path);
                }
            }
        }
        if let Some(fb) = &self.fallback {
            match fb(process_type_hash, cell) {
                Some(b) if looks_like_elf(&b) => return Some(b),
                _ => log::warn!("Process fallback missing or not ELF"),
            }
        }
        None
    }
}

/// Process **state** cell: built-in TYPE_ID type; lock references Process code TYPE_ID (`hash_type=type`).
pub fn state_is_process_lock(entry: &ProcessEntry, cell: &LiveCell) -> bool {
    let Some(ts) = cell.type_script() else {
        log::warn!("Process state cell has no type script");
        return false;
    };
    let code_hash: [u8; 32] = ts.code_hash().unpack();
    if code_hash != TYPE_ID_CODE_HASH {
        log::warn!("Process state type is not built-in TYPE_ID");
        return false;
    }
    let Ok(ht) = ScriptHashType::try_from(ts.hash_type()) else {
        return false;
    };
    if ht != ScriptHashType::Type {
        log::warn!("Process state type script hash_type={ht:?} (requires type)");
        return false;
    }
    let args: Vec<u8> = ts.args().unpack();
    if args.len() != 32 || args.as_slice() != entry.identity.args.as_slice() {
        log::warn!("Process state TYPE_ID args do not match configured identity");
        return false;
    }
    if script_hash32(&ts) != entry.identity_type_hash {
        log::warn!("Process state type hash does not match identity_type_hash");
        return false;
    }
    let lock = cell.lock();
    let lock_code: [u8; 32] = lock.code_hash().unpack();
    let expected_lock = entry.code.type_script_hash();
    if lock_code != expected_lock {
        log::warn!("Process lock code_hash does not match Process code TYPE_ID");
        return false;
    }
    let Ok(lock_ht) = ScriptHashType::try_from(lock.hash_type()) else {
        return false;
    };
    if lock_ht != ScriptHashType::Type {
        log::warn!("Process lock hash_type={lock_ht:?} (requires type)");
        return false;
    }
    true
}

fn looks_like_elf(data: &[u8]) -> bool {
    data.len() >= 4 && data[0] == 0x7f && data[1] == b'E' && data[2] == b'L' && data[3] == b'F'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{LiveCell, Rpc, RpcError, TxStatus};
    use crate::type_id::{type_id_script, type_id_script_hash};
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::Script;
    use ckb_types::packed::{Byte32, CellOutput, OutPoint};
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
        fn find_live_cells_by_type_script(
            &self,
            type_script: &Script,
            max_results: usize,
        ) -> Result<Vec<LiveCell>, RpcError> {
            let want = script_hash32(type_script);
            let mut out = Vec::new();
            if let Some(cell) = self.cell.borrow().clone() {
                if cell
                    .type_script()
                    .map(|ts| script_hash32(&ts) == want)
                    .unwrap_or(false)
                {
                    out.push(cell);
                }
            }
            out.truncate(max_results);
            Ok(out)
        }
        fn submit_transaction(&self, _tx: &TransactionView) -> Result<Byte32, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
        fn get_transaction_status(&self, _tx_hash: &Byte32) -> Result<TxStatus, RpcError> {
            Err(RpcError::Unavailable("no".into()))
        }
        fn tip_block_number(&self) -> Result<u64, RpcError> {
            Ok(0)
        }
    }

    fn test_entry(state_args: [u8; 32], code_args: [u8; 32]) -> ProcessEntry {
        ProcessEntry {
            identity_type_hash: type_id_script_hash(&state_args),
            identity: TypeIdIdentity::new(state_args),
            code: TypeIdIdentity::new(code_args),
            ssri_elf_path: None,
        }
    }

    fn process_state_cell(entry: &ProcessEntry, data: Vec<u8>) -> ([u8; 32], LiveCell) {
        let hash = entry.identity_type_hash;
        let lock = Script::new_builder()
            .code_hash(entry.code.type_script_hash().pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let ts = type_id_script(&entry.identity.args);
        let op = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x77; 32]).unwrap())
            .index(0u32.pack())
            .build();
        let cell = LiveCell {
            out_point: op,
            output: CellOutput::new_builder()
                .capacity(500_0000_0000u64.pack())
                .lock(lock)
                .type_(Some(ts).pack())
                .build(),
            data,
        };
        (hash, cell)
    }

    fn code_live(tx_byte: u8, data: Vec<u8>) -> LiveCell {
        LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[tx_byte; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output: CellOutput::new_builder()
                .capacity(1000u64.pack())
                .lock(
                    Script::new_builder()
                        .code_hash([0u8; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args(Vec::<u8>::new().pack())
                        .build(),
                )
                .build(),
            data,
        }
    }

    #[test]
    fn resolve_uses_code_cell_elf_not_state_data() {
        let entry = test_entry([0xab; 32], [0xcd; 32]);
        let (hash, state) = process_state_cell(&entry, vec![0x7f, b'E', b'L', b'F', 0xAA]);
        let code = code_live(0x10, vec![0x7f, b'E', b'L', b'F', 0xBB]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code));
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("resolved");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F', 0xBB]);
        assert_eq!(got.type_hash, hash);
        assert!(got.code_cell.is_some());
    }

    #[test]
    fn resolve_skips_missing_and_wrong_identity_hash() {
        let entry = test_entry([0x11; 32], [0x22; 32]);
        let rpc = TestRpc {
            cell: RefCell::new(None),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        assert!(resolver.resolve(&rpc, &[0x11; 32]).unwrap().is_none());

        let wrong_entry = test_entry([0x33; 32], [0x44; 32]);
        let (_, cell) = process_state_cell(&wrong_entry, vec![]);
        *rpc.cell.borrow_mut() = Some(cell);
        assert!(resolver.resolve(&rpc, &[0x99; 32]).unwrap().is_none());
    }

    #[test]
    fn resolve_skips_when_state_not_process_lock() {
        let entry = test_entry([0xcf; 32], [0xd0; 32]);
        let (hash, mut state) = process_state_cell(&entry, vec![]);
        // Wrong lock: data hash instead of Process code TYPE_ID
        let bad_lock = Script::new_builder()
            .code_hash([0x99u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        state.output = CellOutput::new_builder()
            .capacity(state.capacity().pack())
            .lock(bad_lock)
            .type_(state.type_script().pack())
            .build();
        let code = code_live(0x10, vec![0x7f, b'E', b'L', b'F']);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code));
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn fallback_used_when_code_cell_not_elf() {
        let entry = test_entry([0x51; 32], [0x52; 32]);
        let (hash, state) = process_state_cell(&entry, vec![]);
        let code = code_live(0x10, vec![0x00, 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver =
            ProcessResolver::for_entry(entry).with_fallback_binary(vec![0x7f, b'E', b'L', b'F']);
        resolver.set_tick_process_code(Some(&code));
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("fallback");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F']);
    }

    #[test]
    fn resolve_refetches_live_cell_out_point_after_recreate() {
        let entry = test_entry([0xaa; 32], [0xbb; 32]);
        let (hash, cell) = process_state_cell(&entry, vec![]);
        let code = code_live(0x21, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell.clone())),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code));
        let first = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        let mut cell2 = cell;
        cell2.out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x88; 32]).unwrap())
            .index(1u32.pack())
            .build();
        *rpc.cell.borrow_mut() = Some(cell2.clone());
        let second = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(second.live_cell.out_point, cell2.out_point);
        assert_eq!(second.binary, first.binary);
    }

    #[test]
    fn type_id_mode_cache_invalidates_on_code_outpoint_change() {
        let entry = test_entry([0xcf; 32], [0xd1; 32]);
        let (hash, state) = process_state_cell(&entry, vec![]);
        let code_v1 = code_live(0x21, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let code_v2 = code_live(0x22, vec![0x7f, b'E', b'L', b'F', 0x02]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code_v1));
        assert_eq!(
            resolver
                .resolve(&rpc, &hash)
                .unwrap()
                .unwrap()
                .binary
                .last(),
            Some(&0x01)
        );
        resolver.set_tick_process_code(Some(&code_v2));
        assert_eq!(
            resolver
                .resolve(&rpc, &hash)
                .unwrap()
                .unwrap()
                .binary
                .last(),
            Some(&0x02)
        );
    }

    #[test]
    fn type_id_mode_without_tick_snapshot_skips() {
        let entry = test_entry([0xd0; 32], [0xd2; 32]);
        let (hash, state) = process_state_cell(&entry, vec![]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn state_is_process_lock_rejects_wrong_type_code_hash() {
        let entry = test_entry([0xee; 32], [0xff; 32]);
        let (hash, state) = process_state_cell(&entry, vec![]);
        let mut bad = state.clone();
        let ts = Script::new_builder()
            .code_hash([0x51u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(entry.identity.args.to_vec().pack())
            .build();
        bad.output = CellOutput::new_builder()
            .capacity(bad.capacity().pack())
            .lock(bad.lock())
            .type_(Some(ts).pack())
            .build();
        assert!(!state_is_process_lock(&entry, &bad));
        let code = code_live(0x11, vec![0x7f, b'E', b'L', b'F']);
        let rpc = TestRpc {
            cell: RefCell::new(Some(bad)),
        };
        let mut resolver = ProcessResolver::for_entry(entry);
        resolver.set_tick_process_code(Some(&code));
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }
}
