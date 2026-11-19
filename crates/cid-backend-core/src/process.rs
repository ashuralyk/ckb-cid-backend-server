use crate::rpc::{out_point_key, LiveCell, Rpc, RpcError};
use ckb_types::core::ScriptHashType;
use ckb_types::prelude::*;
use std::collections::HashMap;

pub type ProcessFallback = Box<dyn Fn(&[u8; 32], &LiveCell) -> Option<Vec<u8>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct ResolvedProcess {
    pub type_hash: [u8; 32],
    pub live_cell: LiveCell,
    pub binary: Vec<u8>,
}

struct CachedBinary {
    /// Fingerprint of the SSRI source (code-cell outpoint key, or state data).
    source_key: Vec<u8>,
    binary: Vec<u8>,
}

/// How `ProcessResolver` chooses the RISC-V ELF for SSRI.
///
/// - [`SsriBinarySource::StateCellData`]: legacy / local data1 fixtures — ELF
///   is the live Process **state** cell data (same bytes often also published
///   as a code cell). Daemon production without Process Type-ID uses this.
/// - [`SsriBinarySource::TypeIdCodeCell`]: Process **code** is discovered via
///   Type ID; SSRI must execute the resolved code-cell bytes for the tick, and
///   the tx CellDep must reference that same outpoint. The live Process
///   **state** type script must use `hash_type=type` and `code_hash` equal to
///   the derived Process code TYPE_ID script hash (fail-closed otherwise).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SsriBinarySource {
    #[default]
    StateCellData,
    TypeIdCodeCell,
}

pub struct ProcessResolver {
    fallback: Option<ProcessFallback>,
    binary_cache: HashMap<[u8; 32], CachedBinary>,
    ssri_source: SsriBinarySource,
    /// Derived TYPE_ID script hash for Process **code** (`hash_type=type`
    /// `code_hash` expected on live Process state cells). Required in
    /// [`SsriBinarySource::TypeIdCodeCell`] mode.
    expected_process_type_code_hash: Option<[u8; 32]>,
    /// Per-tick Process code snapshot when `ssri_source == TypeIdCodeCell`.
    tick_process_code: Option<(Vec<u8>, Vec<u8>)>,
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
            ssri_source: SsriBinarySource::StateCellData,
            expected_process_type_code_hash: None,
            tick_process_code: None,
        }
    }

    pub fn with_ssri_source(ssri_source: SsriBinarySource) -> Self {
        Self {
            fallback: None,
            binary_cache: HashMap::new(),
            ssri_source,
            expected_process_type_code_hash: None,
            tick_process_code: None,
        }
    }

    /// Type-ID Process code mode with the derived TYPE_ID script hash that
    /// live Process **state** type scripts must reference (`code_hash`).
    pub fn with_type_id_process_code(expected_type_code_hash: [u8; 32]) -> Self {
        Self {
            fallback: None,
            binary_cache: HashMap::new(),
            ssri_source: SsriBinarySource::TypeIdCodeCell,
            expected_process_type_code_hash: Some(expected_type_code_hash),
            tick_process_code: None,
        }
    }

    pub fn with_fallback(fallback: ProcessFallback) -> Self {
        Self {
            fallback: Some(fallback),
            binary_cache: HashMap::new(),
            ssri_source: SsriBinarySource::StateCellData,
            expected_process_type_code_hash: None,
            tick_process_code: None,
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

    pub fn ssri_source(&self) -> SsriBinarySource {
        self.ssri_source
    }

    pub fn expected_process_type_code_hash(&self) -> Option<[u8; 32]> {
        self.expected_process_type_code_hash
    }

    /// Install (or clear) this tick's Process **code** cell snapshot.
    /// Required when `ssri_source == TypeIdCodeCell`. Clears the binary cache
    /// when the code outpoint changes.
    pub fn set_tick_process_code(&mut self, code: Option<&LiveCell>) {
        let next = code.map(|c| (out_point_key(&c.out_point), c.data.clone()));
        if self.tick_process_code.as_ref().map(|(k, _)| k.as_slice())
            != next.as_ref().map(|(k, _)| k.as_slice())
        {
            self.binary_cache.clear();
        }
        self.tick_process_code = next;
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
        let source_key = self.source_key(&cell);
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
        }))
    }

    fn source_key(&self, cell: &LiveCell) -> Vec<u8> {
        match self.ssri_source {
            SsriBinarySource::TypeIdCodeCell => self
                .tick_process_code
                .as_ref()
                .map(|(k, _)| k.clone())
                .unwrap_or_default(),
            SsriBinarySource::StateCellData => cell.data.clone(),
        }
    }

    /// Fail-closed: Process state type script must use hash_type=type and
    /// code_hash == derived Process code TYPE_ID script hash.
    fn state_binds_to_process_code(&self, cell: &LiveCell) -> bool {
        let Some(expected) = self.expected_process_type_code_hash else {
            log::warn!("Type-ID Process code mode missing expected type code_hash");
            return false;
        };
        let Some(ts) = cell.type_script() else {
            log::warn!("Process state cell has no type script");
            return false;
        };
        let Ok(ht) = ScriptHashType::try_from(ts.hash_type()) else {
            log::warn!("Process state type script hash_type unreadable");
            return false;
        };
        if ht != ScriptHashType::Type {
            log::warn!(
                "Process state type script hash_type={ht:?} (Type-ID code mode requires type)"
            );
            return false;
        }
        let code_hash: [u8; 32] = ts.code_hash().unpack();
        if code_hash != expected {
            log::warn!(
                "Process state type code_hash does not match configured Process code TYPE_ID"
            );
            return false;
        }
        true
    }

    fn derive_binary(&self, process_type_hash: &[u8; 32], cell: &LiveCell) -> Option<Vec<u8>> {
        match self.ssri_source {
            SsriBinarySource::TypeIdCodeCell => {
                if !self.state_binds_to_process_code(cell) {
                    return None;
                }
                let Some((_, data)) = &self.tick_process_code else {
                    log::warn!(
                        "Type-ID Process code mode but no tick process_code snapshot \
                         (code deps refresh missing process_code?)"
                    );
                    return None;
                };
                if looks_like_elf(data) {
                    return Some(data.clone());
                }
                log::warn!(
                    "Type-ID Process code cell data is not a usable ELF ({} bytes)",
                    data.len()
                );
                None
            }
            SsriBinarySource::StateCellData => {
                if looks_like_elf(&cell.data) {
                    return Some(cell.data.clone());
                }
                if let Some(fb) = &self.fallback {
                    match fb(process_type_hash, cell) {
                        Some(b) if looks_like_elf(&b) => return Some(b),
                        _ => {
                            log::warn!("Process cell data empty/unusable and fallback missing");
                            return None;
                        }
                    }
                }
                log::warn!("Process cell data empty/unusable; no fallback installed");
                None
            }
        }
    }
}

fn looks_like_elf(data: &[u8]) -> bool {
    data.len() >= 4 && data[0] == 0x7f && data[1] == b'E' && data[2] == b'L' && data[3] == b'F'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{LiveCell, Rpc, RpcError, TxStatus};
    use crate::type_id::type_id_script_hash;
    use ckb_types::core::{ScriptHashType, TransactionView};
    use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
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
            _: &Script,
            _: usize,
        ) -> Result<Vec<LiveCell>, RpcError> {
            Ok(Vec::new())
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

    fn process_cell_with_type(ts: Script, data: Vec<u8>) -> ([u8; 32], LiveCell) {
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

    fn process_cell(data: Vec<u8>) -> ([u8; 32], LiveCell) {
        let ts = Script::new_builder()
            .code_hash([0x51u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0xABu8; 32].to_vec().pack())
            .build();
        process_cell_with_type(ts, data)
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
        *rpc.cell.borrow_mut() = Some(cell);
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn fallback_used_when_live_data_not_elf() {
        let (hash, cell) = process_cell(vec![0x00, 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver = ProcessResolver::with_fallback_binary(vec![0x7f, b'E', b'L', b'F']);
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("fallback");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F']);
    }

    #[test]
    fn resolve_refetches_live_cell_out_point_after_recreate() {
        let (hash, cell) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell.clone())),
        };
        let mut resolver = ProcessResolver::new();
        let first = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        let mut cell2 = cell;
        cell2.out_point = OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[0x88; 32]).unwrap())
            .index(1u32.pack())
            .build();
        cell2.data = vec![0x7f, b'E', b'L', b'F', 0x01];
        *rpc.cell.borrow_mut() = Some(cell2.clone());
        let second = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(second.live_cell.out_point, cell2.out_point);
        assert_eq!(second.binary, first.binary);
    }

    #[test]
    fn resolve_uses_updated_live_data_for_same_type_hash() {
        let (hash, cell) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver = ProcessResolver::new();
        let first = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(first.binary, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let (_, cell2) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x02]);
        *rpc.cell.borrow_mut() = Some(cell2);
        let second = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(second.binary, vec![0x7f, b'E', b'L', b'F', 0x02]);
    }

    #[test]
    fn resolve_skips_when_live_data_changes_from_elf_to_non_elf_without_fallback() {
        let (hash, cell) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver = ProcessResolver::new();
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_some());
        let (_, cell2) = process_cell(vec![0x00]);
        *rpc.cell.borrow_mut() = Some(cell2);
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn resolve_fallback_when_live_data_changes_from_elf_to_non_elf() {
        let (hash, cell) = process_cell(vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(cell)),
        };
        let mut resolver =
            ProcessResolver::with_fallback_binary(vec![0x7f, b'E', b'L', b'F', 0xFB]);
        let first = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(first.binary, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let (_, cell2) = process_cell(vec![0x00]);
        *rpc.cell.borrow_mut() = Some(cell2);
        let second = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(second.binary, vec![0x7f, b'E', b'L', b'F', 0xFB]);
    }

    #[test]
    fn type_id_mode_uses_code_cell_not_divergent_state_elf() {
        let args = [0xcd; 32];
        let expected = type_id_script_hash(&args);
        let ts = Script::new_builder()
            .code_hash(expected.pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0x01u8; 32].to_vec().pack())
            .build();
        let (hash, state) = process_cell_with_type(ts, vec![0x7f, b'E', b'L', b'F', 0xAA]);
        let code = code_live(0x10, vec![0x7f, b'E', b'L', b'F', 0xBB]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
        resolver.set_tick_process_code(Some(&code));
        let got = resolver.resolve(&rpc, &hash).unwrap().expect("resolved");
        assert_eq!(got.binary, vec![0x7f, b'E', b'L', b'F', 0xBB]);
    }

    #[test]
    fn type_id_mode_rejects_state_with_wrong_code_hash() {
        let expected = type_id_script_hash(&[0xcd; 32]);
        let (hash, state) = process_cell(vec![0x7f, b'E', b'L', b'F', 0xAA]); // code_hash 0x51…
        let code = code_live(0x10, vec![0x7f, b'E', b'L', b'F', 0xBB]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
        resolver.set_tick_process_code(Some(&code));
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn type_id_mode_rejects_state_with_wrong_hash_type() {
        let args = [0xce; 32];
        let expected = type_id_script_hash(&args);
        let ts = Script::new_builder()
            .code_hash(expected.pack())
            .hash_type(ScriptHashType::Data1.into())
            .args([0x01u8; 32].to_vec().pack())
            .build();
        let (hash, state) = process_cell_with_type(ts, vec![0x00]);
        let code = code_live(0x11, vec![0x7f, b'E', b'L', b'F', 0xBB]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
        resolver.set_tick_process_code(Some(&code));
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }

    #[test]
    fn type_id_mode_cache_invalidates_on_code_outpoint_change() {
        let args = [0xcf; 32];
        let expected = type_id_script_hash(&args);
        let ts = Script::new_builder()
            .code_hash(expected.pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0x02u8; 32].to_vec().pack())
            .build();
        let (hash, state) = process_cell_with_type(ts, vec![0x00]);
        let code_v1 = code_live(0x21, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let code_v2 = code_live(0x22, vec![0x7f, b'E', b'L', b'F', 0x02]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
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
        let args = [0xd0; 32];
        let expected = type_id_script_hash(&args);
        let ts = Script::new_builder()
            .code_hash(expected.pack())
            .hash_type(ScriptHashType::Type.into())
            .args([0x03u8; 32].to_vec().pack())
            .build();
        let (hash, state) = process_cell_with_type(ts, vec![0x7f, b'E', b'L', b'F', 0x01]);
        let rpc = TestRpc {
            cell: RefCell::new(Some(state)),
        };
        let mut resolver = ProcessResolver::with_type_id_process_code(expected);
        assert!(resolver.resolve(&rpc, &hash).unwrap().is_none());
    }
}
