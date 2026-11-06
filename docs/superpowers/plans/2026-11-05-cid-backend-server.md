# CID Backend Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** v0 digest daemon: scan CID Intentions (indexer asc), bucket by process_type_hash, in-process SSRI via cid-process-executor, assemble Execution Tx (digest_ok / digest_incompatible), submit; FakeRpc vertical tests only (no live network).

**Architecture:** Approach A — `cid-backend-core` pipeline library + thin `cid-backend-daemon` bin. RPC is a Trait; FakeRpc powers tests. Memory-only LoopState. Process binary from live Process Cell data; tests inject fallback to `cid_process_example`.

**Tech Stack:** Rust workspace; git dep on ashuralyk/ckb-common-intention-dispatcher crates `cid-process-executor` + `cid-protocol` (lock a main revision); ckb-types; no HTTP; env-var config only for daemon.

## Global Constraints

- Independent repo; no monorepo merge with dispatcher
- Digest loop only — no HTTP/metrics/admin
- In-process SSRI (embed ProcessExecutor); no external ssri-server
- Process binary: live cell data in prod; FakeRpc may fallback to cid_process_example; daemon NEVER installs fallback
- In-memory state only; restart = full rescan
- digest_ok: no User Lock in inputs; no extra fee/sponsor input; capacity shortfall → log+skip
- SSRI Incompatible → User Lock exact refund path; other SSRI/VM errors → Unusable skip (NO refund)
- timeout_burn NOT constructed by daemon
- Process recreate: 1in/1out; if assemble lacks Process recreate, copy live cell lock/data/capacity/type
- Daemon config: env vars only (node URL, optional indexer URL, CID lock code hash, optional hash type default Type, poll interval)
- Tests: FakeRpc only; no live network
- Never invent Task numbers beyond this plan; TDD: failing test → implement → pass → commit per task

---

## File structure

Exact paths this plan creates. The digest loop module is `digest_loop` (Rust `loop` is a keyword). Extra files listed here are required by later tasks; do not invent others.

```
Cargo.toml                                          # workspace
Cargo.lock                                          # committed (binary workspace)
.gitignore
rust-toolchain.toml                                 # stable, no extra targets required for v0 host
crates/cid-backend-core/Cargo.toml
crates/cid-backend-core/src/lib.rs
crates/cid-backend-core/src/rpc.rs
crates/cid-backend-core/src/scan.rs
crates/cid-backend-core/src/process.rs
crates/cid-backend-core/src/ssri.rs
crates/cid-backend-core/src/assemble.rs
crates/cid-backend-core/src/submit.rs
crates/cid-backend-core/src/digest_loop.rs          # module name digest_loop
crates/cid-backend-daemon/Cargo.toml
crates/cid-backend-daemon/src/main.rs
crates/cid-backend-daemon/src/config.rs
crates/cid-backend-daemon/src/placeholder_rpc.rs    # compiles; all methods RpcError::Unavailable
tests/Cargo.toml                                    # package cid-backend-tests
tests/src/lib.rs
tests/src/fake_rpc.rs
tests/src/helpers.rs
tests/src/scripted_ssri.rs                          # test-only SsriHost (n=1, bounds, Incompatible)
tests/src/process_resolver.rs                       # FakeRpc process-resolver tests
tests/src/submit_inflight.rs
tests/src/vertical_n1.rs
tests/src/vertical_batch_bounds.rs
tests/src/vertical_incompatible.rs
tests/src/vertical_submit_conflict.rs
tests/fixtures/README.md                            # how to obtain cid_process_example
docs/superpowers/specs/2026-11-05-cid-backend-server-design.md   # already exists; do not rewrite
docs/superpowers/plans/2026-11-05-cid-backend-server.md          # this file
README.md                                           # updated in Task 13
```

`cid-backend-core` does not read env vars, bind TCP, or define `main`. `cid-backend-daemon` contains no digest business rules. `cid-backend-tests` depends on core only (plus protocol/executor/ckb-types for fixtures). No crate talks to a live node.

---

## Upstream APIs (exact — do not rename)

Pinned git revision of [ashuralyk/ckb-common-intention-dispatcher](https://github.com/ashuralyk/ckb-common-intention-dispatcher):

| Field | Value |
|-------|--------|
| Rev | `d9ee77805f7ac930ab149648bed70f484a9886de` |
| Date | 2026-11-05 |
| Note | `main` HEAD at plan time (merge of PR #7: PartialTx is a packed molecule `Transaction`). Older `a84700b` is the `cid_process_example` merge; pin the newer rev so `PartialTx::from_bytes` / `as_molecule_bytes` match the executor we embed. |

Workspace git deps (same rev, two packages):

```toml
cid-process-executor = { git = "https://github.com/ashuralyk/ckb-common-intention-dispatcher", rev = "d9ee77805f7ac930ab149648bed70f484a9886de" }
cid-protocol = { git = "https://github.com/ashuralyk/ckb-common-intention-dispatcher", rev = "d9ee77805f7ac930ab149648bed70f484a9886de" }
```

`ckb-types = "0.119"` (same major as the executor crate).

From `cid-process-executor` (do not invent alternate names):

```rust
pub struct ProcessExecutor { /* binary */ }
impl ProcessExecutor {
    pub fn new(binary: Vec<u8>) -> Self;
    pub fn get_batch_bounds(&self) -> Result<(u32, u32), ExecutorError>;
    pub fn assemble(&self, intentions: &[IntentionInput]) -> Result<PartialTx, ExecutorError>;
}

pub struct IntentionInput {
    pub out_point: OutPoint,
    pub lock_args: Vec<u8>,
    pub capacity: u64,
    pub data: Vec<u8>,
    pub type_script: Option<Vec<u8>>, // packed molecule Script
}

pub struct PartialTx {
    pub outputs: Vec<CellOutput>,
    pub outputs_data: Vec<Vec<u8>>,
    // as_molecule_bytes()
}

pub enum ExecutorError {
    Incompatible(i8),
    VmError(String),
    CyclesExceeded,
    InvalidResponse(String),
}
```

`PartialTx` is parsed from a packed molecule `Transaction` (`PartialTx::from_bytes`). Tests that build a `PartialTx` must go through `from_bytes`, not struct literals (`packed` is private).

From `cid-protocol`: `CidArgs::decode` / `encode`; `CidEntry { version, user_lock, payload }`; lock args = `process_type_hash[32] || molecule(CidEntry)`. Also use `is_standard_xudt`, `XUDT_CODE_HASH`, `XUDT_AMOUNT_LEN`, `SCRIPT_HASH_TYPE_DATA1` for refund exactness. Do not interpret `payload` in production code; pass it through as lock-args bytes to the guest.

### Process example binary for tests that call the real VM

Upstream tests load `build/release/cid_process_example` (sample bounds `(1, 4)`). This repo does **not** vendor RISC-V sources.

Obtain the binary (document again in Task 13):

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build
cp build/release/cid_process_example /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
```

Lookup order in tests: env `CID_PROCESS_EXAMPLE_PATH`, then `tests/fixtures/cid_process_example`. Tests that **require** the real guest skip with a clear `eprintln!` if the file is missing. Pipeline shape tests (Tasks 8–11) use a test-only `ScriptedSsri` implementing `SsriHost`, so FakeRpc verticals do not depend on the ELF.

---

## Shared type names (locked for every task)

These names are used in every later task. Do not rename them.

| Name | Crate | Role |
|------|--------|------|
| `LiveCell` | core `rpc` | `{ out_point, output, data }` |
| `Rpc` | core `rpc` | `list_live_intentions`, `get_live_process_by_type_hash`, `submit_transaction` |
| `RpcError` | core `rpc` | `Unavailable(String)`, `SubmitRejected(String)`, `Conflict(String)` |
| `out_point_key` | core `rpc` | `fn out_point_key(op: &OutPoint) -> Vec<u8>` |
| `script_hash32` | core `rpc` | `fn script_hash32(script: &Script) -> [u8; 32]` |
| `ScannedIntention` | core `scan` | live Intention plus decoded `CidArgs` and raw `lock_args` |
| `scan_intentions` | core `scan` | RPC list + decode, skip bad args |
| `bucket_by_process_type_hash` | core `scan` | first-seen hash order; bucket inner order = asc |
| `ProcessResolver` | core `process` | live-cell binary; optional fallback; memory cache |
| `ResolvedProcess` | core `process` | `{ type_hash, live_cell, binary }` |
| `SsriHost` | core `ssri` | `get_batch_bounds`, `assemble` |
| `EmbeddedSsri` | core `ssri` | wraps `ProcessExecutor` |
| `SsriError` | core `ssri` | `Incompatible(i8)`, `Unusable(String)` |
| `map_executor_error` | core `ssri` | `ExecutorError` → `SsriError` |
| `ExecutionSkeleton` | core `assemble` | `{ tx: TransactionView }` |
| `AssembleError` | core `assemble` | `CapacityShortfall { inputs, outputs }`, `RefundUnbuildable(String)` |
| `assemble_digest_ok` | core `assemble` | Intention destroy + PartialTx outputs + Process 1in/1out |
| `assemble_digest_incompatible` | core `assemble` | Intention destroy + User Lock exact refund; no Process pair |
| `SubmitState` | core `submit` | `in_flight` / `failed` as `HashSet<Vec<u8>>` keys |
| `submit_execution` | core `submit` | RPC submit; memory bookkeeping |
| `LoopConfig` | core `digest_loop` | CID filter + poll interval + extra cell_deps |
| `LoopState` | core `digest_loop` | `{ submit: SubmitState, resolver: ProcessResolver }` |
| `Clock` / `NopClock` / `RealClock` | core `digest_loop` | injectable sleep |
| `DigestLoop` | core `digest_loop` | `tick()` / `run()` |
| `TickReport` | core `digest_loop` | scanned / skipped / submitted / conflicts |
| `FakeRpc` | tests | in-memory cells, asc list, conflict injection |
| `ScriptedSsri` | tests | test `SsriHost` |
| `DaemonConfig` | daemon `config` | env-only parse |
| `PlaceholderRpc` | daemon | all methods `Unavailable` (no live network) |

---

### Task 1: Workspace + Rpc trait + FakeRpc skeleton

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `rust-toolchain.toml`
- Create: `crates/cid-backend-core/Cargo.toml`
- Create: `crates/cid-backend-core/src/lib.rs`
- Create: `crates/cid-backend-core/src/rpc.rs`
- Create: `tests/Cargo.toml`
- Create: `tests/src/lib.rs`
- Create: `tests/src/fake_rpc.rs`
- Create: `tests/src/helpers.rs`
- Test: `tests/src/fake_rpc.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Consumes: nothing in this repo
- Produces:
  - `pub struct LiveCell { pub out_point: OutPoint, pub output: CellOutput, pub data: Vec<u8> }`
  - `impl LiveCell { pub fn capacity(&self) -> u64; pub fn lock(&self) -> Script; pub fn type_script(&self) -> Option<Script>; }`
  - `pub enum RpcError { Unavailable(String), SubmitRejected(String), Conflict(String) }`
  - `pub trait Rpc { fn list_live_intentions(&self, cid_code_hash: &[u8; 32], hash_type: ScriptHashType) -> Result<Vec<LiveCell>, RpcError>; fn get_live_process_by_type_hash(&self, process_type_hash: &[u8; 32]) -> Result<Option<LiveCell>, RpcError>; fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError>; }`
  - `pub fn out_point_key(op: &OutPoint) -> Vec<u8>`
  - `pub fn script_hash32(script: &Script) -> [u8; 32]`
  - `pub struct FakeRpc` with `new`, `insert_cell`, `live_cells`, `inject_conflict_once`, `submitted`, `consume`
  - `impl Rpc for FakeRpc`

- [ ] **Step 1: Write the failing test**

Create `tests/src/helpers.rs`:

```rust
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use cid_backend_core::rpc::LiveCell;

pub fn out_point(tx_fill: u8, index: u32) -> OutPoint {
    OutPoint::new_builder()
        .tx_hash(Byte32::from_slice(&[tx_fill; 32]).expect("32 bytes"))
        .index(index.pack())
        .build()
}

pub fn lock_script(code_hash: [u8; 32], hash_type: ScriptHashType, args: Vec<u8>) -> Script {
    Script::new_builder()
        .code_hash(code_hash.pack())
        .hash_type(hash_type.into())
        .args(args.pack())
        .build()
}

pub fn live_cell(
    op: OutPoint,
    lock: Script,
    type_script: Option<Script>,
    capacity: u64,
    data: Vec<u8>,
) -> LiveCell {
    let mut output = CellOutput::new_builder()
        .capacity(capacity.pack())
        .lock(lock);
    if let Some(ts) = type_script {
        output = output.type_(Some(ts).pack());
    }
    LiveCell {
        out_point: op,
        output: output.build(),
        data,
    }
}
```

Create `tests/src/lib.rs`:

```rust
pub mod fake_rpc;
pub mod helpers;
```

Create `tests/src/fake_rpc.rs` with the test first (types will not exist yet — that is the fail):

```rust
use ckb_types::core::ScriptHashType;
use cid_backend_core::rpc::Rpc;

use crate::helpers::{live_cell, lock_script, out_point};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_rpc::FakeRpc;

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
```

Create `tests/Cargo.toml`:

```toml
[package]
name = "cid-backend-tests"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
cid-backend-core = { path = "../crates/cid-backend-core" }
ckb-types = { workspace = true }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests fakerpc_stores_and_lists_intention_cells_in_insert_order --offline` (or without `--offline` if the workspace is not generated yet)

Expected: FAIL / compile error — package `cid-backend-tests` or `cid-backend-core` / `FakeRpc` / `Rpc` not found.

- [ ] **Step 3: Write minimal implementation**

`.gitignore`:

```
/target
**/*.rs.bk
```

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
```

Workspace `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "crates/cid-backend-core",
    "crates/cid-backend-daemon",
    "tests",
]

[workspace.dependencies]
ckb-types = "0.119"
log = "0.4"
cid-process-executor = { git = "https://github.com/ashuralyk/ckb-common-intention-dispatcher", rev = "d9ee77805f7ac930ab149648bed70f484a9886de" }
cid-protocol = { git = "https://github.com/ashuralyk/ckb-common-intention-dispatcher", rev = "d9ee77805f7ac930ab149648bed70f484a9886de" }
```

Do **not** add `crates/cid-backend-daemon` members until Task 12 if `cargo metadata` fails on a missing crate. Prefer creating a stub daemon package so the workspace is valid now:

`crates/cid-backend-daemon/Cargo.toml`:

```toml
[package]
name = "cid-backend-daemon"
version = "0.1.0"
edition = "2021"

[dependencies]
```

`crates/cid-backend-daemon/src/main.rs`:

```rust
fn main() {
    eprintln!("cid-backend-daemon: not wired until Task 12");
}
```

`crates/cid-backend-core/Cargo.toml`:

```toml
[package]
name = "cid-backend-core"
version = "0.1.0"
edition = "2021"

[dependencies]
ckb-types = { workspace = true }
log = { workspace = true }
```

`crates/cid-backend-core/src/rpc.rs`:

```rust
use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use std::fmt;

#[derive(Clone, Debug)]
pub struct LiveCell {
    pub out_point: OutPoint,
    pub output: CellOutput,
    pub data: Vec<u8>,
}

impl LiveCell {
    pub fn capacity(&self) -> u64 {
        self.output.capacity().unpack()
    }

    pub fn lock(&self) -> Script {
        self.output.lock()
    }

    pub fn type_script(&self) -> Option<Script> {
        self.output.type_().to_opt()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Unavailable(String),
    SubmitRejected(String),
    Conflict(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Unavailable(m) => write!(f, "rpc unavailable: {m}"),
            RpcError::SubmitRejected(m) => write!(f, "submit rejected: {m}"),
            RpcError::Conflict(m) => write!(f, "submit conflict: {m}"),
        }
    }
}

impl std::error::Error for RpcError {}

pub fn out_point_key(op: &OutPoint) -> Vec<u8> {
    op.as_slice().to_vec()
}

pub fn script_hash32(script: &Script) -> [u8; 32] {
    script.calc_script_hash().unpack()
}

pub trait Rpc {
    fn list_live_intentions(
        &self,
        cid_code_hash: &[u8; 32],
        hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError>;

    fn get_live_process_by_type_hash(
        &self,
        process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError>;

    fn submit_transaction(&self, tx: &TransactionView) -> Result<Byte32, RpcError>;
}
```

`crates/cid-backend-core/src/lib.rs`:

```rust
pub mod rpc;

pub use rpc::{out_point_key, script_hash32, LiveCell, Rpc, RpcError};
```

`tests/src/fake_rpc.rs` (full file: impl + tests from Step 1):

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests fakerpc_stores_and_lists_intention_cells_in_insert_order`

Expected: PASS (git fetch of workspace git deps is allowed; no CKB node).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml .gitignore rust-toolchain.toml \
  crates/cid-backend-core crates/cid-backend-daemon tests
git add Cargo.lock
git commit -m "feat: add workspace, Rpc trait, and FakeRpc skeleton"
```

---

### Task 2: scan — decode CidArgs, skip bad args, bucket by process_type_hash

**Files:**
- Modify: `crates/cid-backend-core/Cargo.toml` (add `cid-protocol`)
- Create: `crates/cid-backend-core/src/scan.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Test: `crates/cid-backend-core/src/scan.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: `LiveCell`, `Rpc`, `RpcError`, `CidArgs::decode`
- Produces:
  - `pub struct ScannedIntention { pub out_point: OutPoint, pub capacity: u64, pub type_script: Option<Script>, pub data: Vec<u8>, pub lock_args: Vec<u8>, pub args: CidArgs }`
  - `pub fn decode_cells(cells: Vec<LiveCell>) -> Vec<ScannedIntention>`
  - `pub fn scan_intentions<R: Rpc>(rpc: &R, cid_code_hash: &[u8; 32], hash_type: ScriptHashType) -> Result<Vec<ScannedIntention>, RpcError>`
  - `pub fn bucket_by_process_type_hash(intentions: &[ScannedIntention]) -> Vec<([u8; 32], Vec<ScannedIntention>)>`
  - `pub fn to_intention_input(scanned: &ScannedIntention) -> IntentionInput` is **Task 4**; do not add it here

- [ ] **Step 1: Write the failing test**

Put these tests at the bottom of `crates/cid-backend-core/src/scan.rs`. They fail to compile until Step 3 adds `decode_cells` / `bucket_by_process_type_hash`.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::LiveCell;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;
    use cid_protocol::{CidArgs, Payload, Script as CidScript};

    fn op(fill: u8) -> OutPoint {
        OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[fill; 32]).unwrap())
            .index(0u32.pack())
            .build()
    }

    fn cell(fill: u8, args: Vec<u8>) -> LiveCell {
        let lock = Script::new_builder()
            .code_hash([0xC1u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(args.pack())
            .build();
        LiveCell {
            out_point: op(fill),
            output: CellOutput::new_builder()
                .capacity(100_0000_0000u64.pack())
                .lock(lock)
                .build(),
            data: vec![],
        }
    }

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0x01],
        }
    }

    fn cid_args(process: [u8; 32]) -> Vec<u8> {
        CidArgs::new(process, user_lock(), Payload { data: vec![0xAA] })
            .encode()
            .unwrap()
    }

    #[test]
    fn decode_skips_bad_args_and_keeps_good() {
        let good = cell(1, cid_args([0x11; 32]));
        let bad = cell(2, vec![0x00; 8]);
        let good2 = cell(3, cid_args([0x22; 32]));
        let decoded = decode_cells(vec![good.clone(), bad, good2.clone()]);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].out_point, good.out_point);
        assert_eq!(decoded[0].args.process_type_hash, [0x11; 32]);
        assert_eq!(decoded[1].out_point, good2.out_point);
        assert_eq!(decoded[1].lock_args, cid_args([0x22; 32]));
    }

    #[test]
    fn bucket_preserves_asc_and_first_seen_hash_order() {
        let p_a = [0xAA; 32];
        let p_b = [0xBB; 32];
        let cells = vec![
            cell(1, cid_args(p_a)),
            cell(2, cid_args(p_b)),
            cell(3, cid_args(p_a)),
        ];
        let decoded = decode_cells(cells);
        let buckets = bucket_by_process_type_hash(&decoded);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].0, p_a);
        assert_eq!(buckets[1].0, p_b);
        assert_eq!(buckets[0].1.len(), 2);
        assert_eq!(
            buckets[0].1[0].out_point.tx_hash(),
            Byte32::from_slice(&[1u8; 32]).unwrap()
        );
        assert_eq!(
            buckets[0].1[1].out_point.tx_hash(),
            Byte32::from_slice(&[3u8; 32]).unwrap()
        );
        assert_eq!(buckets[1].1.len(), 1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-core decode_skips_bad_args_and_keeps_good --offline`

Expected: FAIL — `scan` module / `decode_cells` not found, or `cid-protocol` missing.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-core/Cargo.toml` dependencies:

```toml
[dependencies]
ckb-types = { workspace = true }
log = { workspace = true }
cid-protocol = { workspace = true }
```

`crates/cid-backend-core/src/lib.rs`:

```rust
pub mod rpc;
pub mod scan;

pub use rpc::{out_point_key, script_hash32, LiveCell, Rpc, RpcError};
pub use scan::{
    bucket_by_process_type_hash, decode_cells, scan_intentions, ScannedIntention,
};
```

`crates/cid-backend-core/src/scan.rs` implementation (place tests from Step 1 under `#[cfg(test)]` in this same file):

```rust
use crate::rpc::{LiveCell, Rpc, RpcError};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{OutPoint, Script};
use ckb_types::prelude::*;
use cid_protocol::CidArgs;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ScannedIntention {
    pub out_point: OutPoint,
    pub capacity: u64,
    pub type_script: Option<Script>,
    pub data: Vec<u8>,
    pub lock_args: Vec<u8>,
    pub args: CidArgs,
}

pub fn decode_cells(cells: Vec<LiveCell>) -> Vec<ScannedIntention> {
    let mut out = Vec::new();
    for cell in cells {
        match decode_one(&cell) {
            Some(item) => out.push(item),
            None => {
                log::warn!(
                    "skipping intention with undecodable CID lock args, out_point={:?}",
                    cell.out_point
                );
            }
        }
    }
    out
}

fn decode_one(cell: &LiveCell) -> Option<ScannedIntention> {
    let lock_args: Vec<u8> = cell.lock().args().unpack();
    let args = CidArgs::decode(&lock_args).ok()?;
    Some(ScannedIntention {
        out_point: cell.out_point.clone(),
        capacity: cell.capacity(),
        type_script: cell.type_script(),
        data: cell.data.clone(),
        lock_args,
        args,
    })
}

pub fn scan_intentions<R: Rpc>(
    rpc: &R,
    cid_code_hash: &[u8; 32],
    hash_type: ScriptHashType,
) -> Result<Vec<ScannedIntention>, RpcError> {
    let cells = rpc.list_live_intentions(cid_code_hash, hash_type)?;
    Ok(decode_cells(cells))
}

pub fn bucket_by_process_type_hash(
    intentions: &[ScannedIntention],
) -> Vec<([u8; 32], Vec<ScannedIntention>)> {
    let mut first_seen: Vec<[u8; 32]> = Vec::new();
    let mut map: HashMap<[u8; 32], Vec<ScannedIntention>> = HashMap::new();
    for item in intentions {
        let hash = item.args.process_type_hash;
        if !map.contains_key(&hash) {
            first_seen.push(hash);
        }
        map.entry(hash).or_default().push(item.clone());
    }
    first_seen
        .into_iter()
        .map(|h| (h, map.remove(&h).unwrap_or_default()))
        .collect()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-core scan::`

Expected: PASS. Bad args skipped; buckets ordered by first appearance; inner order matches input asc.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/Cargo.toml crates/cid-backend-core/src/lib.rs crates/cid-backend-core/src/scan.rs Cargo.lock
git commit -m "feat: scan CID intentions, skip bad args, bucket by process type hash"
```

---

### Task 3: Process resolver — live cell data, skip missing/empty, test-only fallback

**Files:**
- Create: `crates/cid-backend-core/src/process.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Modify: `tests/Cargo.toml` (already depends on core)
- Create: `tests/src/process_resolver.rs`
- Modify: `tests/src/lib.rs`
- Test: `crates/cid-backend-core/src/process.rs` and `tests/src/process_resolver.rs`

**Interfaces:**
- Consumes: `Rpc::get_live_process_by_type_hash`, `LiveCell`
- Produces:
  - `pub type ProcessFallback = Box<dyn Fn(&[u8; 32], &LiveCell) -> Option<Vec<u8>> + Send + Sync>`
  - `pub struct ResolvedProcess { pub type_hash: [u8; 32], pub live_cell: LiveCell, pub binary: Vec<u8> }`
  - `pub struct ProcessResolver { /* fallback: Option<ProcessFallback>, binary_cache: HashMap<[u8; 32], Vec<u8>> */ }`
  - `impl ProcessResolver { pub fn new() -> Self; pub fn with_fallback(fallback: ProcessFallback) -> Self; pub fn with_fallback_binary(binary: Vec<u8>) -> Self; pub fn resolve<R: Rpc>(&mut self, rpc: &R, process_type_hash: &[u8; 32]) -> Result<Option<ResolvedProcess>, RpcError>; }`
  - Production path (`new()`): no fallback. Empty data / missing cell → `Ok(None)` (caller logs + skips bucket). Cache stores **binary only**; every `resolve` re-fetches the live Process cell so a recreate’s new out_point is used on the next tick.

- [ ] **Step 1: Write the failing tests**

Core unit tests in `process.rs` (a tiny `TestRpc`):

```rust
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
            .args([0xABu8; 32].pack())
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
```

`tests/src/process_resolver.rs`:

```rust
#[cfg(test)]
mod tests {
    use ckb_types::core::ScriptHashType;
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{script_hash32, Rpc};

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{live_cell, lock_script, out_point};

    fn process_type(args_fill: u8) -> ckb_types::packed::Script {
        lock_script([0x51; 32], ScriptHashType::Type, vec![args_fill; 32])
    }

    #[test]
    fn fakerpc_resolve_skips_missing_process() {
        let rpc = FakeRpc::new();
        let mut resolver = ProcessResolver::new();
        assert!(resolver
            .resolve(&rpc, &[0xAB; 32])
            .unwrap()
            .is_none());
    }

    #[test]
    fn fakerpc_resolve_reads_live_data() {
        let rpc = FakeRpc::new();
        let ts = process_type(0x01);
        let hash = script_hash32(&ts);
        rpc.insert_cell(live_cell(
            out_point(0x71, 0),
            lock_script([0u8; 32], ScriptHashType::Type, vec![]),
            Some(ts),
            500_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 0x99],
        ));
        let mut resolver = ProcessResolver::new();
        let got = resolver.resolve(&rpc, &hash).unwrap().unwrap();
        assert_eq!(got.binary.last().copied(), Some(0x99));
    }
}
```

Modify `tests/src/lib.rs`:

```rust
pub mod fake_rpc;
pub mod helpers;
pub mod process_resolver;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-core resolve_uses_live_cell_data_when_non_empty`

Expected: FAIL — `process` / `ProcessResolver` not found.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-core/src/process.rs`:

```rust
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
```

`lib.rs` add `pub mod process;` and `pub use process::{ProcessResolver, ResolvedProcess};`.

- [ ] **Step 4: Run test to verify it passes**

Run:

```
cargo test -p cid-backend-core process::
cargo test -p cid-backend-tests fakerpc_resolve
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/src/process.rs crates/cid-backend-core/src/lib.rs \
  tests/src/lib.rs tests/src/process_resolver.rs
git commit -m "feat: resolve Process binary from live cell with test-only fallback"
```

---

### Task 4: ssri wrap — EmbeddedSsri around ProcessExecutor

**Files:**
- Modify: `crates/cid-backend-core/Cargo.toml` (add `cid-process-executor`)
- Create: `crates/cid-backend-core/src/ssri.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Create: `tests/fixtures/README.md`
- Test: `crates/cid-backend-core/src/ssri.rs`

**Interfaces:**
- Consumes: `ProcessExecutor::new`, `get_batch_bounds`, `assemble`, `ExecutorError`, `IntentionInput`, `PartialTx`
- Produces:
  - `pub enum SsriError { Incompatible(i8), Unusable(String) }`
  - `pub fn map_executor_error(err: ExecutorError) -> SsriError` — `Incompatible(c)` stays `Incompatible(c)`; `VmError`, `CyclesExceeded`, `InvalidResponse` become `Unusable`
  - `pub trait SsriHost { fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError>; fn assemble(&self, binary: &[u8], intentions: &[IntentionInput]) -> Result<PartialTx, SsriError>; }`
  - `pub struct EmbeddedSsri;`
  - `impl SsriHost for EmbeddedSsri` using `ProcessExecutor` only (in-process; no HTTP)
  - `pub fn to_intention_input(scanned: &ScannedIntention) -> IntentionInput`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use cid_process_executor::ExecutorError;

    #[test]
    fn maps_incompatible_vs_unusable() {
        assert!(matches!(
            map_executor_error(ExecutorError::Incompatible(21)),
            SsriError::Incompatible(21)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::VmError("x".into())),
            SsriError::Unusable(_)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::CyclesExceeded),
            SsriError::Unusable(_)
        ));
        assert!(matches!(
            map_executor_error(ExecutorError::InvalidResponse("bad".into())),
            SsriError::Unusable(_)
        ));
    }

    #[test]
    fn empty_binary_is_unusable_not_incompatible() {
        let err = EmbeddedSsri
            .get_batch_bounds(&[])
            .expect_err("empty binary");
        assert!(matches!(err, SsriError::Unusable(_)));
        let err = EmbeddedSsri
            .assemble(&[0xff; 8], &[])
            .expect_err("garbage");
        assert!(matches!(err, SsriError::Unusable(_)));
    }

    #[test]
    fn real_example_binary_batch_bounds_if_present() {
        let Some(bin) = load_example_binary() else {
            eprintln!(
                "skip: cid_process_example missing. Set CID_PROCESS_EXAMPLE_PATH or copy to tests/fixtures/cid_process_example (see tests/fixtures/README.md)"
            );
            return;
        };
        let (min, max) = EmbeddedSsri
            .get_batch_bounds(&bin)
            .expect("Process.batch");
        assert!(min >= 1);
        assert!(max >= min);
    }

    fn load_example_binary() -> Option<Vec<u8>> {
        if let Ok(p) = std::env::var("CID_PROCESS_EXAMPLE_PATH") {
            return std::fs::read(p).ok();
        }
        let here = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let candidates = [
            here.join("../../tests/fixtures/cid_process_example"),
            here.join("../tests/fixtures/cid_process_example"),
        ];
        for p in candidates {
            if p.exists() {
                return std::fs::read(p).ok();
            }
        }
        None
    }
}
```

`tests/fixtures/README.md`:

```markdown
# cid_process_example fixture

This directory optionally holds the RISC-V guest built from
`ashuralyk/ckb-common-intention-dispatcher` at rev
`d9ee77805f7ac930ab149648bed70f484a9886de`.

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
```

Or: `export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example`.

Tests that need the real guest skip with a printed message if the file is absent.
FakeRpc verticals (Tasks 8–11) use `ScriptedSsri` and do not require the ELF.
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-core maps_incompatible_vs_unusable`

Expected: FAIL — `ssri` / `map_executor_error` not found.

- [ ] **Step 3: Write minimal implementation**

Add to core `Cargo.toml`:

```toml
cid-process-executor = { workspace = true }
```

`crates/cid-backend-core/src/ssri.rs`:

```rust
use crate::scan::ScannedIntention;
use cid_process_executor::{ExecutorError, IntentionInput, PartialTx, ProcessExecutor};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsriError {
    Incompatible(i8),
    Unusable(String),
}

impl fmt::Display for SsriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SsriError::Incompatible(c) => write!(f, "ssri incompatible exit {c}"),
            SsriError::Unusable(m) => write!(f, "ssri unusable: {m}"),
        }
    }
}

impl std::error::Error for SsriError {}

pub fn map_executor_error(err: ExecutorError) -> SsriError {
    match err {
        ExecutorError::Incompatible(code) => SsriError::Incompatible(code),
        ExecutorError::VmError(m) => SsriError::Unusable(format!("vm: {m}")),
        ExecutorError::CyclesExceeded => SsriError::Unusable("cycles exceeded".into()),
        ExecutorError::InvalidResponse(m) => SsriError::Unusable(format!("invalid response: {m}")),
    }
}

pub trait SsriHost {
    fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError>;
    fn assemble(
        &self,
        binary: &[u8],
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError>;
}

pub struct EmbeddedSsri;

impl SsriHost for EmbeddedSsri {
    fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError> {
        ProcessExecutor::new(binary.to_vec())
            .get_batch_bounds()
            .map_err(map_executor_error)
    }

    fn assemble(
        &self,
        binary: &[u8],
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError> {
        ProcessExecutor::new(binary.to_vec())
            .assemble(intentions)
            .map_err(map_executor_error)
    }
}

pub fn to_intention_input(scanned: &ScannedIntention) -> IntentionInput {
    IntentionInput {
        out_point: scanned.out_point.clone(),
        lock_args: scanned.lock_args.clone(),
        capacity: scanned.capacity,
        data: scanned.data.clone(),
        type_script: scanned
            .type_script
            .as_ref()
            .map(|s| s.as_slice().to_vec()),
    }
}
```

Export from `lib.rs`: `pub mod ssri;` and `pub use ssri::{map_executor_error, to_intention_input, EmbeddedSsri, SsriError, SsriHost};`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-core ssri::`

Expected: PASS. `real_example_binary_batch_bounds_if_present` may skip (print skip message) if the ELF is absent — that is allowed.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/Cargo.toml crates/cid-backend-core/src/ssri.rs \
  crates/cid-backend-core/src/lib.rs tests/fixtures/README.md Cargo.lock
git commit -m "feat: wrap ProcessExecutor as in-process SSRI host"
```

---

### Task 5: assemble digest_ok — destroy Intention, Process 1in/1out, no fee, no User Lock input

**Files:**
- Create: `crates/cid-backend-core/src/assemble.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Test: `crates/cid-backend-core/src/assemble.rs`

**Interfaces:**
- Consumes: `ScannedIntention`, `ResolvedProcess`, `PartialTx`, `CidArgs.entry.user_lock`
- Produces:
  - `pub struct ExecutionSkeleton { pub tx: TransactionView }`
  - `pub enum AssembleError { CapacityShortfall { inputs: u64, outputs: u64 }, RefundUnbuildable(String) }`
  - `pub fn assemble_digest_ok(intentions: &[ScannedIntention], process: &ResolvedProcess, partial: &PartialTx, extra_cell_deps: &[CellDep]) -> Result<ExecutionSkeleton, AssembleError>`
  - Rules (fixed):
    1. Every Intention is an input; no Intention outputs (1 in / 0 out per CID group).
    2. Append `partial.outputs` / `partial.outputs_data` except any output whose type script hash equals `process.type_hash` (that output is the Process recreate).
    3. Process: one input = `process.live_cell.out_point`; one output = recreate from PartialTx if present, else **copy** live cell lock / data / capacity / type.
    4. No additional fee/sponsor input.
    5. Do not add the User Lock as an input. Do not construct `timeout_burn` (no User Lock input, no header deps).
    6. If `sum(input.capacity) < sum(output.capacity)` return `CapacityShortfall` (caller logs + skips). Zero fee is allowed.

- [ ] **Step 1: Write the failing tests**

Helpers local to `assemble.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::ResolvedProcess;
    use crate::rpc::LiveCell;
    use crate::scan::ScannedIntention;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{
        Byte32, CellOutput, OutPoint, RawTransaction, Script, Transaction,
    };
    use ckb_types::prelude::*;
    use cid_protocol::{CidArgs, Payload, Script as CidScript};
    use cid_process_executor::PartialTx;

    fn pack_partial(outputs: Vec<CellOutput>, data: Vec<Vec<u8>>) -> PartialTx {
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

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0xEE],
        }
    }

    fn intention(fill: u8, process_hash: [u8; 32], cap: u64) -> ScannedIntention {
        let args = CidArgs::new(
            process_hash,
            user_lock(),
            Payload { data: vec![0xAA] },
        );
        let lock_args = args.encode().unwrap();
        ScannedIntention {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[fill; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            capacity: cap,
            type_script: None,
            data: vec![],
            lock_args,
            args,
        }
    }

    fn process(hash: [u8; 32], cap: u64) -> ResolvedProcess {
        let ts = Script::new_builder()
            .code_hash([0x51u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(hash.pack())
            .build();
        let lock = Script::new_builder()
            .code_hash([0u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(Vec::<u8>::new().pack())
            .build();
        let output = CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(lock)
            .type_(Some(ts).pack())
            .build();
        let live = LiveCell {
            out_point: OutPoint::new_builder()
                .tx_hash(Byte32::from_slice(&[0x51; 32]).unwrap())
                .index(0u32.pack())
                .build(),
            output,
            data: vec![0x7f, b'E', b'L', b'F'],
        };
        ResolvedProcess {
            type_hash: crate::rpc::script_hash32(&live.type_script().unwrap()),
            live_cell: live,
            binary: vec![0x7f, b'E', b'L', b'F'],
        }
    }

    fn beneficiary(cap: u64) -> CellOutput {
        CellOutput::new_builder()
            .capacity(cap.pack())
            .lock(
                Script::new_builder()
                    .code_hash([0x11u8; 32].pack())
                    .hash_type(ScriptHashType::Type.into())
                    .args(vec![0x01u8].pack())
                    .build(),
            )
            .build()
    }

    #[test]
    fn digest_ok_destroys_intention_adds_process_pair_no_fee_no_user_lock_input() {
        let proc = process([0xAB; 32], 200_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 100_0000_0000);
        let partial = pack_partial(vec![beneficiary(100_0000_0000)], vec![vec![]]);
        let skel = assemble_digest_ok(&[intent.clone()], &proc, &partial, &[])
            .expect("assemble");
        let tx = skel.tx;
        assert_eq!(tx.inputs().len(), 2, "intention + process only; no fee input");
        let in_ops: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert_eq!(in_ops[0], intent.out_point);
        assert_eq!(in_ops[1], proc.live_cell.out_point);
        assert_eq!(tx.outputs().len(), 2, "beneficiary + process recreate");
        let proc_out = tx.outputs().get(1).unwrap();
        assert_eq!(
            proc_out.type_().to_opt().map(|t| crate::rpc::script_hash32(&t)),
            Some(hash)
        );
        assert_eq!(proc_out.capacity(), proc.live_cell.output.capacity());
        assert_eq!(proc_out.lock(), proc.live_cell.lock());
        assert_eq!(
            in_ops.len(),
            2,
            "User Lock is not an input; only intention + process outpoints"
        );
        assert!(
            tx.header_deps().is_empty(),
            "timeout_burn uses header deps; digest_ok must not"
        );
    }

    #[test]
    fn digest_ok_skips_on_capacity_shortfall() {
        let proc = process([0xAB; 32], 50_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 10_0000_0000);
        let partial = pack_partial(vec![beneficiary(500_0000_0000)], vec![vec![]]);
        let err = assemble_digest_ok(&[intent], &proc, &partial, &[]).unwrap_err();
        assert!(matches!(
            err,
            AssembleError::CapacityShortfall { .. }
        ));
    }

    #[test]
    fn digest_ok_uses_partial_process_recreate_when_present() {
        let proc = process([0xAB; 32], 200_0000_0000);
        let hash = proc.type_hash;
        let intent = intention(1, hash, 100_0000_0000);
        let recreate = CellOutput::new_builder()
            .capacity(180_0000_0000u64.pack())
            .lock(proc.live_cell.lock())
            .type_(proc.live_cell.type_script().pack())
            .build();
        let partial = pack_partial(
            vec![beneficiary(100_0000_0000), recreate.clone()],
            vec![vec![], vec![0x01]],
        );
        let skel = assemble_digest_ok(&[intent], &proc, &partial, &[]).unwrap();
        assert_eq!(skel.tx.outputs().len(), 2);
        let proc_out = skel.tx.outputs().get(1).unwrap();
        let cap: u64 = proc_out.capacity().unpack();
        assert_eq!(cap, 180_0000_0000);
        let data: Vec<u8> = skel.tx.outputs_data().get(1).unwrap().unpack();
        assert_eq!(data, vec![0x01]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-core digest_ok_destroys_intention_adds_process_pair_no_fee_no_user_lock_input`

Expected: FAIL — `assemble_digest_ok` not found.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-core/src/assemble.rs`:

```rust
use crate::process::ResolvedProcess;
use crate::rpc::script_hash32;
use crate::scan::ScannedIntention;
use ckb_types::core::{TransactionBuilder, TransactionView};
use ckb_types::packed::{CellDep, CellInput, CellOutput};
use ckb_types::prelude::*;
use cid_process_executor::PartialTx;
use std::fmt;

#[derive(Clone, Debug)]
pub struct ExecutionSkeleton {
    pub tx: TransactionView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    CapacityShortfall { inputs: u64, outputs: u64 },
    RefundUnbuildable(String),
}

impl fmt::Display for AssembleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssembleError::CapacityShortfall { inputs, outputs } => {
                write!(f, "capacity shortfall in={inputs} out={outputs}")
            }
            AssembleError::RefundUnbuildable(m) => write!(f, "refund unbuildable: {m}"),
        }
    }
}

impl std::error::Error for AssembleError {}

fn cell_input(op: &ckb_types::packed::OutPoint) -> CellInput {
    CellInput::new_builder()
        .previous_output(op.clone())
        .since(0u64.pack())
        .build()
}

fn sum_output_capacity(outputs: &[CellOutput]) -> u64 {
    outputs
        .iter()
        .map(|o| {
            let c: u64 = o.capacity().unpack();
            c
        })
        .sum()
}

fn finish(
    inputs: Vec<CellInput>,
    input_capacity: u64,
    outputs: Vec<CellOutput>,
    outputs_data: Vec<Vec<u8>>,
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    let output_capacity = sum_output_capacity(&outputs);
    if input_capacity < output_capacity {
        return Err(AssembleError::CapacityShortfall {
            inputs: input_capacity,
            outputs: output_capacity,
        });
    }
    let tx = TransactionBuilder::default()
        .inputs(inputs)
        .outputs(outputs)
        .outputs_data(outputs_data.into_iter().map(|d| d.pack()))
        .cell_deps(extra_cell_deps.iter().cloned())
        .build();
    Ok(ExecutionSkeleton { tx })
}

pub fn assemble_digest_ok(
    intentions: &[ScannedIntention],
    process: &ResolvedProcess,
    partial: &PartialTx,
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    let mut inputs: Vec<CellInput> = intentions
        .iter()
        .map(|i| cell_input(&i.out_point))
        .collect();
    inputs.push(cell_input(&process.live_cell.out_point));

    let input_capacity = intentions.iter().map(|i| i.capacity).sum::<u64>()
        + process.live_cell.capacity();

    let mut outputs = Vec::new();
    let mut outputs_data = Vec::new();
    let mut recreate: Option<(CellOutput, Vec<u8>)> = None;
    for (i, out) in partial.outputs.iter().enumerate() {
        let is_proc = out
            .type_()
            .to_opt()
            .map(|t| script_hash32(&t) == process.type_hash)
            .unwrap_or(false);
        if is_proc && recreate.is_none() {
            recreate = Some((out.clone(), partial.outputs_data[i].clone()));
        } else {
            outputs.push(out.clone());
            outputs_data.push(partial.outputs_data[i].clone());
        }
    }
    let (proc_out, proc_data) = recreate.unwrap_or_else(|| {
        (
            process.live_cell.output.clone(),
            process.live_cell.data.clone(),
        )
    });
    outputs.push(proc_out);
    outputs_data.push(proc_data);

    finish(inputs, input_capacity, outputs, outputs_data, extra_cell_deps)
}
```

Do not add `assemble_digest_incompatible` in this task. Task 6 adds that function.

`lib.rs`:

```rust
pub mod assemble;
pub use assemble::{assemble_digest_ok, AssembleError, ExecutionSkeleton};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-core assemble::`

Expected: PASS. Input count is 2; outputs include Process type hash; capacity shortfall errors; PartialTx recreate preferred over copy.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/src/assemble.rs crates/cid-backend-core/src/lib.rs Cargo.lock
git commit -m "feat: assemble digest_ok skeleton without fee or user-lock inputs"
```

---

### Task 6: assemble digest_incompatible — User Lock exact refund, no Process pair

**Files:**
- Modify: `crates/cid-backend-core/src/assemble.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Test: `crates/cid-backend-core/src/assemble.rs`

**Interfaces:**
- Consumes: `ScannedIntention`, `cid_protocol::is_standard_xudt`, `XUDT_AMOUNT_LEN`, `finish` / `cell_input` already in `assemble.rs`
- Produces:
  - `pub fn assemble_digest_incompatible(intentions: &[ScannedIntention], extra_cell_deps: &[CellDep]) -> Result<ExecutionSkeleton, AssembleError>`
  - Destroy each Intention (1 in / 0 out).
  - No Process input and no Process-typed output.
  - One refund output per Intention: lock = `CidEntry.user_lock`; capacity exact; if type script present it must be standard xUDT (`is_standard_xudt`) with `data.len() == XUDT_AMOUNT_LEN`, copied unchanged; otherwise `RefundUnbuildable`.
  - User Lock must not appear as an input. No fee input. No header deps (`timeout_burn` is not this hop).
  - Capacity shortfall → `CapacityShortfall`. Unbuildable refund → `RefundUnbuildable` (loop logs + skips; does not submit a partial tx).

- [ ] **Step 1: Write the failing tests**

Append to `assemble.rs` tests (reuse `intention` / `user_lock` helpers already in that module from Task 5):

```rust
    #[test]
    fn digest_incompatible_refunds_user_lock_without_process_pair() {
        let intent = intention(1, [0xAB; 32], 100_0000_0000);
        let skel = assemble_digest_incompatible(&[intent.clone()], &[]).unwrap();
        let tx = skel.tx;
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.inputs().get(0).unwrap().previous_output(), intent.out_point);
        assert_eq!(tx.outputs().len(), 1);
        let out = tx.outputs().get(0).unwrap();
        let cap: u64 = out.capacity().unpack();
        assert_eq!(cap, 100_0000_0000);
        let lock = out.lock();
        let code: [u8; 32] = lock.code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(out.type_().to_opt().is_none());
        assert!(tx.header_deps().is_empty());
    }

    #[test]
    fn digest_incompatible_preserves_standard_xudt() {
        use cid_protocol::{XUDT_AMOUNT_LEN, XUDT_CODE_HASH, SCRIPT_HASH_TYPE_DATA1};
        let mut intent = intention(1, [0xAB; 32], 100_0000_0000);
        let ts = Script::new_builder()
            .code_hash(XUDT_CODE_HASH.pack())
            .hash_type(SCRIPT_HASH_TYPE_DATA1.into())
            .args([0x33u8; 32].pack())
            .build();
        intent.type_script = Some(ts.clone());
        intent.data = 1_000u128.to_le_bytes()[..XUDT_AMOUNT_LEN].to_vec();
        let skel = assemble_digest_incompatible(&[intent], &[]).unwrap();
        let out = skel.tx.outputs().get(0).unwrap();
        let got = out.type_().to_opt().expect("xudt type");
        assert_eq!(got.as_slice(), ts.as_slice());
        let data: Vec<u8> = skel.tx.outputs_data().get(0).unwrap().unpack();
        assert_eq!(data, 1_000u128.to_le_bytes()[..XUDT_AMOUNT_LEN].to_vec());
    }

    #[test]
    fn digest_incompatible_rejects_non_standard_type() {
        let mut intent = intention(1, [0xAB; 32], 100_0000_0000);
        intent.type_script = Some(
            Script::new_builder()
                .code_hash([0xFFu8; 32].pack())
                .hash_type(ScriptHashType::Type.into())
                .args(Vec::<u8>::new().pack())
                .build(),
        );
        intent.data = vec![1, 2, 3];
        let err = assemble_digest_incompatible(&[intent], &[]).unwrap_err();
        assert!(matches!(err, AssembleError::RefundUnbuildable(_)));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-core digest_incompatible_refunds_user_lock_without_process_pair`

Expected: FAIL — `assemble_digest_incompatible` unresolved.

- [ ] **Step 3: Write minimal implementation**

Add to `assemble.rs`. Extend the packed import with `Script` (`use ckb_types::packed::{CellDep, CellInput, CellOutput, Script};`).

```rust
fn protocol_script_to_packed(script: &cid_protocol::Script) -> Script {
    Script::new_builder()
        .code_hash(script.code_hash.pack())
        .hash_type(script.hash_type.into())
        .args(script.args.clone().pack())
        .build()
}

pub fn assemble_digest_incompatible(
    intentions: &[ScannedIntention],
    extra_cell_deps: &[CellDep],
) -> Result<ExecutionSkeleton, AssembleError> {
    if intentions.is_empty() {
        return Err(AssembleError::RefundUnbuildable(
            "no intentions to refund".into(),
        ));
    }
    let mut inputs = Vec::new();
    let mut input_capacity = 0u64;
    let mut outputs = Vec::new();
    let mut outputs_data = Vec::new();
    for intent in intentions {
        inputs.push(cell_input(&intent.out_point));
        input_capacity += intent.capacity;
        let (out, data) = refund_output(intent)?;
        outputs.push(out);
        outputs_data.push(data);
    }
    finish(inputs, input_capacity, outputs, outputs_data, extra_cell_deps)
}

fn refund_output(
    intent: &ScannedIntention,
) -> Result<(CellOutput, Vec<u8>), AssembleError> {
    let user = protocol_script_to_packed(&intent.args.entry.user_lock);
    let mut builder = CellOutput::new_builder()
        .capacity(intent.capacity.pack())
        .lock(user);
    if let Some(ts) = &intent.type_script {
        let code: [u8; 32] = ts.code_hash().unpack();
        let ht: u8 = ts.hash_type().into();
        if !cid_protocol::is_standard_xudt(&code, ht) {
            return Err(AssembleError::RefundUnbuildable(
                "non-standard type script; cannot exact-refund".into(),
            ));
        }
        if intent.data.len() != cid_protocol::XUDT_AMOUNT_LEN {
            return Err(AssembleError::RefundUnbuildable(
                "xUDT amount is not 16 bytes".into(),
            ));
        }
        builder = builder.type_(Some(ts.clone()).pack());
    }
    Ok((builder.build(), intent.data.clone()))
}
```

Export `assemble_digest_incompatible` from `lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-core digest_incompatible`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/src/assemble.rs crates/cid-backend-core/src/lib.rs
git commit -m "feat: assemble digest_incompatible User Lock exact refund"
```

---

### Task 7: submit + in-flight memory

**Files:**
- Create: `crates/cid-backend-core/src/submit.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Create: `tests/src/submit_inflight.rs`
- Modify: `tests/src/lib.rs`
- Test: `tests/src/submit_inflight.rs`

**Interfaces:**
- Consumes: `Rpc::submit_transaction`, `ExecutionSkeleton`, `RpcError::Conflict`
- Produces:
  - `pub struct SubmitState { pub in_flight: HashSet<Vec<u8>>, pub failed: HashSet<Vec<u8>> }` with `Default`
  - `pub fn submit_execution<R: Rpc>(rpc: &R, skeleton: &ExecutionSkeleton, state: &mut SubmitState, consumed: &[OutPoint]) -> Result<Byte32, RpcError>`
  - On `Ok`: insert each consumed out_point key into `in_flight`, remove from `failed`.
  - On `Conflict`: insert into `failed`, remove from `in_flight` (retry allowed next tick if cell still live). Do not rewrite the tx. Do not persist to disk.

- [ ] **Step 1: Write the failing test**

`tests/src/submit_inflight.rs`:

```rust
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
```

Add `pub mod submit_inflight;` to `tests/src/lib.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests submit_success_marks_in_flight_conflict_marks_failed`

Expected: FAIL — `submit` module not found.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-core/src/submit.rs`:

```rust
use crate::assemble::ExecutionSkeleton;
use crate::rpc::{out_point_key, Rpc, RpcError};
use ckb_types::packed::{Byte32, OutPoint};
use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct SubmitState {
    pub in_flight: HashSet<Vec<u8>>,
    pub failed: HashSet<Vec<u8>>,
}

pub fn submit_execution<R: Rpc>(
    rpc: &R,
    skeleton: &ExecutionSkeleton,
    state: &mut SubmitState,
    consumed: &[OutPoint],
) -> Result<Byte32, RpcError> {
    match rpc.submit_transaction(&skeleton.tx) {
        Ok(hash) => {
            for op in consumed {
                let k = out_point_key(op);
                state.in_flight.insert(k.clone());
                state.failed.remove(&k);
            }
            Ok(hash)
        }
        Err(e @ RpcError::Conflict(_)) => {
            for op in consumed {
                let k = out_point_key(op);
                state.failed.insert(k.clone());
                state.in_flight.remove(&k);
            }
            Err(e)
        }
        Err(e) => Err(e),
    }
}
```

`lib.rs`: `pub mod submit;` and `pub use submit::{submit_execution, SubmitState};`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests submit_success_marks_in_flight_conflict_marks_failed`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/src/submit.rs crates/cid-backend-core/src/lib.rs \
  tests/src/lib.rs tests/src/submit_inflight.rs
git commit -m "feat: submit execution tx with in-memory in-flight set"
```

---

### Task 8: digest_loop tick + FakeRpc vertical n=1

**Files:**
- Create: `crates/cid-backend-core/src/digest_loop.rs`
- Modify: `crates/cid-backend-core/src/lib.rs`
- Modify: `tests/Cargo.toml` (add `cid-protocol`, `cid-process-executor`)
- Create: `tests/src/scripted_ssri.rs`
- Modify: `tests/src/helpers.rs`
- Create: `tests/src/vertical_n1.rs`
- Modify: `tests/src/lib.rs`
- Test: `tests/src/vertical_n1.rs`

**Interfaces:**
- Consumes: `scan_intentions`, `bucket_by_process_type_hash`, `ProcessResolver::resolve`, `SsriHost`, `to_intention_input`, `assemble_digest_ok`, `assemble_digest_incompatible`, `submit_execution`, `SubmitState`
- Produces:
  - `pub struct LoopConfig { pub cid_code_hash: [u8; 32], pub hash_type: ScriptHashType, pub poll_interval: Duration, pub extra_cell_deps: Vec<CellDep> }`
  - `pub struct LoopState { pub submit: SubmitState, pub resolver: ProcessResolver }`
  - `pub trait Clock { fn sleep(&self, duration: Duration); }`
  - `pub struct NopClock;` / `pub struct RealClock;` (`RealClock` calls `std::thread::sleep`)
  - `pub struct TickReport { pub scanned: usize, pub skipped: usize, pub submitted: usize, pub conflicts: usize }`
  - `pub struct DigestLoop<R: Rpc, C: Clock, S: SsriHost> { ... }`
  - `impl DigestLoop { pub fn new(rpc: R, clock: C, ssri: S, config: LoopConfig, resolver: ProcessResolver) -> Self; pub fn tick(&mut self) -> TickReport; pub fn run(&mut self); }`
  - `tick()` implements design §7.1. `run()` is `loop { tick(); clock.sleep(poll_interval); }` and is not called from tests.
  - Skip policies (do not abort the tick): bad args (already dropped in scan); missing Process; SSRI Unusable; live count `< min`; capacity shortfall; refund unbuildable; RPC unavailable on scan (log, empty report). `timeout_burn` is never assembled.

Tick algorithm (this is the code you will write):

```
scan_intentions
drop in_flight keys whose cells are no longer live
filter out still-in-flight outpoints
bucket_by_process_type_hash (first-seen order)
for each bucket:
  resolve Process; None → log+skip bucket
  ssri.get_batch_bounds; Unusable → log+skip bucket
  remaining = bucket
  while remaining.len() >= min:
    n = min(remaining.len(), max as usize)
    batch = remaining[..n]
    remaining = remaining[n..]
    ssri.assemble
      Ok → assemble_digest_ok; CapacityShortfall → log+skip
      Incompatible → assemble_digest_incompatible; RefundUnbuildable → log+skip
      Unusable → log+skip (NO refund)
    submit_execution
      Ok → submitted++
      Conflict → conflicts++
      other → log+skip
```

- [ ] **Step 1: Write the failing test**

Expand `tests/src/helpers.rs` to the full file:

```rust
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
use ckb_types::prelude::*;
use cid_backend_core::rpc::LiveCell;
use cid_protocol::{CidArgs, Payload, Script as CidScript};

pub const CID_CODE: [u8; 32] = [0xC1; 32];

pub fn out_point(tx_fill: u8, index: u32) -> OutPoint {
    OutPoint::new_builder()
        .tx_hash(Byte32::from_slice(&[tx_fill; 32]).expect("32 bytes"))
        .index(index.pack())
        .build()
}

pub fn lock_script(code_hash: [u8; 32], hash_type: ScriptHashType, args: Vec<u8>) -> Script {
    Script::new_builder()
        .code_hash(code_hash.pack())
        .hash_type(hash_type.into())
        .args(args.pack())
        .build()
}

pub fn live_cell(
    op: OutPoint,
    lock: Script,
    type_script: Option<Script>,
    capacity: u64,
    data: Vec<u8>,
) -> LiveCell {
    let mut output = CellOutput::new_builder()
        .capacity(capacity.pack())
        .lock(lock);
    if let Some(ts) = type_script {
        output = output.type_(Some(ts).pack());
    }
    LiveCell {
        out_point: op,
        output: output.build(),
        data,
    }
}

pub fn user_lock() -> CidScript {
    CidScript {
        code_hash: [0x42; 32],
        hash_type: 1,
        args: vec![0x01, 0x02],
    }
}

pub fn process_type_script(unique: u8) -> Script {
    lock_script([0x51; 32], ScriptHashType::Type, vec![unique; 32])
}

pub fn cid_lock_args(process_type_hash: [u8; 32]) -> Vec<u8> {
    CidArgs::new(
        process_type_hash,
        user_lock(),
        Payload { data: vec![0xAA] },
    )
    .encode()
    .unwrap()
}

pub fn intention_cell(fill: u8, process_type_hash: [u8; 32], capacity: u64) -> LiveCell {
    live_cell(
        out_point(fill, 0),
        lock_script(
            CID_CODE,
            ScriptHashType::Type,
            cid_lock_args(process_type_hash),
        ),
        None,
        capacity,
        vec![],
    )
}

pub fn process_cell(unique: u8, capacity: u64, data: Vec<u8>) -> LiveCell {
    live_cell(
        out_point(0x50, unique as u32),
        lock_script([0u8; 32], ScriptHashType::Type, vec![]),
        Some(process_type_script(unique)),
        capacity,
        data,
    )
}
```

`tests/src/scripted_ssri.rs`:

```rust
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{CellOutput, RawTransaction, Script, Transaction};
use ckb_types::prelude::*;
use cid_backend_core::ssri::{SsriError, SsriHost};
use cid_process_executor::{IntentionInput, PartialTx};

pub struct ScriptedSsri {
    pub bounds: (u32, u32),
    pub mode: Mode,
}

pub enum Mode {
    MimicExample,
    AlwaysIncompatible(i8),
    Unusable,
}

impl ScriptedSsri {
    pub fn mimic(bounds: (u32, u32)) -> Self {
        Self {
            bounds,
            mode: Mode::MimicExample,
        }
    }

    pub fn incompatible(code: i8) -> Self {
        Self {
            bounds: (1, 4),
            mode: Mode::AlwaysIncompatible(code),
        }
    }

    pub fn unusable() -> Self {
        Self {
            bounds: (1, 4),
            mode: Mode::Unusable,
        }
    }
}

fn pack_partial(outputs: Vec<CellOutput>, data: Vec<Vec<u8>>) -> PartialTx {
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

impl SsriHost for ScriptedSsri {
    fn get_batch_bounds(&self, _binary: &[u8]) -> Result<(u32, u32), SsriError> {
        match self.mode {
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            _ => Ok(self.bounds),
        }
    }

    fn assemble(
        &self,
        _binary: &[u8],
        intentions: &[IntentionInput],
    ) -> Result<PartialTx, SsriError> {
        match self.mode {
            Mode::AlwaysIncompatible(c) => Err(SsriError::Incompatible(c)),
            Mode::Unusable => Err(SsriError::Unusable("scripted".into())),
            Mode::MimicExample => {
                let mut outputs = Vec::new();
                let mut data = Vec::new();
                for it in intentions {
                    let lock = Script::new_builder()
                        .code_hash([0x11u8; 32].pack())
                        .hash_type(ScriptHashType::Type.into())
                        .args(it.lock_args.clone().pack())
                        .build();
                    outputs.push(
                        CellOutput::new_builder()
                            .capacity(it.capacity.pack())
                            .lock(lock)
                            .build(),
                    );
                    data.push(it.data.clone());
                }
                Ok(pack_partial(outputs, data))
            }
        }
    }
}
```

`tests/src/vertical_n1.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};

    use crate::helpers::{intention_cell, process_cell, process_type_script, CID_CODE};
    use crate::scripted_ssri::ScriptedSsri;
    use crate::fake_rpc::FakeRpc;

    #[test]
    fn tick_n1_digest_ok_shape_accepted_by_fakerpc() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(1, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 1]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        assert_eq!(report.conflicts, 0);

        let txs = loop_.rpc().submitted();
        assert_eq!(txs.len(), 1);
        let tx = &txs[0];
        assert_eq!(tx.inputs().len(), 2, "intention + process; no fee input");
        let ops: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        assert_eq!(ops[0], intent.out_point);
        assert_eq!(ops[1], proc.out_point);
        assert_eq!(tx.outputs().len(), 2, "beneficiary + process recreate");
        let proc_out = tx.outputs().get(1).unwrap();
        assert_eq!(
            proc_out.type_().to_opt().map(|t| script_hash32(&t)),
            Some(hash)
        );
        assert!(tx.header_deps().is_empty(), "must not build timeout_burn");
        assert_eq!(
            ops.len(),
            2,
            "User Lock is not an input; only intention + process outpoints"
        );
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .all(|c| c.out_point != intent.out_point));
    }

    #[test]
    fn tick_unusable_ssri_skips_without_refund_or_submit() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(1, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 1]));

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::unusable(),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
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
            "Unusable must skip, not refund/destroy"
        );
    }
}
```

`DigestLoop` exposes `pub fn rpc(&self) -> &R` and `pub fn state(&self) -> &LoopState` (implemented in Step 3).

`tests/src/lib.rs`:

```rust
pub mod fake_rpc;
pub mod helpers;
pub mod process_resolver;
pub mod scripted_ssri;
pub mod submit_inflight;
pub mod vertical_n1;
```

`tests/Cargo.toml` dependencies:

```toml
[dependencies]
cid-backend-core = { path = "../crates/cid-backend-core" }
ckb-types = { workspace = true }
cid-protocol = { workspace = true }
cid-process-executor = { workspace = true }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests tick_n1_digest_ok_shape_accepted_by_fakerpc`

Expected: FAIL — `digest_loop` / `DigestLoop` not found.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-core/src/digest_loop.rs`:

```rust
use crate::assemble::{assemble_digest_incompatible, assemble_digest_ok, AssembleError};
use crate::process::{ProcessResolver, ResolvedProcess};
use crate::rpc::{out_point_key, Rpc, RpcError};
use crate::scan::{bucket_by_process_type_hash, scan_intentions, ScannedIntention};
use crate::ssri::{to_intention_input, SsriError, SsriHost};
use crate::submit::{submit_execution, SubmitState};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::CellDep;
use std::collections::HashSet;
use std::time::Duration;

pub struct LoopConfig {
    pub cid_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
    pub extra_cell_deps: Vec<CellDep>,
}

pub struct LoopState {
    pub submit: SubmitState,
    pub resolver: ProcessResolver,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: usize,
    pub skipped: usize,
    pub submitted: usize,
    pub conflicts: usize,
}

pub trait Clock {
    fn sleep(&self, duration: Duration);
}

pub struct NopClock;

impl Clock for NopClock {
    fn sleep(&self, _duration: Duration) {}
}

pub struct RealClock;

impl Clock for RealClock {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

pub struct DigestLoop<R: Rpc, C: Clock, S: SsriHost> {
    rpc: R,
    clock: C,
    ssri: S,
    config: LoopConfig,
    state: LoopState,
}

impl<R: Rpc, C: Clock, S: SsriHost> DigestLoop<R, C, S> {
    pub fn new(
        rpc: R,
        clock: C,
        ssri: S,
        config: LoopConfig,
        resolver: ProcessResolver,
    ) -> Self {
        Self {
            rpc,
            clock,
            ssri,
            config,
            state: LoopState {
                submit: SubmitState::default(),
                resolver,
            },
        }
    }

    pub fn rpc(&self) -> &R {
        &self.rpc
    }

    pub fn state(&self) -> &LoopState {
        &self.state
    }

    pub fn run(&mut self) {
        loop {
            let _ = self.tick();
            self.clock.sleep(self.config.poll_interval);
        }
    }

    pub fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        let scanned = match scan_intentions(
            &self.rpc,
            &self.config.cid_code_hash,
            self.config.hash_type,
        ) {
            Ok(v) => v,
            Err(RpcError::Unavailable(m)) => {
                log::warn!("scan unavailable: {m}");
                return report;
            }
            Err(e) => {
                log::warn!("scan error: {e}");
                return report;
            }
        };
        report.scanned = scanned.len();

        let live_keys: HashSet<Vec<u8>> = scanned
            .iter()
            .map(|i| out_point_key(&i.out_point))
            .collect();
        self.state
            .submit
            .in_flight
            .retain(|k| live_keys.contains(k));

        let eligible: Vec<ScannedIntention> = scanned
            .into_iter()
            .filter(|i| {
                !self
                    .state
                    .submit
                    .in_flight
                    .contains(&out_point_key(&i.out_point))
            })
            .collect();

        for (_hash, bucket) in bucket_by_process_type_hash(&eligible) {
            self.process_bucket(bucket, &mut report);
        }
        report
    }

    fn process_bucket(&mut self, bucket: Vec<ScannedIntention>, report: &mut TickReport) {
        if bucket.is_empty() {
            return;
        }
        let hash = bucket[0].args.process_type_hash;
        let process = match self.state.resolver.resolve(&self.rpc, &hash) {
            Ok(Some(p)) => p,
            Ok(None) => {
                log::warn!("skip bucket: no usable Process binary");
                report.skipped += bucket.len();
                return;
            }
            Err(e) => {
                log::warn!("skip bucket: process resolve {e}");
                report.skipped += bucket.len();
                return;
            }
        };
        let (min, max) = match self.ssri.get_batch_bounds(&process.binary) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("skip bucket: batch bounds {e}");
                report.skipped += bucket.len();
                return;
            }
        };
        if min == 0 || max < min {
            log::warn!("skip bucket: malformed bounds min={min} max={max}");
            report.skipped += bucket.len();
            return;
        }
        let mut remaining = bucket;
        while remaining.len() >= min as usize {
            let n = remaining.len().min(max as usize);
            let batch: Vec<ScannedIntention> = remaining.drain(..n).collect();
            self.process_batch(&process, batch, report);
        }
        if !remaining.is_empty() {
            report.skipped += remaining.len();
        }
    }

    fn process_batch(
        &mut self,
        process: &ResolvedProcess,
        batch: Vec<ScannedIntention>,
        report: &mut TickReport,
    ) {
        let inputs: Vec<_> = batch.iter().map(to_intention_input).collect();
        let assembled = self.ssri.assemble(&process.binary, &inputs);
        let skeleton = match assembled {
            Ok(partial) => match assemble_digest_ok(
                &batch,
                process,
                &partial,
                &self.config.extra_cell_deps,
            ) {
                Ok(s) => s,
                Err(AssembleError::CapacityShortfall { inputs, outputs }) => {
                    log::warn!("skip batch: capacity in={inputs} out={outputs}");
                    report.skipped += batch.len();
                    return;
                }
                Err(e) => {
                    log::warn!("skip batch: digest_ok {e}");
                    report.skipped += batch.len();
                    return;
                }
            },
            Err(SsriError::Incompatible(code)) => {
                log::info!("ssri incompatible {code}; attempting user-lock refund");
                match assemble_digest_incompatible(&batch, &self.config.extra_cell_deps) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("skip batch: refund unbuildable {e}");
                        report.skipped += batch.len();
                        return;
                    }
                }
            }
            Err(SsriError::Unusable(m)) => {
                log::warn!("skip batch: ssri unusable {m} (no refund)");
                report.skipped += batch.len();
                return;
            }
        };
        let consumed: Vec<_> = skeleton
            .tx
            .inputs()
            .into_iter()
            .map(|i| i.previous_output())
            .collect();
        match submit_execution(&self.rpc, &skeleton, &mut self.state.submit, &consumed) {
            Ok(_) => report.submitted += 1,
            Err(RpcError::Conflict(m)) => {
                log::warn!("submit conflict: {m}");
                report.conflicts += 1;
            }
            Err(e) => {
                log::warn!("submit failed: {e}");
                report.skipped += batch.len();
            }
        }
    }
}
```

`lib.rs` add `pub mod digest_loop;` and re-export `Clock, DigestLoop, LoopConfig, LoopState, NopClock, RealClock, TickReport`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests tick_n1_digest_ok_shape_accepted_by_fakerpc`

Expected: PASS. One tx, 2 inputs (Intention + Process), 2 outputs, no header deps, FakeRpc consumed the Intention.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-core/src/digest_loop.rs crates/cid-backend-core/src/lib.rs \
  tests/src tests/Cargo.toml Cargo.lock
git commit -m "feat: digest_loop tick with n=1 FakeRpc digest_ok vertical"
```

---

### Task 9: Vertical — batch bounds

**Files:**
- Create: `tests/src/vertical_batch_bounds.rs`
- Modify: `tests/src/lib.rs`
- Test: `tests/src/vertical_batch_bounds.rs`

**Interfaces:**
- Consumes: `DigestLoop::tick`, `ScriptedSsri::mimic`, `FakeRpc`, `LoopConfig`, `NopClock`, `ProcessResolver::new`
- Produces: no new production types. Coverage for design §7.2 / §9.1 item 2:
  - live count `< min` → no submit, cells remain
  - live count `== max` → one submit whose Intention input count is `max`
  - live count `> max` → first submit takes the first `max` cells in asc; leftover `< min` stays live (same tick then continues while `remaining >= min`)

Use `ScriptedSsri::mimic((2, 2))` so leftover 1 after taking 2 is observable.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::script_hash32;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    fn setup(n_intentions: u8) -> (FakeRpc, [u8; 32]) {
        let rpc = FakeRpc::new();
        let ts = process_type_script(2);
        let hash = script_hash32(&ts);
        rpc.insert_cell(process_cell(2, 400_0000_0000, vec![0x7f, b'E', b'L', b'F', 2]));
        for i in 1..=n_intentions {
            rpc.insert_cell(intention_cell(i, hash, 100_0000_0000));
        }
        (rpc, hash)
    }

    fn cfg() -> LoopConfig {
        LoopConfig {
            cid_code_hash: CID_CODE,
            hash_type: ScriptHashType::Type,
            poll_interval: Duration::from_secs(1),
            extra_cell_deps: vec![],
        }
    }

    #[test]
    fn below_min_does_not_submit() {
        let (rpc, _) = setup(1);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 0);
        assert_eq!(loop_.rpc().submitted().len(), 0);
        assert_eq!(
            loop_
                .rpc()
                .live_cells()
                .iter()
                .filter(|c| c.lock().code_hash().as_slice() == CID_CODE)
                .count(),
            1
        );
    }

    #[test]
    fn at_max_submits_exactly_max() {
        let (rpc, _) = setup(2);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3, "2 intentions + 1 process");
    }

    #[test]
    fn above_max_takes_first_max_asc_leaves_remainder_below_min() {
        let (rpc, _) = setup(3);
        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((2, 2)),
            cfg(),
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);
        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 3, "first max=2 intentions + process");
        let remaining_intentions = loop_
            .rpc()
            .live_cells()
            .iter()
            .filter(|c| c.lock().code_hash().as_slice() == CID_CODE)
            .count();
        assert_eq!(remaining_intentions, 1);
        let first_intent_fill = tx.inputs().get(0).unwrap().previous_output();
        assert_eq!(
            first_intent_fill.tx_hash().as_slice(),
            &[1u8; 32],
            "asc bucket head"
        );
    }
}
```

Add `pub mod vertical_batch_bounds;` to `tests/src/lib.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests below_min_does_not_submit`

Expected: FAIL until the module is wired. If Task 8 already implements the while-loop correctly, these tests may PASS on first run after adding the file — that is acceptable (the production code already exists). If they PASS immediately, keep the tests and still commit this task as the vertical coverage. If `above_max` fails because the loop consumes remainder incorrectly, fix **only** `process_bucket` in `digest_loop.rs`.

If `tx_hash().as_slice()` does not equal `&[1u8; 32]` because Byte32 includes extra molecule headers, compare via `Unpack::<[u8; 32]>::unpack(&tx_hash)` instead:

```rust
let h: [u8; 32] = tx.inputs().get(0).unwrap().previous_output().tx_hash().unpack();
assert_eq!(h, [1u8; 32]);
```

- [ ] **Step 3: Write minimal implementation**

No production change if Task 8 already drains `min..=max` then continues. If `above_max` submits two transactions (2 then the leftover 1 because `min` was treated as 1), set bounds in the test to `(2, 2)` as written — leftover 1 must not submit.

Do not change skip-on-`< min` into a wait that blocks other buckets.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests vertical_batch_bounds::`

Expected: PASS — below min 0 txs; at max 1 tx with 2 Intention inputs; above max first tx uses fill=1 then fill=2 (asc), one Intention remains.

- [ ] **Step 5: Commit**

```bash
git add tests/src/vertical_batch_bounds.rs tests/src/lib.rs
git commit -m "test: FakeRpc vertical for SSRI batch bounds"
```

---

### Task 10: Vertical — Incompatible refund

**Files:**
- Create: `tests/src/vertical_incompatible.rs`
- Modify: `tests/src/lib.rs`
- Test: `tests/src/vertical_incompatible.rs`

**Interfaces:**
- Consumes: `ScriptedSsri::incompatible(21)`, `assemble_digest_incompatible` via `tick`, `FakeRpc`
- Produces: no new production types. Coverage for design §7.2 / §9.1 item 3 / D Incompatible path:
  - Guest/host returns `SsriError::Incompatible`
  - FakeRpc chain shows a User Lock output with exact capacity
  - Live Process cell is **still present** (no Process pair in the tx)
  - Intention cell is consumed
  - Tx inputs do not include a cell locked by the User Lock
  - No header deps

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use ckb_types::prelude::*;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::script_hash32;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_type_script, user_lock, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn incompatible_refunds_user_lock_without_process_pair() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(3);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        let proc = process_cell(3, 200_0000_0000, vec![0x7f, b'E', b'L', b'F', 3]);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(proc.clone());

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::incompatible(21),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
            ProcessResolver::new(),
        );
        let report = loop_.tick();
        assert_eq!(report.submitted, 1);

        let tx = &loop_.rpc().submitted()[0];
        assert_eq!(tx.inputs().len(), 1);
        assert_eq!(tx.inputs().get(0).unwrap().previous_output(), intent.out_point);
        assert_eq!(tx.outputs().len(), 1);
        let out = tx.outputs().get(0).unwrap();
        let cap: u64 = out.capacity().unpack();
        assert_eq!(cap, 100_0000_0000);
        let code: [u8; 32] = out.lock().code_hash().unpack();
        assert_eq!(code, user_lock().code_hash);
        assert!(out.type_().to_opt().is_none());
        assert!(tx.header_deps().is_empty());

        let live = loop_.rpc().live_cells();
        assert!(
            live.iter().any(|c| c.out_point == proc.out_point),
            "process cell must remain live"
        );
        assert!(live.iter().all(|c| c.out_point != intent.out_point));
        assert!(live.iter().any(|c| {
            let code: [u8; 32] = c.lock().code_hash().unpack();
            code == user_lock().code_hash && c.capacity() == 100_0000_0000
        }));
    }
}
```

Add `pub mod vertical_incompatible;` to `tests/src/lib.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests incompatible_refunds_user_lock_without_process_pair`

Expected: FAIL if `tick` does not route `SsriError::Incompatible` to `assemble_digest_incompatible`. If Task 8 already routes it, the new test PASSes; still commit the vertical file.

- [ ] **Step 3: Write minimal implementation**

Only if Step 2 failed: in `digest_loop.rs` `process_batch`, the `Err(SsriError::Incompatible(code))` arm must call `assemble_digest_incompatible` and must **not** call `assemble_digest_ok`. VM/Unusable must still skip without refund.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests vertical_incompatible::`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/src/vertical_incompatible.rs tests/src/lib.rs crates/cid-backend-core/src/digest_loop.rs
git commit -m "test: FakeRpc vertical for Incompatible User Lock refund"
```

---

### Task 11: Vertical — submit conflict then retry next tick

**Files:**
- Create: `tests/src/vertical_submit_conflict.rs`
- Modify: `tests/src/lib.rs`
- Test: `tests/src/vertical_submit_conflict.rs`

**Interfaces:**
- Consumes: `FakeRpc::inject_conflict_once`, `DigestLoop::tick`, `SubmitState` in-memory only
- Produces: no new production types. Coverage for design §7.2 / §8 / §9.1 item 4 / D6:
  - First `tick`: `inject_conflict_once` → `report.conflicts == 1`, Intention still live, `failed` contains the out_point, `in_flight` does not
  - Second `tick` without injecting conflict: submit succeeds, Intention consumed
  - No disk files are created (do not add any `std::fs` in core)

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ckb_types::core::ScriptHashType;
    use cid_backend_core::digest_loop::{DigestLoop, LoopConfig, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::{out_point_key, script_hash32};

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        intention_cell, process_cell, process_type_script, CID_CODE,
    };
    use crate::scripted_ssri::ScriptedSsri;

    #[test]
    fn conflict_then_retry_next_tick_succeeds() {
        let rpc = FakeRpc::new();
        let ts = process_type_script(4);
        let hash = script_hash32(&ts);
        let intent = intention_cell(1, hash, 100_0000_0000);
        rpc.insert_cell(intent.clone());
        rpc.insert_cell(process_cell(
            4,
            200_0000_0000,
            vec![0x7f, b'E', b'L', b'F', 4],
        ));
        rpc.inject_conflict_once();

        let mut loop_ = DigestLoop::new(
            rpc,
            NopClock,
            ScriptedSsri::mimic((1, 4)),
            LoopConfig {
                cid_code_hash: CID_CODE,
                hash_type: ScriptHashType::Type,
                poll_interval: Duration::from_secs(1),
                extra_cell_deps: vec![],
            },
            ProcessResolver::new(),
        );

        let first = loop_.tick();
        assert_eq!(first.submitted, 0);
        assert_eq!(first.conflicts, 1);
        assert!(loop_
            .state()
            .submit
            .failed
            .contains(&out_point_key(&intent.out_point)));
        assert!(!loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .any(|c| c.out_point == intent.out_point));

        let second = loop_.tick();
        assert_eq!(second.submitted, 1);
        assert_eq!(second.conflicts, 0);
        assert!(loop_
            .rpc()
            .live_cells()
            .iter()
            .all(|c| c.out_point != intent.out_point));
        assert!(loop_
            .state()
            .submit
            .in_flight
            .contains(&out_point_key(&intent.out_point)));
    }
}
```

Add `pub mod vertical_submit_conflict;` to `tests/src/lib.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-tests conflict_then_retry_next_tick_succeeds`

Expected: FAIL if in_flight is set on conflict (would skip the retry) or if failed cells are filtered from the next scan. The correct behavior is: conflict does **not** add in_flight; next tick scans the still-live cell and submits again.

- [ ] **Step 3: Write minimal implementation**

If the test fails because `tick` filters `state.submit.failed` as well as `in_flight`, remove that filter — only `in_flight` is skipped (indexer lag after success). `failed` is informational.

If FakeRpc consumed the cell on conflict, that is a FakeRpc bug: `inject_conflict_once` must return `Conflict` **before** removing inputs (already specified in Task 1).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cid-backend-tests vertical_submit_conflict::`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/src/vertical_submit_conflict.rs tests/src/lib.rs \
  crates/cid-backend-core/src/digest_loop.rs crates/cid-backend-core/src/submit.rs \
  tests/src/fake_rpc.rs
git commit -m "test: FakeRpc vertical for submit conflict retry on next tick"
```

---

### Task 12: Thin daemon — env config, PlaceholderRpc, no Process fallback

**Files:**
- Modify: `crates/cid-backend-daemon/Cargo.toml`
- Create: `crates/cid-backend-daemon/src/config.rs`
- Create: `crates/cid-backend-daemon/src/placeholder_rpc.rs`
- Modify: `crates/cid-backend-daemon/src/main.rs`
- Test: `crates/cid-backend-daemon/src/config.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: `DigestLoop::new`, `RealClock`, `EmbeddedSsri`, `ProcessResolver::new` (no fallback), `LoopConfig`
- Produces:
  - `pub struct DaemonConfig { pub node_url: String, pub indexer_url: String, pub cid_lock_code_hash: [u8; 32], pub hash_type: ScriptHashType, pub poll_interval: Duration }`
  - `impl DaemonConfig { pub fn from_env() -> Result<Self, ConfigError>; pub fn from_vars<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, ConfigError>; pub fn to_loop_config(&self) -> LoopConfig; }`
  - `pub enum ConfigError { Missing(String), Invalid(String) }`
  - Env vars (only; no config file, no CLI flags):
    | Var | Required | Meaning |
    |-----|----------|---------|
    | `CID_CKB_RPC_URL` | yes | node JSON-RPC URL |
    | `CID_INDEXER_URL` | no | indexer URL; default = `CID_CKB_RPC_URL` |
    | `CID_LOCK_CODE_HASH` | yes | 32-byte hex, optional `0x` prefix |
    | `CID_LOCK_HASH_TYPE` | no | `type` (default) / `data` / `data1` / `data2` |
    | `CID_POLL_INTERVAL_SECS` | yes | u64 ≥ 1 |
  - Process exits process-status 1 on `ConfigError` (the only start-fail path besides panic). Illegal config does not enter `run()`.
  - `PlaceholderRpc` implements `Rpc` by returning `RpcError::Unavailable` on every method. No HTTP client. No live network. A later change may replace this type; v0 tests never call it.
  - `main` constructs `ProcessResolver::new()` — **never** `with_fallback` / `with_fallback_binary`.

- [ ] **Step 1: Write the failing test**

`crates/cid-backend-daemon/src/config.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ckb_types::core::ScriptHashType;

    #[test]
    fn from_vars_parses_required_and_defaults() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://127.0.0.1:8114".into()),
            "CID_LOCK_CODE_HASH" => Some("11".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("5".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.node_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.indexer_url, "http://127.0.0.1:8114");
        assert_eq!(cfg.cid_lock_code_hash, [0x11u8; 32]);
        assert_eq!(cfg.hash_type, ScriptHashType::Type);
        assert_eq!(cfg.poll_interval, Duration::from_secs(5));
    }

    #[test]
    fn from_vars_reads_optional_indexer_and_hash_type() {
        let cfg = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://node".into()),
            "CID_INDEXER_URL" => Some("http://indexer".into()),
            "CID_LOCK_CODE_HASH" => Some("0x".to_string() + &"ab".repeat(32)),
            "CID_LOCK_HASH_TYPE" => Some("data1".into()),
            "CID_POLL_INTERVAL_SECS" => Some("2".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.indexer_url, "http://indexer");
        assert_eq!(cfg.hash_type, ScriptHashType::Data1);
        assert_eq!(cfg.cid_lock_code_hash[0], 0xab);
    }

    #[test]
    fn missing_required_fails() {
        let err = DaemonConfig::from_vars(|_| None).unwrap_err();
        assert!(matches!(err, ConfigError::Missing(_)));
    }

    #[test]
    fn zero_interval_invalid() {
        let err = DaemonConfig::from_vars(|k| match k {
            "CID_CKB_RPC_URL" => Some("http://n".into()),
            "CID_LOCK_CODE_HASH" => Some("00".repeat(32)),
            "CID_POLL_INTERVAL_SECS" => Some("0".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cid-backend-daemon from_vars_parses_required_and_defaults`

Expected: FAIL — `DaemonConfig` not found.

- [ ] **Step 3: Write minimal implementation**

`crates/cid-backend-daemon/Cargo.toml`:

```toml
[package]
name = "cid-backend-daemon"
version = "0.1.0"
edition = "2021"

[dependencies]
cid-backend-core = { path = "../cid-backend-core" }
ckb-types = { workspace = true }
log = { workspace = true }
env_logger = "0.11"
```

`crates/cid-backend-daemon/src/config.rs`:

```rust
use cid_backend_core::digest_loop::LoopConfig;
use ckb_types::core::ScriptHashType;
use std::fmt;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub node_url: String,
    pub indexer_url: String,
    pub cid_lock_code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Missing(String),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Missing(k) => write!(f, "missing env {k}"),
            ConfigError::Invalid(m) => write!(f, "invalid config: {m}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl DaemonConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    pub fn from_vars<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, ConfigError> {
        let node_url = get("CID_CKB_RPC_URL")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ConfigError::Missing("CID_CKB_RPC_URL".into()))?;
        let indexer_url = get("CID_INDEXER_URL")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| node_url.clone());
        let hash_raw = get("CID_LOCK_CODE_HASH")
            .ok_or_else(|| ConfigError::Missing("CID_LOCK_CODE_HASH".into()))?;
        let cid_lock_code_hash = parse_hex32(&hash_raw)?;
        let hash_type = match get("CID_LOCK_HASH_TYPE") {
            None => ScriptHashType::Type,
            Some(s) => parse_hash_type(&s)?,
        };
        let secs_raw = get("CID_POLL_INTERVAL_SECS")
            .ok_or_else(|| ConfigError::Missing("CID_POLL_INTERVAL_SECS".into()))?;
        let secs: u64 = secs_raw
            .parse()
            .map_err(|_| ConfigError::Invalid("CID_POLL_INTERVAL_SECS not u64".into()))?;
        if secs == 0 {
            return Err(ConfigError::Invalid(
                "CID_POLL_INTERVAL_SECS must be >= 1".into(),
            ));
        }
        Ok(Self {
            node_url,
            indexer_url,
            cid_lock_code_hash,
            hash_type,
            poll_interval: Duration::from_secs(secs),
        })
    }

    pub fn to_loop_config(&self) -> LoopConfig {
        LoopConfig {
            cid_code_hash: self.cid_lock_code_hash,
            hash_type: self.hash_type,
            poll_interval: self.poll_interval,
            extra_cell_deps: vec![],
        }
    }
}

fn parse_hash_type(s: &str) -> Result<ScriptHashType, ConfigError> {
    match s.to_ascii_lowercase().as_str() {
        "type" => Ok(ScriptHashType::Type),
        "data" => Ok(ScriptHashType::Data),
        "data1" => Ok(ScriptHashType::Data1),
        "data2" => Ok(ScriptHashType::Data2),
        other => Err(ConfigError::Invalid(format!(
            "CID_LOCK_HASH_TYPE={other}"
        ))),
    }
}

fn parse_hex32(s: &str) -> Result<[u8; 32], ConfigError> {
    let s = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if s.len() != 64 {
        return Err(ConfigError::Invalid(
            "CID_LOCK_CODE_HASH must be 32 bytes hex".into(),
        ));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| ConfigError::Invalid("CID_LOCK_CODE_HASH not hex".into()))?;
    }
    Ok(out)
}
```

`crates/cid-backend-daemon/src/placeholder_rpc.rs`:

```rust
use ckb_types::core::{ScriptHashType, TransactionView};
use ckb_types::packed::Byte32;
use cid_backend_core::rpc::{LiveCell, Rpc, RpcError};

pub struct PlaceholderRpc {
    pub node_url: String,
    pub indexer_url: String,
}

impl Rpc for PlaceholderRpc {
    fn list_live_intentions(
        &self,
        _cid_code_hash: &[u8; 32],
        _hash_type: ScriptHashType,
    ) -> Result<Vec<LiveCell>, RpcError> {
        Err(RpcError::Unavailable(format!(
            "v0 placeholder; no live JSON-RPC (configured node {})",
            self.node_url
        )))
    }

    fn get_live_process_by_type_hash(
        &self,
        _process_type_hash: &[u8; 32],
    ) -> Result<Option<LiveCell>, RpcError> {
        Err(RpcError::Unavailable(format!(
            "v0 placeholder; no live JSON-RPC (configured indexer {})",
            self.indexer_url
        )))
    }

    fn submit_transaction(&self, _tx: &TransactionView) -> Result<Byte32, RpcError> {
        Err(RpcError::Unavailable(
            "v0 placeholder; no live JSON-RPC submit".into(),
        ))
    }
}
```

`crates/cid-backend-daemon/src/main.rs`:

```rust
mod config;
mod placeholder_rpc;

use cid_backend_core::digest_loop::{DigestLoop, RealClock};
use cid_backend_core::process::ProcessResolver;
use cid_backend_core::ssri::EmbeddedSsri;
use config::DaemonConfig;
use placeholder_rpc::PlaceholderRpc;

fn main() {
    env_logger::init();
    let config = match DaemonConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cid-backend-daemon invalid config: {e}");
            std::process::exit(1);
        }
    };
    let rpc = PlaceholderRpc {
        node_url: config.node_url.clone(),
        indexer_url: config.indexer_url.clone(),
    };
    let loop_config = config.to_loop_config();
    let mut digest = DigestLoop::new(
        rpc,
        RealClock,
        EmbeddedSsri,
        loop_config,
        ProcessResolver::new(),
    );
    digest.run();
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```
cargo test -p cid-backend-daemon
cargo test -p cid-backend-tests
cargo test -p cid-backend-core
```

Expected: PASS. Do not run the daemon binary against a node. `from_vars` does not touch process environment.

- [ ] **Step 5: Commit**

```bash
git add crates/cid-backend-daemon Cargo.lock
git commit -m "feat: thin cid-backend-daemon with env-only config and placeholder RPC"
```

---

### Task 13: Docs / README

**Files:**
- Modify: `README.md`
- Modify: `tests/fixtures/README.md` only if the build steps drifted (keep the Task 4 copy if still accurate)
- Test: none (markdown). Verify by reading the files; run `cargo test -p cid-backend-core && cargo test -p cid-backend-daemon && cargo test -p cid-backend-tests` as a final non-network check.

**Interfaces:**
- Consumes: env var names from Task 12; git rev from this plan; design spec path
- Produces: operator-facing README that points at design + this plan, lists env vars, explains FakeRpc-only tests, and how to build `cid_process_example`

- [ ] **Step 1: Write the failing check**

There is no code test. Replace `README.md` contents; the previous README still says “Implementation has not started” — that sentence must disappear.

- [ ] **Step 2: Confirm the old README is stale**

Run: `rg -n "Implementation has not started" README.md`

Expected: a match (current text). After Step 3, that string is gone.

- [ ] **Step 3: Write the README**

```markdown
# ckb-cid-backend-server

Public CID digest daemon: scan Intention cells (indexer `asc`), run Process SSRI in-process via `cid-process-executor`, submit execution transactions (`digest_ok` / `digest_incompatible`).

This repository is independent of [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher) (protocol, lock, executor, `cid_process_example`). Link them with git dependencies and on-chain conventions — do not merge the monorepos.

## Docs

- Design: [`docs/superpowers/specs/2026-11-05-cid-backend-server-design.md`](docs/superpowers/specs/2026-11-05-cid-backend-server-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-11-05-cid-backend-server.md`](docs/superpowers/plans/2026-11-05-cid-backend-server.md)

v0 is the digest loop only. No HTTP API, metrics, admin, or disk state. Restart = full rescan.

## Workspace

- `crates/cid-backend-core` — scan / process / ssri / assemble / submit / `digest_loop`
- `crates/cid-backend-daemon` — env config + placeholder RPC + `loop.run()`
- `tests/` (`cid-backend-tests`) — FakeRpc verticals; no live network

Upstream git pin: `d9ee77805f7ac930ab149648bed70f484a9886de` (`cid-process-executor`, `cid-protocol`).

## Daemon env vars

| Variable | Required | Default |
|----------|----------|---------|
| `CID_CKB_RPC_URL` | yes | — |
| `CID_INDEXER_URL` | no | same as `CID_CKB_RPC_URL` |
| `CID_LOCK_CODE_HASH` | yes | 32-byte hex (`0x` optional) |
| `CID_LOCK_HASH_TYPE` | no | `type` |
| `CID_POLL_INTERVAL_SECS` | yes | integer ≥ 1 |

The daemon does **not** install a Process-binary fallback. Production binaries come from live Process cell data. v0 ships `PlaceholderRpc` (methods return unavailable); FakeRpc covers tests.

```bash
cargo run -p cid-backend-daemon
```

Illegal config (missing URL or CID lock code hash) exits immediately.

## Tests

```bash
cargo test -p cid-backend-core
cargo test -p cid-backend-daemon
cargo test -p cid-backend-tests
```

Do not point these tests at a CKB node. Verticals: n=1 `digest_ok`, batch bounds, Incompatible refund, submit conflict retry.

Optional real guest for `EmbeddedSsri` tests:

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
# or: export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example
```

See `tests/fixtures/README.md`.
```

- [ ] **Step 4: Re-run the suite (still FakeRpc only)**

Run:

```
cargo test -p cid-backend-core
cargo test -p cid-backend-daemon
cargo test -p cid-backend-tests
```

Expected: PASS. `rg "Implementation has not started" README.md` matches nothing.

- [ ] **Step 5: Commit**

```bash
git add README.md tests/fixtures/README.md
git commit -m "docs: README for v0 digest daemon, env vars, and process fixture"
```

---

## Spec coverage map

| Spec item | Task |
|-----------|------|
| §2.1 workspace core + daemon + tests/ | 1, 12 |
| §2.1 digest loop scan→bucket→resolve→SSRI→N→assemble→submit | 8 |
| §2.1 digest_ok destroy Intention, Process 1in/1out, no fee input | 5, 8 |
| §2.1 Incompatible → User Lock refund; unbuildable log+skip | 6, 8, 10 |
| §2.1 RPC Trait + FakeRpc | 1 |
| §2.1 Process binary live cell; test fallback | 3, 12 |
| D1 independent repo | 1 (own workspace; git dep only) |
| D2 digest loop only / no HTTP | 8, 12, 13 |
| D3 in-process ProcessExecutor | 4, 12 (`EmbeddedSsri`) |
| D4 prod live data; tests fallback; daemon never installs fallback | 3, 12 |
| D5 Approach A library + thin bin + FakeRpc | 1, 12 |
| D6 memory only; restart = rescan | 7 (`SubmitState`), 8 (`LoopState`), 11 |
| D7 digest_ok no User Lock input | 5, 8 |
| D8 no fee/sponsor input; shortfall log+skip | 5, 8 |
| D9 Rust workspace; git pin executor | 1, 4 |
| §7.1 tick algorithm + multi-batch + first-seen bucket order | 2, 8, 9 |
| §7.2 bad args skip | 2 |
| §7.2 no live Process skip | 3, 8 |
| §7.2 Incompatible refund / unbuildable skip | 6, 8, 10 |
| §7.2 VM/invalid Unusable skip, no refund | 4 (`map_executor_error`), 8 (`tick_unusable_ssri_skips_without_refund_or_submit`) |
| §7.2 `< min` skip; `> max` take first max asc | 9 |
| §7.2 conflict memory + next tick retry | 7, 11 |
| §7.2 capacity / no fee input skip | 5, 8 |
| §7.2 RPC unavailable log, next tick | 8, 12 (`PlaceholderRpc`) |
| §7.3 timeout_burn not constructed | 5, 6, 8, 10 |
| §8 in-flight / failed memory; no WAL | 7, 11 |
| §9.1 vertical n=1 | 8 |
| §9.1 vertical batch bounds | 9 |
| §9.1 vertical Incompatible | 10 |
| §9.1 vertical conflict retry | 11 |
| §9.2 scan unit tests | 2 |
| §9.2 assemble unit tests | 5, 6 |
| §6 daemon env only | 12 |
| Process recreate copy live if assemble omits it | 5 |
| No live-network tests | 1–13 (`PlaceholderRpc` / FakeRpc) |

Rejected routes from the spec stay rejected: no HTTP-first (Approach B), no external ssri-server (Approach C), no persistent cursor.
