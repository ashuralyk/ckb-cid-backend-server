# ckb-cid-backend-server

Public digest daemon for CKB [Common Intention Dispatch (CID)](https://github.com/ashuralyk/ckb-common-intention-dispatcher): scan live Intention cells from the indexer in `asc` order, run SSRI in-process via `cid-process-executor` (ckb-vm) against the live Process binary, assemble and submit Execution Tx.

This repository is **independent** of [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher) (protocol, CID lock, executor, `cid_process_example`). Link them with git dependencies and on-chain conventions — do not merge the monorepos.

The current implementation is the digest loop (v0) plus optional health/metrics HTTP. No admin API, no disk state. Restart = full indexer rescan.

## Docs

- Confirmation depth / reorg / retry: [`docs/confirmation.md`](docs/confirmation.md)
- Logs, health, metrics: [`docs/observability.md`](docs/observability.md)
- Isolated local **dev** chain closure: [`docs/local-dev-chain.md`](docs/local-dev-chain.md)
- Design (historical decisions; some v0 scope was filled in later phases — the code wins): [`docs/superpowers/specs/2026-11-05-cid-backend-server-design.md`](docs/superpowers/specs/2026-11-05-cid-backend-server-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-11-05-cid-backend-server.md`](docs/superpowers/plans/2026-11-05-cid-backend-server.md)
- Real SSRI fixture: [`tests/fixtures/README.md`](tests/fixtures/README.md)

---

## Purpose

The daemon continuously digests CID Intentions:

1. Fetch live Intentions from the indexer by CID lock `code_hash` / `hash_type` (`get_cells`, `order=asc`, paginate until exhausted).
2. Decode lock args to get `process_type_hash` and bucket by that hash.
3. Resolve the matching live Process cell: production uses only the RISC-V **ELF in cell data** (the daemon does **not** install a Process-binary fallback).
4. In-process SSRI: `Process.batch` yields `[min, max]`; `Process.assemble` yields a partial tx.
5. Assemble an Execution Tx and `send_transaction`:
   - **`digest_ok`**: destroy this batch of Intentions; Process **1 in / 1 out** recreate. No extra fee / sponsor input; User Lock is not placed in inputs (no signature after Intention creation). Skip on capacity shortfall.
   - **`digest_incompatible`**: when SSRI returns `Incompatible(code)`, refund Intention capacity to the **User Lock** in args (does not spend Process). If the refund cannot be built, log and skip.
6. After a successful submit, occupy spent inputs (and the `digest_ok` Process recreate) until confirmation depth is met.

Out of scope: timeout-burn proxy, multi-sig fee sponsor, official ssri-server, multi-instance coordination, testnet/mainnet ops scripts.

Upstream git pin (`cid-process-executor` / `cid-protocol`): `d9ee77805f7ac930ab149648bed70f484a9886de`.

---

## How to run

### Prerequisites

- Rust **stable** ([`rust-toolchain.toml`](rust-toolchain.toml)), edition 2021; workspace `ckb-types` 0.119.
- Running the daemon / FakeRpc tests: `cargo` only.
- Running the **real SSRI guest** (ckb-vm `Process.batch` / `Process.assemble`): `clang-18` (or `CLANG=clang`) + `riscv64imac-unknown-none-elf`, via [`scripts/build-cid-process-example.sh`](scripts/build-cid-process-example.sh). The ELF is **not** committed.
- Isolated local dev-chain closure: also pinned `ckb` v0.119.0 (the script downloads it). **Not** testnet / mainnet. See [`docs/local-dev-chain.md`](docs/local-dev-chain.md).

### Build

```bash
cargo build --locked -p cid-backend-daemon
```

### Configuration (env vars the daemon actually reads)

`DaemonConfig::from_env()` ([`crates/cid-backend-daemon/src/config.rs`](crates/cid-backend-daemon/src/config.rs)). Missing required values or illegal config logs `config.load_failed` and **exits 1**.

| Variable | Required | Default / meaning |
|----------|----------|-------------------|
| `CID_CKB_RPC_URL` | yes | Node JSON-RPC |
| `CID_INDEXER_URL` | no | Indexer JSON-RPC; empty uses `CID_CKB_RPC_URL` |
| `CID_LOCK_CODE_HASH` | yes | CID lock `code_hash`, 32-byte hex (`0x` optional) |
| `CID_LOCK_HASH_TYPE` | no | `type` (default) / `data` / `data1` / `data2` |
| `CID_POLL_INTERVAL_SECS` | yes | Seconds between ticks, integer **≥ 1** |
| `CID_LOCK_CELL_DEP` | local-chain | `<tx_hash>:<index>` CID lock code cell (`DepType::Code`) |
| `CID_PROCESS_CODE_CELL_DEP` | local-chain | Process ELF code cell |
| `CID_PROCESS_LOCK_CELL_DEP` | local-chain | always-success code cell |
| `CID_EXTRA_CELL_DEPS` | no | Extra `<tx_hash>:<index>` list, comma-separated |
| `CID_PROCESS_CODE_HASH` | local-chain | Process ELF **data hash**, used as indexer type-script prefix; CID args store `calc_script_hash`, not code hash. Comma-separated list allowed |
| `CID_HEALTH_BIND` | no | `host:port` (e.g. `127.0.0.1:9100`); unset or empty **disables** HTTP |
| `CID_CONFIRMATION_DEPTH` | no | Blocks after a committed Execution Tx before settle; default **24**, must be **≥ 1** |
| `CID_LOG_FORMAT` | no | `json` (default) or `pretty` (`text` is an alias for pretty) |
| `RUST_LOG` | no | `tracing` EnvFilter, default `info` |

Cell-dep format: `<tx_hash>:<index>`; index may be decimal or `0x` hex. The four cell-dep variables are merged into `LoopConfig.extra_cell_deps`.

The daemon does **not** read `CID_PROCESS_EXAMPLE_PATH` / `CID_LOCAL_CHAIN_E2E` (test fixture / local-chain E2E switch). Production Process binaries come only from live Process cell data. `CkbRpc` talks JSON-RPC to `CID_CKB_RPC_URL` / `CID_INDEXER_URL`; tests use in-memory `FakeRpc`.

### Start the daemon

```bash
export CID_CKB_RPC_URL=http://127.0.0.1:8114
export CID_LOCK_CODE_HASH=   # 32-byte hex
export CID_POLL_INTERVAL_SECS=5
# local chain also needs CID_LOCK_CELL_DEP / CID_PROCESS_* / CID_PROCESS_CODE_HASH; see docs/local-dev-chain.md
cargo run -p cid-backend-daemon
```

`main` sleeps `poll_interval` after each `tick()`. Illegal config exits immediately.

### Isolated local dev chain

Do not use public RPC. The script downloads pinned `ckb` v0.119.0, sets `min_fee_rate` to **0** (digest tx has no fee input), enables the Indexer module, builds RISC-V guests, starts `ckb run` + `ckb miner` on `127.0.0.1:8114`, then runs the ignored E2E:

```bash
./scripts/local-dev-chain.sh
```

Equivalent to:

```bash
CID_LOCAL_CHAIN_E2E=1 cargo test -p cid-backend-daemon --test local_chain_e2e -- --ignored --nocapture
```

Success prints `local-chain execution tx committed:` and `local-chain closure ok:`. Details and local-chain prerequisites (Process data must be an ELF, Process lock is always-success, no post-Intention signature) are in [`docs/local-dev-chain.md`](docs/local-dev-chain.md).

### Health / metrics

Unset `CID_HEALTH_BIND` means no listener. When enabled it does **not** block the digest loop.

```bash
export CID_HEALTH_BIND=127.0.0.1:9100
cargo run -p cid-backend-daemon
curl -s http://127.0.0.1:9100/healthz    # liveness: 200 while the process is up
curl -s http://127.0.0.1:9100/readyz    # 200 after a successful indexer scan, else 503
curl -s http://127.0.0.1:9100/stats     # JSON counters
curl -s http://127.0.0.1:9100/metrics   # Prometheus text
```

`127.0.0.1:0` binds an ephemeral port; the daemon logs `health.listen` with the bound address. Limits (at most 32 workers, 2s header-read deadline, no request body) are in [`docs/observability.md`](docs/observability.md).

### Tests

Default `cargo test` does **not** start a CKB node. CI (`.github/workflows/ci.yml`) on `push` / `pull_request` / `workflow_dispatch` builds `cid_process_example`, then runs these three (match them locally):

```bash
cargo test --locked -p cid-backend-core -- --include-ignored --nocapture
cargo test --locked -p cid-backend-daemon -- --nocapture
cargo test --locked -p cid-backend-tests -- --nocapture
```

| Suite | Coverage |
|-------|----------|
| `cid-backend-core --include-ignored` | Library unit tests plus the **must-execute** real SSRI guest (`real_example_binary_batch_bounds_and_assemble`). Missing ELF panics; a skip that still prints `ok` is not allowed. |
| `cid-backend-daemon` | HTTP mock: adapter mapping (including `PoolIsFull` retry vs verify/malformed reject, unknown committed height vs explicit genesis 0), indexer pagination, Pending/Dropped, health limits. `local_chain_e2e` is **ignored**. |
| `cid-backend-tests` | FakeRpc verticals: n=1 `digest_ok`, batch bounds, Incompatible refund, conflict retry, confirmation depth, reorg, Dropped/Unavailable, `SubmitRejected` combo isolation and starvation regressions. No network. |

Default local `cargo test -p cid-backend-core` **ignores** `real_example_binary_batch_bounds_and_assemble` (fixture is not in git). That `ignored` count is **not** a real-SSRI pass. To run the guest locally:

```bash
./scripts/build-cid-process-example.sh
cargo test --locked -p cid-backend-core -- --include-ignored --nocapture
```

Lookup order: `CID_PROCESS_EXAMPLE_PATH`, then `tests/fixtures/cid_process_example`. FakeRpc verticals use `ScriptedSsri` and do not need the ELF.

The only real local-chain evidence is ignored `local_chain_e2e` (`./scripts/local-dev-chain.sh` above).

---

## Architecture / design

### Crate layout (Approach A)

| Crate | Role |
|-------|------|
| `crates/cid-backend-core` | Digest pipeline library: `scan` / `process` / `ssri` / `assemble` / `submit` / `digest_loop`. `Rpc` is a trait. |
| `crates/cid-backend-daemon` | Thin layer: env config, `CkbRpc` (CKB JSON-RPC + Indexer), optional health HTTP, `tick` + sleep in `main`. Business rules stay in core. |
| `tests/` (`cid-backend-tests`) | FakeRpc verticals; depends on core only. |

The SSRI host is in-process `EmbeddedSsri` (`cid-process-executor`), not an external ssri-server.

### Digest loop

```mermaid
flowchart TD
  start["tick.start"] --> scan["scan_intentions: indexer get_cells ASC"]
  scan -->|Unavailable| endTick["tick.end / keep occupancy"]
  scan --> tip["refresh_tip + reconcile_pending"]
  tip --> filter["filter in_flight / rejected"]
  filter --> bucket["bucket by process_type_hash"]
  bucket --> resolve["ProcessResolver: live cell data = ELF"]
  resolve --> bounds["SSRI Process.batch → min,max"]
  bounds --> isolate["isolate_window legal sub-batches"]
  isolate --> peek["lookup rejected_combos fingerprint"]
  peek -->|cache hit| split["CachedRejected: split window, no send"]
  peek --> assemble["SSRI Process.assemble"]
  assemble -->|ok| dok["assemble_digest_ok<br/>Intention + Process 1in/1out"]
  assemble -->|Incompatible| dinc["assemble_digest_incompatible<br/>User Lock refund, do not spend Process"]
  assemble -->|Unusable| skip["log + skip"]
  dok --> send["send_transaction"]
  dinc --> send
  send -->|ok| pending["pending occupancy<br/>wait confirmation_depth"]
  send -->|SubmitRejected| combo["record combo fingerprint and keep splitting"]
  send -->|Conflict / Unavailable| retry["no occupancy this tick; retry next tick"]
  split --> isolate
  combo --> isolate
```

Each tick ([`digest_loop.rs`](crates/cid-backend-core/src/digest_loop.rs)):

1. Scan live Intentions; skip cells whose lock args fail to decode.
2. Refresh tip; ask `get_transaction_status` for txs in `pending`. After `Committed`, re-read tip: if the tip rewound, header/tip is unavailable, or `tip < commit_height`, do **not** settle this tick (see [`docs/confirmation.md`](docs/confirmation.md)).
3. Bucket by Process; `isolate_window` splits into legal `[min, max]` sub-batches.
4. `digest_ok` occupies the Process recreate until settle; User Lock refunds in the same tick may still submit.

### Memory-only state and restart

All bookkeeping is process memory: `in_flight` / `pending` / `rejected_combos` / `isolate_resume` / `isolate_expanded` / health counters. No SQLite / Redis / cursor file. It is dropped on exit; the next tick is a full indexer rescan from the head. After restart, a still-mempool tx may be submitted again (D6); already-spent cells surface as `Conflict`.

### Confirmation depth and reorg

`Committed` is not settled on first sight. Occupancy is held until:

```
confirmations = 0                      if tip < commit_block
              = tip - commit_block + 1  otherwise
confirmations >= CID_CONFIRMATION_DEPTH
```

Default is 24 blocks (~3 minutes at ~8s). FakeRpc and the local-chain E2E use `1`. A tip rewind logs `reorg.tip_rewind` and drops the confirmation count. A confirming tx that becomes `Dropped` releases occupancy and retries next tick if the cells are still live. A reorg deeper than `CID_CONFIRMATION_DEPTH` **after** settlement is accepted residual risk.

### Retry vs permanent failure

`CkbRpc` keeps the JSON-RPC `error.code` from `send_transaction`:

| Outcome | Retry? | Occupancy |
|---------|--------|-----------|
| `Dropped` | yes (if cells still live) | released |
| RPC/indexer `Unavailable` (including `-1106` PoolIsFull, `-1105` ancestors, `-1104` min fee, unclassified) | yes | held if already pending; not taken on a failed submit |
| `Conflict` (`-301`, `-1107`, unknown-input, etc.) | yes | not taken |
| `SubmitRejected` (`-302` / `-1102` / `-1108`) | isolate that **combo**; do not send it again this process lifetime | not taken; Process is not written to `rejected` |
| SSRI `Incompatible` | User Lock refund | does not spend Process |

Missing `block_hash` / `get_header` returning null is `Unavailable`, not fabricated genesis height 0; explicit `block_number: 0x0` is `Committed{0}`.

### Combo isolation and budgets

`SubmitRejected` records a fingerprint of Intention outpoints **in order**; `digest_ok` also includes the Process outpoint, refunds omit it. The same combo is not sent again until restart (or the Process outpoint changes). The window is split into legal `[min, max]` sub-batches (shrink prefix + drop-first) so a valid sibling can still submit when `min = 1`.

- At most **64** new `send_transaction` or assemble-skip attempts per bucket per tick. Hits on `rejected_combos` do **not** consume that budget.
- A separate cap of **256** cached splits per bucket per tick bounds exponential diamond expansion of already-rejected windows.
- Each window fingerprint is expanded at most once per isolate sweep (`isolate_expanded`). When a budget cuts the tick, that set is kept with the resume cursor so the next tick continues remaining unique windows instead of re-walking the tree from the ASC head.
- Uncached no-send skips (`RefundUnbuildable`, capacity shortfall, SSRI Unusable) count toward the 64 work bound and are **not** written to `rejected` / `rejected_combos`.
- When `min > 1` and the window cannot split further, members are not written wholesale into `rejected`; a valid B that only ever appears next to a failing A may stay unsubmitted — accepted; blacklisting B would be worse.

A singleton `{A, Process}` failure does **not** prove A is permanently invalid. Details: [`docs/confirmation.md`](docs/confirmation.md).

### Observability

Core uses `tracing` (target `cid_backend`, stable field `event`). The daemon uses `tracing-subscriber` JSON/pretty and bridges remaining `log::` calls.

Common `event` values: `tick.start` / `tick.end` (with `scanned` / `skipped` / `submitted` / `conflicts` / `scan_ok` / `isolate_windows`), `scan.unavailable`, `submit.ok` / `submit.conflict` / `submit.rejected` / `submit.rejected_combo`, `pending.committed` / `pending.settled` / `pending.dropped` / `pending.reorg` / `pending.tip_rewind`, `reorg.tip_rewind`, `config.load_failed`. Full tables: [`docs/observability.md`](docs/observability.md) and the Logs section of [`docs/confirmation.md`](docs/confirmation.md).
