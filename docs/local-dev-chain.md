# Local CKB dev-chain closure (phase 2)

Isolated **dev** chain only. No testnet, mainnet, or public RPC.

## Pins

| Component | Pin |
|-----------|-----|
| CKB node | `v0.119.0` (matches workspace `ckb-types` 0.119) |
| Dispatcher / Process example / CID lock | `5ea581fad5922ad0e4104d99757be143ee6706f8` |

## What is HTTP-mock vs real chain

| Coverage | Where | Network |
|----------|-------|---------|
| Adapter mapping: `get_cells` asc, Process-by-script-hash, Conflict, Pending/Dropped, indexer lag, restart rescan | `cid-backend-daemon` unit tests | loopback JSON-RPC mock |
| FakeRpc digest verticals | `cid-backend-tests` | none |
| Real SSRI guest (`Process.batch` / `assemble` on ckb-vm) | `cid-backend-core` ignored test; CI `--include-ignored` | none |
| **scan → resolve → SSRI assemble → submit → committed execution tx** | `scripts/local-dev-chain.sh` + ignored `local_chain_e2e` | **local `ckb` dev node only** |

## Run

Default auto-download is **Linux x86_64** (`ckb_${version}_x86_64-unknown-linux-gnu.tar.gz`). Host tools: `curl`, `tar`, `python3`, `git`, `make`, plus `clang-18` (or `CLANG`) to build guests. If `$CKB_DIR/ckb` is already executable (default `CKB_DIR=.local-ckb`), the download is skipped — that is how macOS / ARM Linux should supply a matching `ckb` v0.119.0.

```bash
./scripts/local-dev-chain.sh
```

The script inits `dev` with genesis miner lock, sets **`min_fee_rate = 0`** (D8: no fee input; `digest_ok` pays from Process recreate; Incompatible refunds default to exact/zero-fee under an empty bounded-refund allowlist — required here so local refunds clear against the pinned lock), enables the Indexer RPC module, builds RISC-V guests, starts `ckb run` + `ckb miner` on `127.0.0.1:8114`, exports `CID_CKB_RPC_URL`, `CID_PROCESS_EXAMPLE_PATH`, `CID_LOCK_ELF_PATH`, `CID_ALWAYS_SUCCESS_PATH`, `CID_LOCAL_CHAIN_E2E=1`, then runs the ignored E2E. Setting only `CID_LOCAL_CHAIN_E2E=1` and invoking `cargo test` on a clean machine is **not** equivalent.

Success prints `local-chain execution tx committed:` and `local-chain closure ok:`.

## Daemon env (production / local)

| Variable | Config parser | Meaning |
|----------|---------------|---------|
| `CID_CKB_RPC_URL` | required | Node JSON-RPC |
| `CID_INDEXER_URL` | optional | Indexer JSON-RPC (default: node URL) |
| `CID_LOCK_CODE_HASH` | required | CID lock `code_hash` |
| `CID_LOCK_HASH_TYPE` | optional | `type` (default) / `data` / `data1` / `data2` |
| `CID_POLL_INTERVAL_SECS` | required | ≥ 1 |
| `CID_LOCK_CELL_DEP` | optional; needed at run | `<tx_hash>:<index>` CID lock code cell (`DepType::Code`) |
| `CID_PROCESS_CODE_CELL_DEP` | optional; needed at run | Process type code cell |
| `CID_PROCESS_LOCK_CELL_DEP` | optional; needed at run | Process lock code cell (local-chain: always-success) |
| `CID_PROCESS_CODE_HASH` | deprecated | Ignored; register Processes via registry API |
| `CID_EXTRA_CELL_DEPS` | optional | Extra `tx_hash:index` list |
| `CID_HEALTH_BIND` | optional | Opt-in health/stats HTTP (`docs/observability.md`) |
| `CID_CONFIRMATION_DEPTH` | optional | Blocks before a committed execution tx is settled (default 24; see `docs/confirmation.md`) |
| `CID_FEE_RATE` | optional | Shannons/KW for digest_ok (Process recreate) and bounded refunds (default 1000) |
| `CID_BOUNDED_REFUND_LOCKS` | optional | Comma list `0xhash:hash_type` of audited locks allowed to use ≤ `MAX_REFUND_FEE` refund haircuts (default **empty** / legacy exact). Never undeployed candidates. |
| `CID_LEGACY_SCAN_LOCKS` | optional | Extra CID lock identities to scan (exact refund unless also allowlisted). Scan-only; draining Data/Data1/Data2 after a Type-ID upgrade also needs a live matching old binary as an unlabeled static CellDep — see below and `docs/refund-fee-migration.md`. |
| `CID_LOG_FORMAT` / `RUST_LOG` | optional | Structured logs (`docs/observability.md`) |
| `CID_CONFIG` | optional | TOML path (`--config` wins). See `examples/cid.network.toml.example`. No keys. |

Production/dev operators may put the same public RPC / Type-ID / static cell-dep values in a TOML file. `network.genesis_hash` is declared identity only unless `validate_genesis = true`.

**Local-chain fixture stays static `data1` cell deps** (CID lock ELF / Process ELF / always-success outpoints) with `ProcessResolver` SSRI source = state-cell ELF. Explicit `code_hash` + `hash_type=data1` without `type_id_args` remains a supported **local/fixture** primary. Type-ID discovery (`*_TYPE_ID_ARGS`) is the **network default**: Intention scan uses the derived TYPE_ID type-script hash with `hash_type=type`, and code CellDeps are resolved each tick (bounded exact TYPE_ID type-script search with cell data). When Process Type-ID is set, SSRI binds to that code-cell ELF snapshot.

**Operator path after a Type-ID redeploy (not this local fixture):** list old Data / Data1 / Data2 Intentions under `legacy_scan_locks` / `CID_LEGACY_SCAN_LOCKS` so the indexer still finds them. That setting is **scan-only**. Assembly still attaches the **current** Type-ID code cell (new bytes) plus static extras; after an ELF change that Type-ID cell cannot locate a legacy data-hash lock. Keep a **live** cell whose **exact** old binary hashes to that `data_hash`, and attach it as an **unlabeled** `[[cell_deps]]` / `CID_EXTRA_CELL_DEPS` entry (do **not** use `label = "cid_lock"` — labeled role deps conflict with Type-ID for the same role). Full steps: [`docs/refund-fee-migration.md`](refund-fee-migration.md) (step 5). See `examples/cid.testnet.toml` / `examples/cid.network.toml.example` for Type-ID config shape.

`assemble` uses the tick's merged CellDeps (Type-ID resolved + static; same-outpoint dep_type conflicts fail closed). Confirmation/reorg reconciliation runs even if Type-ID refresh fails. Process state is resolved from the on-disk registry (exact TYPE_ID type script per entry).

State is memory-only. Restarting the daemon drops in-flight occupancy and full-rescans indexer `asc`.

## Essential local-chain prerequisites (not phase 3)

- **`min_fee_rate = 0`** on the node. No fee input (D8); `digest_ok` still overpays from Process when `loop.fee_rate > 0`. Local fixture keeps an empty bounded-refund allowlist so pinned lock rules still clear exact refunds.
- Process cell **data is the RISC-V ELF** so `ProcessResolver` (D4) can SSRI it; the same ELF is also a code cell so the Process **type** script can execute (`hash_type = data1`).
- Process **lock** is always-success so the daemon does not hold secp keys (no post-Intention signatures).
- Register each Process with `POST /v1/processes` after deploy; the worker discovers live state cells by exact TYPE_ID args from that entry.
