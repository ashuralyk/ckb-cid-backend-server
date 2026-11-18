# Local CKB dev-chain closure (phase 2)

Isolated **dev** chain only. No testnet, mainnet, or public RPC.

## Pins

| Component | Pin |
|-----------|-----|
| CKB node | `v0.119.0` (matches workspace `ckb-types` 0.119) |
| Dispatcher / Process example / CID lock | `d9ee77805f7ac930ab149648bed70f484a9886de` |

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

The script inits `dev` with genesis miner lock, sets **`min_fee_rate = 0`** (D8: daemon never adds a fee input; without this the node rejects zero-fee txs), enables the Indexer RPC module, builds RISC-V guests, starts `ckb run` + `ckb miner` on `127.0.0.1:8114`, exports `CID_CKB_RPC_URL`, `CID_PROCESS_EXAMPLE_PATH`, `CID_LOCK_ELF_PATH`, `CID_ALWAYS_SUCCESS_PATH`, `CID_LOCAL_CHAIN_E2E=1`, then runs the ignored E2E. Setting only `CID_LOCAL_CHAIN_E2E=1` and invoking `cargo test` on a clean machine is **not** equivalent.

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
| `CID_PROCESS_CODE_HASH` | optional; needed at run | Process type script **code_hash** used as indexer type prefix (must match that type's `hash_type`; data1 local-chain uses ELF data hash). CID args store `calc_script_hash`, not this prefix |
| `CID_EXTRA_CELL_DEPS` | optional | Extra `tx_hash:index` list |
| `CID_HEALTH_BIND` | optional | Opt-in health/stats HTTP (`docs/observability.md`) |
| `CID_CONFIRMATION_DEPTH` | optional | Blocks before a committed execution tx is settled (default 24; see `docs/confirmation.md`) |
| `CID_LOG_FORMAT` / `RUST_LOG` | optional | Structured logs (`docs/observability.md`) |
| `CID_CONFIG` | optional | TOML path (`--config` wins). See `examples/cid.network.toml.example`. No keys. |

Production/dev operators may put the same public RPC / Type-ID / static cell-dep values in a TOML file. `network.genesis_hash` is declared identity only unless `validate_genesis = true`.

**Local-chain fixture stays static `data1` cell deps** (CID lock ELF / Process ELF / always-success outpoints). Type-ID discovery (`*_TYPE_ID_ARGS`) is for `hash_type=type` networks; it is optional here and does not replace the local data1 path. On Type-ID networks, code CellDeps are resolved each tick from configured `type_id_args` (exact TYPE_ID type script); static `[[cell_deps]]` remain for extras.

`assemble` uses the tick's merged CellDeps (Type-ID resolved + static). `CkbRpc` uses `CID_PROCESS_CODE_HASH` (and auto-derived Process code `type_id` when configured) as a type-script prefix, then filters `calc_script_hash`.

State is memory-only. Restarting the daemon drops in-flight occupancy and full-rescans indexer `asc`.

## Essential local-chain prerequisites (not phase 3)

- **`min_fee_rate = 0`** on the node. The digest tx has no extra fee input (D8).
- Process cell **data is the RISC-V ELF** so `ProcessResolver` (D4) can SSRI it; the same ELF is also a code cell so the Process **type** script can execute (`hash_type = data1`).
- Process **lock** is always-success so the daemon does not hold secp keys (no post-Intention signatures).
- Lookup of live Process cells uses `CID_PROCESS_CODE_HASH` as the Process type's `code_hash` indexer prefix, then filters `calc_script_hash`. On this local chain that prefix is the Process ELF data hash because the type uses `hash_type = data1`.
