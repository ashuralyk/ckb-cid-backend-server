# Local CKB dev-chain closure (phase 2)

Isolated **dev** chain only. No testnet, mainnet, or public RPC.

## Pins

| Component | Pin |
|-----------|-----|
| CKB node | `v0.119.0` (matches workspace `ckb-types` 0.119) |
| Dispatcher / Process example / CID lock | `b2d01e0a5a9760c8ee627633365aa98dd7a99859` |

## What is HTTP-mock vs real chain

| Coverage | Where | Network |
|----------|-------|---------|
| Adapter mapping: `get_cells` asc, Process-by-script-hash, Conflict, Pending/Dropped, indexer lag, restart rescan | `cid-backend-daemon` unit tests | loopback JSON-RPC mock |
| FakeRpc digest verticals | `cid-backend-tests` | none |
| Real SSRI guest (`Process.filter`, `Process.batch`, and `Process.assemble` on ckb-vm) | `cid-backend-core` ignored test; CI `--include-ignored` | none |
| **scan → resolve → filter → assemble → submit → committed execution tx** | `scripts/local-dev-chain.sh` + ignored `local_chain_e2e` | **local `ckb` dev node only** |

## Run

Default auto-download is **Linux x86_64** (`ckb_${version}_x86_64-unknown-linux-gnu.tar.gz`). Host tools: `curl`, `tar`, `python3`, `git`, `make`, plus `clang-18` (or `CLANG`) to build guests. If `$CKB_DIR/ckb` is already executable (default `CKB_DIR=.local-ckb`), the download is skipped — that is how macOS / ARM Linux should supply a matching `ckb` v0.119.0.

```bash
./scripts/local-dev-chain.sh
```

The script inits `dev` with genesis miner lock, sets **`min_fee_rate = 0`** (D8: no fee input; `digest_ok` pays from Process recreate; Incompatible refunds default to exact/zero-fee under an empty bounded-refund allowlist — required here so local refunds clear against the pinned lock), enables the Indexer RPC module, builds RISC-V guests, starts `ckb run` + `ckb miner` on `127.0.0.1:8114`, exports `CID_CKB_RPC_URL`, `CID_PROCESS_EXAMPLE_PATH`, `CID_LOCK_ELF_PATH`, `CID_ALWAYS_SUCCESS_PATH`, `CID_LOCAL_CHAIN_E2E=1`, then runs the ignored E2E. Setting only `CID_LOCAL_CHAIN_E2E=1` and invoking `cargo test` on a clean machine is **not** equivalent.

Before signing setup, the ignored fixture requires a numeric loopback HTTP RPC URL and checks that the node reports `ckb_dev`. The fixture disables HTTP redirects and proxies. Success confirms that the committed transaction spends the original Intention and produces the expected beneficiary output.

## Daemon env (production / local)

| Variable | Config parser | Meaning |
|----------|---------------|---------|
| `CID_CKB_RPC_URL` | required | Node JSON-RPC |
| `CID_INDEXER_URL` | optional | Indexer JSON-RPC (default: node URL) |
| `CID_LOCK_CODE_HASH` | required | CID lock `code_hash` |
| `CID_LOCK_HASH_TYPE` | optional | `type` (default), required for the primary CID lock |
| `CID_POLL_INTERVAL_SECS` | required | ≥ 1 |
| `CID_LOCK_CELL_DEP` | optional; needed at run | `<tx_hash>:<index>` CID lock code cell (`DepType::Code`) |
| `CID_PROCESS_CODE_CELL_DEP` | optional; needed at run | Process type code cell |
| `CID_PROCESS_LOCK_CELL_DEP` | optional; needed at run | Process lock code cell (local-chain: always-success) |
| `CID_PROCESS_CODE_HASH` | deprecated | Ignored; register Processes via registry API |
| `CID_EXTRA_CELL_DEPS` | optional | Extra `tx_hash:index` list |
| `CID_HEALTH_BIND` | optional | Opt-in health/stats HTTP (`docs/observability.md`) |
| `CID_CONFIRMATION_DEPTH` | optional | Blocks before a committed execution tx is settled (default 24; see `docs/confirmation.md`) |
| `CID_FEE_RATE` | optional | Shannons/KW for bounded refunds (default 1000) |
| `CID_BOUNDED_REFUND_LOCKS` | optional | Comma list `0xhash:hash_type` of audited locks allowed to use ≤ `MAX_REFUND_FEE` refund haircuts (default **empty** / legacy exact). Never undeployed candidates. |
| `CID_LEGACY_SCAN_LOCKS` | optional | Scan-only CID lock identities, including legacy `data`, `data1`, and `data2` locks. |
| `CID_LOG_FORMAT` / `RUST_LOG` | optional | Structured logs (`docs/observability.md`) |
| `CID_CONFIG` | optional | TOML path (`--config` wins). See `examples/cid.network.toml.example`. No keys. |

Production/dev operators may put the same public RPC / Type-ID / static cell-dep values in a TOML file. `network.genesis_hash` is declared identity only unless `validate_genesis = true`.

The local fixture creates TYPE_ID code cells for CID and the example Process. It registers the Process state in memory. The Process state has a separate TYPE_ID and its lock points at the Process code cell, with args that bind the CID code Type ID. The always-success beneficiary remains a static Data1 dependency.

The daemon resolves the code CellDeps at every tick and uses the live Process code binary for filter, batch bounds, and assembly.

State is memory-only. Restarting the daemon drops in-flight occupancy and full-rescans indexer `asc`.

The local node uses `min_fee_rate = 0`. The Process guest pays the execution fee from its recreated state cell. The fixture uses a 600 CKB Intention, the example Process minimum for CKB deposits.
