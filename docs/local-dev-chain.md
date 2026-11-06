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

```bash
./scripts/local-dev-chain.sh
```

The script downloads pinned `ckb`, inits `dev` with genesis miner lock, sets **`min_fee_rate = 0`** (D8: daemon never adds a fee input; without this the node rejects zero-fee txs), enables the Indexer RPC module, builds RISC-V guests, starts `ckb run` + `ckb miner` on `127.0.0.1:8114`, then runs:

```bash
CID_LOCAL_CHAIN_E2E=1 cargo test -p cid-backend-daemon --test local_chain_e2e -- --ignored --nocapture
```

Success prints `local-chain execution tx committed:` and `local-chain closure ok:`.

## Daemon env (production / local)

| Variable | Required | Meaning |
|----------|----------|---------|
| `CID_CKB_RPC_URL` | yes | Node JSON-RPC |
| `CID_INDEXER_URL` | no | Indexer JSON-RPC (default: node URL) |
| `CID_LOCK_CODE_HASH` | yes | CID lock `code_hash` |
| `CID_LOCK_HASH_TYPE` | no | `type` (default) / `data` / `data1` / `data2` |
| `CID_POLL_INTERVAL_SECS` | yes | ≥ 1 |
| `CID_LOCK_CELL_DEP` | local-chain | `<tx_hash>:<index>` CID lock code cell |
| `CID_PROCESS_CODE_CELL_DEP` | local-chain | Process ELF code cell |
| `CID_PROCESS_LOCK_CELL_DEP` | local-chain | always-success code cell |
| `CID_PROCESS_CODE_HASH` | local-chain | Process ELF **data hash** used as indexer type-script prefix (CID args store `calc_script_hash`, not code hash) |
| `CID_EXTRA_CELL_DEPS` | no | Extra `tx_hash:index` list |

State is memory-only. Restarting the daemon drops in-flight occupancy and full-rescans indexer `asc`.

## Essential local-chain prerequisites (not phase 3)

- **`min_fee_rate = 0`** on the node. The digest tx has no extra fee input (D8).
- Process cell **data is the RISC-V ELF** so `ProcessResolver` (D4) can SSRI it; the same ELF is also a code cell so the Process **type** script can execute (`hash_type = data1`).
- Process **lock** is always-success so the daemon does not hold secp keys (no post-Intention signatures).
- Lookup of live Process cells uses `CID_PROCESS_CODE_HASH` + indexer type prefix, then filters `calc_script_hash`.
