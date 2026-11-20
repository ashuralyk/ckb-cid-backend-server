# CID backend runbook (Process-as-lock)

Keyless digest daemon for CKB Common Intention Dispatcher after the
Process-as-lock / full-tx SSRI cutover.

## What the daemon does

1. Scan by the CID code Type ID and route with the shared Intention ownership helpers.
2. Resolve the live registered Process state and verify its CID binding. Execute `Process.filter` with the live Process code-cell ELF before batch selection. Submit accepted batches after validation and dry-run. Reasons 4–7 go through the refund builder. Malformed-cell reason 1 and reserved reasons 2 and 3 are logged and leave the affected cells live. Unknown reasons, invalid wire responses, and count mismatches defer that candidate window.
3. Track submitted inputs through confirmation depth and reconcile dropped transactions and reorgs.

Missing `Process.filter` defers the window by default. `loop.allow_missing_filter = true` (or `CID_ALLOW_MISSING_FILTER=true`) enables fallback on the explicit method-missing exit. The daemon logs this once per Process code-cell version.

Refunds require the verdict-producing live Process ELF and state. Each refund includes the code cell as a dependency, and the worker checks the state against the node immediately before submitting. For unaudited guests, the refund also includes the authorizing state outpoint as a cell dependency so CKB enforces its liveness. A pending digest that spends that state can delay the refund until the worker resolves the recreated state and obtains a fresh verdict. Audited example guests preserve the state lock, so their refunds can use the code dependency alone and proceed while a digest is pending. The sweep considers up to 16 singleton refunds per Process per tick, advances past the last attempt, and stops on RPC unavailability.

No private keys, no signing, no `.env`, no contract `deployment/` writes.

## Config

See `examples/cid.testnet.toml` / `examples/cid.network.toml.example`.

| Section | Role |
|---------|------|
| `[cid_lock]` | CID lock **code** Type ID (scan identity) |
| `[process].example_policy_data_hashes` | Optional additional audited example ELF data hashes, as 32-byte hex strings. The pinned v0.1.4 example is recognized automatically. |
| `[registry]` | `path` + `max_entries` for on-disk Process registry |
| `[observe].api_bind` | HTTP Process registry API |
| `[observe].health_bind` | `/healthz` `/readyz` `/stats` `/metrics` |
| `[loop]` | poll interval, confirmation depth, missing-filter fallback |

Register existing Process state cells through the HTTP API.

The primary CID identity must use `hash_type=type`. Move legacy data-hash identities to `cid_lock.legacy_scan_locks` or `CID_LEGACY_SCAN_LOCKS` for discovery only.

## Process registry API

**Security:** The registry HTTP API has **no authentication or API keys**. Anyone who can reach the bind address can list, register, or disable Processes. The daemon does **not** hold signing keys, but registration changes which Process workers run and can enable or disable execution for a `process_type_hash`.

**Bind to loopback by default** (`127.0.0.1:<port>`). Non-loopback exposure requires an explicit operator opt-in (`allow_non_loopback_api` in config, or `CID_ALLOW_NON_LOOPBACK_API=true` when supported). Without that flag, config should reject public binds fail-closed.

Bind: `observe.api_bind` / `CID_API_BIND`.

| Method | Path | Body |
|--------|------|------|
| `GET` | `/v1/processes` | — |
| `GET` | `/v1/processes/0x<identity_type_hash>` | — |
| `POST`/`PUT` | `/v1/processes` | JSON registration |
| `DELETE` | `/v1/processes/0x<identity_type_hash>` | — |

### Testnet v0.1.4

The contract pin is `b2d01e0a5a9760c8ee627633365aa98dd7a99859`. These are the code cells from the contracts repository's `deployment/testnet` records. The daemon resolves their current outpoints by Type ID.

| Code cell | v0.1.4 outpoint (index 0) | v0.1.4 data hash |
|-----------|-------------------------|------------------|
| CID lock | `0x48ee645be85d43c1b531b43c857c2792ea275c471df58a51066e0adddf7a9e73` | `0x816d279f64d00533c61a5c851ed5ce5c6285c5dcc8b3253505432d05aa11e2a4` |
| Example Process | `0x3b9d86096d5be14cd560e2dd161845d9b4773171695629053d0bafaf6a7dcc4d` | `0x2374e455d8cdcae5d5387257ec0d5b33114d3b5338d823d5e388b5a0a767f226` |

The pinned `Process.filter` runs without Process state and accepts an otherwise valid Intention whose beneficiary uses the Process lock code hash. `Process.assemble` reads the state and rejects that beneficiary with exit 32. The worker isolates the cell and considers a refund only when the audited example policy confirms the per-cell rejection against the live Process state.

Send this JSON to `POST http://127.0.0.1:9101/v1/processes`. Replace the state placeholder with the 32-byte TYPE_ID args of your live Process instance.

```json
{
  "identity_type_id_args": "0x<PROCESS_STATE_TYPE_ID_ARGS>",
  "code_type_id_args": "0x1666a7d877fcabcf28bbc0903df791742a4c83a85dfb666a6c5db7adde76ace7",
  "code_type_id": "0x9efaaa094bdbdc463813d26c82021a2bcc2fc9cf886a3c2cfd04612e44c5f16d"
}
```

The state cell lock uses `code_hash` equal to `code_type_id` above and `hash_type=type`. Its args are `cid_code_hash[32] || cid_hash_type[1] || salt`. On testnet use `0xf3f8a56c2594030f99f05b09ff1b6a1b07e6e07ed04309c1c4e246561faad326` followed by byte `01` and an optional instance salt. For short legacy args such as `0x01`, an incorrect CID binding, or a missing or ambiguous state cell, POST returns HTTP 400. An unavailable indexer returns HTTP 503. Persisted registrations undergo the same state checks during resolution.

- `identity_*` = Process **state** cell built-in TYPE_ID (Intention
  `process_type_hash` = derived type-script hash).
- `code_*` = Process **code** cell Type ID (lock `code_hash` + SSRI ELF).
- Optional `*_type_id` fields fail-closed if they disagree with derived hashes.
- Idempotent re-register; replacing `code_*` for the same identity updates the
  binding. Bounded by `registry.max_entries` (default 64).
- DELETE returns 404 if missing. After deletion is queued, the worker stops
  accepting new batches. It continues tracking pending transactions until it
  can shut down (`process.delete_deferred`).

Registry file: atomic write+rename JSON at `registry.path`. Loaded on startup;
register/delete persist immediately and enqueue live worker spawn/stop.

## Host queries

During `Process.assemble`, the live digest path passes an RPC-backed
`RpcHostQuery` into `ProcessExecutor::assemble` (generic over `HostQuery`).
Every host-query syscall (`find_cell_by_out_point` 2287, `find_cell_data`
2297, `get_live_cell` 2317, `get_header` 2327, `get_header_by_number` 2337,
`get_block_hash` 2347, `get_cells` 2367) uses a **10s per-request** timeout
against node or indexer JSON-RPC. `get_cells` uses opaque variable-length
indexer cursors and honors `asc`/`desc` order. Offline/`MockHost` seeding
remains for unit tests only; MockHost Prefix/Exact order is by matching
script raw bytes then block/tx/output index (see contract
`docs/ssri-backend-interface.md`).

Pinned `cid_process_example` pays fee from Process state and **rejects extra
funding inputs** with `UnsupportedFundingLock` (38). Do not pass funding cells
to the example guest / calculator submit helper. Exit 21 leaves the cells live. It can report a batch-level mismatch.

Exits 39 (deposit below minimum) and 40 (CID lock mismatch) trigger diagnostic assembly. Exit 32 (invalid beneficiary) also triggers it for audited example ELF hashes. If the guest requires more than one Intention, the worker shares a budget of 64 probes per Process per tick between finding a working batch and substituting candidates into it. It resumes unfinished searches on later ticks for the same Process snapshot and advances to the next window when the search is exhausted. When a recombined batch hits exit 21, 37, 39, or 40, the worker rebuilds and validates the successful control subset. Substitution failures exclude candidates from the current batch. Only audited example ELF hashes can qualify those candidates for refunds, and each retry rechecks the per-cell verdict against the current Process state. Unaudited assembly failures leave Intentions live for later assembly attempts.

Exit 41 (invalid Process state lock args) logs `process.unhealthy` and pauses assembly and refunds for that state and Process code version. Host query failures (36) are retried. Insufficient state capacity (37) leaves Intentions live and the comparison search advances to the next subset, which may need a smaller fee.

## Ops checklist

1. For testnet v0.1.4 use `examples/cid.testnet.toml` and identify your live Process state cell.
2. Start the daemon with `--config examples/cid.testnet.toml`.
3. `POST /v1/processes` for each Process you operate.
4. Watch `/readyz` and logs (`process.unregistered` = skip; `submit.ok` =
   passthrough submit).
5. To disable a Process, call `DELETE /v1/processes/0x…`. The worker drains any pending transactions.
