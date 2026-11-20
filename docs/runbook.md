# CID backend runbook (Process-as-lock)

Keyless digest daemon for CKB Common Intention Dispatcher after the
Process-as-lock / full-tx SSRI cutover.

## What the daemon does

1. Scan by the CID code Type ID and route with the shared Intention ownership helpers.
2. Resolve the live registered Process state and validate its `ProcessLockArgs` and CID binding. Run `Process.filter` before selecting a batch. Reason 0 advances to assembly and reason 1 is logged with the cell left live. Only reasons 4–7 authorize automatic refunds. Reserved reasons 2 and 3, unknown codes, malformed responses, and count mismatches defer the whole Process bucket.
3. Track submitted inputs through confirmation depth and reconcile dropped transactions and reorgs.

Missing `Process.filter` defers the window by default. `loop.allow_missing_filter = true` (or `CID_ALLOW_MISSING_FILTER=true`) enables fallback on the explicit method-missing exit. The daemon logs this once per Process code-cell version.

Refunds require an exact node reread of the Intention, including its output and data. The worker verifies the verdict-producing ELF against the node and checks the authorizing state snapshot just before submission. Every refund includes both the Process code and state outpoints as cell dependencies. If a reorg replaces the state under the same Type ID, the original outpoint is unavailable and CKB cannot commit a refund that depends on it. The worker defers refunds while its pending digest spends that state. Once the recreated state is visible, the worker gives deferred refunds the next submission turn after checking assembly for Process faults. A fresh filter verdict is required. The worker waits to observe those refunds committed or dropped before it submits another digest that would spend their state dependency. The sweep attempts up to 16 singleton refunds per Process per tick, resumes after the last attempt, and stops on RPC unavailability.

No private keys, no signing, no `.env`, no contract `deployment/` writes.

## Config

See `examples/cid.testnet.toml` / `examples/cid.network.toml.example`.

| Section | Role |
|---------|------|
| `[cid_lock]` | CID lock **code** Type ID (scan identity) |
| `[registry]` | `path` + `max_entries` for on-disk Process registry |
| `[observe].api_bind` | HTTP Process registry API |
| `[observe].health_bind` | `/healthz` `/readyz` `/stats` `/metrics` |
| `[loop]` | poll interval, confirmation depth, missing-filter fallback |

Register existing Process state cells through the HTTP API. The former `process.example_policy_data_hashes` and `CID_EXAMPLE_POLICY_DATA_HASHES` settings are obsolete and can be removed.

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

The pinned `Process.filter` runs without Process state and accepts an otherwise valid Intention whose beneficiary uses the Process lock code hash. `Process.assemble` reads the state and rejects that beneficiary with exit 32. The worker logs the assembly failure, can submit a working subset, and leaves the excluded cell live.

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

Pinned `cid_process_example` pays the digest fee from Process state and rejects extra funding inputs with exit 38 (`UnsupportedFundingLock`).

Assembly errors log `assemble.batch_failed`. Exit 21 can report a batch mismatch and exit 37 can report insufficient state capacity. The worker probes legal subsets of the filter-accepted cells, starting at the minimum size and including shifted batches across window boundaries. Exits 32, 36, 39, 40 and other assembly errors use the same search. Only a working subset goes through validation and dry-run. Excluded cells stay live and are eligible for a fresh filter pass on later ticks.

Each Process gets 64 subset probes per tick. While the original search runs, up to 16 go to subsets containing arrivals, paired with each other or with existing candidates. The newest group gets the first turn and earlier arrival groups resume in rotation. Both searches retain their cursors across ticks for the same Process snapshot. When the search runs out of subsets, the worker advances to the next window.

Exit 41 (invalid Process state lock args) from either initial assembly or a probe logs `process.unhealthy`, clears pending isolation work, and pauses assembly and filter refunds for that state and code version.

## Ops checklist

1. For testnet v0.1.4 use `examples/cid.testnet.toml` and identify your live Process state cell.
2. Start the daemon with `--config examples/cid.testnet.toml`.
3. `POST /v1/processes` for each Process you operate.
4. Watch `/readyz` and logs (`process.unregistered` = skip; `submit.ok` =
   passthrough submit).
5. To disable a Process, call `DELETE /v1/processes/0x…`. The worker drains any pending transactions.
