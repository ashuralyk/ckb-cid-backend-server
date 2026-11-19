# CID backend runbook (Process-as-lock)

Keyless digest daemon for CKB Common Intention Dispatcher after the
Process-as-lock / full-tx SSRI cutover.

## What the daemon does

1. Scans live Intention cells by CID lock Type ID (`hash_type=type`).
2. Skips Intentions whose `process_type_hash` is **not registered**.
3. For each registered Process, a worker pre-forms a draft (Intentions +
   Process state cell), calls `Process.assemble` via SSRI, **validates** the
   returned molecule transaction (TYPE_ID continuity, batch Intentions
   present, size), and **submits it unchanged** (no fee shaving, no witness
   rewrite).
4. Tracks in-flight Process occupancy per worker; confirmation depth + reorg
   recovery unchanged.

No private keys, no signing, no `.env`, no contract `deployment/` writes.

## Config

See `examples/cid.testnet.toml` / `examples/cid.network.toml.example`.

| Section | Role |
|---------|------|
| `[cid_lock]` | CID lock **code** Type ID (scan identity) |
| `[registry]` | `path` + `max_entries` for on-disk Process registry |
| `[observe].api_bind` | HTTP Process registry API |
| `[observe].health_bind` | `/healthz` `/readyz` `/stats` `/metrics` |
| `[loop]` | poll interval, confirmation depth |

**Processes are not listed in TOML.** Register them after deploy.

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

Registration JSON:

```json
{
  "identity_type_id_args": "0x…",
  "identity_type_id": "0x…",
  "code_type_id_args": "0x…",
  "code_type_id": "0x…"
}
```

- `identity_*` = Process **state** cell built-in TYPE_ID (Intention
  `process_type_hash` = derived type-script hash).
- `code_*` = Process **code** cell Type ID (lock `code_hash` + SSRI ELF).
- Optional `*_type_id` fields fail-closed if they disagree with derived hashes.
- Idempotent re-register; replacing `code_*` for the same identity updates the
  binding. Bounded by `registry.max_entries` (default 64).
- DELETE returns 404 if missing. Worker stop is deferred while that Process
  still has pending submits (`process.delete_deferred`).

Registry file: atomic write+rename JSON at `registry.path`. Loaded on startup;
register/delete persist immediately and enqueue live worker spawn/stop.

## Host queries

During `Process.assemble`, the offline SSRI host serves ckb-ssri-std syscalls
(`find_cell_by_out_point` 2287, …, `get_cells` 2367) from CKB RPC/indexer with
bounded pagination/timeouts. See contract
`docs/ssri-backend-interface.md`.

## Ops checklist

1. Deploy CID lock + Process code/state (Lucas). Fill CID Type ID placeholders
   in config — do not invent live values.
2. Start daemon with `--config`.
3. `POST /v1/processes` for each Process you operate.
4. Watch `/readyz` and logs (`process.unregistered` = skip; `submit.ok` =
   passthrough submit).
5. To disable a Process: `DELETE /v1/processes/0x…` (retry if in-flight).
