# Observability (phase 4)

The daemon holds occupancy and readiness in memory. After a restart, readiness
returns with the next successful indexer scan.

## Logs

Stack: `tracing` in `cid-backend-core` (structured events, no HTTP deps) +
`tracing-subscriber` JSON/pretty in the thin daemon. Remaining `log::` calls
are bridged via `tracing-log`.

| Env | Default | Meaning |
|-----|---------|---------|
| `RUST_LOG` | `info` | EnvFilter, e.g. `cid_backend=debug` |
| `CID_LOG_FORMAT` | `json` | `json` or `pretty` (`text` is an alias for pretty) |

Stable field `event` (target `cid_backend`):

| `event` | When |
|---------|------|
| `tick.start` / `tick.end` | Each digest tick (`scanned`, `skipped`, `submitted`, `conflicts`, `scan_ok`, `isolate_windows`) |
| `scan.unavailable` | Indexer/RPC scan failed |
| `process.resolve_miss` | No usable live Process binary |
| `assemble.batch_failed` | Assembly error and attempted Intention outpoints |
| `assemble.probe_failed` | Assembly error for a subset examined during isolation |
| `assemble.probe_deferred` | A candidate liveness lookup was unavailable |
| `assemble.invalid` | The assembled transaction failed structural or funding-input validation |
| `assemble.process_unfunded` | Exit 37 from the audited v0.1.4 example ELF with zero state fee headroom paused assembly for the state and code |
| `assemble.batch_isolated` | An assembled subset passed transaction validation |
| `process.unhealthy` | Exit 41 quarantined the Process state/code pair and paused its filter refunds |
| `ssri.filter_failed` | Filter error deferred the Process bucket |
| `refund.process_state_pending` | Refund deferred while a pending digest spends or creates its state dependency |
| `refund.snapshot_mismatch` | Node re-read disagrees with the Intention, Process state, or code snapshot |
| `refund.path` | Singleton User Lock refund built (`path=user_lock`, `out_point`, `fee`) or unbuildable (`path=unbuildable`, `error`) |
| `refund.fee_unaffordable` | The singleton bounded refund cannot cover the fee and its Intention stays live |
| `dry_run.conflict` | Digest dry-run mapped to Conflict |
| `submit.ok` | `tx_hash` |
| `submit.conflict` | Submit mapped as Conflict |
| `submit.rejected` | Transaction-level submit rejection (`TransactionFailedToVerify` / malformed / outputs-validator); that combo is skipped, not every consumed outpoint |
| `submit.rejected_combo` | Isolation skipped a combo already in `rejected_combos` (no new `send_transaction`) |
| `pending.committed` | First sight of Committed (occupancy still held) |
| `pending.settled` | A canonical commit reached confirmation depth |
| `pending.noncanonical` | The reported commit block is missing or differs from the canonical hash at its height |
| `pending.block_lookup` | The canonical commit-height hash lookup failed |
| `pending.dropped` / `pending.reorg` | Pending/confirming tx gone |
| `reorg.tip_rewind` | Tip height moved backwards |
| `config.load_failed` | Illegal env / TOML / health bind failure (process exits) |
| `config.loaded` | Successful load: path, `network_name`, declared `genesis_hash`, `genesis_status` (`declared_unverified` / `declared_will_verify` / `unset`), rpc/indexer **host only** (no userinfo) |
| `genesis.verified` | `validate_genesis=true` and `get_block_hash(0x0)` matched (fail-closed otherwise: `genesis.mismatch`) |

Each tick starts a fresh assembly scan of the current filter-accepted candidates,
capped at 64 subset probes per Process. `isolate_windows` counts dispatched
assembly jobs, and the assembly events identify attempted subsets. Some
combinations may repeatedly fall outside the cap. This bounded-search starvation
is accepted by design. Filter reasons 4–7 can authorize refunds after node
re-reads and interlock checks, independently of assembly progress.

## Health HTTP

Unset or empty `CID_HEALTH_BIND` disables the listener. Example:
`CID_HEALTH_BIND=127.0.0.1:9100` (or `127.0.0.1:0` for an ephemeral port; the
daemon logs `health.listen` with the bound address).

| Path | Meaning |
|------|---------|
| `GET /healthz` | Liveness: process is up (always 200 while the server runs) |
| `GET /readyz` | Readiness: last tick’s indexer scan succeeded (`scan_ok`). 503 until then, and 503 again if the last scan was Unavailable |
| `GET /stats` | JSON: `ticks`, `scanned`, `skipped`, `submitted`, `conflicts`, `pending_in_flight`, `last_tick_at` (unix ms), `last_error`, `ready` |
| `GET /metrics` | Prometheus text of the same counters/gauges |

The listener is a `std::net` accept thread plus per-connection workers
(at most 32 at once; further clients get `503` and the socket is shut down
on the accept thread). Reading the request head has an **absolute 2s
deadline from the first read** — each `recv` uses the remaining time, not a
fresh 2s window — so a client that drips one byte every <2s cannot hold a
worker past that deadline (`408`, then `shutdown`). Request bodies are not
accepted (`413`, then `shutdown` without draining). A slow, incomplete, or
drip-fed request cannot occupy `/healthz` / `/readyz` / `/stats` /
`/metrics` indefinitely. The digest loop is not blocked. Counters use
atomics/`Mutex` only for the last error string.
