# Observability (phase 4)

Digest correctness is unchanged. Logs, health, and counters are memory-only: a
restart drops occupancy and readiness until the next successful indexer scan.

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
| `tick.start` / `tick.end` | Each digest tick (`scanned`, `skipped`, `submitted`, `conflicts`, `scan_ok`) |
| `scan.unavailable` | Indexer/RPC scan failed |
| `process.resolve_miss` | No usable live Process binary |
| `assemble.batch_failed` | Assembly error and attempted Intention outpoints |
| `assemble.batch_isolated` | Working subset found during isolation |
| `process.unhealthy` | Exit 41 quarantined the state and paused its filter refunds |
| `ssri.filter_failed` | Filter error deferred the Process bucket |
| `refund.process_state_pending` | Refund deferred while a pending digest spends its state dependency |
| `refund.path` | User Lock refund construction (`path=user_lock`, `out_point`, `fee`) |
| `refund.partition` | DigestIncompatible refunds split by lock identity (Exact vs Bounded kept separate; no mixed-identity Bounded tx) |
| `refund.zero_fee` | Legacy exact Incompatible refund submitted with fee 0 (public-net gap vs `min_fee_rate`; default when allowlist empty / lock not listed) |
| `tick.skip` (`reason=fee_unaffordable`) | Process recreate cannot fund fee for this window; isolate/shrink (non-permanent) |
| `tick.skip` (`reason=refund_fee_unaffordable`) | Bounded refund budgets cannot cover miner fee; isolate/shrink like fee_unaffordable (non-permanent) |
| `submit.ok` | `tx_hash` |
| `submit.conflict` | Submit mapped as Conflict |
| `submit.rejected` | Transaction-level submit rejection (`TransactionFailedToVerify` / malformed / outputs-validator); that combo is skipped, not every consumed outpoint |
| `submit.rejected_combo` | Isolation skipped a combo already in `rejected_combos` (no new `send_transaction`) |
| `pending.committed` | First sight of Committed (occupancy still held) |
| `pending.settled` | Confirmation depth reached |
| `pending.dropped` / `pending.reorg` | Pending/confirming tx gone |
| `reorg.tip_rewind` | Tip height moved backwards |
| `config.load_failed` | Illegal env / TOML / health bind failure (process exits) |
| `config.loaded` | Successful load: path, `network_name`, declared `genesis_hash`, `genesis_status` (`declared_unverified` / `declared_will_verify` / `unset`), rpc/indexer **host only** (no userinfo) |
| `genesis.verified` | `validate_genesis=true` and `get_block_hash(0x0)` matched (fail-closed otherwise: `genesis.mismatch`) |

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
