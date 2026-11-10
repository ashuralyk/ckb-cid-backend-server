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
| `ssri.incompatible` | SSRI `Incompatible(code)` |
| `refund.path` | User-lock refund assemble (`path=user_lock`) |
| `submit.ok` | `tx_hash` |
| `submit.conflict` | Submit mapped as Conflict |
| `pending.committed` / `pending.dropped` | Pending execution tx status |
| `config.load_failed` | Illegal env / health bind failure (process exits) |

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

The listener is a `std::net` accept thread plus per-connection workers. Each
socket has a 2s read/write deadline. Request bodies are not accepted (`413`,
then `shutdown`); a slow or incomplete body cannot occupy `/healthz` /
`/readyz` / `/stats` / `/metrics`. The digest loop is not blocked. Counters
use atomics/`Mutex` only for the last error string.
