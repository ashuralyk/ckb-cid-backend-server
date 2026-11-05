# ckb-cid-backend-server

Public CID digest daemon: scan Intention cells (indexer `asc`), run Process SSRI in-process via `cid-process-executor`, submit execution transactions (`digest_ok` / `digest_incompatible`).

This repository is independent of [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher) (protocol, lock, executor, `cid_process_example`). Link them with git dependencies and on-chain conventions — do not merge the monorepos.

## Docs

- Design: [`docs/superpowers/specs/2026-11-05-cid-backend-server-design.md`](docs/superpowers/specs/2026-11-05-cid-backend-server-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-11-05-cid-backend-server.md`](docs/superpowers/plans/2026-11-05-cid-backend-server.md)

v0 is the digest loop only. No HTTP API, metrics, admin, or disk state. Restart = full rescan.

## Workspace

- `crates/cid-backend-core` — scan / process / ssri / assemble / submit / `digest_loop`
- `crates/cid-backend-daemon` — env config + placeholder RPC + `loop.run()`
- `tests/` (`cid-backend-tests`) — FakeRpc verticals; no live network

Upstream git pin: `d9ee77805f7ac930ab149648bed70f484a9886de` (`cid-process-executor`, `cid-protocol`).

## Daemon env vars

| Variable | Required | Default |
|----------|----------|---------|
| `CID_CKB_RPC_URL` | yes | — |
| `CID_INDEXER_URL` | no | same as `CID_CKB_RPC_URL` |
| `CID_LOCK_CODE_HASH` | yes | 32-byte hex (`0x` optional) |
| `CID_LOCK_HASH_TYPE` | no | `type` |
| `CID_POLL_INTERVAL_SECS` | yes | integer ≥ 1 |

The daemon does **not** install a Process-binary fallback. Production binaries come from live Process cell data. v0 ships `PlaceholderRpc` (methods return unavailable); FakeRpc covers tests.

```bash
cargo run -p cid-backend-daemon
```

Illegal config (missing URL or CID lock code hash) exits immediately.

## Tests

```bash
cargo test -p cid-backend-core
cargo test -p cid-backend-daemon
cargo test -p cid-backend-tests
```

Do not point these tests at a CKB node. Verticals: n=1 `digest_ok`, batch bounds, Incompatible refund, submit conflict retry.

Optional real guest for `EmbeddedSsri` tests:

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
# or: export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example
```

See `tests/fixtures/README.md`.
