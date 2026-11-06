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
cargo test --locked -p cid-backend-core
cargo test --locked -p cid-backend-daemon
cargo test --locked -p cid-backend-tests
```

Do not point these tests at a CKB node. Verticals: n=1 `digest_ok`, batch bounds, Incompatible refund, submit conflict retry.

Default local `cargo test` **ignores** the real SSRI guest test (`real_example_binary_batch_bounds_and_assemble`) when `cid_process_example` is absent — that `ignored` count is **not** a real-SSRI pass.

GitHub Actions (`.github/workflows/ci.yml`) on `push`, `pull_request` (including drafts), and `workflow_dispatch`:

1. Builds `cid_process_example` from pin `d9ee77805f7ac930ab149648bed70f484a9886de` (`clang-18` + `riscv64imac-unknown-none-elf`).
2. Fails if the ELF is missing.
3. Runs `cargo test --locked -p cid-backend-core -- --include-ignored` so the guest executes `Process.batch` / `Process.assemble` on ckb-vm, plus the FakeRpc daemon/tests suites.

The ELF is not committed; see `tests/fixtures/README.md` and `./scripts/build-cid-process-example.sh`.
