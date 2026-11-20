# cid_process_example fixture

RISC-V Process guest used by the real SSRI (`EmbeddedSsri` / ckb-vm) test in
`cid-backend-core`. Built from [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher)
at the same git pin as the workspace crates:

`b2d01e0a5a9760c8ee627633365aa98dd7a99859`

The ELF is **not** committed (reproducible from that pin). Expected SSRI
behavior includes `Process.filter` accepting a 600 CKB deposit and returning reason 5 below that minimum. `Process.batch` returns `(1, 4)`.

## CI (must-load)

GitHub Actions clones the pin, builds with `clang-18` +
`riscv64imac-unknown-none-elf`, copies the ELF here (or sets
`CID_PROCESS_EXAMPLE_PATH`), then runs:

```bash
cargo test --locked -p cid-backend-core -- --include-ignored --nocapture
```

If the ELF is missing, that test **panics** and the job fails. A silent
early-return that still prints `ok` is not allowed in CI.

## Local (soft skip)

Default `cargo test` ignores the two opt-in core guest tests. The daemon and FakeRpc guest tests use the fixture when it is present.

To run it locally:

```bash
# clang-18 (or CLANG=clang) and riscv64imac-unknown-none-elf
./scripts/build-cid-process-example.sh
cargo test --locked -p cid-backend-core real_example_binary_batch_bounds_and_assemble -- --ignored --nocapture
```

Or copy a binary you already built:

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout b2d01e0a5a9760c8ee627633365aa98dd7a99859
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build CLANG=clang-18
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
# or: export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example
```

The exact deployed v0.1.4 guest can also be retrieved from CKB testnet with the read-only call `get_live_cell` and `with_data=true` for outpoint `0x3b9d86096d5be14cd560e2dd161845d9b4773171695629053d0bafaf6a7dcc4d:0`. Hex-decode `cell.data.content` and verify its BLAKE2b-256 hash with personalization `ckb-default-hash` equals `0x2374e455d8cdcae5d5387257ec0d5b33114d3b5338d823d5e388b5a0a767f226` before using it.

Lookup order: `CID_PROCESS_EXAMPLE_PATH`, then
`tests/fixtures/cid_process_example`.

Most FakeRpc tests use `ScriptedSsri`. The embedded SSRI vertical uses
the built fixture when it is available. The opt-in zero-fee-headroom
regression requires the exact deployed v0.1.4 guest with the hash above.
Run it from this repository with:

```bash
CID_PROCESS_EXAMPLE_PATH=/abs/path/to/deployed-v0.1.4-cid_process_example \
  cargo test --locked -p cid-backend-tests \
  audited_guest_with_zero_fee_headroom_waits_for_state_or_code_change -- --ignored --nocapture
```

Local-chain E2E also builds `ckb_common_intention_dispatcher` (CID lock) and
a tiny `always_success` guest; those ELFs are gitignored. See
`docs/local-dev-chain.md`.
