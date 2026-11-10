# cid_process_example fixture

RISC-V Process guest used by the real SSRI (`EmbeddedSsri` / ckb-vm) test in
`cid-backend-core`. Built from [`ckb-common-intention-dispatcher`](https://github.com/ashuralyk/ckb-common-intention-dispatcher)
at the same git pin as the workspace crates:

`d9ee77805f7ac930ab149648bed70f484a9886de`

The ELF is **not** committed (reproducible from that pin). Expected SSRI
behavior: `Process.batch` → `(1, 4)`; `Process.assemble` with `n=0` →
`Incompatible(21)`.

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

Default `cargo test` **ignores** `real_example_binary_batch_bounds_and_assemble`
(`1 ignored`). That is not a real-SSRI pass.

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
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build CLANG=clang-18
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
# or: export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example
```

Lookup order: `CID_PROCESS_EXAMPLE_PATH`, then
`tests/fixtures/cid_process_example`.

FakeRpc verticals (`cid-backend-tests`) use `ScriptedSsri` and do not need
the ELF.

Local-chain E2E also builds `ckb_common_intention_dispatcher` (CID lock) and
a tiny `always_success` guest; those ELFs are gitignored. See
`docs/local-dev-chain.md`.
