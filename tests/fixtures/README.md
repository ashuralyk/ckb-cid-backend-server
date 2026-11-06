# cid_process_example fixture

This directory optionally holds the RISC-V guest built from
`ashuralyk/ckb-common-intention-dispatcher` at rev
`d9ee77805f7ac930ab149648bed70f484a9886de`.

```bash
git clone https://github.com/ashuralyk/ckb-common-intention-dispatcher
cd ckb-common-intention-dispatcher
git checkout d9ee77805f7ac930ab149648bed70f484a9886de
rustup target add riscv64imac-unknown-none-elf
make prepare
make CONTRACT=cid_process_example build
cp build/release/cid_process_example \
  /path/to/ckb-cid-backend-server/tests/fixtures/cid_process_example
```

Or: `export CID_PROCESS_EXAMPLE_PATH=/abs/path/to/cid_process_example`.

Tests that need the real guest skip with a printed message if the file is absent.
FakeRpc verticals (Tasks 8–11) use `ScriptedSsri` and do not require the ELF.
