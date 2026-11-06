#!/usr/bin/env bash
# Build the pinned cid_process_example RISC-V guest and copy it to
# tests/fixtures/cid_process_example (or CID_PROCESS_EXAMPLE_PATH).
#
# Pin must match workspace.dependencies cid-process-executor / cid-protocol rev.
set -euo pipefail

PIN="${CID_DISPATCHER_REV:-d9ee77805f7ac930ab149648bed70f484a9886de}"
CLANG="${CLANG:-clang-18}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${CID_PROCESS_EXAMPLE_PATH:-$ROOT/tests/fixtures/cid_process_example}"
SRC="${CID_DISPATCHER_SRC:-}"
CLEANUP=""

if [ -z "$SRC" ]; then
  SRC="$(mktemp -d "${TMPDIR:-/tmp}/cid-dispatcher.XXXXXX")"
  CLEANUP="$SRC"
  git clone --depth 1 https://github.com/ashuralyk/ckb-common-intention-dispatcher "$SRC"
  git -C "$SRC" fetch --depth 1 origin "$PIN"
  git -C "$SRC" checkout --detach "$PIN"
fi

rustup target add riscv64imac-unknown-none-elf
make -C "$SRC" CONTRACT=cid_process_example build "CLANG=$CLANG"
ELF="$SRC/build/release/cid_process_example"
if [ ! -s "$ELF" ]; then
  echo "error: build produced no ELF at $ELF" >&2
  exit 1
fi
mkdir -p "$(dirname "$DEST")"
cp "$ELF" "$DEST"
python3 - "$DEST" <<'PY'
import os, sys
p = sys.argv[1]
with open(p, "rb") as f:
    magic = f.read(4)
if magic != b"\x7fELF":
    sys.stderr.write(f"error: {p} is not an ELF (magic={magic!r})\n")
    sys.exit(1)
print(f"wrote {p} ({os.path.getsize(p)} bytes, ELF)")
PY
if command -v sha256sum >/dev/null; then
  sha256sum "$DEST"
fi
if [ -n "$CLEANUP" ]; then
  rm -rf "$CLEANUP"
fi
