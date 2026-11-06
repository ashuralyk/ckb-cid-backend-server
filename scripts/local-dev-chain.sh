#!/usr/bin/env bash
# Start an isolated CKB *dev* chain (no mainnet/testnet) and run the phase-2
# local-chain closure: scan → Process resolve → real SSRI assemble → submit →
# committed execution tx.
#
# Pins:
#   CKB           v0.119.0  (matches workspace ckb-types 0.119)
#   dispatcher    d9ee77805f7ac930ab149648bed70f484a9886de
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CKB_VERSION="${CKB_VERSION:-v0.119.0}"
CKB_DIR="${CKB_DIR:-$ROOT/.local-ckb}"
DATA_DIR="${CKB_DATA_DIR:-$CKB_DIR/dev}"
RPC_URL="${CID_CKB_RPC_URL:-http://127.0.0.1:8114}"
PIN="${CID_DISPATCHER_REV:-d9ee77805f7ac930ab149648bed70f484a9886de}"
CLANG="${CLANG:-clang-18}"

# Genesis issued secp key from specs/dev.toml (dev chain only).
BA_ARG="0xc8328aabcd9b9e8e64fbc566c4385c3bdeb219d7"
BA_CODE_HASH="0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8"

mkdir -p "$CKB_DIR"
if [ ! -x "$CKB_DIR/ckb" ]; then
  tarball="ckb_${CKB_VERSION}_x86_64-unknown-linux-gnu.tar.gz"
  url="https://github.com/nervosnetwork/ckb/releases/download/${CKB_VERSION}/${tarball}"
  echo "downloading $url"
  curl -fsSL -o "$CKB_DIR/$tarball" "$url"
  tar -xzf "$CKB_DIR/$tarball" -C "$CKB_DIR" --strip-components=1
  chmod +x "$CKB_DIR/ckb"
fi

if [ ! -f "$DATA_DIR/ckb.toml" ]; then
  "$CKB_DIR/ckb" init -C "$DATA_DIR" --chain dev \
    --ba-arg "$BA_ARG" --ba-hash-type type --ba-code-hash "$BA_CODE_HASH"
fi

python3 - "$DATA_DIR/ckb.toml" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
t = p.read_text()
t = t.replace(
    'modules = ["Net", "Pool", "Miner", "Chain", "Stats", "Subscription", "Experiment", "Debug"]',
    'modules = ["Net", "Pool", "Miner", "Chain", "Stats", "Subscription", "Experiment", "Debug", "Indexer"]',
)
t = t.replace("min_fee_rate = 1_000", "min_fee_rate = 0")
t = t.replace("min_rbf_rate = 1_500", "min_rbf_rate = 0")
p.write_text(t)
print("patched", p)
PY

rustup target add riscv64imac-unknown-none-elf >/dev/null
AS_OUT="$ROOT/tests/fixtures/always_success"
if [ ! -s "$AS_OUT" ]; then
  rustc --target riscv64imac-unknown-none-elf \
    -C opt-level=s -C panic=abort \
    -o "$AS_OUT" "$ROOT/scripts/always_success.rs"
fi
python3 -c 'import sys; assert open(sys.argv[1],"rb").read(4)==b"\x7fELF"' "$AS_OUT"

export CID_DISPATCHER_REV="$PIN"
export CID_DISPATCHER_SRC="${CID_DISPATCHER_SRC:-}"
export CID_PROCESS_EXAMPLE_PATH="${CID_PROCESS_EXAMPLE_PATH:-$ROOT/tests/fixtures/cid_process_example}"
export CID_LOCK_ELF_PATH="${CID_LOCK_ELF_PATH:-$ROOT/tests/fixtures/ckb_common_intention_dispatcher}"
export CLANG
bash "$ROOT/scripts/build-cid-process-example.sh"
bash "$ROOT/scripts/build-cid-lock.sh"

if ! curl -fsS -m 1 -H 'content-type: application/json' \
    --data '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
    "$RPC_URL" >/dev/null 2>&1; then
  echo "starting ckb at $DATA_DIR"
  "$CKB_DIR/ckb" run -C "$DATA_DIR" >"$CKB_DIR/ckb.log" 2>&1 &
  echo $! >"$CKB_DIR/ckb.pid"
  "$CKB_DIR/ckb" miner -C "$DATA_DIR" >"$CKB_DIR/miner.log" 2>&1 &
  echo $! >"$CKB_DIR/miner.pid"
  for i in $(seq 1 60); do
    if curl -fsS -m 1 -H 'content-type: application/json' \
        --data '{"id":1,"jsonrpc":"2.0","method":"get_tip_block_number","params":[]}' \
        "$RPC_URL" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

export CID_CKB_RPC_URL="$RPC_URL"
export CID_INDEXER_URL="$RPC_URL"
export CID_LOCAL_CHAIN_E2E=1
export CID_ALWAYS_SUCCESS_PATH="$AS_OUT"
cd "$ROOT"
exec cargo test -p cid-backend-daemon --test local_chain_e2e -- --ignored --nocapture
