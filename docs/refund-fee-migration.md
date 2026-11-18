# Refund fee migration (capability gate)

## Status

Backend assembly can opt into **bounded** Incompatible refund fee cuts
(`MAX_REFUND_FEE = 1_000_000` shannons / 0.01 CKB **per Intention**). The
matching CID lock candidate is **under independent security review and is not
final** (known finding: arbitrary outputs can siphon a haircut unless the
contract enforces miner-fee-only accounting). Do **not** treat candidate
hashes as production-safe; do **not** replace current testnet `code_hash` with
an undeployed binary.

## Defaults (safe for existing deployments)

| Knob | Default | Effect |
|------|---------|--------|
| `cid_lock.bounded_refund_fee` / `CID_BOUNDED_REFUND_FEE` | **false** | Exact User Lock capacity; fee 0 |
| `loop.fee_rate` / `CID_FEE_RATE` | 1000 | Funds `digest_ok` via Process recreate; also used when bounded refunds are enabled |

Legacy exact refunds are required for currently deployed CID locks. Enabling
the flag alone does **not** fix public-net `-1104` against an old lock that
still demands exact capacity.

## Enabling bounded refunds (after revised lock ships)

1. Wait for a **revised** CID lock that accepts the fee window **and** closes
   the siphon finding (miner-fee-only). Redeploy under OWNER control.
2. Point `cid_lock.code_hash` / Type-ID at the **new** deployed hash only.
   Leave old Intention cells under the old lock (exact-refund rules).
3. Set `cid_lock.bounded_refund_fee = true` (or `CID_BOUNDED_REFUND_FEE=true`)
   **explicitly** for that deployment. Never enable against undeployed
   candidates.
4. Keep `digest_ok` Process fee path unchanged (`loop.fee_rate`).

## Assembly rules (bounded mode)

- Fee = `ceil(serialized_size_in_block * fee_rate / 1000)`.
- Cuts allocated left-to-right across refund outputs; each cut ≤
  `min(MAX_REFUND_FEE, capacity - occupied)`.
- `sum(cuts) == fee` exactly — **no siphon outputs**.
- Fail closed / non-permanent (`refund_fee_unaffordable`) if budget &lt; fee.
- **Never** auto-haircut when the capability flag is off.
- **Never** co-bundle fee-shaved Incompatible refunds with Process /
  DigestOk (Process example requires exact User Lock capacity when DigestOk
  is present). Separate transactions only.
- **TimeoutBurn** stays exact; this daemon does not assemble TimeoutBurn and
  must not apply the refund fee window to that path.

## Local / CI

Local-dev chain keeps `min_fee_rate = 0` and `bounded_refund_fee = false` so
the pinned lock fixture continues to clear exact refunds.
