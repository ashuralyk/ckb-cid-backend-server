# Refund fee migration (allowlist gate)

## Status

Backend can opt into **bounded** Incompatible refund fee cuts
(`MAX_REFUND_FEE = 1_000_000` shannons / 0.01 CKB **per Intention**) for
**explicitly allowlisted** CID lock identities only.

Reviewed contract rules live in
[ckb-common-intention-dispatcher PR #12](https://github.com/ashuralyk/ckb-common-intention-dispatcher/pull/12)
at head **`d6c5a86`** (or newer): per-Intention ceiling via
`refund_capacity_allowed`, plus **tx-wide** `F >= S`
(`fee_covers_shortfalls`) so shortfalls cannot fund other outputs.
`S` / DigestOk counting is scoped to the executing lock's
`code_hash` + `hash_type` (not across deployments). A contracts PR #12
follow-up rejects **mixed CID identities** in one DigestIncompatible tx —
that on-chain rule is the real defense against cross-identity `S` undercount;
assembler partitioning is defense-in-depth only. TimeoutBurn stays exact.

Do **not** treat undeployed candidate hashes as production defaults; do **not**
replace current testnet `code_hash` with an undeployed binary (including
provisional data hashes such as `0x2edb827f…` — document as example only,
never as an active allowlist entry).

### Historical note

An earlier candidate (`fcd33d9`) allowed capacity shortfall without tx-wide
fee binding; review found arbitrary outputs could siphon the haircut. PR #12
(`e0e7f18` → `d6c5a86`) binds haircuts to miner fee (`F >= S`) and restores
TimeoutBurn exact capacity.

## Defaults (safe for existing deployments)

| Knob | Default | Effect |
|------|---------|--------|
| `cid_lock.bounded_refund_locks` / `CID_BOUNDED_REFUND_LOCKS` | **empty** | Exact User Lock capacity; fee 0 |
| `cid_lock.legacy_scan_locks` / `CID_LEGACY_SCAN_LOCKS` | empty | No extra scan targets |
| `loop.fee_rate` / `CID_FEE_RATE` | 1000 | Funds `digest_ok` via Process recreate; also used for allowlisted bounded refunds |

Empty allowlist ⇒ legacy exact for every Intention. Listing a lock on the
allowlist alone does **not** fix `-1104` against an old lock that still
demands exact capacity.

## Enabling bounded refunds (after revised lock ships)

1. Deploy the audited CID lock that enforces PR #12 rules (OWNER redeploy).
2. Point primary `cid_lock.code_hash` / Type-ID at the **new** deployed hash.
3. Add that identity to `[[cid_lock.bounded_refund_locks]]` (or
   `CID_BOUNDED_REFUND_LOCKS=0x…:hash_type`). Never list undeployed candidates.
4. Optionally list old locks under `[[cid_lock.legacy_scan_locks]]` so the
   daemon continues draining exact-refund Intentions after redeploy.
5. Keep `digest_ok` Process fee path unchanged (`loop.fee_rate`).

## Assembly rules (bounded mode)

- Fee = `ceil(serialized_size_in_block * fee_rate / 1000)`.
- Cuts allocated left-to-right; each cut ≤
  `min(MAX_REFUND_FEE, capacity - occupied)`.
- `sum(cuts) == fee` exactly — **no siphon outputs** (implies contract
  `F >= S` when there are no extra outputs).
- Fail closed / isolate-shrink (`FeeUnaffordable`) if the budget is
  insufficient (non-permanent, no `rejected_combos`).
- Intentions whose lock is **not** on the allowlist always use exact refunds.
- **Never** mix allowlisted and legacy locks in one bounded refund tx.
- **Never** co-bundle two different bounded-capable lock identities in one
  Execution tx — the loop partitions DigestIncompatible assembly by
  `(code_hash, hash_type)`. The allowlist may list multiple verified
  deployed identities after migration, but they must not share a refund tx.
- **Never** co-bundle fee-shaved Incompatible refunds with Process /
  DigestOk.
- **TimeoutBurn** stays exact; this daemon does not assemble it.

## Fee sizing vs submit

Assemble sizes an empty-witness skeleton and attaches a `FeeGuard`.
`submit_execution` re-checks that the tx size is unchanged and paid fee still
covers `fee_rate`, fail-closed otherwise.

### Release gate (not FakeRpc CI)

Against the intended CKB node with the node's real `min_fee_rate`, confirm
both DigestOk and allowlisted bounded-refund txs are accepted by the tx-pool
after contract PR #12 is deployed and pinned. Mixed old/new Intention sets
must drain without haircutting legacy locks.

## Local / CI

Local-dev chain keeps `min_fee_rate = 0` and an **empty** allowlist so the
pinned lock fixture continues to clear exact refunds.
