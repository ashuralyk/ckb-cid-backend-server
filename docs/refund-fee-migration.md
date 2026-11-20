# Refund fee migration (allowlist gate)

## Status

Backend can opt into **bounded** Incompatible refund fee cuts
(`MAX_REFUND_FEE = 1_000_000` shannons / 0.01 CKB **per Intention**) for
**explicitly allowlisted** CID lock identities only.

Reviewed contract rules live in
[ckb-common-intention-dispatcher PR #12](https://github.com/ashuralyk/ckb-common-intention-dispatcher/pull/12)
at head **`9bc8193`** (or newer): per-Intention ceiling via
`refund_capacity_allowed`, plus **tx-wide** `F >= S`
(`fee_covers_shortfalls`) so shortfalls cannot fund other outputs.
`S` / DigestOk counting is scoped to the executing lock's
`code_hash` + `hash_type` (not across deployments). DigestIncompatible
now rejects **mixed CID identities** (`MixedCidLock`) even at zero
shortfall — that on-chain rule is the real protocol defense against
cross-version fee reuse. The daemon's per-`(code_hash, hash_type)`
partition is defense-in-depth so an honest assembler never submits a
mixed-identity tx. TimeoutBurn stays exact.

Do **not** treat undeployed candidate hashes as production defaults; do **not**
replace current testnet `code_hash` with an undeployed binary. Operators
fill in the deployed identity after redeploy. Never list unpublished /
provisional data hashes as allowlist entries (examples only, not defaults:
`0x2edb827f…`, `0x9897167b…`).

### Historical note

An earlier candidate (`fcd33d9`) allowed capacity shortfall without tx-wide
fee binding; review found arbitrary outputs could siphon the haircut. PR #12
(`e0e7f18` → `d6c5a86`) binds haircuts to miner fee (`F >= S`) and restores
TimeoutBurn exact capacity. Head `9bc8193` adds on-chain `MixedCidLock`
rejection for mixed CID identities in one DigestIncompatible tx.

## Defaults (safe for existing deployments)

| Knob | Default | Effect |
|------|---------|--------|
| `cid_lock.bounded_refund_locks` / `CID_BOUNDED_REFUND_LOCKS` | **empty** | Exact User Lock capacity; fee 0 |
| `cid_lock.legacy_scan_locks` / `CID_LEGACY_SCAN_LOCKS` | empty | No extra scan targets |
| `loop.fee_rate` / `CID_FEE_RATE` | 1000 | Fee rate for allowlisted bounded refunds |

Empty allowlist ⇒ legacy exact for every Intention. Listing a lock on the
allowlist alone does **not** fix `-1104` against an old lock that still
demands exact capacity, and **does not** make the daemon scan that lock.
`bounded_refund_locks` selects fee mode only for identities already in the
scan set (primary or `legacy_scan_locks`). Config load fail-closes if an
allowlist entry is not a scanned identity.

## Enabling bounded refunds (after revised lock ships)

1. Deploy the audited CID lock that enforces PR #12 rules (OWNER redeploy).
2. Point primary CID identity at the **new** deployed lock — prefer
   `cid_lock.type_id_args` so scan `code_hash` is the Type-ID type-script hash
   with `hash_type=type` (CellDeps refresh across upgrades). Do not point the
   primary at a stale binary `data_hash` on Type-ID networks.
3. Add that **already-scanned** identity to `[[cid_lock.bounded_refund_locks]]`
   (or `CID_BOUNDED_REFUND_LOCKS=0x…:hash_type`). Never list undeployed
   candidates. Never use the allowlist as a scan source.
4. List old data-hash locks under `[[cid_lock.legacy_scan_locks]]` so the
   daemon continues draining those Intentions after redeploy; add them to
   `bounded_refund_locks` only if that legacy lock also accepts bounded fees.
5. **Legacy Data1/Data/Data2 code CellDep (required for drain):**
   `legacy_scan_locks` only adds an indexer query. Assembly still attaches
   the current Type-ID code cell (new binary) plus configured static deps.
   After an upgrade that changes the ELF, that Type-ID cell cannot locate a
   legacy Data1 lock (`code_hash` = old `data_hash`; see
   [RFC 0022 code locating](https://github.com/nervosnetwork/rfcs/blob/master/rfcs/0022-transaction-structure/0022-transaction-structure.md#code-locating)).
   Keep a **live** cell whose **exact** old bytes hash to that `data_hash`,
   and attach it as an unlabeled `[[cell_deps]]` / `CID_EXTRA_CELL_DEPS`
   entry (do **not** label it `cid_lock` — that conflicts with Type-ID
   discovery for the current role). Without that dep, FakeRpc may still
   “accept” the tx, but a real node rejects it.
6. The Process guest controls the fee for `digest_ok`.

## Assembly rules (bounded mode)

- Fee = `ceil(serialized_size_in_block * fee_rate / 1000)`.
- Cuts allocated left-to-right; each cut ≤
  `min(MAX_REFUND_FEE, capacity - occupied)`.
- `sum(cuts) == fee` exactly — **no siphon outputs** (implies contract
  `F >= S` when there are no extra outputs).
- Fail closed with soft `RetryLater` (`FeeUnaffordable`) if the budget is
  insufficient (non-permanent, no `rejected_combos`, no isolate-shrink;
  below-min leftovers retry refund once marked pending).
- Intentions whose lock is **not** on the allowlist always use exact refunds.
- **Never** mix allowlisted and legacy locks in one bounded refund tx.
- **Never** co-bundle two different bounded-capable lock identities in one
  Execution tx — the loop partitions DigestIncompatible assembly by
  `(code_hash, hash_type)` (defense-in-depth; on-chain `MixedCidLock` is
  the real defense). The allowlist may list multiple verified deployed
  identities after migration, but they must not share a refund tx.
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
