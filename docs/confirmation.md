# Confirmation, reorg, and retry (phase 3)

Digest correctness of assemble/submit is unchanged. All of this bookkeeping is
**memory-only**: a restart drops in-flight occupancy, confirmation tracking, and
rejected combos. The next tick is a full indexer rescan.

## Confirmation depth

A `Committed` execution tx is **not** settled on first sight. Occupancy
(Process + Intention `in_flight` / `pending`) stays held until:

```
confirmations = 0                    if tip < commit_block
              = tip - commit_block + 1   otherwise
confirmations >= CID_CONFIRMATION_DEPTH
```

| Env | Default | Meaning |
|-----|---------|---------|
| `CID_CONFIRMATION_DEPTH` | `24` | Must be ≥ 1. `1` settles when the tx is committed in the current tip block. |

24 CKB blocks is a conservative default (~3 minutes at ~8s/block). Local tests
use `1` so existing FakeRpc verticals keep settling on first `Committed` at the
tip. The ignored local-chain E2E uses `1` as well.

While confirming, occupancy covers both the spent inputs **and** the tx's
outputs (the Process recreate). A later `digest_ok` that would spend that
recreate is skipped until settlement (`process_occupied`). Incompatible User
Lock refunds still submit; they do not spend Process.

## Reorg recovery

| Observation | Action |
|-------------|--------|
| Tip rewinds (`tip < last_tip`) | Log `reorg.tip_rewind`. Confirmation count drops. Occupancy stays held until depth is met again. |
| Confirming or committed tx becomes `Dropped` | Log `pending.reorg` (if we had seen Committed) or `pending.dropped`. Release occupancy. If cells are live, the next tick retries. |
| Status or tip RPC unavailable | Keep occupancy. Do not settle. |

After a restart there is no pending set, so a still-mempool tx may be submitted
again (same as v0 D6). Cells already consumed surface as `Conflict`.

A reorg deeper than `CID_CONFIRMATION_DEPTH` after settlement is accepted
residual risk: occupancy is already released; a fresh scan retries if the
cells are live again.

## Retry vs permanent failure

The adapter keeps the JSON-RPC `error.code` from `send_transaction` and
classifies it. Transient node/pool errors are `Unavailable`. Core
`SubmitRejected` is a **transaction-level** permanent failure of that exact
Intention combo (plus Process outpoint when the tx spends Process). It does
**not** prove any one Intention is invalid, so consumed outpoints are not
written to `rejected`.

| Outcome | Retry? | Occupancy |
|---------|--------|-----------|
| `Dropped` (pool eviction / reorg) | yes, next tick if cells live | released |
| RPC/indexer `Unavailable` | yes | held if already pending; not taken on a failed submit |
| `Conflict` | yes, next tick if cells live | not taken (`failed` is informational) |
| `SubmitRejected` (positively identified permanent send failure of this tx) | isolate / skip that **combo** this process lifetime | not taken; Process outpoint is not blacklisted |
| SSRI `Incompatible` | refund via User Lock (unchanged) | — |

**Combo isolation.** A `SubmitRejected` batch is recorded as a fingerprint of
the Intention outpoints in order plus the Process outpoint if `digest_ok`
spent it (refunds omit Process). The same combo is not submitted again until
restart (or a new Process outpoint). Within the tick, the window is split
into legal `[min, max]` sub-batches (shrink prefix; drop-first remainder)
so a valid sibling can still submit when `min = 1`.

At most 64 **new** `send_transaction` attempts per bucket per tick. Hits on
`rejected_combos` do not consume that budget. Before SSRI assemble, the
digest_ok fingerprint (Intention outpoints plus this Process outpoint) is
looked up; a hit skips assemble and submit. After assemble, the actual
fingerprint is checked (digest_ok includes Process; User Lock refunds omit
it) so a cached refund does not skip a digest_ok combo and vice versa.
Cache hits still walk/split so later Intentions can be reached on a later
tick. Isolation is iterative (no recursion on long prefixes). Remaining
work after the new-attempt budget is left for the next tick.

A singleton `{A, Process}` failure is **not** treated as proof that A is
permanently invalid (`-302` / `-1102` / `-1108` may come from Process, lock,
deps, or the combination). Only that combo is skipped. If the live Process
cell changes, A may be tried again.

**min > 1.** A rejected window is not written wholesale into `rejected`. If
the window cannot be split below `min`, members stay eligible as cells but
the **same combo** is not resubmitted. A valid B that only appears next to a
failing A in every min-sized window stays unsubmitted until the partner set
or Process outpoint changes, or the process restarts. That is accepted;
blacklisting B would be worse.

All of this is memory-only. Restarting the daemon drops combos and retries
from a full indexer rescan.

`send_transaction` code mapping (`CkbRpc::map_rpc_error`):

| Code | CKB error | Mapped to |
|------|-----------|-----------|
| `-301` | `TransactionFailedToResolve` | `Conflict` (also any message matching unknown-input / dead-cell / duplicate) |
| `-1107` | `PoolRejectedDuplicatedTransaction` | `Conflict` |
| `-302` | `TransactionFailedToVerify` | `SubmitRejected` |
| `-1102` | `PoolRejectedTransactionByOutputsValidator` | `SubmitRejected` |
| `-1108` | `PoolRejectedMalformedTransaction` | `SubmitRejected` |
| `-1106` | `PoolIsFull` | `Unavailable` (retry) |
| `-1105` | `PoolRejectedTransactionByMaxAncestorsCountLimit` | `Unavailable` (retry) |
| `-1104` | `PoolRejectedTransactionByMinFeeRate` | `Unavailable` (node policy; same tx may succeed if the node config changes) |
| other / missing code | — | `Unavailable` (do **not** blacklist an Intention on an unclassified error) |

A committed execution tx with a **known** `tx_status.block_number` of `0` is
genesis height and is used as-is. Missing/invalid `block_hash` or `get_header`
returning null is `Unavailable`, not height 0: occupancy stays held until a
real height is observed.

`rejected_combos` is memory-only. Restarting the daemon will try those
Intentions again.

## Logs

| `event` | When |
|---------|------|
| `pending.committed` | First sight of Committed (not yet settled) |
| `pending.confirming` | Depth not met (`debug`) |
| `pending.settled` | Confirmation depth reached; occupancy released from `pending` |
| `pending.dropped` | Pending tx gone before we saw Committed |
| `pending.reorg` | A previously committed/confirming tx is now Dropped |
| `reorg.tip_rewind` | Tip height moved backwards |
| `submit.rejected_combo` | Cached combo skipped without a new submit (`debug`) |
| `submit.rejected` | Transaction-level send_transaction reject (`-302` / `-1102` / `-1108`); combo skipped, sub-batches may still run |
