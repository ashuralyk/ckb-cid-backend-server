# Confirmation, reorg, and retry (phase 3)

Digest correctness of assemble/submit is unchanged. All of this bookkeeping is
**memory-only**: a restart drops in-flight occupancy, confirmation tracking, and
the permanent-reject set. The next tick is a full indexer rescan.

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
classifies it. Core `SubmitRejected` still means **permanent for this process
lifetime**; transient node/pool errors are mapped to `Unavailable` so they are
not written to `rejected`.

| Outcome | Retry? | Occupancy |
|---------|--------|-----------|
| `Dropped` (pool eviction / reorg) | yes, next tick if cells live | released |
| RPC/indexer `Unavailable` | yes | held if already pending; not taken on a failed submit |
| `Conflict` | yes, next tick if cells live | not taken (`failed` is informational) |
| `SubmitRejected` (positively identified permanent send failure) | **no** (this process lifetime) | not taken; outpoints go to `rejected` and are skipped so they cannot starve later Intentions in the bucket |
| SSRI `Incompatible` | refund via User Lock (unchanged) | — |

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

`rejected` is memory-only. Restarting the daemon will try those Intentions
again.

## Logs

| `event` | When |
|---------|------|
| `pending.committed` | First sight of Committed (not yet settled) |
| `pending.confirming` | Depth not met (`debug`) |
| `pending.settled` | Confirmation depth reached; occupancy released from `pending` |
| `pending.dropped` | Pending tx gone before we saw Committed |
| `pending.reorg` | A previously committed/confirming tx is now Dropped |
| `reorg.tip_rewind` | Tip height moved backwards |
| `submit.rejected` | Permanent send_transaction reject (`-302` / `-1102` / `-1108`) |
