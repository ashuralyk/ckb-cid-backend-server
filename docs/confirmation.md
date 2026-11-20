# Confirmation, reorg, and retry (phase 3)

Digest correctness of assemble/submit is unchanged. All of this bookkeeping is
**memory-only**: a restart drops in-flight occupancy, confirmation tracking, and
rejected combos. The next tick is a full indexer rescan.

## Confirmation depth

A `Committed` execution tx is **not** settled on first sight. Occupancy
(Process + Intention `in_flight` / `pending`) stays held until the
committed height is still canonical **and**:

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

Before settling, the loop re-reads the tip after `get_transaction_status`
returns `Committed` and checks the canonical block hash at the reported commit
height. A rewind during reconciliation, a tip below the commit height, a
missing or mismatched hash, or an unavailable RPC keeps occupancy in place.
Settlement waits for a later tick with a canonical commit at sufficient depth.

While confirming, occupancy covers both the spent inputs **and** the tx's
outputs (the Process recreate). A later `digest_ok` that would spend that
recreate is skipped until settlement (`process_occupied`).

A filter-authorized User Lock refund spends the Intention and uses the live
Process state and code as cell dependencies. The worker defers the refund while
a pending digest consumes or creates its authorizing state. Once that digest
releases the state, eligible refunds get a turn before another digest. The refund
gate uses the validated Process lock args, a fresh filter reason of 4–7, matching
node snapshots, and the state/code quarantine. Refund eligibility is independent
of assembly progress. In the other direction, a pending refund blocks digest
submissions that would consume its state dependency.
The worker lifts that dependency interlock after the node reports the refund
committed, a fresh tip read places the tip at or above the commit height
without a rewind, and the canonical hash matches the reported commit block. The worker holds the interlock again
if these checks fail or the refund returns to the pool before settlement.

## Reorg recovery

| Observation | Action |
|-------------|--------|
| Tip rewinds (`tip < last_tip`) | Log `reorg.tip_rewind`. Confirmation count drops. Occupancy stays held until depth is met again. |
| Tip rewinds while reading a Committed status (stale start-of-tick tip, or `tip < commit height`) | Re-read tip before settling. Keep occupancy this tick (`pending.tip_rewind` / confirming). Do not settle against the stale height. |
| Confirming refund returns to `Pending` | Keep the Intention occupied and block digests that would spend the refund's Process state dependency. |
| Confirming or committed tx becomes `Dropped` | Log `pending.reorg` (if we had seen Committed) or `pending.dropped`. Release occupancy. If cells are live, the next tick retries. |
| Commit block missing or replaced at the same height | Keep occupancy and the refund dependency interlock. Log `pending.noncanonical`. |
| Status, tip, or block hash RPC unavailable | Keep occupancy until reconciliation succeeds. |

Exit 41 quarantines a Process state/code pair. A replacement pair can run
independently. The worker retains the old quarantine for its lifetime, so the
fault still blocks assembly and filter refunds if the pair returns in a reorg.

After a restart there is no pending set, so a still-mempool tx may be submitted
again (same as v0 D6). Cells already consumed surface as `Conflict`.

A reorg deeper than `CID_CONFIRMATION_DEPTH` after settlement is accepted
residual risk: occupancy is already released; a fresh scan retries if the
cells are live again.

## Retry vs permanent failure

The adapter keeps the JSON-RPC `error.code` from `send_transaction` and
classifies it. Transient node/pool errors are `Unavailable`. A send that
returns `Unavailable` reserves its inputs and outputs under the locally
computed transaction hash for status reconciliation. Refunds also retain
their authorizing state dependency. This covers timeouts, unusable receipts,
and duplicate sends. Core
`SubmitRejected` is a **transaction-level** permanent failure of that exact
Intention combo under its Process state and execution code. It does
**not** prove any one Intention is invalid, so consumed outpoints are not
written to `rejected`.

| Outcome | Retry? | Occupancy |
|---------|--------|-----------|
| `Dropped` (pool eviction / reorg) | yes, next tick if cells live | released |
| RPC/indexer `Unavailable` | yes | existing occupancy stays held |
| `send_transaction` returns `Unavailable` | after node status reports a drop | held under the local transaction hash through reconciliation |
| `Conflict` | yes. During assembly isolation, try later subsets and reconsider conflicts on the next search. | not taken (`failed` is informational) |
| `SubmitRejected` (positively identified permanent send failure of this tx) | isolate / skip that **combo** this process lifetime | not taken; Process outpoint is not blacklisted |
| `Process.assemble` error | log and try another batch, or quarantine the state/code pair on exit 41 | affected cells stay live |
| `Process.filter` reasons 4–7 | User Lock refund after matching node snapshots | Intention input occupied on successful submit |

**Combo isolation.** A rejected digest fingerprint includes the ordered
Intention outpoints, the spent Process state outpoint, and the execution code
identity. The worker tries smaller or shifted windows and searches the
remaining legal subsets, including noncontiguous batches.
A rejected filter refund fingerprints the singleton Intention, the authorizing
Process state dependency, and the execution code identity, with a tag for the
refund path. A replacement state has a different outpoint, so the worker can
reconsider the refund after a fresh filter verdict of 4–7 and matching node
snapshots for the Intention, replacement state, and code.

Each Process can dispatch at most 64 assembly worker jobs and consider at
most 64 subset probes per tick, including the initial window. A cache hit skips assembly and submission
for that combination. The worker uses the ELF from the registered Process
code cell and verifies the indexed Process state against the node before
filtering or assembly.

Each tick starts a fresh capped scan of the current filter-accepted candidates.
Large searches share the budget across legal batch sizes, sample arrivals early,
and vary the remaining candidate order by tick. Some combinations may repeatedly
fall outside the cap. This bounded-search starvation is accepted by design.

Candidate liveness is read when a probe reaches the cell and reused within that
search. Isolation allows up to one window plus 64 candidate reads per Process
per tick. A reached candidate is checked again on a later tick, so revived cells
can be used. An unavailable lookup defers the search.

Windows and probes select at most one input per full CID lock script. Probes
skip non-live Intentions and subsets with non-live guest-added funding inputs.
Adding an unselected Intention as funding rejects that attempt. The worker tries
another subset after a validation failure, dry-run or submit rejection, or
conflict when budget remains.

For the audited v0.1.4 example ELF, exit 37 on a Process state with zero fee
headroom pauses assembly until the state or Process code changes. Other guests
continue through capped isolation. Intentions excluded during assembly stay
live. The filter refund sweep considers up to 16 current verdicts per tick.

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
| `pending.tip_rewind` | Committed status observed a tip rewind; occupancy kept this tick |
| `submit.rejected_combo` | Cached combo skipped without a new submit (`debug`) |
| `submit.rejected` | Transaction-level send_transaction reject (`-302` / `-1102` / `-1108`); combo skipped, sub-batches may still run |
