//! Execution fee helpers for the keyless digest daemon.
//!
//! ## `digest_ok`
//!
//! Fee is paid by reducing the recreated Process state output capacity.
//! There is no fee input and beneficiary outputs are never reduced.
//!
//! ## `digest_incompatible` refunds
//!
//! Two modes, selected per Intention by an **explicit allowlist** of audited
//! CID lock identities ([`CidLockIdentity`]), **defaulting to legacy
//! exact-refund** when the allowlist is empty or the Intention's lock is
//! absent:
//!
//! - [`RefundFeeMode::Exact`] (default): User Lock capacity copied exactly;
//!   paid fee is always 0. Required for currently deployed CID locks that
//!   enforce exact capacity.
//! - [`RefundFeeMode::Bounded`]: each refund output may burn at most
//!   [`MAX_REFUND_FEE`] shannons (never below occupied). Cuts are allocated
//!   so **`sum(cuts) == miner fee`** (no siphon outputs). Requires an
//!   OWNER-redeployed CID lock that accepts the window **and** that lock's
//!   `(code_hash, hash_type)` on the allowlist.
//!
//! Never mix allowlisted and legacy locks in one bounded refund tx —
//! partition into homogeneous groups first.
//!
//! ## Contract rules (PR #12 reviewed)
//!
//! Reviewed contract head (or newer): `d6c5a86` on
//! `ckb-common-intention-dispatcher` PR #12. On-chain DigestIncompatible
//! requires per-Intention ceiling via [`refund_capacity_allowed`] and
//! **tx-wide** `F >= S` (`fee_covers_shortfalls`: miner fee covers aggregate
//! shortfall so shortfalls cannot fund other outputs). `S` / DigestOk
//! counting is scoped to the executing lock's `code_hash` + `hash_type`
//! (not cross-deployment). TimeoutBurn stays exact. Backend assembly uses
//! the stronger `sum(cuts) == F` with no extra outputs, which implies
//! `F >= S`.
//!
//! ### Historical note
//!
//! An earlier candidate (`fcd33d9`) allowed capacity shortfall without
//! tx-wide fee binding; review found arbitrary outputs could siphon the
//! haircut. That finding is addressed by PR #12's `fee_covers_shortfalls`.
//! Do **not** treat undeployed candidate hashes as production defaults
//! (including provisional data hashes such as `0x2edb827f…`).
//!
//! ## Process example / DigestOk co-bundling
//!
//! When a Process input is present (`DigestOk`), the Process example requires
//! **exact** capacity/data/type on User Lock obligations. Fee-shaved
//! Incompatible refunds must be separate transactions (no Process input).
//!
//! ## TimeoutBurn
//!
//! Remains exact; this daemon never assembles TimeoutBurn and must not apply
//! the refund fee window to that user-signed path.
//!
//! ## Fee sizing
//!
//! CKB tx-pool admission uses `serialized_size_in_block` (molecule bytes +
//! 4-byte vector offset) and `floor(fee_rate * size / 1000)`. We charge
//! `ceil(size * fee_rate / 1000)`. Capacity is fixed-width so one
//! measure-after-structure pass is stable. Submit must re-check the sized
//! skeleton (see [`FeeGuard`]) — empty-witness sizing is only valid if later
//! stages add no witnesses/I/O. Real-node `min_fee_rate` acceptance is a
//! **release gate** (not exercised in FakeRpc CI).

use ckb_types::core::{Capacity, ScriptHashType, TransactionView};
use ckb_types::packed::CellOutput;
use ckb_types::prelude::*;

/// Default fee rate: 1000 shannons per 1000 bytes (KW), matching typical
/// public-node `min_fee_rate`.
pub const DEFAULT_FEE_RATE: u64 = 1000;

/// Inclusive upper bound for configured `fee_rate` (shannons/KW).
/// 10_000_000 ≈ 10k shannons/byte — far above any realistic relay policy.
pub const MAX_FEE_RATE: u64 = 10_000_000;

/// Maximum CKB capacity (shannons) a **bounded** refund output may fall short
/// of its Intention. Dual-written with contracts `cid-protocol`
/// (`MAX_REFUND_FEE = 1_000_000` = 0.01 CKB). Workspace still pins the legacy
/// protocol crate; keep this mirror in sync when the pin moves.
///
/// Bound is **per Intention** (each refund output independently).
pub const MAX_REFUND_FEE: u64 = 1_000_000;

/// Audited CID lock script identity that may use bounded refund fee mode.
///
/// Operators must list only **deployed** hashes that enforce PR #12 rules
/// (`refund_capacity_allowed` + `fee_covers_shortfalls`). Never insert an
/// undeployed candidate as a default allowlist entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CidLockIdentity {
    pub code_hash: [u8; 32],
    pub hash_type: ScriptHashType,
}

impl CidLockIdentity {
    pub fn new(code_hash: [u8; 32], hash_type: ScriptHashType) -> Self {
        Self {
            code_hash,
            hash_type,
        }
    }
}

/// How Incompatible User Lock refunds fund the miner fee.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RefundFeeMode {
    /// Exact Intention capacity on every refund output (legacy). Fee = 0.
    #[default]
    Exact,
    /// Shortfall ≤ [`MAX_REFUND_FEE`] per output; `sum(cuts) == miner fee`.
    /// Only for allowlisted redeployed lock identities.
    Bounded,
}

/// Snapshot of fee sizing used at assemble time. Submit must confirm the
/// transaction size is unchanged and paid fee still covers `fee_rate`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeeGuard {
    pub sized_size: u64,
    pub fee_rate: u64,
    pub paid_fee: u64,
}

impl FeeGuard {
    /// Fail closed if the tx grew or paid fee no longer covers the rate.
    pub fn verify(&self, tx: &TransactionView) -> Result<(), FeeGuardError> {
        let size_now = tx_size_in_block(tx);
        if size_now != self.sized_size {
            return Err(FeeGuardError::SizeChanged {
                sized: self.sized_size,
                actual: size_now,
            });
        }
        let need = fee_for_size(size_now, self.fee_rate);
        if self.paid_fee < need {
            return Err(FeeGuardError::FeeShort {
                paid: self.paid_fee,
                need,
                size: size_now,
                fee_rate: self.fee_rate,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeGuardError {
    SizeChanged {
        sized: u64,
        actual: u64,
    },
    FeeShort {
        paid: u64,
        need: u64,
        size: u64,
        fee_rate: u64,
    },
}

impl std::fmt::Display for FeeGuardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeeGuardError::SizeChanged { sized, actual } => {
                write!(
                    f,
                    "tx size changed after fee sizing sized={sized} actual={actual}"
                )
            }
            FeeGuardError::FeeShort {
                paid,
                need,
                size,
                fee_rate,
            } => write!(
                f,
                "fee short at submit paid={paid} need={need} size={size} fee_rate={fee_rate}"
            ),
        }
    }
}

impl std::error::Error for FeeGuardError {}

/// True when `id` is on the bounded-refund allowlist.
pub fn lock_allows_bounded_refund(allowlist: &[CidLockIdentity], id: &CidLockIdentity) -> bool {
    allowlist.iter().any(|a| a == id)
}

/// True when a refund output's CKB capacity is within the allowed fee window
/// (mirrors contracts `cid_protocol::refund_capacity_allowed`).
pub fn refund_capacity_allowed(got: u64, want: u64, occupied: u64) -> bool {
    if got > want || got < occupied {
        return false;
    }
    got >= want.saturating_sub(MAX_REFUND_FEE)
}

/// Mirrors contracts `fee_covers_shortfalls`: miner fee `F` must cover
/// aggregate DigestIncompatible shortfall `S` (`F >= S`).
pub fn fee_covers_shortfalls(fee: u64, total_shortfall: u64) -> bool {
    fee >= total_shortfall
}

/// Deterministic left-to-right allocation of `fee` across per-output budgets.
///
/// Returns cuts whose sum equals `fee`, or `Err(shortfall)` if budgets cannot
/// cover `fee`.
pub fn allocate_refund_fee_cuts(budgets: &[u64], fee: u64) -> Result<Vec<u64>, u64> {
    let mut remaining = fee;
    let mut cuts = vec![0u64; budgets.len()];
    for (i, &budget) in budgets.iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let cut = remaining.min(budget);
        cuts[i] = cut;
        remaining -= cut;
    }
    if remaining > 0 {
        Err(remaining)
    } else {
        debug_assert_eq!(cuts.iter().sum::<u64>(), fee);
        Ok(cuts)
    }
}

/// Maximum fee this refund output can contribute under bounded mode.
pub fn refund_fee_budget(capacity: u64, occupied: u64) -> u64 {
    let headroom = capacity.saturating_sub(occupied);
    headroom.min(MAX_REFUND_FEE)
}

/// `ceil(size * fee_rate / 1000)` in shannons.
pub fn fee_for_size(size: u64, fee_rate: u64) -> u64 {
    if size == 0 || fee_rate == 0 {
        return 0;
    }
    let num = (size as u128).saturating_mul(fee_rate as u128);
    num.div_ceil(1000) as u64
}

/// Molecule size of `tx` as counted in a block (includes the 4-byte offset).
pub fn tx_size_in_block(tx: &TransactionView) -> u64 {
    tx.data().serialized_size_in_block() as u64
}

/// Required fee for `tx` at `fee_rate` (ceil).
pub fn required_fee(tx: &TransactionView, fee_rate: u64) -> u64 {
    fee_for_size(tx_size_in_block(tx), fee_rate)
}

/// Minimum capacity the Process recreate must keep (occupied capacity for
/// its lock/type/data). Returns `None` only if capacity arithmetic overflows
/// (pathological cell); callers treat that as unaffordable.
pub fn process_min_capacity(output: &CellOutput, data: &[u8]) -> Option<u64> {
    let data_cap = Capacity::bytes(data.len()).ok()?;
    output.occupied_capacity(data_cap).ok().map(|c| c.as_u64())
}

/// Occupied capacity for a refund (or any) cell output.
pub fn cell_occupied_capacity(output: &CellOutput, data: &[u8]) -> Option<u64> {
    process_min_capacity(output, data)
}

/// Replace `output`'s capacity field (lock/type/data unchanged).
pub fn with_capacity(output: &CellOutput, capacity: u64) -> CellOutput {
    output
        .clone()
        .as_builder()
        .capacity(capacity.pack())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_for_size_ceils() {
        assert_eq!(fee_for_size(0, 1000), 0);
        assert_eq!(fee_for_size(574, 1000), 574);
        assert_eq!(fee_for_size(574, 1500), 861);
        assert_eq!(fee_for_size(574, 1), 1);
        assert_eq!(fee_for_size(1000, 1), 1);
        assert_eq!(fee_for_size(1001, 1), 2);
    }

    #[test]
    fn fee_for_size_matches_ckb_floor_when_rate_is_1000() {
        for size in [1u64, 573, 574, 601, 4096, 100_000] {
            assert_eq!(fee_for_size(size, 1000), size);
        }
    }

    #[test]
    fn fee_for_size_never_underpays_ckb_feerate_floor() {
        use ckb_types::core::FeeRate;
        for rate in [1u64, 999, 1000, 1001, 1500, 2500, 10_000] {
            for size in [1u64, 17, 573, 574, 601, 1000, 1001, 4096, 50_000] {
                let ours = fee_for_size(size, rate);
                let ckb = FeeRate::from_u64(rate).fee(size).as_u64();
                assert!(
                    ours >= ckb,
                    "size={size} rate={rate}: ceil={ours} < ckb_floor={ckb}"
                );
            }
        }
    }

    #[test]
    fn refund_capacity_allowed_mirrors_contract_rules() {
        let occupied = 61_0000_0000u64;
        let want = occupied + 10_0000_0000;
        assert!(refund_capacity_allowed(want, want, occupied));
        assert!(refund_capacity_allowed(
            want - MAX_REFUND_FEE,
            want,
            occupied
        ));
        assert!(!refund_capacity_allowed(
            want - MAX_REFUND_FEE - 1,
            want,
            occupied
        ));
        assert!(!refund_capacity_allowed(want + 1, want, occupied));
        assert!(!refund_capacity_allowed(occupied - 1, want, occupied));
    }

    #[test]
    fn fee_covers_shortfalls_mirrors_contract() {
        assert!(fee_covers_shortfalls(MAX_REFUND_FEE, MAX_REFUND_FEE));
        assert!(fee_covers_shortfalls(MAX_REFUND_FEE + 1, MAX_REFUND_FEE));
        assert!(!fee_covers_shortfalls(MAX_REFUND_FEE - 1, MAX_REFUND_FEE));
        assert!(!fee_covers_shortfalls(MAX_REFUND_FEE, MAX_REFUND_FEE * 2));
    }

    #[test]
    fn allocate_refund_fee_cuts_exact_and_fail_closed() {
        assert_eq!(
            allocate_refund_fee_cuts(&[100, 100], 150).unwrap(),
            vec![100, 50]
        );
        assert_eq!(allocate_refund_fee_cuts(&[10, 10], 25).unwrap_err(), 5);
    }

    #[test]
    fn refund_fee_budget_respects_cap_and_occupied() {
        assert_eq!(refund_fee_budget(100, 50), 50.min(MAX_REFUND_FEE));
        assert_eq!(
            refund_fee_budget(MAX_REFUND_FEE + 10_000, 0),
            MAX_REFUND_FEE
        );
        assert_eq!(refund_fee_budget(50, 50), 0);
    }

    #[test]
    fn refund_fee_mode_defaults_exact() {
        assert_eq!(RefundFeeMode::default(), RefundFeeMode::Exact);
    }

    #[test]
    fn lock_allowlist_empty_denies_all() {
        let id = CidLockIdentity::new([0xC1; 32], ScriptHashType::Type);
        assert!(!lock_allows_bounded_refund(&[], &id));
        assert!(lock_allows_bounded_refund(&[id], &id));
        assert!(!lock_allows_bounded_refund(
            &[CidLockIdentity::new([0xC2; 32], ScriptHashType::Type)],
            &id
        ));
    }

    #[test]
    fn fee_guard_rejects_size_change_and_short_fee() {
        use ckb_types::core::TransactionBuilder;
        let tx = TransactionBuilder::default().build();
        let size = tx_size_in_block(&tx);
        let guard = FeeGuard {
            sized_size: size,
            fee_rate: 1000,
            paid_fee: size,
        };
        assert!(guard.verify(&tx).is_ok());
        let bad = FeeGuard {
            sized_size: size + 1,
            fee_rate: 1000,
            paid_fee: size,
        };
        assert!(matches!(
            bad.verify(&tx),
            Err(FeeGuardError::SizeChanged { .. })
        ));
        let short = FeeGuard {
            sized_size: size,
            fee_rate: 1000,
            paid_fee: size.saturating_sub(1),
        };
        assert!(matches!(
            short.verify(&tx),
            Err(FeeGuardError::FeeShort { .. })
        ));
    }
}
