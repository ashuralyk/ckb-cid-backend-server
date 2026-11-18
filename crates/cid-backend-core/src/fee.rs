//! Execution fee helpers for the keyless digest daemon.
//!
//! ## `digest_ok`
//!
//! Fee is paid by reducing the recreated Process state output capacity.
//! There is no fee input and beneficiary outputs are never reduced.
//!
//! ## `digest_incompatible` refunds
//!
//! Two modes, selected by explicit deployment/protocol capability config
//! ([`RefundFeeMode`]), **defaulting to legacy exact-refund**:
//!
//! - [`RefundFeeMode::Exact`] (default): User Lock capacity copied exactly;
//!   paid fee is always 0. Required for currently deployed CID locks that
//!   enforce exact `refund_assets_match`. Never auto-haircut old deployments.
//! - [`RefundFeeMode::Bounded`]: provisional candidate protocol — each refund
//!   output may burn at most [`MAX_REFUND_FEE`] shannons (and never below
//!   occupied capacity). Cuts are allocated deterministically across refund
//!   outputs so their **sum equals** the tx-size miner fee (no siphon
//!   outputs). Enabling this requires OWNER redeployment of a CID lock that
//!   accepts the window **and** an explicit config flag for that hash.
//!
//! **Contract candidate status (not final):** the provisional lock candidate
//! (`fcd33d9…`) is under independent security review. A known finding is that
//! arbitrary non-refund outputs can siphon the haircut unless the contract
//! enforces miner-fee-only accounting. Backend assembly already refuses
//! siphon outputs (cuts == fee), but do **not** treat the candidate as
//! production-safe or final until revised contract rules land.
//!
//! **Process example / DigestOk co-bundling:** when a Process input is present
//! (`DigestOk` path), the Process example requires **exact** capacity/data/type
//! on User Lock obligations (`verify.rs` claim path). Fee-shaved Incompatible
//! refunds must therefore be **separate transactions**: no Process input, and
//! never mixed into a DigestOk skeleton. This daemon already separates the
//! paths (SSRI Ok → Process fee; SSRI Incompatible → refund-only).
//!
//! **TimeoutBurn:** remains exact from this daemon's perspective — the keyless
//! loop never assembles TimeoutBurn, and must not apply the refund fee window
//! to that user-signed path.
//!
//! CKB tx-pool admission uses `serialized_size_in_block` (molecule bytes +
//! 4-byte vector offset) and `floor(fee_rate * size / 1000)`. We charge
//! `ceil(size * fee_rate / 1000)` so we never underpay that check.
//!
//! Changing only a capacity `uint64` does not change serialized size, so one
//! measure-after-structure pass is stable (no fixed-point iteration needed).

use ckb_types::core::{Capacity, TransactionView};
use ckb_types::packed::CellOutput;
use ckb_types::prelude::*;

/// Default fee rate: 1000 shannons per 1000 bytes (KW), matching typical
/// public-node `min_fee_rate`.
pub const DEFAULT_FEE_RATE: u64 = 1000;

/// Inclusive upper bound for configured `fee_rate` (shannons/KW).
/// 10_000_000 ≈ 10k shannons/byte — far above any realistic relay policy.
pub const MAX_FEE_RATE: u64 = 10_000_000;

/// Maximum CKB capacity (shannons) a **bounded** refund output may fall short
/// of its Intention. Dual-written with the contracts `cid-protocol` candidate
/// (`MAX_REFUND_FEE = 1_000_000` = 0.01 CKB). Workspace still pins the legacy
/// protocol crate; keep this mirror in sync when the pin moves.
///
/// Bound is **per Intention** (each refund output independently).
pub const MAX_REFUND_FEE: u64 = 1_000_000;

/// How Incompatible User Lock refunds fund the miner fee.
///
/// Defaults to [`RefundFeeMode::Exact`] so existing CID lock deployments keep
/// exact-refund behavior unless operators explicitly opt in after redeploy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RefundFeeMode {
    /// Exact Intention capacity on every refund output (legacy / current
    /// public deployments). Fee paid is always 0.
    #[default]
    Exact,
    /// Allow shortfall ≤ [`MAX_REFUND_FEE`] per refund output, never below
    /// occupied capacity; sum of capacity cuts must equal the miner fee.
    /// Provisional — see module docs. Requires redeployed lock + explicit
    /// config enable for that code hash.
    Bounded,
}

/// True when a refund output's CKB capacity is within the allowed fee window
/// (mirrors contracts `cid_protocol::refund_capacity_allowed`).
///
/// Rules:
/// - `got <= want` (no capacity inflation)
/// - `got >= want.saturating_sub(MAX_REFUND_FEE)` (bounded shortfall)
/// - `got >= occupied` (refund cell must remain liveable)
pub fn refund_capacity_allowed(got: u64, want: u64, occupied: u64) -> bool {
    if got > want || got < occupied {
        return false;
    }
    got >= want.saturating_sub(MAX_REFUND_FEE)
}

/// Deterministic left-to-right allocation of `fee` across per-output budgets.
///
/// Each index `i` contributes `min(remaining, budgets[i])`. Returns the cut
/// vector whose sum equals `fee`, or `Err(shortfall)` if the budgets cannot
/// cover `fee` (fail closed — caller must not submit a partial-fee tx).
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
        // rate 1500: ceil(574 * 1500 / 1000) = ceil(861) = 861
        assert_eq!(fee_for_size(574, 1500), 861);
        // rate 1: ceil(574 / 1000) = 1
        assert_eq!(fee_for_size(574, 1), 1);
        // rate 1: ceil(1000 / 1000) = 1
        assert_eq!(fee_for_size(1000, 1), 1);
        // rate 1: ceil(1001 / 1000) = 2
        assert_eq!(fee_for_size(1001, 1), 2);
    }

    #[test]
    fn fee_for_size_matches_ckb_floor_when_rate_is_1000() {
        // With fee_rate == 1000, ceil and floor of size*rate/1000 coincide.
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
        assert!(refund_capacity_allowed(
            want - MAX_REFUND_FEE / 2,
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
        let tight = occupied + MAX_REFUND_FEE / 2;
        assert!(refund_capacity_allowed(occupied, tight, occupied));
        assert!(!refund_capacity_allowed(occupied - 1, tight, occupied));
    }

    #[test]
    fn allocate_refund_fee_cuts_exact_and_fail_closed() {
        assert_eq!(allocate_refund_fee_cuts(&[100, 100], 150).unwrap(), vec![
            100, 50
        ]);
        assert_eq!(allocate_refund_fee_cuts(&[40, 40, 40], 90).unwrap(), vec![
            40, 40, 10
        ]);
        assert_eq!(allocate_refund_fee_cuts(&[5], 0).unwrap(), vec![0]);
        assert_eq!(allocate_refund_fee_cuts(&[10, 10], 25).unwrap_err(), 5);
        let cuts = allocate_refund_fee_cuts(&[MAX_REFUND_FEE, MAX_REFUND_FEE], 1_200_000).unwrap();
        assert_eq!(cuts.iter().sum::<u64>(), 1_200_000);
        assert!(cuts.iter().all(|&c| c <= MAX_REFUND_FEE));
    }

    #[test]
    fn refund_fee_budget_respects_cap_and_occupied() {
        assert_eq!(refund_fee_budget(100, 50), 50.min(MAX_REFUND_FEE));
        assert_eq!(
            refund_fee_budget(MAX_REFUND_FEE + 10_000, 0),
            MAX_REFUND_FEE
        );
        assert_eq!(refund_fee_budget(50, 50), 0);
        assert_eq!(refund_fee_budget(40, 50), 0);
    }

    #[test]
    fn refund_fee_mode_defaults_exact() {
        assert_eq!(RefundFeeMode::default(), RefundFeeMode::Exact);
    }
}
