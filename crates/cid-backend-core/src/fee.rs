//! Execution fee helpers for the keyless digest daemon.
//!
//! Fee is paid by reducing the recreated Process state output capacity
//! (`digest_ok` only). There is no fee input and beneficiary/refund outputs
//! are never reduced.
//!
//! CKB tx-pool admission uses `serialized_size_in_block` (molecule bytes +
//! 4-byte vector offset) and `floor(fee_rate * size / 1000)`. We charge
//! `ceil(size * fee_rate / 1000)` so we never underpay that check.
//!
//! Changing only a capacity `uint64` does not change serialized size, so one
//! measure-after-structure pass is stable (no fixed-point iteration needed).
//!
//! ## Incompatible / refund path
//!
//! `assemble_digest_incompatible` does not spend Process (User Lock exact
//! refund). Including Process to fund the fee would put the Process type hash
//! in both I/O; the Process example contract then classifies matching
//! Intentions as DigestOk (beneficiary obligations), not user_lock refunds.
//! Refunds therefore remain zero-fee; public nodes with `min_fee_rate > 0`
//! may reject them until a contract-compatible fee source exists.

use ckb_types::core::{Capacity, TransactionView};
use ckb_types::packed::CellOutput;
use ckb_types::prelude::*;

/// Default fee rate: 1000 shannons per 1000 bytes (KW), matching typical
/// public-node `min_fee_rate`.
pub const DEFAULT_FEE_RATE: u64 = 1000;

/// Inclusive upper bound for configured `fee_rate` (shannons/KW).
/// 10_000_000 ≈ 10k shannons/byte — far above any realistic relay policy.
pub const MAX_FEE_RATE: u64 = 10_000_000;

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
}
