use crate::rpc::{LiveCell, Rpc, RpcError};
use ckb_types::core::ScriptHashType;
use ckb_types::packed::{OutPoint, Script};
use ckb_types::prelude::*;
use cid_protocol::CidArgs;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ScannedIntention {
    pub out_point: OutPoint,
    pub capacity: u64,
    pub type_script: Option<Script>,
    pub data: Vec<u8>,
    pub lock_args: Vec<u8>,
    pub args: CidArgs,
}

pub fn decode_cells(cells: Vec<LiveCell>) -> Vec<ScannedIntention> {
    let mut out = Vec::new();
    for cell in cells {
        match decode_one(&cell) {
            Some(item) => out.push(item),
            None => {
                log::warn!(
                    "skipping intention with undecodable CID lock args, out_point={:?}",
                    cell.out_point
                );
            }
        }
    }
    out
}

fn decode_one(cell: &LiveCell) -> Option<ScannedIntention> {
    let lock_args: Vec<u8> = cell.lock().args().unpack();
    let args = CidArgs::decode(&lock_args).ok()?;
    Some(ScannedIntention {
        out_point: cell.out_point.clone(),
        capacity: cell.capacity(),
        type_script: cell.type_script(),
        data: cell.data.clone(),
        lock_args,
        args,
    })
}

pub fn scan_intentions<R: Rpc>(
    rpc: &R,
    cid_code_hash: &[u8; 32],
    hash_type: ScriptHashType,
) -> Result<Vec<ScannedIntention>, RpcError> {
    let cells = rpc.list_live_intentions(cid_code_hash, hash_type)?;
    Ok(decode_cells(cells))
}

pub fn bucket_by_process_type_hash(
    intentions: &[ScannedIntention],
) -> Vec<([u8; 32], Vec<ScannedIntention>)> {
    let mut first_seen: Vec<[u8; 32]> = Vec::new();
    let mut map: HashMap<[u8; 32], Vec<ScannedIntention>> = HashMap::new();
    for item in intentions {
        let hash = item.args.process_type_hash;
        if !map.contains_key(&hash) {
            first_seen.push(hash);
        }
        map.entry(hash).or_default().push(item.clone());
    }
    first_seen
        .into_iter()
        .map(|h| (h, map.remove(&h).unwrap_or_default()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::LiveCell;
    use ckb_types::core::ScriptHashType;
    use ckb_types::packed::{Byte32, CellOutput, OutPoint, Script};
    use ckb_types::prelude::*;
    use cid_protocol::{CidArgs, Payload, Script as CidScript};

    fn op(fill: u8) -> OutPoint {
        OutPoint::new_builder()
            .tx_hash(Byte32::from_slice(&[fill; 32]).unwrap())
            .index(0u32.pack())
            .build()
    }

    fn cell(fill: u8, args: Vec<u8>) -> LiveCell {
        let lock = Script::new_builder()
            .code_hash([0xC1u8; 32].pack())
            .hash_type(ScriptHashType::Type.into())
            .args(args.pack())
            .build();
        LiveCell {
            out_point: op(fill),
            output: CellOutput::new_builder()
                .capacity(100_0000_0000u64.pack())
                .lock(lock)
                .build(),
            data: vec![],
        }
    }

    fn user_lock() -> CidScript {
        CidScript {
            code_hash: [0x42; 32],
            hash_type: 1,
            args: vec![0x01],
        }
    }

    fn cid_args(process: [u8; 32]) -> Vec<u8> {
        CidArgs::new(process, user_lock(), Payload { data: vec![0xAA] })
            .encode()
            .unwrap()
    }

    #[test]
    fn decode_skips_bad_args_and_keeps_good() {
        let good = cell(1, cid_args([0x11; 32]));
        let bad = cell(2, vec![0x00; 8]);
        let good2 = cell(3, cid_args([0x22; 32]));
        let decoded = decode_cells(vec![good.clone(), bad, good2.clone()]);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].out_point, good.out_point);
        assert_eq!(decoded[0].args.process_type_hash, [0x11; 32]);
        assert_eq!(decoded[1].out_point, good2.out_point);
        assert_eq!(decoded[1].lock_args, cid_args([0x22; 32]));
    }

    #[test]
    fn bucket_preserves_asc_and_first_seen_hash_order() {
        let p_a = [0xAA; 32];
        let p_b = [0xBB; 32];
        let cells = vec![
            cell(1, cid_args(p_a)),
            cell(2, cid_args(p_b)),
            cell(3, cid_args(p_a)),
        ];
        let decoded = decode_cells(cells);
        let buckets = bucket_by_process_type_hash(&decoded);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].0, p_a);
        assert_eq!(buckets[1].0, p_b);
        assert_eq!(buckets[0].1.len(), 2);
        assert_eq!(
            buckets[0].1[0].out_point.tx_hash(),
            Byte32::from_slice(&[1u8; 32]).unwrap()
        );
        assert_eq!(
            buckets[0].1[1].out_point.tx_hash(),
            Byte32::from_slice(&[3u8; 32]).unwrap()
        );
        assert_eq!(buckets[1].1.len(), 1);
    }
}
