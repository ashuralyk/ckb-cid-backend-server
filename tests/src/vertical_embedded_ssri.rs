//! End-to-end worker tick with the real example Process (EmbeddedSsri / ckb-vm):
//! submitted tx bytes must equal `AssembledTx::as_molecule_bytes()` (backend fills
//! nothing — one witness per input from the guest).

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use cid_backend_core::digest_loop::{DigestLoop, NopClock};
    use cid_backend_core::process::ProcessResolver;
    use cid_backend_core::rpc::script_hash32;
    use cid_backend_core::ssri::{EmbeddedSsri, SsriError, SsriHost};
    use cid_process_executor::{AssembledTx, MockHost};
    use cid_protocol::{CidArgs, Payload};
    use ckb_types::prelude::*;

    use crate::fake_rpc::FakeRpc;
    use crate::helpers::{
        default_loop_config, fixture_process_entry, intention_cell, process_cell,
        process_type_script, user_lock,
    };

    fn load_example_binary() -> Option<Vec<u8>> {
        if let Ok(path) = std::env::var("CID_PROCESS_EXAMPLE_PATH") {
            if let Ok(bytes) = std::fs::read(&path) {
                if bytes.len() >= 4 && bytes[0] == 0x7f && &bytes[1..4] == b"ELF" {
                    return Some(bytes);
                }
            }
        }
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/cid_process_example");
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() >= 4 && bytes[0] == 0x7f && &bytes[1..4] == b"ELF" {
            Some(bytes)
        } else {
            None
        }
    }

    /// Records the last successful assemble molecule bytes for byte-identical checks.
    struct RecordingSsri {
        inner: EmbeddedSsri,
        last: Mutex<Option<Vec<u8>>>,
    }

    impl RecordingSsri {
        fn new() -> Self {
            Self {
                inner: EmbeddedSsri,
                last: Mutex::new(None),
            }
        }

        fn last_assembled(&self) -> Option<Vec<u8>> {
            self.last.lock().unwrap().clone()
        }
    }

    impl SsriHost for RecordingSsri {
        fn filter(&self, binary: &[u8], request: &[u8]) -> Result<Vec<i8>, SsriError> {
            self.inner.filter(binary, request)
        }

        fn get_batch_bounds(&self, binary: &[u8]) -> Result<(u32, u32), SsriError> {
            self.inner.get_batch_bounds(binary)
        }

        fn assemble(
            &self,
            binary: &[u8],
            draft_tx: &[u8],
            host: MockHost,
        ) -> Result<AssembledTx, SsriError> {
            let assembled = self.inner.assemble(binary, draft_tx, host)?;
            *self.last.lock().unwrap() = Some(assembled.as_molecule_bytes().to_vec());
            Ok(assembled)
        }
    }

    #[test]
    fn embedded_ssri_worker_submits_byte_identical_full_tx() {
        let Some(bin) = load_example_binary() else {
            eprintln!("cid_process_example missing; skipping embedded SSRI e2e");
            return;
        };

        let rpc = FakeRpc::new();
        rpc.set_tip(0);
        rpc.insert_block_hash(0, [0xAB; 32]);
        let ts = process_type_script(1);
        let hash = script_hash32(&ts);
        let mut intent = intention_cell(1, hash, 600_0000_0000);
        let user = user_lock();
        let payload = Payload::from_script(&user).unwrap();
        let args = CidArgs::new(hash, user, payload).encode().unwrap();
        let lock = intent.lock().as_builder().args(args.pack()).build();
        intent.output = intent.output.as_builder().lock(lock).build();
        // Capacity must leave room for Process fee = floor((len+4)*1000/1000).
        let proc = process_cell(1, 500_0000_0000, vec![]);
        rpc.insert_cell(intent);
        rpc.insert_cell(proc);

        let entry = fixture_process_entry(1);
        let mut loop_ = DigestLoop::new_with_resolver(
            rpc,
            NopClock,
            RecordingSsri::new(),
            default_loop_config(),
            ProcessResolver::for_entry(entry).with_fallback_binary(bin),
        );

        let report = loop_.tick();
        assert_eq!(
            report.submitted, 1,
            "expected digest_ok submit: {:?}",
            report.last_error
        );
        let submitted = loop_.rpc().submitted();
        assert_eq!(submitted.len(), 1);
        let assembled = loop_
            .ssri()
            .last_assembled()
            .expect("assemble must have recorded molecule bytes");
        assert_eq!(
            submitted[0].data().as_slice(),
            assembled.as_slice(),
            "submitted tx must be byte-identical to Process.assemble output"
        );
        assert_eq!(
            submitted[0].witnesses().len(),
            submitted[0].inputs().len(),
            "one witness per input; backend fills nothing"
        );
        // Guest-owned fee: process recreate capacity < input capacity.
        let proc_out_cap: u64 = submitted[0].outputs().get(0).unwrap().capacity().unpack();
        assert!(
            proc_out_cap < 500_0000_0000,
            "Process must deduct floor((len+4)*1000/1000) fee from recreate"
        );
    }
}
