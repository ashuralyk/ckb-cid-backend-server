//! Shared Process busy/pending map for the registry HTTP API (P1-6).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Tracks which Process identities currently have pending occupancy.
#[derive(Clone, Default)]
pub struct ProcessBusyMap {
    inner: Arc<Mutex<HashSet<[u8; 32]>>>,
}

impl ProcessBusyMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pending(&self, pending: impl IntoIterator<Item = [u8; 32]>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *g = pending.into_iter().collect();
    }

    pub fn is_pending(&self, hash: &[u8; 32]) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(hash)
    }
}
