//! One logical worker per configured Process. Owns in-flight Process tracking for that Process.

use crate::process::{ProcessEntry, ProcessResolver};
use crate::submit::SubmitState;
use std::collections::{HashMap, HashSet};

/// One logical worker per configured Process. Owns in-flight Process tracking for that Process.
pub struct ProcessWorker {
    pub entry: ProcessEntry,
    pub resolver: ProcessResolver,
    pub submit: SubmitState,
    pub isolate_resume: Option<Vec<u8>>,
    pub isolate_expanded: HashSet<Vec<u8>>,
}

impl ProcessWorker {
    pub fn new(entry: ProcessEntry) -> Self {
        let resolver = ProcessResolver::for_entry(entry.clone());
        Self {
            entry,
            resolver,
            submit: SubmitState::default(),
            isolate_resume: None,
            isolate_expanded: HashSet::new(),
        }
    }

    pub fn process_type_hash(&self) -> [u8; 32] {
        self.entry.identity_type_hash
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerRemoveError {
    InFlight,
}

pub struct WorkerPool {
    workers: HashMap<[u8; 32], ProcessWorker>,
}

impl WorkerPool {
    pub fn from_entries(entries: Vec<ProcessEntry>) -> Self {
        let mut workers = HashMap::new();
        for entry in entries {
            let key = entry.identity_type_hash;
            if workers.contains_key(&key) {
                log::warn!(
                    "duplicate ProcessEntry for type hash {:02x?}; keeping first",
                    key
                );
                continue;
            }
            workers.insert(key, ProcessWorker::new(entry));
        }
        Self { workers }
    }

    pub fn get_mut(&mut self, process_type_hash: &[u8; 32]) -> Option<&mut ProcessWorker> {
        self.workers.get_mut(process_type_hash)
    }

    pub fn get(&self, process_type_hash: &[u8; 32]) -> Option<&ProcessWorker> {
        self.workers.get(process_type_hash)
    }

    pub fn for_each_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut ProcessWorker),
    {
        for w in self.workers.values_mut() {
            f(w);
        }
    }

    /// Insert or update a Process worker. Returns `true` when the worker is newly created.
    pub fn register(&mut self, entry: ProcessEntry) -> bool {
        let key = entry.identity_type_hash;
        match self.workers.get_mut(&key) {
            None => {
                self.workers.insert(key, ProcessWorker::new(entry));
                true
            }
            Some(worker) => {
                let code_changed = worker.entry.code != entry.code;
                worker.entry = entry.clone();
                worker.resolver = ProcessResolver::for_entry(entry);
                if code_changed {
                    worker.submit.rejected_combos.clear();
                    worker.isolate_expanded.clear();
                }
                false
            }
        }
    }

    pub fn try_remove(
        &mut self,
        hash: &[u8; 32],
    ) -> Result<Option<ProcessWorker>, WorkerRemoveError> {
        let Some(worker) = self.workers.get(hash) else {
            return Ok(None);
        };
        if !worker.submit.pending.is_empty() {
            return Err(WorkerRemoveError::InFlight);
        }
        Ok(self.workers.remove(hash))
    }

    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.workers.contains_key(hash)
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    pub fn hashes(&self) -> Vec<[u8; 32]> {
        let mut keys: Vec<_> = self.workers.keys().copied().collect();
        keys.sort_by(|a, b| a.cmp(b));
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_id::{type_id_script_hash, TypeIdIdentity};

    fn entry(state_args: [u8; 32], code_args: [u8; 32]) -> ProcessEntry {
        ProcessEntry {
            identity_type_hash: type_id_script_hash(&state_args),
            identity: TypeIdIdentity::new(state_args),
            code: TypeIdIdentity::new(code_args),
            ssri_elf_path: None,
        }
    }

    #[test]
    fn register_new_and_update_preserves_submit_when_code_same() {
        let mut pool = WorkerPool::from_entries(vec![]);
        let e1 = entry([0x01; 32], [0x02; 32]);
        let hash = e1.identity_type_hash;
        assert!(pool.register(e1.clone()));
        pool.get_mut(&hash).unwrap().submit.in_flight.insert(vec![0xaa]);
        pool.get_mut(&hash).unwrap().isolate_resume = Some(vec![0xbb]);

        let mut e1b = e1.clone();
        e1b.ssri_elf_path = None;
        assert!(!pool.register(e1b));
        let w = pool.get(&hash).unwrap();
        assert!(w.submit.in_flight.contains(&vec![0xaa]));
        assert_eq!(w.isolate_resume.as_deref(), Some(&[0xbb][..]));
    }

    #[test]
    fn register_code_change_clears_rejected_combos_and_isolate_expanded() {
        let mut pool = WorkerPool::from_entries(vec![entry([0x10; 32], [0x20; 32])]);
        let hash = type_id_script_hash(&[0x10; 32]);
        {
            let w = pool.get_mut(&hash).unwrap();
            w.submit.rejected_combos.insert(vec![0x01]);
            w.isolate_expanded.insert(vec![0x02]);
        }
        assert!(!pool.register(entry([0x10; 32], [0x21; 32])));
        let w = pool.get(&hash).unwrap();
        assert!(w.submit.rejected_combos.is_empty());
        assert!(w.isolate_expanded.is_empty());
    }

    #[test]
    fn try_remove_in_flight_when_pending_nonempty() {
        let mut pool = WorkerPool::from_entries(vec![entry([0x30; 32], [0x31; 32])]);
        let hash = type_id_script_hash(&[0x30; 32]);
        pool.get_mut(&hash).unwrap().submit.pending.insert(
            vec![0x99],
            crate::submit::PendingTx {
                consumed: vec![],
                created: vec![],
                seen_committed: false,
            },
        );
        assert!(matches!(
            pool.try_remove(&hash),
            Err(WorkerRemoveError::InFlight)
        ));
        assert!(pool.contains(&hash));
    }

    #[test]
    fn try_remove_ok_when_idle() {
        let mut pool = WorkerPool::from_entries(vec![entry([0x40; 32], [0x41; 32])]);
        let hash = type_id_script_hash(&[0x40; 32]);
        assert!(pool.try_remove(&hash).unwrap().is_some());
        assert!(!pool.contains(&hash));
        assert!(pool.try_remove(&hash).unwrap().is_none());
    }

    #[test]
    fn len_hashes_is_empty() {
        let pool = WorkerPool::from_entries(vec![
            entry([0x50; 32], [0x51; 32]),
            entry([0x52; 32], [0x53; 32]),
        ]);
        assert_eq!(pool.len(), 2);
        assert!(!pool.is_empty());
        assert_eq!(pool.hashes().len(), 2);
    }
}