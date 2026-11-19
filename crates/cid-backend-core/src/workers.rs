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
}