use crate::process::ProcessEntry;
use std::collections::HashSet;
use std::sync::{Condvar, Mutex};

/// Deferred register/delete from the HTTP registry API (applied at tick start).
#[derive(Clone, Debug)]
pub enum WorkerCommand {
    Register(ProcessEntry),
    /// `generation` stamps the worker when a delete is deferred. Re-register
    /// bumps the generation so a stale deferred delete is a no-op.
    Delete {
        hash: [u8; 32],
        generation: Option<u64>,
    },
}

impl WorkerCommand {
    pub(crate) fn identity(&self) -> [u8; 32] {
        match self {
            Self::Register(entry) => entry.identity_type_hash,
            Self::Delete { hash, .. } => *hash,
        }
    }

    pub fn delete(hash: [u8; 32]) -> Self {
        Self::Delete {
            hash,
            generation: None,
        }
    }
}

#[derive(Default)]
struct CommandState {
    queue: Vec<WorkerCommand>,
    submitting: HashSet<[u8; 32]>,
    pending: HashSet<[u8; 32]>,
}

impl CommandState {
    fn enqueue(&mut self, command: WorkerCommand) {
        if let WorkerCommand::Register(entry) = &command {
            self.queue.retain(|cmd| {
                !matches!(cmd, WorkerCommand::Delete { hash, .. } if *hash == entry.identity_type_hash)
            });
        }
        self.queue.push(command);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RegistrationQueueError<E> {
    Busy,
    Unavailable,
    Persist(E),
}

/// Serializes registry updates with admission to each transaction submission.
#[derive(Default)]
pub struct WorkerCommands {
    state: Mutex<CommandState>,
    submission_finished: Condvar,
}

impl WorkerCommands {
    pub fn enqueue(&self, command: WorkerCommand) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        state.enqueue(command);
        true
    }

    /// Persist a registration and enqueue it under the submission admission lock.
    pub fn enqueue_registration<T, E>(
        &self,
        entry: ProcessEntry,
        require_idle: bool,
        persist: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, RegistrationQueueError<E>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RegistrationQueueError::Unavailable)?;
        let hash = &entry.identity_type_hash;
        if require_idle && (state.submitting.contains(hash) || state.pending.contains(hash)) {
            return Err(RegistrationQueueError::Busy);
        }
        let result = persist().map_err(RegistrationQueueError::Persist)?;
        state.enqueue(WorkerCommand::Register(entry));
        Ok(result)
    }

    /// Refresh after reconciliation, while no batch is submitting.
    pub(crate) fn set_pending(&self, pending: &[[u8; 32]]) {
        if let Ok(mut state) = self.state.lock() {
            state.pending = pending.iter().copied().collect();
        }
    }

    pub(crate) fn drain(&self) -> Option<Vec<WorkerCommand>> {
        let mut state = self.state.lock().ok()?;
        Some(std::mem::take(&mut state.queue))
    }

    pub(crate) fn requeue(&self, mut commands: Vec<WorkerCommand>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        commands.retain(|old| {
            !state
                .queue
                .iter()
                .any(|new| new.identity() == old.identity())
        });
        state.queue.extend(commands);
    }

    pub fn queued(&self) -> Vec<WorkerCommand> {
        self.state.lock().expect("worker commands").queue.clone()
    }

    pub(crate) fn has_update(&self, hash: [u8; 32]) -> bool {
        self.state.lock().map_or(true, |state| {
            state.queue.iter().any(|cmd| cmd.identity() == hash)
        })
    }

    pub(crate) fn begin_submission(&self, hash: [u8; 32]) -> Option<Submission<'_>> {
        let mut state = self.state.lock().ok()?;
        if state.queue.iter().any(|cmd| cmd.identity() == hash) || !state.submitting.insert(hash) {
            return None;
        }
        Some(Submission {
            commands: self,
            hash,
        })
    }

    /// Call after enqueueing a delete, outside the registry lock. An admitted
    /// submission can finish bookkeeping before the API acknowledges deletion.
    pub fn wait_for_submission(&self, hash: &[u8; 32]) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        self.submission_finished
            .wait_while(state, |state| state.submitting.contains(hash))
            .is_ok()
    }
}

pub(crate) struct Submission<'a> {
    commands: &'a WorkerCommands,
    hash: [u8; 32],
}

impl Submission<'_> {
    pub(crate) fn record_pending(&self) {
        if let Ok(mut state) = self.commands.state.lock() {
            state.pending.insert(self.hash);
        }
    }
}

impl Drop for Submission<'_> {
    fn drop(&mut self) {
        let mut state = self
            .commands
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.submitting.remove(&self.hash);
        self.commands.submission_finished.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_id::TypeIdIdentity;

    #[test]
    fn rebind_waits_for_submission_and_its_pending_status() {
        let commands = WorkerCommands::default();
        let identity = TypeIdIdentity::new([1; 32]);
        let entry = ProcessEntry {
            identity_type_hash: identity.type_script_hash(),
            identity,
            code: TypeIdIdentity::new([2; 32]),
            ssri_elf_path: None,
        };
        let hash = entry.identity_type_hash;
        let persist = || -> Result<(), ()> { panic!("busy rebind must not persist") };

        let send = commands.begin_submission(hash).unwrap();
        assert_eq!(
            commands.enqueue_registration(entry.clone(), true, persist),
            Err(RegistrationQueueError::Busy)
        );
        send.record_pending();
        drop(send);
        assert_eq!(
            commands.enqueue_registration(entry.clone(), true, persist),
            Err(RegistrationQueueError::Busy)
        );
        assert!(commands.queued().is_empty());

        commands.set_pending(&[]);
        commands
            .enqueue_registration(entry, true, || Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(commands.queued().len(), 1);
        assert!(commands.begin_submission(hash).is_none());
    }
}
