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
        if let WorkerCommand::Register(entry) = &command {
            state.queue.retain(|cmd| {
                !matches!(cmd, WorkerCommand::Delete { hash, .. } if *hash == entry.identity_type_hash)
            });
        }
        state.queue.push(command);
        true
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
