use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use anyhow::{Result, anyhow};

use crate::ownership::Ownership;

/// Daemon stop flag and ownership state, shared by the main loop and handlers.
///
/// Every write to either goes through a method here; the fields stay private to
/// this module so no caller can change them behind the main loop's back. Both
/// writes happen under the ownership lock.
pub(super) struct Control {
    stop: AtomicBool,
    ownership: Mutex<Ownership>,
}

impl Control {
    pub fn new(ownership: Ownership) -> Self {
        Self {
            stop: AtomicBool::new(false),
            ownership: Mutex::new(ownership),
        }
    }

    /// Never fails: a poisoned lock is recovered, because stopping must always work.
    /// Must not be called from an `update_ownership` closure.
    pub fn request_stop(&self) {
        let _ownership = self
            .ownership
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.stop.store(true, Ordering::Release);
    }

    pub fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// `f` must not call back into `Control`; the lock is not reentrant.
    pub fn update_ownership<T>(&self, f: impl FnOnce(&mut Ownership) -> T) -> Result<T> {
        let mut ownership = self.lock()?;
        Ok(f(&mut ownership))
    }

    pub fn is_owner(&self, generation: u64) -> Result<bool> {
        Ok(self.lock()?.is_owner(generation))
    }

    /// Main loop only. Returns whether the daemon should stop.
    pub fn tick(&self, now: Instant) -> Result<bool> {
        let mut ownership = self.lock()?;
        Ok(self.stop.load(Ordering::Acquire) || ownership.tick(now))
    }

    fn lock(&self) -> Result<MutexGuard<'_, Ownership>> {
        self.ownership
            .lock()
            .map_err(|_| anyhow!("ownership lock poisoned"))
    }
}
