use std::time::{Duration, Instant};

use anyhow::{Result, bail};

const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Handoff {
    pub ticket: String,
    pub expires_at: u64,
    deadline: Instant,
}

pub(crate) enum Ownership {
    Unmanaged,
    Starting(Instant),
    Owned(Option<Handoff>),
    Waiting(Handoff),
    Stopped,
}

impl Ownership {
    pub fn new(owned: bool, now: Instant) -> Self {
        if owned {
            Self::Starting(now + ACQUIRE_TIMEOUT)
        } else {
            Self::Unmanaged
        }
    }

    pub fn tick(&mut self, now: Instant) -> bool {
        match self {
            Self::Starting(deadline) if now >= *deadline => *self = Self::Stopped,
            Self::Waiting(handoff) if now >= handoff.deadline => *self = Self::Stopped,
            Self::Owned(Some(handoff)) if now >= handoff.deadline => *self = Self::Owned(None),
            _ => {}
        }
        matches!(self, Self::Stopped)
    }

    pub fn claim(&mut self, ticket: Option<&str>, now: Instant) -> Result<()> {
        self.tick(now);
        match &*self {
            Self::Starting(_) if ticket.is_none() => {}
            Self::Waiting(handoff) if ticket == Some(handoff.ticket.as_str()) => {}
            Self::Unmanaged => bail!("daemon was not started with --owned"),
            Self::Owned(_) => bail!("daemon already has a live owner"),
            Self::Stopped => bail!("daemon ownership deadline expired or daemon is stopping"),
            _ => bail!("invalid handoff ticket"),
        }
        *self = Self::Owned(None);
        Ok(())
    }

    pub fn prepare(&mut self, now: Instant, unix_ms: u64) -> Result<Handoff> {
        self.tick(now);
        let Self::Owned(handoff) = self else {
            bail!("handoff requires the owner connection");
        };
        Ok(handoff
            .get_or_insert_with(|| Handoff {
                ticket: format!("{:032x}", rand::random::<u128>()),
                expires_at: unix_ms + HANDOFF_TIMEOUT.as_millis() as u64,
                deadline: now + HANDOFF_TIMEOUT,
            })
            .clone())
    }

    pub fn disconnect(&mut self, now: Instant) {
        self.tick(now);
        *self = match self {
            Self::Owned(Some(handoff)) => Self::Waiting(handoff.clone()),
            _ => Self::Stopped,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquisition_is_exclusive_and_bounded() {
        let now = Instant::now();
        let mut owner = Ownership::new(true, now);
        assert!(owner.claim(Some("unexpected"), now).is_err());
        owner.claim(None, now).unwrap();
        assert!(owner.claim(None, now).is_err());
        owner.disconnect(now);
        assert!(owner.tick(now));
        assert!(owner.claim(None, now).is_err());

        let mut starting = Ownership::new(true, now);
        assert!(!starting.tick(now + ACQUIRE_TIMEOUT - Duration::from_millis(1)));
        assert!(starting.claim(None, now + ACQUIRE_TIMEOUT).is_err());
        assert!(starting.tick(now + ACQUIRE_TIMEOUT));
        assert!(Ownership::new(false, now).claim(None, now).is_err());
    }

    #[test]
    fn handoff_is_nonrenewing_and_consumed_by_claim() {
        let now = Instant::now();
        let mut owner = Ownership::new(true, now);
        owner.claim(None, now).unwrap();
        let handoff = owner.prepare(now, 1000).unwrap();
        assert_eq!(handoff.expires_at, 121_000);
        assert_eq!(
            owner
                .prepare(now + Duration::from_secs(60), 61_000)
                .unwrap(),
            handoff
        );
        assert!(owner.claim(Some(&handoff.ticket), now).is_err());
        owner.disconnect(now);
        assert!(owner.claim(None, now).is_err());
        assert!(owner.claim(Some("wrong"), now).is_err());
        owner.claim(Some(&handoff.ticket), now).unwrap();
        assert!(owner.claim(Some(&handoff.ticket), now).is_err());
        owner.disconnect(now);
        assert!(owner.tick(now));
    }

    #[test]
    fn expiry_only_stops_a_disconnected_owner() {
        let now = Instant::now();
        let mut owner = Ownership::new(true, now);
        owner.claim(None, now).unwrap();
        let handoff = owner.prepare(now, 1000).unwrap();
        assert!(!owner.tick(handoff.deadline));
        assert!(matches!(owner, Ownership::Owned(None)));
        owner.disconnect(handoff.deadline);
        assert!(owner.tick(handoff.deadline));

        let mut owner = Ownership::new(true, now);
        owner.claim(None, now).unwrap();
        let handoff = owner.prepare(now, 1000).unwrap();
        owner.disconnect(now);
        assert!(!owner.tick(handoff.deadline - Duration::from_millis(1)));
        assert!(
            owner
                .claim(Some(&handoff.ticket), handoff.deadline)
                .is_err()
        );
        assert!(owner.tick(handoff.deadline));
    }
}
