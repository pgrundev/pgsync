//! Exponential backoff for transient failures.

use crate::config::RetryBackoff;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Backoff {
    policy: RetryBackoff,
    attempt: u32,
    next: Duration,
}

impl Backoff {
    pub fn new(policy: &RetryBackoff) -> Self {
        Backoff {
            policy: policy.clone(),
            attempt: 0,
            next: policy.initial,
        }
    }

    /// Delay before the next attempt, or `None` when attempts are exhausted.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.policy.max_attempts != 0 && self.attempt >= self.policy.max_attempts {
            return None;
        }
        self.attempt += 1;
        let d = self.next.min(self.policy.max);
        let scaled = self.next.as_secs_f64() * self.policy.multiplier;
        self.next = Duration::from_secs_f64(scaled.min(self.policy.max.as_secs_f64()));
        Some(d)
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Call after a successful operation so the next failure starts from `initial`.
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.next = self.policy.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_and_caps() {
        let p = RetryBackoff {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(5),
            multiplier: 2.0,
            max_attempts: 0,
        };
        let mut b = Backoff::new(&p);
        let seq: Vec<u64> = (0..5).map(|_| b.next_delay().unwrap().as_secs()).collect();
        assert_eq!(seq, vec![1, 2, 4, 5, 5]);
        b.reset();
        assert_eq!(b.next_delay().unwrap().as_secs(), 1);
    }

    #[test]
    fn max_attempts_exhausts() {
        let p = RetryBackoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(10),
            multiplier: 1.0,
            max_attempts: 2,
        };
        let mut b = Backoff::new(&p);
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());
        assert_eq!(b.attempt(), 2);
    }
}
