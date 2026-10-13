//! Job lifecycle states.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Observable state of a replication job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    Initializing,
    Snapshotting,
    CatchingUp,
    Streaming,
    Paused,
    Error,
    ResyncRequired,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Initializing => "INITIALIZING",
            JobState::Snapshotting => "SNAPSHOTTING",
            JobState::CatchingUp => "CATCHING_UP",
            JobState::Streaming => "STREAMING",
            JobState::Paused => "PAUSED",
            JobState::Error => "ERROR",
            JobState::ResyncRequired => "RESYNC_REQUIRED",
        }
    }

    /// States that permit `pgsync run` to resume CDC.
    pub fn is_resumable(self) -> bool {
        matches!(
            self,
            JobState::CatchingUp | JobState::Streaming | JobState::Paused | JobState::Error
        )
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "INITIALIZING" => JobState::Initializing,
            "SNAPSHOTTING" => JobState::Snapshotting,
            "CATCHING_UP" => JobState::CatchingUp,
            "STREAMING" => JobState::Streaming,
            "PAUSED" => JobState::Paused,
            "ERROR" => JobState::Error,
            "RESYNC_REQUIRED" => JobState::ResyncRequired,
            other => return Err(format!("unknown job state {other:?}")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_text() {
        for s in [
            JobState::Initializing,
            JobState::Snapshotting,
            JobState::CatchingUp,
            JobState::Streaming,
            JobState::Paused,
            JobState::Error,
            JobState::ResyncRequired,
        ] {
            assert_eq!(s.as_str().parse::<JobState>().unwrap(), s);
            assert_eq!(
                serde_json::to_string(&s).unwrap(),
                format!("\"{}\"", s.as_str())
            );
        }
        assert!("STREAMIN".parse::<JobState>().is_err());
    }

    #[test]
    fn resync_required_and_snapshotting_do_not_resume() {
        assert!(!JobState::ResyncRequired.is_resumable());
        assert!(!JobState::Snapshotting.is_resumable());
        assert!(!JobState::Initializing.is_resumable());
        assert!(JobState::Streaming.is_resumable());
    }
}
