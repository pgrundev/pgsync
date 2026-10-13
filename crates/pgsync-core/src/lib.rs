//! PGSync core: lifecycle, configuration, transaction model, checkpoint contracts, and
//! backpressure primitives. Database-specific code lives in `pgsync-postgres`.

pub mod channel;
pub mod checkpoint;
pub mod config;
pub mod engine;
pub mod error;
pub mod event;
pub mod lsn;
pub mod metrics;
pub mod retry;
pub mod state;
pub mod traits;

pub use checkpoint::{Checkpoint, CheckpointStore};
pub use config::Config;
pub use error::{Error, ErrorKind, Result, ResultExt};
pub use event::{Column, Op, Relation, ReplicaIdentity, SourceEvent, Tuple, Value};
pub use lsn::Lsn;
pub use metrics::{Metrics, MetricsSnapshot};
pub use state::JobState;
