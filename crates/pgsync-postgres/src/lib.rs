//! PGSync PostgreSQL adapter: replication protocol client, pgoutput decoder, source
//! preflight, snapshot copier, and the PostgreSQL sink with transactional checkpoints.

pub mod client;
pub mod errors;
pub mod job;
pub mod replication;
pub mod schema;
pub mod sink;
pub mod snapshot;
pub mod source;
pub mod tls;
pub mod url;
