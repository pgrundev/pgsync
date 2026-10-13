//! The logical change model shared by decoders, transformers (Beta 2) and sinks.
//!
//! Values are kept in the wire representation (`Bytes`) so that forwarding a change
//! does not allocate per column; conversion happens in the sink.

use crate::lsn::Lsn;
use bytes::Bytes;
use std::sync::Arc;

/// `pg_class.relreplident`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// Primary key (`d`).
    Default,
    /// No identity (`n`): UPDATE/DELETE cannot be replicated.
    Nothing,
    /// All columns (`f`).
    Full,
    /// A unique index (`i`).
    Index,
}

impl ReplicaIdentity {
    pub fn from_char(c: u8) -> Option<Self> {
        Some(match c {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Part of the replica identity key.
    pub is_key: bool,
}

/// Relation metadata as sent by pgoutput. Cached by the sink by `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub schema: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<Column>,
}

impl Relation {
    pub fn key_columns(&self) -> impl Iterator<Item = (usize, &Column)> {
        self.columns.iter().enumerate().filter(|(_, c)| c.is_key)
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    /// TOASTed value that did not change; the sink must leave the stored value alone.
    Unchanged,
    /// Text representation (output function). The default for Beta 1.
    Text(Bytes),
    /// Binary send-function representation (`binary` option; unused in Beta 1).
    Binary(Bytes),
}

impl Value {
    pub fn byte_len(&self) -> usize {
        match self {
            Value::Null | Value::Unchanged => 0,
            Value::Text(b) | Value::Binary(b) => b.len(),
        }
    }

    pub fn text(s: &str) -> Value {
        Value::Text(Bytes::copy_from_slice(s.as_bytes()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tuple {
    pub values: Vec<Value>,
}

impl Tuple {
    pub fn byte_len(&self) -> usize {
        self.values.iter().map(|v| v.byte_len() + 8).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Insert {
        relation: u32,
        new: Tuple,
    },
    Update {
        relation: u32,
        /// Old key (`K`) when a key column changed.
        key: Option<Tuple>,
        /// Old full row (`O`) for REPLICA IDENTITY FULL.
        old: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        relation: u32,
        key: Option<Tuple>,
        old: Option<Tuple>,
    },
    Truncate {
        relations: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
}

impl Op {
    pub fn relation_ids(&self) -> Vec<u32> {
        match self {
            Op::Insert { relation, .. }
            | Op::Update { relation, .. }
            | Op::Delete { relation, .. } => {
                vec![*relation]
            }
            Op::Truncate { relations, .. } => relations.clone(),
        }
    }

    pub fn byte_len(&self) -> usize {
        let base = 32;
        match self {
            Op::Insert { new, .. } => base + new.byte_len(),
            Op::Update { key, old, new, .. } => {
                base + new.byte_len()
                    + key.as_ref().map_or(0, Tuple::byte_len)
                    + old.as_ref().map_or(0, Tuple::byte_len)
            }
            Op::Delete { key, old, .. } => {
                base + key.as_ref().map_or(0, Tuple::byte_len)
                    + old.as_ref().map_or(0, Tuple::byte_len)
            }
            Op::Truncate { relations, .. } => base + relations.len() * 4,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Op::Insert { .. } => "insert",
            Op::Update { .. } => "update",
            Op::Delete { .. } => "delete",
            Op::Truncate { .. } => "truncate",
        }
    }
}

/// Ordered events produced by a replication source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceEvent {
    Begin {
        /// LSN of the transaction's commit record (equals `Commit::commit_lsn`).
        final_lsn: Lsn,
        /// PostgreSQL timestamp (microseconds since 2000-01-01).
        commit_ts: i64,
        xid: u32,
    },
    Relation(Arc<Relation>),
    Op(Op),
    Commit {
        commit_lsn: Lsn,
        /// LSN after the commit record; this is what gets acknowledged.
        end_lsn: Lsn,
        commit_ts: i64,
    },
    /// Server keepalive. Safe to acknowledge only between transactions.
    Keepalive {
        wal_end: Lsn,
    },
}

impl SourceEvent {
    /// Approximate memory footprint, used for inflight accounting.
    pub fn byte_len(&self) -> usize {
        match self {
            SourceEvent::Begin { .. }
            | SourceEvent::Commit { .. }
            | SourceEvent::Keepalive { .. } => 32,
            SourceEvent::Relation(r) => 64 + r.columns.len() * 48,
            SourceEvent::Op(op) => op.byte_len(),
        }
    }
}

/// Converts a PostgreSQL timestamp (µs since 2000-01-01) to unix microseconds.
pub fn pg_micros_to_unix_micros(pg: i64) -> i64 {
    pg + 946_684_800_000_000
}

/// Current time as a PostgreSQL timestamp (µs since 2000-01-01).
pub fn now_pg_micros() -> i64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    unix - 946_684_800_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_accounting_counts_payloads() {
        let t = Tuple {
            values: vec![Value::text("abc"), Value::Null, Value::Unchanged],
        };
        assert_eq!(t.byte_len(), 3 + 3 * 8);
        let op = Op::Insert {
            relation: 1,
            new: t.clone(),
        };
        assert_eq!(op.byte_len(), 32 + t.byte_len());
        assert_eq!(SourceEvent::Op(op).byte_len(), 32 + t.byte_len());
        assert_eq!(SourceEvent::Keepalive { wal_end: Lsn(1) }.byte_len(), 32);
    }

    #[test]
    fn epoch_conversion() {
        // 2000-01-01T00:00:00Z == 946684800 unix seconds.
        assert_eq!(pg_micros_to_unix_micros(0), 946_684_800_000_000);
        assert!(now_pg_micros() > 0);
    }

    #[test]
    fn replica_identity_chars() {
        assert_eq!(
            ReplicaIdentity::from_char(b'd'),
            Some(ReplicaIdentity::Default)
        );
        assert_eq!(
            ReplicaIdentity::from_char(b'f'),
            Some(ReplicaIdentity::Full)
        );
        assert_eq!(ReplicaIdentity::from_char(b'x'), None);
    }
}
