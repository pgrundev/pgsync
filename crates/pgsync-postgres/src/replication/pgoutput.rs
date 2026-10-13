//! Decoder for the CopyBoth payloads of logical replication: XLogData / keepalive framing
//! and the pgoutput protocol version 1 message set.
//!
//! Byte layouts follow `docs/protocol-reference.md`. Tuple values are zero-copy slices
//! of the incoming buffer.

use bytes::{Buf, Bytes};
use pgsync_core::event::{Column, Relation, ReplicaIdentity, Tuple, Value};
use pgsync_core::{Error, Lsn, Result};

/// Outer replication message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        server_clock: i64,
        body: LogicalMessage,
    },
    PrimaryKeepalive {
        wal_end: Lsn,
        server_clock: i64,
        reply_requested: bool,
    },
}

/// pgoutput message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalMessage {
    Begin {
        final_lsn: Lsn,
        commit_ts: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_ts: i64,
    },
    Origin {
        commit_lsn: Lsn,
        name: String,
    },
    Relation(Relation),
    Type {
        oid: u32,
        schema: String,
        name: String,
    },
    Insert {
        relation: u32,
        new: Tuple,
    },
    Update {
        relation: u32,
        key: Option<Tuple>,
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
    Message {
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Bytes,
    },
    /// A tag this decoder does not implement (streaming, two-phase, ...). The engine stops.
    Unsupported {
        tag: u8,
    },
}

pub fn decode(mut buf: Bytes) -> Result<ReplicationMessage> {
    let tag = take_u8(&mut buf, "replication message tag")?;
    match tag {
        b'w' => {
            let wal_start = Lsn(take_u64(&mut buf, "wal_start")?);
            let wal_end = Lsn(take_u64(&mut buf, "wal_end")?);
            let server_clock = take_i64(&mut buf, "server_clock")?;
            let body = decode_logical(buf)?;
            Ok(ReplicationMessage::XLogData {
                wal_start,
                wal_end,
                server_clock,
                body,
            })
        }
        b'k' => {
            let wal_end = Lsn(take_u64(&mut buf, "wal_end")?);
            let server_clock = take_i64(&mut buf, "server_clock")?;
            let reply_requested = take_u8(&mut buf, "reply")? != 0;
            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end,
                server_clock,
                reply_requested,
            })
        }
        other => Err(Error::internal(format!(
            "unknown replication message tag {:?}",
            other as char
        ))),
    }
}

pub fn decode_logical(mut buf: Bytes) -> Result<LogicalMessage> {
    let tag = take_u8(&mut buf, "pgoutput tag")?;
    let msg = match tag {
        b'B' => LogicalMessage::Begin {
            final_lsn: Lsn(take_u64(&mut buf, "final_lsn")?),
            commit_ts: take_i64(&mut buf, "commit_ts")?,
            xid: take_u32(&mut buf, "xid")?,
        },
        b'C' => LogicalMessage::Commit {
            flags: take_u8(&mut buf, "flags")?,
            commit_lsn: Lsn(take_u64(&mut buf, "commit_lsn")?),
            end_lsn: Lsn(take_u64(&mut buf, "end_lsn")?),
            commit_ts: take_i64(&mut buf, "commit_ts")?,
        },
        b'O' => LogicalMessage::Origin {
            commit_lsn: Lsn(take_u64(&mut buf, "origin lsn")?),
            name: take_cstr(&mut buf, "origin name")?,
        },
        b'R' => {
            let id = take_u32(&mut buf, "relation id")?;
            let schema = take_cstr(&mut buf, "namespace")?;
            let name = take_cstr(&mut buf, "relation name")?;
            let ident = take_u8(&mut buf, "replica identity")?;
            let replica_identity = ReplicaIdentity::from_char(ident).ok_or_else(|| {
                Error::internal(format!("unknown replica identity {:?}", ident as char))
            })?;
            let ncols = take_u16(&mut buf, "column count")? as usize;
            let mut columns = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                let flags = take_u8(&mut buf, "column flags")?;
                let name = take_cstr(&mut buf, "column name")?;
                let type_oid = take_u32(&mut buf, "column type")?;
                let type_modifier = take_i32(&mut buf, "column typmod")?;
                columns.push(Column {
                    name,
                    type_oid,
                    type_modifier,
                    is_key: flags & 1 == 1,
                });
            }
            LogicalMessage::Relation(Relation {
                id,
                schema: if schema.is_empty() {
                    "pg_catalog".to_owned()
                } else {
                    schema
                },
                name,
                replica_identity,
                columns,
            })
        }
        b'Y' => LogicalMessage::Type {
            oid: take_u32(&mut buf, "type oid")?,
            schema: take_cstr(&mut buf, "type namespace")?,
            name: take_cstr(&mut buf, "type name")?,
        },
        b'I' => {
            let relation = take_u32(&mut buf, "relation id")?;
            let kind = take_u8(&mut buf, "tuple kind")?;
            if kind != b'N' {
                return Err(Error::internal(format!(
                    "insert: expected 'N' tuple, got {:?}",
                    kind as char
                )));
            }
            LogicalMessage::Insert {
                relation,
                new: take_tuple(&mut buf)?,
            }
        }
        b'U' => {
            let relation = take_u32(&mut buf, "relation id")?;
            let mut key = None;
            let mut old = None;
            let mut kind = take_u8(&mut buf, "tuple kind")?;
            if kind == b'K' {
                key = Some(take_tuple(&mut buf)?);
                kind = take_u8(&mut buf, "tuple kind")?;
            } else if kind == b'O' {
                old = Some(take_tuple(&mut buf)?);
                kind = take_u8(&mut buf, "tuple kind")?;
            }
            if kind != b'N' {
                return Err(Error::internal(format!(
                    "update: expected 'N' tuple, got {:?}",
                    kind as char
                )));
            }
            LogicalMessage::Update {
                relation,
                key,
                old,
                new: take_tuple(&mut buf)?,
            }
        }
        b'D' => {
            let relation = take_u32(&mut buf, "relation id")?;
            let kind = take_u8(&mut buf, "tuple kind")?;
            let tuple = take_tuple(&mut buf)?;
            match kind {
                b'K' => LogicalMessage::Delete {
                    relation,
                    key: Some(tuple),
                    old: None,
                },
                b'O' => LogicalMessage::Delete {
                    relation,
                    key: None,
                    old: Some(tuple),
                },
                other => {
                    return Err(Error::internal(format!(
                        "delete: unexpected tuple kind {:?}",
                        other as char
                    )));
                }
            }
        }
        b'T' => {
            let n = take_u32(&mut buf, "relation count")? as usize;
            let options = take_u8(&mut buf, "truncate options")?;
            let mut relations = Vec::with_capacity(n);
            for _ in 0..n {
                relations.push(take_u32(&mut buf, "relation id")?);
            }
            LogicalMessage::Truncate {
                relations,
                cascade: options & 1 != 0,
                restart_identity: options & 2 != 0,
            }
        }
        b'M' => {
            let flags = take_u8(&mut buf, "message flags")?;
            let lsn = Lsn(take_u64(&mut buf, "message lsn")?);
            let prefix = take_cstr(&mut buf, "message prefix")?;
            let len = take_u32(&mut buf, "message length")? as usize;
            if buf.len() < len {
                return Err(truncated("message content"));
            }
            let content = buf.split_to(len);
            LogicalMessage::Message {
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content,
            }
        }
        other => LogicalMessage::Unsupported { tag: other },
    };
    Ok(msg)
}

fn take_tuple(buf: &mut Bytes) -> Result<Tuple> {
    let n = take_u16(buf, "tuple column count")? as usize;
    let mut values = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = take_u8(buf, "tuple value kind")?;
        let v = match kind {
            b'n' => Value::Null,
            b'u' => Value::Unchanged,
            b't' | b'b' => {
                let len = take_u32(buf, "value length")? as usize;
                if buf.len() < len {
                    return Err(truncated("tuple value"));
                }
                let data = buf.split_to(len);
                if kind == b't' {
                    Value::Text(data)
                } else {
                    Value::Binary(data)
                }
            }
            other => {
                return Err(Error::internal(format!(
                    "unknown tuple value kind {:?}",
                    other as char
                )));
            }
        };
        values.push(v);
    }
    Ok(Tuple { values })
}

fn truncated(what: &str) -> Error {
    Error::internal(format!("truncated pgoutput message while reading {what}"))
}

fn take_u8(buf: &mut Bytes, what: &str) -> Result<u8> {
    if buf.remaining() < 1 {
        return Err(truncated(what));
    }
    Ok(buf.get_u8())
}
fn take_u16(buf: &mut Bytes, what: &str) -> Result<u16> {
    if buf.remaining() < 2 {
        return Err(truncated(what));
    }
    Ok(buf.get_u16())
}
fn take_u32(buf: &mut Bytes, what: &str) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(truncated(what));
    }
    Ok(buf.get_u32())
}
fn take_i32(buf: &mut Bytes, what: &str) -> Result<i32> {
    Ok(take_u32(buf, what)? as i32)
}
fn take_u64(buf: &mut Bytes, what: &str) -> Result<u64> {
    if buf.remaining() < 8 {
        return Err(truncated(what));
    }
    Ok(buf.get_u64())
}
fn take_i64(buf: &mut Bytes, what: &str) -> Result<i64> {
    Ok(take_u64(buf, what)? as i64)
}
fn take_cstr(buf: &mut Bytes, what: &str) -> Result<String> {
    let end = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| truncated(what))?;
    let s = buf.split_to(end);
    buf.advance(1);
    String::from_utf8(s.to_vec()).map_err(|_| Error::internal(format!("{what}: invalid UTF-8")))
}

/// Encode a standby status update (`'r'`).
pub fn encode_standby_status(
    written: Lsn,
    flushed: Lsn,
    applied: Lsn,
    clock: i64,
    reply: bool,
) -> [u8; 34] {
    let mut out = [0u8; 34];
    out[0] = b'r';
    out[1..9].copy_from_slice(&written.0.to_be_bytes());
    out[9..17].copy_from_slice(&flushed.0.to_be_bytes());
    out[17..25].copy_from_slice(&applied.0.to_be_bytes());
    out[25..33].copy_from_slice(&clock.to_be_bytes());
    out[33] = reply as u8;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(v: &[&[u8]]) -> Bytes {
        Bytes::from(v.concat())
    }
    fn u64be(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn u32be(v: u32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn u16be(v: u16) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }
    fn text(s: &str) -> Vec<u8> {
        let mut v = vec![b't'];
        v.extend(u32be(s.len() as u32));
        v.extend(s.as_bytes());
        v
    }

    #[test]
    fn keepalive() {
        let m = decode(b(&[b"k", &u64be(0x16B3748), &u64be(123), &[1]])).unwrap();
        assert_eq!(
            m,
            ReplicationMessage::PrimaryKeepalive {
                wal_end: Lsn(0x16B3748),
                server_clock: 123,
                reply_requested: true
            }
        );
        assert!(decode(b(&[b"k", &u64be(1)])).is_err());
        assert!(decode(b(&[b"x"])).is_err());
    }

    #[test]
    fn begin_and_commit() {
        let m = decode(b(&[
            b"w",
            &u64be(10),
            &u64be(20),
            &u64be(30),
            b"B",
            &u64be(100),
            &u64be(777),
            &u32be(42),
        ]))
        .unwrap();
        match m {
            ReplicationMessage::XLogData {
                wal_start,
                wal_end,
                server_clock,
                body,
            } => {
                assert_eq!((wal_start, wal_end, server_clock), (Lsn(10), Lsn(20), 30));
                assert_eq!(
                    body,
                    LogicalMessage::Begin {
                        final_lsn: Lsn(100),
                        commit_ts: 777,
                        xid: 42
                    }
                );
            }
            _ => panic!(),
        }
        let c = decode_logical(b(&[b"C", &[0], &u64be(100), &u64be(108), &u64be(777)])).unwrap();
        assert_eq!(
            c,
            LogicalMessage::Commit {
                flags: 0,
                commit_lsn: Lsn(100),
                end_lsn: Lsn(108),
                commit_ts: 777
            }
        );
    }

    #[test]
    fn relation() {
        let m = decode_logical(b(&[
            b"R",
            &u32be(16385),
            &cstr("public"),
            &cstr("users"),
            b"d",
            &u16be(2),
            &[1],
            &cstr("id"),
            &u32be(20),
            &u32be(0xFFFF_FFFF),
            &[0],
            &cstr("email"),
            &u32be(25),
            &u32be(0xFFFF_FFFF),
        ]))
        .unwrap();
        let LogicalMessage::Relation(r) = m else {
            panic!()
        };
        assert_eq!(r.id, 16385);
        assert_eq!(r.qualified_name(), "public.users");
        assert_eq!(r.replica_identity, ReplicaIdentity::Default);
        assert_eq!(r.columns.len(), 2);
        assert!(r.columns[0].is_key && !r.columns[1].is_key);
        assert_eq!(r.columns[0].type_oid, 20);
        assert_eq!(r.columns[1].type_modifier, -1);
        assert_eq!(r.key_columns().map(|(i, _)| i).collect::<Vec<_>>(), vec![0]);

        // Empty namespace means pg_catalog.
        let m = decode_logical(b(&[
            b"R",
            &u32be(1),
            &cstr(""),
            &cstr("t"),
            b"n",
            &u16be(0),
        ]))
        .unwrap();
        let LogicalMessage::Relation(r) = m else {
            panic!()
        };
        assert_eq!(r.schema, "pg_catalog");
    }

    #[test]
    fn insert_update_delete_tuples() {
        let ins = decode_logical(b(&[
            b"I",
            &u32be(7),
            b"N",
            &u16be(3),
            &text("1"),
            b"n",
            b"u",
        ]))
        .unwrap();
        assert_eq!(
            ins,
            LogicalMessage::Insert {
                relation: 7,
                new: Tuple {
                    values: vec![Value::text("1"), Value::Null, Value::Unchanged]
                }
            }
        );

        // Update with old key.
        let upd = decode_logical(b(&[
            b"U",
            &u32be(7),
            b"K",
            &u16be(1),
            &text("1"),
            b"N",
            &u16be(1),
            &text("2"),
        ]))
        .unwrap();
        assert_eq!(
            upd,
            LogicalMessage::Update {
                relation: 7,
                key: Some(Tuple {
                    values: vec![Value::text("1")]
                }),
                old: None,
                new: Tuple {
                    values: vec![Value::text("2")]
                }
            }
        );
        // Update with full old row.
        let upd = decode_logical(b(&[
            b"U",
            &u32be(7),
            b"O",
            &u16be(1),
            &text("1"),
            b"N",
            &u16be(1),
            &text("2"),
        ]))
        .unwrap();
        assert!(matches!(
            upd,
            LogicalMessage::Update {
                old: Some(_),
                key: None,
                ..
            }
        ));
        // Update without old tuple.
        let upd = decode_logical(b(&[b"U", &u32be(7), b"N", &u16be(1), &text("2")])).unwrap();
        assert!(matches!(
            upd,
            LogicalMessage::Update {
                old: None,
                key: None,
                ..
            }
        ));

        let del = decode_logical(b(&[b"D", &u32be(7), b"K", &u16be(1), &text("1")])).unwrap();
        assert_eq!(
            del,
            LogicalMessage::Delete {
                relation: 7,
                key: Some(Tuple {
                    values: vec![Value::text("1")]
                }),
                old: None
            }
        );
        let del = decode_logical(b(&[b"D", &u32be(7), b"O", &u16be(1), &text("1")])).unwrap();
        assert!(matches!(
            del,
            LogicalMessage::Delete {
                old: Some(_),
                key: None,
                ..
            }
        ));
        assert!(decode_logical(b(&[b"D", &u32be(7), b"N", &u16be(0)])).is_err());
    }

    #[test]
    fn binary_values_and_truncation() {
        let ins = decode_logical(b(&[
            b"I",
            &u32be(7),
            b"N",
            &u16be(1),
            b"b",
            &u32be(2),
            &[0xAB, 0xCD],
        ]))
        .unwrap();
        assert_eq!(
            ins,
            LogicalMessage::Insert {
                relation: 7,
                new: Tuple {
                    values: vec![Value::Binary(Bytes::from_static(&[0xAB, 0xCD]))]
                }
            }
        );
        // Declared length longer than payload.
        assert!(
            decode_logical(b(&[
                b"I",
                &u32be(7),
                b"N",
                &u16be(1),
                b"t",
                &u32be(5),
                b"ab"
            ]))
            .is_err()
        );
        // Unknown value kind.
        assert!(decode_logical(b(&[b"I", &u32be(7), b"N", &u16be(1), b"z"])).is_err());
    }

    #[test]
    fn truncate_origin_type_message_unsupported() {
        let t = decode_logical(b(&[b"T", &u32be(2), &[3], &u32be(1), &u32be(2)])).unwrap();
        assert_eq!(
            t,
            LogicalMessage::Truncate {
                relations: vec![1, 2],
                cascade: true,
                restart_identity: true
            }
        );
        let o = decode_logical(b(&[b"O", &u64be(5), &cstr("node1")])).unwrap();
        assert_eq!(
            o,
            LogicalMessage::Origin {
                commit_lsn: Lsn(5),
                name: "node1".into()
            }
        );
        let y = decode_logical(b(&[b"Y", &u32be(99), &cstr("public"), &cstr("mood")])).unwrap();
        assert_eq!(
            y,
            LogicalMessage::Type {
                oid: 99,
                schema: "public".into(),
                name: "mood".into()
            }
        );
        let m =
            decode_logical(b(&[b"M", &[1], &u64be(9), &cstr("pfx"), &u32be(3), b"abc"])).unwrap();
        assert_eq!(
            m,
            LogicalMessage::Message {
                transactional: true,
                lsn: Lsn(9),
                prefix: "pfx".into(),
                content: Bytes::from_static(b"abc")
            }
        );
        for tag in [b'S', b'E', b'c', b'A', b'b', b'P', b'K', b'r', b'p'] {
            assert_eq!(
                decode_logical(b(&[&[tag], &[0; 40]])).unwrap(),
                LogicalMessage::Unsupported { tag }
            );
        }
    }

    #[test]
    fn standby_status_layout() {
        let s = encode_standby_status(Lsn(1), Lsn(2), Lsn(3), 4, true);
        assert_eq!(s[0], b'r');
        assert_eq!(&s[1..9], &u64be(1)[..]);
        assert_eq!(&s[9..17], &u64be(2)[..]);
        assert_eq!(&s[17..25], &u64be(3)[..]);
        assert_eq!(&s[25..33], &u64be(4)[..]);
        assert_eq!(s[33], 1);
    }
}
