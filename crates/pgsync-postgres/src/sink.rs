//! PostgreSQL sink: applies decoded changes inside target transactions and stores the
//! job checkpoint in the same transaction.

use crate::client::PgClient;
use crate::errors::from_pg;
use crate::schema::{TargetColumn, target_columns};
use bytes::{Bytes, BytesMut};
use futures_util::future::try_join_all;
use pgsync_core::checkpoint::Checkpoint;
use pgsync_core::config::{TableName, quote_ident};
use pgsync_core::event::{Op, Relation, ReplicaIdentity, Tuple, Value};
use pgsync_core::state::JobState;
use pgsync_core::traits::ReplicationSink;
use pgsync_core::{Error, Lsn, Result};
use postgres_types::{IsNull, PgLsn, ToSql, Type, to_sql_checked};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio_postgres::Statement;
use tracing::{debug, warn};

pub const SCHEMA_SQL: &str = r#"
SET client_min_messages = warning;
CREATE SCHEMA IF NOT EXISTS pgsync;
CREATE TABLE IF NOT EXISTS pgsync.job (
    job_id            text PRIMARY KEY,
    state             text NOT NULL,
    source_system_id  text NOT NULL,
    source_dbname     text NOT NULL,
    slot              text NOT NULL,
    publication       text NOT NULL,
    slot_owned        boolean NOT NULL DEFAULT false,
    consistent_point  pg_lsn,
    commit_lsn        pg_lsn NOT NULL DEFAULT '0/0',
    committed_lsn     pg_lsn NOT NULL DEFAULT '0/0',
    acked_lsn         pg_lsn NOT NULL DEFAULT '0/0',
    last_xid          bigint,
    last_commit_ts    bigint,
    txns_applied      bigint NOT NULL DEFAULT 0,
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pgsync.table_sync (
    job_id       text NOT NULL REFERENCES pgsync.job (job_id) ON DELETE CASCADE,
    schema_name  text NOT NULL,
    table_name   text NOT NULL,
    state        text NOT NULL,
    rows_copied  bigint NOT NULL DEFAULT 0,
    bytes_copied bigint NOT NULL DEFAULT 0,
    copied_at    timestamptz,
    PRIMARY KEY (job_id, schema_name, table_name)
);
"#;

/// A text-format parameter. Statements are prepared with all parameters typed `text` and
/// cast server-side to the target column type.
#[derive(Debug)]
struct TextParam(Option<Bytes>);

impl ToSql for TextParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.0 {
            None => Ok(IsNull::Yes),
            Some(b) => {
                out.extend_from_slice(b);
                Ok(IsNull::No)
            }
        }
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
    to_sql_checked!();
}

struct ResolvedRelation {
    relation: Arc<Relation>,
    quoted: String,
    /// Target type name per source column index.
    types: Vec<String>,
    /// Columns that UPDATE must never assign (GENERATED ALWAYS AS IDENTITY).
    identity_always: Vec<bool>,
    insert_sql: String,
}

struct Pending {
    stmt: Statement,
    params: Vec<TextParam>,
    /// Expected affected rows for UPDATE/DELETE.
    expect_one: bool,
    describe: &'static str,
    table: String,
}

pub struct SinkLimits {
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}

pub struct PgSink {
    client: PgClient,
    relations: HashMap<u32, ResolvedRelation>,
    statements: HashMap<String, Statement>,
    pending: Vec<Pending>,
    pending_bytes: usize,
    in_txn: bool,
    limits: SinkLimits,
    /// `schema.table` names this job is configured to replicate. A relation outside this
    /// set (publication changed after bootstrap, `FOR ALL TABLES`) is refused rather than
    /// applied to a same-named target table that was never snapshotted.
    allowed_tables: Option<BTreeSet<String>>,
}

impl PgSink {
    /// Prepare a connected target client: create the metadata schema and configure the
    /// session for replica apply.
    pub async fn new(client: PgClient, limits: SinkLimits) -> Result<Self> {
        client
            .batch_execute(SCHEMA_SQL)
            .await
            .map_err(|e| from_pg("create pgsync schema on target", e))?;
        match client
            .batch_execute("SET session_replication_role = replica")
            .await
        {
            Ok(()) => debug!(
                "session_replication_role = replica (triggers and FK checks deferred to the source)"
            ),
            Err(e) => {
                warn!(error = %e, "cannot set session_replication_role = replica; target triggers and FK checks stay active")
            }
        }
        configure_session(&client).await?;
        Ok(PgSink {
            client,
            relations: HashMap::new(),
            statements: HashMap::new(),
            pending: Vec::new(),
            pending_bytes: 0,
            in_txn: false,
            limits,
            allowed_tables: None,
        })
    }

    /// Restrict apply to the configured tables (`run` path). Without this, every
    /// published relation that exists on the target is accepted (`status`/`cleanup` never
    /// apply anything, so they do not need it).
    pub fn restrict_to(mut self, tables: &[TableName]) -> Self {
        self.allowed_tables = Some(tables.iter().map(|t| t.to_string()).collect());
        self
    }

    pub fn client(&self) -> &PgClient {
        &self.client
    }

    async fn prepare(&mut self, sql: &str, nparams: usize) -> Result<Statement> {
        if let Some(s) = self.statements.get(sql) {
            return Ok(s.clone());
        }
        if self.statements.len() > 1024 {
            self.statements.clear();
        }
        let types = vec![Type::TEXT; nparams];
        let stmt = self
            .client
            .prepare_typed(sql, &types)
            .await
            .map_err(|e| from_pg("prepare", e))?;
        self.statements.insert(sql.to_owned(), stmt.clone());
        Ok(stmt)
    }

    async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        let client = &self.client;
        // Issued concurrently on one connection: tokio-postgres pipelines them in order.
        let futs = batch.iter().map(|p| {
            let params: Vec<&(dyn ToSql + Sync)> =
                p.params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
            async move { client.execute(&p.stmt, &params).await }
        });
        let counts = try_join_all(futs)
            .await
            .map_err(|e| from_pg("apply batch", e))?;
        for (p, n) in batch.iter().zip(counts) {
            if p.expect_one && n != 1 {
                return Err(Error::consistency(format!(
                    "{} on {} affected {n} rows (expected 1): source and target have diverged",
                    p.describe, p.table
                )));
            }
        }
        Ok(())
    }

    fn resolved(&self, id: u32) -> Result<&ResolvedRelation> {
        self.relations
            .get(&id)
            .ok_or_else(|| Error::internal(format!("change for unknown relation id {id}")))
    }

    fn push(
        &mut self,
        stmt: Statement,
        params: Vec<TextParam>,
        expect_one: bool,
        describe: &'static str,
        table: String,
    ) {
        self.pending_bytes += params
            .iter()
            .map(|p| p.0.as_ref().map_or(0, |b| b.len()) + 16)
            .sum::<usize>();
        self.pending.push(Pending {
            stmt,
            params,
            expect_one,
            describe,
            table,
        });
    }

    fn needs_flush(&self) -> bool {
        self.pending.len() >= self.limits.max_batch_rows
            || self.pending_bytes >= self.limits.max_batch_bytes
    }

    async fn apply_insert(&mut self, rel_id: u32, new: Tuple) -> Result<()> {
        let (sql, table, n) = {
            let r = self.resolved(rel_id)?;
            if new.values.len() != r.relation.columns.len() {
                return Err(Error::unsupported(format!(
                    "{}: insert has {} values but relation has {} columns (schema change?)",
                    r.quoted,
                    new.values.len(),
                    r.relation.columns.len()
                )));
            }
            (
                r.insert_sql.clone(),
                r.quoted.clone(),
                r.relation.columns.len(),
            )
        };
        let stmt = self.prepare(&sql, n).await?;
        let mut params = Vec::with_capacity(n);
        for v in new.values {
            params.push(param(v, &table, "insert")?);
        }
        self.push(stmt, params, false, "INSERT", table);
        Ok(())
    }

    async fn apply_update(
        &mut self,
        rel_id: u32,
        key: Option<Tuple>,
        old: Option<Tuple>,
        new: Tuple,
    ) -> Result<()> {
        let (sql, params, table) = {
            let r = self.resolved(rel_id)?;
            let (sql, params) = build_update(r, key.as_ref(), old.as_ref(), &new)?;
            (sql, params, r.quoted.clone())
        };
        let stmt = self.prepare(&sql, params.len()).await?;
        self.push(stmt, params, true, "UPDATE", table);
        Ok(())
    }

    async fn apply_delete(
        &mut self,
        rel_id: u32,
        key: Option<Tuple>,
        old: Option<Tuple>,
    ) -> Result<()> {
        let (sql, params, table) = {
            let r = self.resolved(rel_id)?;
            let mut params = Vec::new();
            let where_clause = build_where(r, key.as_ref(), old.as_ref(), None, &mut params)?;
            (
                format!("DELETE FROM {} WHERE {where_clause}", r.quoted),
                params,
                r.quoted.clone(),
            )
        };
        let stmt = self.prepare(&sql, params.len()).await?;
        self.push(stmt, params, true, "DELETE", table);
        Ok(())
    }

    async fn apply_truncate(
        &mut self,
        relations: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    ) -> Result<()> {
        self.flush().await?;
        let mut names = Vec::new();
        for id in relations {
            names.push(self.resolved(id)?.quoted.clone());
        }
        let mut sql = format!("TRUNCATE TABLE {}", names.join(", "));
        if restart_identity {
            sql.push_str(" RESTART IDENTITY");
        }
        if cascade {
            sql.push_str(" CASCADE");
        }
        self.client
            .batch_execute(&sql)
            .await
            .map_err(|e| from_pg("truncate", e))?;
        Ok(())
    }

    /// Record a table as copied during the snapshot (inside the caller's transaction).
    pub async fn record_table_copied(
        client: &PgClient,
        job_id: &str,
        schema: &str,
        table: &str,
        rows: u64,
        bytes: u64,
    ) -> Result<()> {
        client
            .execute(
                "INSERT INTO pgsync.table_sync (job_id, schema_name, table_name, state, rows_copied, bytes_copied, copied_at) \
                 VALUES ($1, $2, $3, 'COPIED', $4, $5, now()) \
                 ON CONFLICT (job_id, schema_name, table_name) DO UPDATE SET state = 'COPIED', rows_copied = $4, bytes_copied = $5, copied_at = now()",
                &[&job_id, &schema, &table, &(rows as i64), &(bytes as i64)],
            )
            .await
            .map_err(|e| from_pg("record table copy", e))?;
        Ok(())
    }
}

fn param(v: Value, table: &str, op: &str) -> Result<TextParam> {
    Ok(match v {
        Value::Null => TextParam(None),
        Value::Text(b) => TextParam(Some(b)),
        Value::Binary(_) => {
            return Err(Error::unsupported(format!(
                "{table}: binary tuple values are not supported ({op})"
            )));
        }
        Value::Unchanged => {
            return Err(Error::unsupported(format!(
                "{table}: unchanged TOAST value in {op} where a value is required"
            )));
        }
    })
}

/// Build `UPDATE ... SET ... WHERE ...` for one change.
fn build_update(
    r: &ResolvedRelation,
    key: Option<&Tuple>,
    old: Option<&Tuple>,
    new: &Tuple,
) -> Result<(String, Vec<TextParam>)> {
    let rel = &r.relation;
    if new.values.len() != rel.columns.len() {
        return Err(Error::unsupported(format!(
            "{}: update tuple width mismatch (schema change?)",
            r.quoted
        )));
    }
    let mut sql = format!("UPDATE {} SET ", r.quoted);
    let mut params = Vec::new();
    let mut first = true;
    // Without an old key the key did not change, so key columns need no SET.
    let key_unchanged = key.is_none();
    for (i, v) in new.values.iter().enumerate() {
        if matches!(v, Value::Unchanged) || r.identity_always[i] {
            continue;
        }
        if key_unchanged && rel.columns[i].is_key && rel.replica_identity != ReplicaIdentity::Full {
            continue;
        }
        if !first {
            sql.push_str(", ");
        }
        first = false;
        params.push(param(v.clone(), &r.quoted, "update")?);
        sql.push_str(&format!(
            "{} = ${}::{}",
            quote_ident(&rel.columns[i].name),
            params.len(),
            r.types[i]
        ));
    }
    if first {
        // Every assignable column is unchanged TOAST or a key: nothing to assign, but the
        // row must still exist (affected-row check). Touch a key column with itself.
        let (_, col) = rel.key_columns().next().ok_or_else(|| {
            Error::unsupported(format!("{}: update with no assignable columns", r.quoted))
        })?;
        sql.push_str(&format!("{0} = {0}", quote_ident(&col.name)));
    }
    let where_clause = build_where(r, key, old, Some(new), &mut params)?;
    sql.push_str(" WHERE ");
    sql.push_str(&where_clause);
    Ok((sql, params))
}

/// Build the WHERE clause identifying the old row.
fn build_where(
    r: &ResolvedRelation,
    key: Option<&Tuple>,
    old: Option<&Tuple>,
    new: Option<&Tuple>,
    params: &mut Vec<TextParam>,
) -> Result<String> {
    let rel = &r.relation;
    let mut parts = Vec::new();
    if let Some(old) = old {
        // REPLICA IDENTITY FULL: match every column we know, null-safely.
        if old.values.len() != rel.columns.len() {
            return Err(Error::unsupported(format!(
                "{}: old tuple width mismatch",
                r.quoted
            )));
        }
        for (i, v) in old.values.iter().enumerate() {
            if matches!(v, Value::Unchanged) {
                continue;
            }
            params.push(param(v.clone(), &r.quoted, "where")?);
            parts.push(format!(
                "{} IS NOT DISTINCT FROM ${}::{}",
                quote_ident(&rel.columns[i].name),
                params.len(),
                r.types[i]
            ));
        }
    } else {
        let source = match (key, new) {
            (Some(k), _) => k,
            (None, Some(n)) => n,
            (None, None) => {
                return Err(Error::unsupported(format!(
                    "{}: delete without key or old tuple",
                    r.quoted
                )));
            }
        };
        if source.values.len() != rel.columns.len() {
            return Err(Error::unsupported(format!(
                "{}: key tuple width mismatch",
                r.quoted
            )));
        }
        for (i, col) in rel.key_columns() {
            let v = &source.values[i];
            if matches!(v, Value::Unchanged | Value::Null) {
                return Err(Error::unsupported(format!(
                    "{}: key column {:?} has no value",
                    r.quoted, col.name
                )));
            }
            params.push(param(v.clone(), &r.quoted, "where")?);
            parts.push(format!(
                "{} = ${}::{}",
                quote_ident(&col.name),
                params.len(),
                r.types[i]
            ));
        }
    }
    if parts.is_empty() {
        return Err(Error::unsupported(format!(
            "{}: no usable key (replica identity {:?}); UPDATE/DELETE cannot be applied",
            r.quoted, rel.replica_identity
        )));
    }
    Ok(parts.join(" AND "))
}

fn build_insert_sql(quoted: &str, rel: &Relation, types: &[String]) -> String {
    let cols: Vec<String> = rel.columns.iter().map(|c| quote_ident(&c.name)).collect();
    let vals: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(i, t)| format!("${}::{t}", i + 1))
        .collect();
    format!(
        "INSERT INTO {quoted} ({}) OVERRIDING SYSTEM VALUE VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    )
}

fn resolve(relation: Arc<Relation>, target: &[TargetColumn]) -> Result<ResolvedRelation> {
    let quoted = format!(
        "{}.{}",
        quote_ident(&relation.schema),
        quote_ident(&relation.name)
    );
    let mut types = Vec::with_capacity(relation.columns.len());
    let mut identity_always = Vec::with_capacity(relation.columns.len());
    for c in &relation.columns {
        let t = target.iter().find(|t| t.name == c.name).ok_or_else(|| {
            Error::unsupported(format!(
                "{quoted}: column {:?} does not exist on the target (schema change on the source?)",
                c.name
            ))
        })?;
        if t.generated {
            return Err(Error::unsupported(format!(
                "{quoted}: column {:?} is generated on the target",
                c.name
            )));
        }
        types.push(t.type_name.clone());
        identity_always.push(t.identity_always);
    }
    if relation.replica_identity == ReplicaIdentity::Nothing {
        warn!(table = %quoted, "relation has REPLICA IDENTITY NOTHING; UPDATE/DELETE will fail");
    }
    let insert_sql = build_insert_sql(&quoted, &relation, &types);
    Ok(ResolvedRelation {
        relation,
        quoted,
        types,
        identity_always,
        insert_sql,
    })
}

impl ReplicationSink for PgSink {
    async fn load_checkpoint(&mut self, job_id: &str) -> Result<Option<Checkpoint>> {
        let row = self
            .client
            .query_opt(
                "SELECT state, source_system_id, source_dbname, slot, publication, slot_owned, consistent_point, \
                 commit_lsn, committed_lsn, acked_lsn, last_xid, last_commit_ts, txns_applied, last_error \
                 FROM pgsync.job WHERE job_id = $1",
                &[&job_id],
            )
            .await
            .map_err(|e| from_pg("load checkpoint", e))?;
        let Some(r) = row else { return Ok(None) };
        let state: String = r.get(0);
        let lsn = |i: usize| -> Lsn { Lsn(u64::from(r.get::<_, PgLsn>(i))) };
        let cp = Checkpoint {
            job_id: job_id.to_owned(),
            state: state.parse().map_err(Error::internal)?,
            source_system_id: r.get(1),
            source_dbname: r.get(2),
            slot: r.get(3),
            publication: r.get(4),
            slot_owned: r.get(5),
            consistent_point: r.get::<_, Option<PgLsn>>(6).map(|l| Lsn(u64::from(l))),
            commit_lsn: lsn(7),
            committed_lsn: lsn(8),
            acked_lsn: lsn(9),
            last_xid: r.get::<_, Option<i64>>(10).map(|x| x as u32),
            last_commit_ts: r.get(11),
            txns_applied: r.get::<_, i64>(12) as u64,
            last_error: r.get(13),
        };
        cp.check_invariants()
            .map_err(|m| Error::internal(format!("stored checkpoint violates invariants: {m}")))?;
        Ok(Some(cp))
    }

    async fn save_checkpoint(&mut self, cp: &Checkpoint) -> Result<()> {
        if self.in_txn {
            return Err(Error::internal(
                "save_checkpoint called inside a data transaction",
            ));
        }
        upsert_checkpoint(&self.client, cp).await
    }

    async fn begin(&mut self) -> Result<()> {
        if self.in_txn {
            return Err(Error::internal("BEGIN while a target transaction is open"));
        }
        self.client
            .batch_execute("BEGIN")
            .await
            .map_err(|e| from_pg("BEGIN on target", e))?;
        self.in_txn = true;
        Ok(())
    }

    async fn relation(&mut self, rel: Arc<Relation>) -> Result<()> {
        if let Some(allowed) = &self.allowed_tables
            && !allowed.contains(&rel.qualified_name())
        {
            // pgoutput resolves publication membership from the historic catalog, so the
            // change stays in the stream even if the table is dropped from the publication
            // later; without a snapshot of the table the target cannot be made consistent.
            return Err(Error::resync_required(format!(
                "table {} is published but not in replication.tables and was never snapshotted, \
                 so its changes cannot be applied. Add it to replication.tables and \
                 re-bootstrap (`cleanup --yes`, then `bootstrap --restart-snapshot`)",
                rel.qualified_name()
            )));
        }
        // A relation message inside a transaction may follow pending statements for the
        // same table; flush so ordering is preserved before metadata changes.
        self.flush().await?;
        let target = target_columns(&self.client, &rel.schema, &rel.name)
            .await?
            .ok_or_else(|| {
                Error::unsupported(format!(
                    "table {}.{} does not exist on the target",
                    rel.schema, rel.name
                ))
            })?;
        let resolved = resolve(rel.clone(), &target)?;
        debug!(table = %resolved.quoted, columns = rel.columns.len(), "relation resolved");
        self.relations.insert(rel.id, resolved);
        Ok(())
    }

    async fn apply(&mut self, op: Op) -> Result<()> {
        if !self.in_txn {
            return Err(Error::internal("apply outside of a target transaction"));
        }
        match op {
            Op::Insert { relation, new } => self.apply_insert(relation, new).await?,
            Op::Update {
                relation,
                key,
                old,
                new,
            } => self.apply_update(relation, key, old, new).await?,
            Op::Delete { relation, key, old } => self.apply_delete(relation, key, old).await?,
            Op::Truncate {
                relations,
                cascade,
                restart_identity,
            } => {
                self.apply_truncate(relations, cascade, restart_identity)
                    .await?
            }
        }
        if self.needs_flush() {
            self.flush().await?;
        }
        Ok(())
    }

    async fn commit(&mut self, cp: &Checkpoint) -> Result<()> {
        if !self.in_txn {
            return Err(Error::internal("COMMIT without an open target transaction"));
        }
        self.flush().await?;
        let n = update_checkpoint(&self.client, cp).await?;
        if n != 1 {
            return Err(Error::internal(format!(
                "checkpoint row for job {:?} is missing on the target",
                cp.job_id
            )));
        }
        self.client
            .batch_execute("COMMIT")
            .await
            .map_err(|e| from_pg("COMMIT on target", e))?;
        self.in_txn = false;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        self.pending.clear();
        self.pending_bytes = 0;
        if self.in_txn {
            self.in_txn = false;
            self.client
                .batch_execute("ROLLBACK")
                .await
                .map_err(|e| from_pg("ROLLBACK on target", e))?;
        }
        Ok(())
    }
}

async fn update_checkpoint(client: &PgClient, cp: &Checkpoint) -> Result<u64> {
    client
        .execute(
            "UPDATE pgsync.job SET state = $2, commit_lsn = $3, committed_lsn = $4, acked_lsn = $5, last_xid = $6, \
             last_commit_ts = $7, txns_applied = $8, last_error = $9, updated_at = now() WHERE job_id = $1",
            &[
                &cp.job_id,
                &cp.state.as_str(),
                &PgLsn::from(cp.commit_lsn.0),
                &PgLsn::from(cp.committed_lsn.0),
                &PgLsn::from(cp.acked_lsn.0),
                &cp.last_xid.map(|x| x as i64),
                &cp.last_commit_ts,
                &(cp.txns_applied as i64),
                &cp.last_error,
            ],
        )
        .await
        .map_err(|e| from_pg("update checkpoint", e))
}

/// Settings every pgsync session on the target needs: a fixed search path, durable commits,
/// and no inherited timeouts. A `statement_timeout` or `idle_in_transaction_session_timeout`
/// set on the target role or database would otherwise cancel long transactions or large
/// copies on every attempt (SQLSTATE 57014 is retried, so the job would stall, not fail).
pub async fn configure_session(client: &PgClient) -> Result<()> {
    client
        .batch_execute(
            "SET search_path = pg_catalog; SET synchronous_commit = on; SET lock_timeout = 0; \
             SET statement_timeout = 0; SET idle_in_transaction_session_timeout = 0; \
             SET idle_session_timeout = 0",
        )
        .await
        .map_err(|e| from_pg("configure target session", e))?;
    // PostgreSQL 17+ only; ignore on older servers.
    let _ = client.batch_execute("SET transaction_timeout = 0").await;
    Ok(())
}

/// Insert or update the job row (autocommit).
pub async fn upsert_checkpoint(client: &PgClient, cp: &Checkpoint) -> Result<()> {
    client
        .execute(
            "INSERT INTO pgsync.job (job_id, state, source_system_id, source_dbname, slot, publication, slot_owned, \
             consistent_point, commit_lsn, committed_lsn, acked_lsn, last_xid, last_commit_ts, txns_applied, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
             ON CONFLICT (job_id) DO UPDATE SET state = EXCLUDED.state, source_system_id = EXCLUDED.source_system_id, \
             source_dbname = EXCLUDED.source_dbname, slot = EXCLUDED.slot, publication = EXCLUDED.publication, \
             slot_owned = EXCLUDED.slot_owned, consistent_point = EXCLUDED.consistent_point, commit_lsn = EXCLUDED.commit_lsn, \
             committed_lsn = EXCLUDED.committed_lsn, acked_lsn = EXCLUDED.acked_lsn, last_xid = EXCLUDED.last_xid, \
             last_commit_ts = EXCLUDED.last_commit_ts, txns_applied = EXCLUDED.txns_applied, last_error = EXCLUDED.last_error, \
             updated_at = now()",
            &[
                &cp.job_id,
                &cp.state.as_str(),
                &cp.source_system_id,
                &cp.source_dbname,
                &cp.slot,
                &cp.publication,
                &cp.slot_owned,
                &cp.consistent_point.map(|l| PgLsn::from(l.0)),
                &PgLsn::from(cp.commit_lsn.0),
                &PgLsn::from(cp.committed_lsn.0),
                &PgLsn::from(cp.acked_lsn.0),
                &cp.last_xid.map(|x| x as i64),
                &cp.last_commit_ts,
                &(cp.txns_applied as i64),
                &cp.last_error,
            ],
        )
        .await
        .map_err(|e| from_pg("save checkpoint", e))?;
    Ok(())
}

/// Delete the job's metadata rows (used by `cleanup`).
pub async fn delete_job(client: &PgClient, job_id: &str) -> Result<u64> {
    client
        .execute("DELETE FROM pgsync.job WHERE job_id = $1", &[&job_id])
        .await
        .map_err(|e| from_pg("delete job", e))
}

pub async fn set_job_state(
    client: &PgClient,
    job_id: &str,
    state: JobState,
    error: Option<&str>,
) -> Result<()> {
    client
        .execute(
            "UPDATE pgsync.job SET state = $2, last_error = $3, updated_at = now() WHERE job_id = $1",
            &[&job_id, &state.as_str(), &error],
        )
        .await
        .map_err(|e| from_pg("set job state", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgsync_core::event::Column;

    fn rel(identity: ReplicaIdentity) -> ResolvedRelation {
        let relation = Arc::new(Relation {
            id: 1,
            schema: "public".into(),
            name: "t".into(),
            replica_identity: identity,
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_key: true,
                },
                Column {
                    name: "v".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
                Column {
                    name: "big".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        });
        let target = vec![
            TargetColumn {
                name: "id".into(),
                type_name: "bigint".into(),
                generated: false,
                identity_always: true,
            },
            TargetColumn {
                name: "v".into(),
                type_name: "text".into(),
                generated: false,
                identity_always: false,
            },
            TargetColumn {
                name: "big".into(),
                type_name: "text".into(),
                generated: false,
                identity_always: false,
            },
        ];
        resolve(relation, &target).unwrap()
    }

    #[test]
    fn insert_sql_overrides_identity_and_casts() {
        let r = rel(ReplicaIdentity::Default);
        assert_eq!(
            r.insert_sql,
            "INSERT INTO \"public\".\"t\" (\"id\", \"v\", \"big\") OVERRIDING SYSTEM VALUE VALUES ($1::bigint, $2::text, $3::text)"
        );
    }

    #[test]
    fn where_uses_key_from_new_tuple_when_no_old_key() {
        let r = rel(ReplicaIdentity::Default);
        let new = Tuple {
            values: vec![Value::text("7"), Value::text("x"), Value::Unchanged],
        };
        let mut params = vec![];
        let w = build_where(&r, None, None, Some(&new), &mut params).unwrap();
        assert_eq!(w, "\"id\" = $1::bigint");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn where_prefers_old_key_and_handles_full_identity() {
        let r = rel(ReplicaIdentity::Default);
        let key = Tuple {
            values: vec![Value::text("5"), Value::Null, Value::Null],
        };
        let new = Tuple {
            values: vec![Value::text("7"), Value::text("x"), Value::text("y")],
        };
        let mut params = vec![];
        let w = build_where(&r, Some(&key), None, Some(&new), &mut params).unwrap();
        assert_eq!(w, "\"id\" = $1::bigint");
        assert_eq!(params[0].0.as_deref(), Some(&b"5"[..]));

        let r = rel(ReplicaIdentity::Full);
        let old = Tuple {
            values: vec![Value::text("5"), Value::Null, Value::Unchanged],
        };
        let mut params = vec![];
        let w = build_where(&r, None, Some(&old), None, &mut params).unwrap();
        assert_eq!(
            w,
            "\"id\" IS NOT DISTINCT FROM $1::bigint AND \"v\" IS NOT DISTINCT FROM $2::text"
        );
        assert!(params[1].0.is_none());
    }

    #[test]
    fn missing_key_is_rejected() {
        let r = rel(ReplicaIdentity::Default);
        let new = Tuple {
            values: vec![Value::Null, Value::text("x"), Value::text("y")],
        };
        let mut params = vec![];
        let err = build_where(&r, None, None, Some(&new), &mut params).unwrap_err();
        assert!(err.message().contains("key column"));
        assert!(build_where(&r, None, None, None, &mut params).is_err());
    }

    #[test]
    fn update_set_skips_identity_and_unchanged_key_columns() {
        let r = rel(ReplicaIdentity::Default);
        let new = Tuple {
            values: vec![Value::text("7"), Value::text("x"), Value::Unchanged],
        };
        let (sql, params) = build_update(&r, None, None, &new).unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"t\" SET \"v\" = $1::text WHERE \"id\" = $2::bigint"
        );
        assert_eq!(params.len(), 2);
        // Only TOAST-unchanged non-key columns: still a valid statement.
        let new = Tuple {
            values: vec![Value::text("7"), Value::Unchanged, Value::Unchanged],
        };
        let (sql, _) = build_update(&r, None, None, &new).unwrap();
        assert_eq!(
            sql,
            "UPDATE \"public\".\"t\" SET \"id\" = \"id\" WHERE \"id\" = $1::bigint"
        );
    }

    #[test]
    fn resolve_rejects_missing_or_generated_columns() {
        let relation = Arc::new(Relation {
            id: 1,
            schema: "public".into(),
            name: "t".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![Column {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
                is_key: true,
            }],
        });
        assert!(resolve(relation.clone(), &[]).is_err());
        let generated = vec![TargetColumn {
            name: "id".into(),
            type_name: "bigint".into(),
            generated: true,
            identity_always: false,
        }];
        assert!(resolve(relation, &generated).is_err());
    }
}
