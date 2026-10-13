//! Schema bootstrap via `pg_dump --schema-only`, and target column verification.

use crate::client::PgClient;
use crate::errors::from_pg;
use crate::url::{ConnectionInfo, TlsPolicy};
use pgsync_core::config::{SafetyConfig, TableName, quote_ident};
use pgsync_core::{Error, ErrorKind, Result};
use std::collections::BTreeSet;
use tracing::info;

/// Run `pg_dump --schema-only` for `tables` and return SQL safe for `batch_execute`.
pub async fn dump_schema(
    pg_dump: Option<&str>,
    info: &ConnectionInfo,
    safety: &SafetyConfig,
    tables: &[TableName],
) -> Result<String> {
    let bin = pg_dump.unwrap_or("pg_dump");
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args([
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "--no-comments",
        "--no-publications",
        "--no-subscriptions",
        "--no-security-labels",
        "--no-tablespaces",
        "--format=plain",
    ]);
    for t in tables {
        cmd.arg("--table").arg(format!(
            "{}.{}",
            quote_ident(&t.schema),
            quote_ident(&t.name)
        ));
    }
    // Credentials go through the environment, never the argument list.
    let url = format!(
        "postgresql://{}@{}:{}/{}",
        info.user, info.host, info.port, info.dbname
    );
    cmd.arg("--dbname").arg(&url);
    cmd.env_remove("PGPASSWORD");
    if let Some(pw) = info.password() {
        cmd.env("PGPASSWORD", String::from_utf8_lossy(pw).into_owned());
    }
    let sslmode = match info.tls {
        TlsPolicy::Disabled => "disable",
        TlsPolicy::Preferred { .. } => "prefer",
        TlsPolicy::Required { verify: true } => "verify-full",
        TlsPolicy::Required { verify: false } => "require",
    };
    cmd.env("PGSSLMODE", sslmode);
    if let Some(ca) = &safety.tls_ca_file {
        cmd.env("PGSSLROOTCERT", ca);
    }
    cmd.stdin(std::process::Stdio::null());
    let out = cmd.output().await.map_err(|e| {
        Error::with_source(
            ErrorKind::Config,
            format!("cannot run {bin} (set schema.pg_dump_path or schema.mode: existing)"),
            e,
        )
    })?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(Error::preflight(format!(
            "{bin} failed ({}): {}",
            out.status,
            stderr.trim()
        )));
    }
    let sql = String::from_utf8(out.stdout)
        .map_err(|_| Error::internal("pg_dump output is not UTF-8"))?;
    info!(tables = tables.len(), bytes = sql.len(), "schema dumped");
    Ok(filter_dump(&sql))
}

/// Remove psql meta-commands and session settings that are not portable across server
/// versions. Statements that define objects are kept verbatim.
pub fn filter_dump(sql: &str) -> String {
    let drop_prefixes = [
        "SET statement_timeout",
        "SET lock_timeout",
        "SET idle_in_transaction_session_timeout",
        "SET transaction_timeout",
        "SET default_tablespace",
        "SET default_table_access_method",
        "SET row_security",
        "SET check_function_bodies",
        "SET xmloption",
        "SET client_min_messages",
    ];
    sql.lines()
        .filter(|l| !l.starts_with('\\'))
        .filter(|l| !drop_prefixes.iter().any(|p| l.starts_with(p)))
        .map(|l| format!("{l}\n"))
        .collect()
}

/// Create the schemas and apply the dump in one transaction.
pub async fn apply_schema(target: &PgClient, tables: &[TableName], sql: &str) -> Result<()> {
    let schemas: BTreeSet<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
    let mut script = String::from("BEGIN;\n");
    for s in schemas {
        script.push_str(&format!(
            "CREATE SCHEMA IF NOT EXISTS {};\n",
            quote_ident(s)
        ));
    }
    script.push_str(sql);
    script.push_str("\nCOMMIT;\n");
    target
        .batch_execute(&script)
        .await
        .map_err(|e| from_pg("apply schema to target", e))?;
    Ok(())
}

/// Target column metadata used by the sink to cast values.
#[derive(Debug, Clone)]
pub struct TargetColumn {
    pub name: String,
    pub type_name: String,
    pub generated: bool,
    /// `GENERATED ALWAYS AS IDENTITY`: cannot be assigned by UPDATE, never changes on the source.
    pub identity_always: bool,
}

pub async fn target_columns(
    target: &PgClient,
    schema: &str,
    name: &str,
) -> Result<Option<Vec<TargetColumn>>> {
    let rows = target
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.attgenerated <> '', a.attidentity = 'a' \
             FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&schema, &name],
        )
        .await
        .map_err(|e| from_pg(&format!("target columns of {schema}.{name}"), e))?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        rows.iter()
            .map(|r| TargetColumn {
                name: r.get(0),
                type_name: r.get(1),
                generated: r.get(2),
                identity_always: r.get(3),
            })
            .collect(),
    ))
}

/// Verify that every source column exists on the target with a compatible declaration.
pub fn verify_columns(
    table: &TableName,
    source: &[crate::source::ColumnInfo],
    target: &[TargetColumn],
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    for s in source.iter().filter(|c| !c.generated) {
        match target.iter().find(|t| t.name == s.name) {
            None => {
                return Err(Error::preflight(format!(
                    "{table}: column {:?} is missing on the target",
                    s.name
                )));
            }
            Some(t) if t.generated => {
                return Err(Error::preflight(format!(
                    "{table}: column {:?} is generated on the target but not on the source",
                    s.name
                )));
            }
            Some(t) if t.type_name != s.type_name => {
                warnings.push(format!(
                    "{table}.{}: source type {} vs target type {}",
                    s.name, s.type_name, t.type_name
                ));
            }
            Some(_) => {}
        }
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_strips_meta_commands_and_timeouts() {
        let dump = "\\restrict abc\nSET statement_timeout = 0;\nSET transaction_timeout = 0;\nSET client_encoding = 'UTF8';\nSELECT pg_catalog.set_config('search_path', '', false);\nCREATE TABLE public.t (\n    id bigint NOT NULL\n);\n\\unrestrict abc\n";
        let out = filter_dump(dump);
        assert_eq!(
            out,
            "SET client_encoding = 'UTF8';\nSELECT pg_catalog.set_config('search_path', '', false);\nCREATE TABLE public.t (\n    id bigint NOT NULL\n);\n"
        );
    }

    #[test]
    fn verify_columns_detects_missing_and_type_drift() {
        let t = TableName::new("public", "t");
        let src = vec![
            crate::source::ColumnInfo {
                attnum: 1,
                name: "id".into(),
                type_name: "bigint".into(),
                not_null: true,
                generated: false,
                identity: "a".into(),
            },
            crate::source::ColumnInfo {
                attnum: 2,
                name: "v".into(),
                type_name: "text".into(),
                not_null: false,
                generated: false,
                identity: "".into(),
            },
            crate::source::ColumnInfo {
                attnum: 3,
                name: "g".into(),
                type_name: "text".into(),
                not_null: false,
                generated: true,
                identity: "".into(),
            },
        ];
        let tgt = vec![
            TargetColumn {
                name: "id".into(),
                type_name: "bigint".into(),
                generated: false,
                identity_always: true,
            },
            TargetColumn {
                name: "v".into(),
                type_name: "character varying(10)".into(),
                generated: false,
                identity_always: false,
            },
        ];
        let w = verify_columns(&t, &src, &tgt).unwrap();
        assert_eq!(w.len(), 1);
        assert!(w[0].contains("text vs target type character varying"));
        let err = verify_columns(&t, &src, &tgt[..1]).unwrap_err();
        assert!(err.message().contains("missing on the target"));
    }
}
