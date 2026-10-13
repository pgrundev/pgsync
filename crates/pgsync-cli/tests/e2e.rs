//! End-to-end scenarios against Docker Compose. Run with:
//!   PGSYNC_E2E=1 cargo test -p pgsync-cli --test e2e -- --test-threads=1
//! Scenarios that pause containers or take minutes are `#[ignore]`d; run them with
//! `--ignored` (see Makefile `e2e-slow`).

mod common;
use common::*;
use std::time::Duration;

const T: Duration = Duration::from_secs(60);

/// Mixed DML that exercises every operation, type, TOAST, NULLs and rollback.
async fn mixed_dml(job: &Job, round: i64) {
    let s = &job.schema;
    job.src
        .batch_execute(&format!(
            "INSERT INTO {s}.accounts (email, name, balance, meta, tags, blob, updated_at) VALUES \
               ('r{round}a@x.test', 'Round {round}', 12.34, '{{\"tier\":\"gold\",\"n\":[1,2,3]}}', ARRAY['a','b'], '\\x00ff10'::bytea, '2024-01-02 03:04:05'), \
               ('r{round}b@x.test', NULL, -0.01, NULL, NULL, NULL, NULL); \
             UPDATE {s}.accounts SET name = 'Renamed ' || id, balance = balance + 1, meta = meta || '{{\"touched\":true}}' WHERE id % 5 = {round} % 5; \
             DELETE FROM {s}.events WHERE id % 11 = {round} % 11; \
             BEGIN; \
               INSERT INTO {s}.events (account_id, kind, amount, payload, note) VALUES (1, 'big', 1e9, '{{\"k\":\"v\"}}', repeat('toast-{round}-', 20000)); \
               UPDATE {s}.accounts SET meta = NULL, tags = '{{}}' WHERE id = 2; \
               UPDATE {s}.events SET amount = amount + 0.5 WHERE note LIKE 'toast-{round}-%'; \
             COMMIT; \
             BEGIN; INSERT INTO {s}.accounts (email) VALUES ('rolled-back-{round}@x.test'); ROLLBACK; \
             UPDATE {s}.events SET note = NULL WHERE account_id = 3; \
             INSERT INTO {s}.accounts (email, name) VALUES ('tmp-{round}@x.test', 'tmp'); \
             DELETE FROM {s}.accounts WHERE email = 'tmp-{round}@x.test';"
        ))
        .await
        .expect("mixed dml");
}

#[tokio::test]
async fn empty_tables_then_cdc() {
    e2e_guard!();
    let mut job = Job::new("empty", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let (code, out) = job.run(&["check"]);
    assert_eq!(code, 0, "{out}");
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    seed_accounts(&job.src, &job.schema, 20).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0, "{}", p.log());
    job.teardown().await;
}

#[tokio::test]
async fn snapshot_existing_rows_then_cdc_and_restart() {
    e2e_guard!();
    let mut job = Job::new("snapshot", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 500).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let row = job
        .tgt
        .query_one(
            "SELECT rows_copied FROM pgsync.table_sync WHERE job_id = $1 AND table_name = 'events'",
            &[&job.job_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1500);
    for r in 1..=3 {
        mixed_dml(&job, r).await;
    }
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    // Graceful restart resumes from the checkpoint.
    assert_eq!(p.terminate(), 0, "{}", p.log());
    mixed_dml(&job, 4).await;
    let mut p = job.spawn(&["run"]);
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let (state, txns, _, _, err) = job.job_row().await.unwrap();
    assert_eq!(state, "STREAMING");
    assert!(txns >= 4, "txns_applied={txns}");
    assert!(err.is_none(), "{err:?}");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn writes_during_snapshot_are_not_lost_or_duplicated() {
    e2e_guard!();
    let mut job = Job::new(
        "during",
        ACCOUNTS_DDL,
        &["accounts", "events"],
        "  max_parallel_copies: 1",
        "",
    )
    .await;
    seed_accounts(&job.src, &job.schema, 20_000).await; // ~80k rows, a few seconds of copy
    let mut p = job.spawn(&["bootstrap"]);
    // Insert / update / delete while the snapshot is running.
    let writer_url = job.source_admin_url.clone();
    let schema = job.schema.clone();
    let writer = tokio::spawn(async move {
        let c = connect(&writer_url).await;
        for i in 0..200i64 {
            c.batch_execute(&format!(
                "INSERT INTO {schema}.accounts (email, name) VALUES ('during-{i}@x.test', 'during'); \
                 UPDATE {schema}.accounts SET balance = balance + 1 WHERE id = {}; \
                 DELETE FROM {schema}.events WHERE id = {};",
                (i % 1000) + 1,
                i * 7 + 1
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    job.wait_state("STREAMING", Duration::from_secs(180)).await;
    writer.await.unwrap();
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn concurrent_transactions_preserve_order() {
    e2e_guard!();
    let mut job = Job::new("concurrent", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 50).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let mut writers = vec![];
    for w in 0..6i64 {
        let url = job.source_admin_url.clone();
        let schema = job.schema.clone();
        writers.push(tokio::spawn(async move {
            let c = connect(&url).await;
            for i in 0..100i64 {
                // Interleaved transactions touching the same rows; the final state depends
                // on commit order, which the target must reproduce.
                c.batch_execute(&format!(
                    "BEGIN; UPDATE {schema}.accounts SET balance = balance + {w} WHERE id = {}; \
                     INSERT INTO {schema}.events (account_id, kind, note) VALUES ({}, 'w{w}', 'i{i}'); COMMIT;",
                    (i % 50) + 1,
                    (i % 50) + 1
                ))
                .await
                .unwrap();
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn transaction_rollback_and_truncate() {
    e2e_guard!();
    let mut job = Job::new("rollback", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 30).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let s = &job.schema;
    job.src
        .batch_execute(&format!(
            "BEGIN; DELETE FROM {s}.events; UPDATE {s}.accounts SET balance = 0; ROLLBACK;"
        ))
        .await
        .unwrap();
    job.src.batch_execute(&format!("TRUNCATE TABLE {s}.events; INSERT INTO {s}.events (account_id, kind) VALUES (1, 'after-truncate');")).await.unwrap();
    job.src.batch_execute(&format!("TRUNCATE TABLE {s}.events, {s}.accounts RESTART IDENTITY CASCADE; INSERT INTO {s}.accounts (email) VALUES ('fresh@x.test');")).await.unwrap();
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let id: i64 = job
        .tgt
        .query_one(&format!("SELECT id FROM {s}.accounts"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(id, 1, "RESTART IDENTITY replicated through the row value");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn null_and_common_types_round_trip() {
    e2e_guard!();
    let ddl = r#"
CREATE TABLE types (
    id int PRIMARY KEY, i2 smallint, i8 bigint, f4 real, f8 double precision, num numeric(20,6),
    b boolean, t text, vc varchar(5), ch char(3), ts timestamp, tstz timestamptz, d date, tm time, iv interval,
    u uuid, j json, jb jsonb, ba bytea, ia int[], ta text[], inet_ inet, big text
);"#;
    let mut job = Job::new("types", ddl, &["types"], "", "").await;
    let s = job.schema.clone();
    job.src.batch_execute(&format!(
        "INSERT INTO {s}.types VALUES \
         (1, -32768, 9223372036854775807, 1.5, 2.25, 123456789012.123456, true, 'tab\there\nnewline \\ back ''quote''', 'abc', 'x', \
          '2024-02-29 12:34:56.789', '2024-02-29 12:34:56.789+05:30', '2024-02-29', '23:59:59.999', '1 year 2 days 03:04:05', \
          'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{{\"a\": [1, 2, {{\"b\": null}}]}}', '{{\"z\": 1, \"a\": \"ü\"}}', '\\x0001fe ff'::bytea, \
          ARRAY[1,2,3], ARRAY['x', 'y,z', 'q\"uote', NULL], '192.168.0.1/24', repeat('T', 300000)), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         (3, 0, 0, 'NaN', 'Infinity', 'NaN', false, '', '', '', 'infinity', '-infinity', '1999-12-31', '00:00', '0', \
          '00000000-0000-0000-0000-000000000000', 'null', '[]', '', '{{}}', '{{}}', '::1', NULL)"
    )).await.unwrap();
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    // CDC path for the same values, plus TOAST-unchanged update and NULL-ing.
    job.src.batch_execute(&format!(
        "INSERT INTO {s}.types SELECT 10 + id, i2, i8, f4, f8, num, b, t, vc, ch, ts, tstz, d, tm, iv, u, j, jb, ba, ia, ta, inet_, big FROM {s}.types; \
         UPDATE {s}.types SET i2 = 7 WHERE id = 11; \
         UPDATE {s}.types SET big = NULL, t = NULL WHERE id = 1; \
         UPDATE {s}.types SET big = repeat('U', 250000) WHERE id = 13; \
         DELETE FROM {s}.types WHERE id = 12;"
    )).await.unwrap();
    job.wait_converged(&[("types", "id")], T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn crash_sigkill_and_resume_without_loss_or_duplication() {
    e2e_guard!();
    let mut job = Job::new("crash", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 100).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    // Non-idempotent effects: counters and appends. Duplicated or skipped transactions
    // would change the final content hash.
    let url = job.source_admin_url.clone();
    let schema = job.schema.clone();
    let writer = tokio::spawn(async move {
        let c = connect(&url).await;
        for i in 0..400i64 {
            c.batch_execute(&format!(
                "BEGIN; UPDATE {schema}.accounts SET balance = balance + 1, name = coalesce(name,'') || 'x' WHERE id = {}; \
                 INSERT INTO {schema}.events (account_id, kind, note) VALUES ({}, 'crash', 'i{i}'); COMMIT;",
                (i % 100) + 1,
                (i % 100) + 1
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    p.kill();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut p = job.spawn(&["run"]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    p.kill();
    let mut p = job.spawn(&["run"]);
    writer.await.unwrap();
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let log = p.log();
    assert!(log.contains("replication stream opened"), "{log}");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn source_walsender_killed_reconnects() {
    e2e_guard!();
    let mut job = Job::new("srcdisc", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 20).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let pid = job.slot_active_pid().await.expect("walsender active");
    job.src
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let log = p.log();
    assert!(log.contains("transient failure; reconnecting"), "{log}");
    assert!(p.try_wait().is_none(), "process should still be running");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn target_connection_killed_reconnects() {
    e2e_guard!();
    let mut job = Job::new("tgtdisc", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 20).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    job.tgt
        .execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'pgsync-apply' AND pid <> pg_backend_pid()", &[])
        .await
        .unwrap();
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    let log = p.log();
    assert!(log.contains("transient failure; reconnecting"), "{log}");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn unsupported_schema_change_stops_visibly() {
    e2e_guard!();
    let mut job = Job::new("ddl", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 10).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let s = &job.schema;
    job.src.batch_execute(&format!("ALTER TABLE {s}.accounts ADD COLUMN extra text DEFAULT 'x'; INSERT INTO {s}.accounts (email, extra) VALUES ('ddl@x.test', 'y');")).await.unwrap();
    job.wait_state("ERROR", T).await;
    let code = p.wait();
    assert_eq!(code, 5, "unsupported exit code; log: {}", p.log());
    let (_, _, _, _, err) = job.job_row().await.unwrap();
    assert!(
        err.unwrap().contains("extra"),
        "error should name the column"
    );
    // The row must not have been applied partially.
    let n: i64 = job
        .tgt
        .query_one(
            &format!("SELECT count(*) FROM {s}.accounts WHERE email = 'ddl@x.test'"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    // Operator applies the DDL on the target, then resumes.
    job.tgt
        .batch_execute(&format!(
            "ALTER TABLE {s}.accounts ADD COLUMN extra text DEFAULT 'x'"
        ))
        .await
        .unwrap();
    let mut p = job.spawn(&["run"]);
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn missing_slot_requires_resync() {
    e2e_guard!();
    let mut job = Job::new("noslot", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    assert_eq!(p.terminate(), 0);
    job.src
        .execute("SELECT pg_drop_replication_slot($1)", &[&job.slot])
        .await
        .unwrap();
    let (code, out) = job.run(&["run"]);
    assert_eq!(code, 4, "{out}");
    assert_eq!(job.job_state().await.as_deref(), Some("RESYNC_REQUIRED"));
    let (code, out) = job.run(&["run"]);
    assert_eq!(code, 4, "second run must still refuse: {out}");
    let (code, out) = job.run(&["cleanup", "--yes"]);
    assert_eq!(code, 0, "{out}");
    assert!(job.job_state().await.is_none());
    job.teardown().await;
}

#[tokio::test]
async fn slot_advanced_by_another_consumer_requires_resync() {
    e2e_guard!();
    let mut job = Job::new("stolen", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    assert_eq!(p.terminate(), 0);
    mixed_dml(&job, 1).await;
    // Someone else consumes and advances the slot past our checkpoint.
    job.src
        .execute(
            "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())",
            &[&job.slot],
        )
        .await
        .unwrap();
    let (code, out) = job.run(&["run"]);
    assert_eq!(code, 4, "{out}");
    assert!(out.contains("another consumer"), "{out}");
    job.teardown().await;
}

#[tokio::test]
async fn preflight_rejects_bad_tables_and_publications() {
    e2e_guard!();
    let ddl = r#"
CREATE TABLE nokey (v int);
CREATE TABLE withkey (id int PRIMARY KEY, v int);
CREATE TABLE ri_nothing (id int PRIMARY KEY, v int);
ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING;
"#;
    let mut job = Job::new(
        "preflight",
        ddl,
        &["nokey", "withkey", "ri_nothing"],
        "",
        "",
    )
    .await;
    let (code, out) = job.run(&["check"]);
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("[FAIL] table e2e_preflight.nokey"), "{out}");
    assert!(
        out.contains("[FAIL] table e2e_preflight.ri_nothing"),
        "{out}"
    );
    assert!(out.contains("[PASS] table e2e_preflight.withkey"), "{out}");
    // Bootstrap must refuse too, without creating a slot.
    let (code, _) = job.run(&["bootstrap"]);
    assert_eq!(code, 3);
    assert!(!job.slot_exists().await);
    // Table in config but not in publication.
    job.write_config(&["withkey", "nokey", "ri_nothing", "missing"], "", "");
    let (_, out) = job.run(&["check"]);
    assert!(out.contains("does not exist"), "{out}");
    job.teardown().await;
}

#[tokio::test]
async fn wal_retention_threshold_stops_job() {
    e2e_guard!();
    let safety = "  wal_retention_alarm_bytes: 1024\n  wal_retention_action: stop";
    let mut job = Job::new("walret", ACCOUNTS_DDL, &["accounts", "events"], "", safety).await;
    seed_accounts(&job.src, &job.schema, 10).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    // Generate WAL the slot must retain (restart_lsn lags while the walsender decodes).
    job.src.batch_execute("CREATE TABLE IF NOT EXISTS public.walfill AS SELECT g, repeat('w', 1000) AS pad FROM generate_series(1, 20000) g; DROP TABLE public.walfill").await.unwrap();
    let code = p.wait();
    assert_ne!(code, 0);
    let log = p.log();
    assert!(log.contains("ALARM: WAL retained"), "{log}");
    assert_eq!(job.job_state().await.as_deref(), Some("PAUSED"));
    let (_, _, _, _, err) = job.job_row().await.unwrap();
    assert!(err.unwrap().contains("Stopping does not release WAL"));
    // Resumable after the operator raises the threshold.
    job.write_config(&["accounts", "events"], "", "");
    let mut p = job.spawn(&["run"]);
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn large_rows_and_toast_updates() {
    e2e_guard!();
    let mut job = Job::new("toast", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let s = job.schema.clone();
    job.src.batch_execute(&format!(
        "INSERT INTO {s}.accounts (email, name, blob) SELECT format('big%s@x.test', g), repeat('n', 60), decode(repeat('ab', 400000), 'hex') FROM generate_series(1, 20) g; \
         INSERT INTO {s}.events (account_id, kind, note) SELECT g, 'toast', repeat(format('%s-', g), 100000) FROM generate_series(1, 20) g;"
    )).await.unwrap();
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    // Updates that leave TOASTed columns untouched ('u' values) and ones that replace them.
    job.src
        .batch_execute(&format!(
            "UPDATE {s}.accounts SET name = 'changed' WHERE id <= 10; \
         UPDATE {s}.events SET kind = 'kept' WHERE id <= 10; \
         UPDATE {s}.events SET note = repeat('new', 100000) WHERE id > 10; \
         UPDATE {s}.accounts SET blob = NULL WHERE id > 15; \
         INSERT INTO {s}.events (account_id, kind, note) VALUES (1, 'huge', repeat('h', 5000000));"
        ))
        .await
        .unwrap();
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn status_and_cleanup_commands() {
    e2e_guard!();
    let mut job = Job::new("status", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let (code, out) = job.run(&["status"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("not bootstrapped"));
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let (code, out) = job.run(&["status"]);
    assert_eq!(code, 0);
    assert!(
        out.contains("STREAMING") && out.contains("slot: active=true"),
        "{out}"
    );
    let (code, out) = job.run(&["--json", "status"]);
    assert_eq!(code, 0);
    assert!(out.contains("\"state\": \"STREAMING\""), "{out}");
    let (code, _) = job.run(&["cleanup"]);
    assert_ne!(code, 0, "cleanup without --yes must refuse");
    assert!(job.slot_exists().await);
    let (code, out) = job.run(&["cleanup", "--yes"]);
    assert_ne!(
        code, 0,
        "cleanup must refuse while the slot is active: {out}"
    );
    assert_eq!(p.terminate(), 0);
    let (code, out) = job.run(&["cleanup", "--yes"]);
    assert_eq!(code, 0, "{out}");
    assert!(!job.slot_exists().await);
    assert!(job.job_state().await.is_none());
    // Bootstrap again from scratch works after cleanup (target tables are dropped by --restart-snapshot).
    let (code, out) = job.run(&["check"]);
    assert_eq!(code, 3, "target tables exist: {out}");
    let mut p = job.spawn(&["bootstrap", "--restart-snapshot"]);
    job.wait_state("STREAMING", T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn bootstrap_refuses_duplicate_and_incomplete_snapshot_restart() {
    e2e_guard!();
    let mut job = Job::new("dupboot", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    assert_eq!(p.terminate(), 0);
    let (code, out) = job.run(&["bootstrap"]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("already exists"), "{out}");
    // Simulate a snapshot that died midway: state SNAPSHOTTING is not resumable.
    job.tgt
        .execute(
            "UPDATE pgsync.job SET state = 'SNAPSHOTTING' WHERE job_id = $1",
            &[&job.job_id],
        )
        .await
        .unwrap();
    let (code, out) = job.run(&["run"]);
    assert_eq!(code, 3, "{out}");
    let mut p = job.spawn(&["bootstrap", "--restart-snapshot"]);
    job.wait_state("STREAMING", T).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

// ---- slow / disruptive scenarios (run with --ignored) ------------------------------

#[tokio::test]
#[ignore]
async fn destination_paused_backpressure_then_recovers() {
    e2e_guard!();
    let resources = "  max_inflight_bytes: 65536\n  channel_capacity: 64";
    let mut job = Job::new(
        "pause",
        ACCOUNTS_DDL,
        &["accounts", "events"],
        resources,
        "",
    )
    .await;
    seed_accounts(&job.src, &job.schema, 50).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    docker(&["pause", "pgsync-target"]);
    // Source keeps producing well beyond the channel limits while the target is frozen.
    let url = job.source_admin_url.clone();
    let schema = job.schema.clone();
    let writer = tokio::spawn(async move {
        let c = connect(&url).await;
        for i in 0..3000i64 {
            c.batch_execute(&format!(
                "INSERT INTO {schema}.events (account_id, kind, note) VALUES ({}, 'bp', repeat('p', 2000));",
                (i % 50) + 1
            ))
            .await
            .unwrap();
        }
    });
    tokio::time::sleep(Duration::from_secs(20)).await;
    // Longer than wal_sender_timeout/2 (30s in compose): the walsender must still be alive
    // because feedback kept flowing while the applier was blocked.
    assert!(
        job.slot_active_pid().await.is_some(),
        "walsender dropped during backpressure"
    );
    docker(&["unpause", "pgsync-target"]);
    writer.await.unwrap();
    job.wait_converged(ACCOUNTS_TABLES, Duration::from_secs(120))
        .await;
    assert!(p.try_wait().is_none());
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
#[ignore]
async fn destination_restart_is_survived() {
    e2e_guard!();
    let mut job = Job::new("tgtrestart", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 20).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    docker(&["restart", "pgsync-target"]);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Reconnect our own admin client as well.
    let tgt = connect(&job.target_url).await;
    job.tgt = tgt;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, Duration::from_secs(90))
        .await;
    assert!(p.log().contains("reconnecting"));
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
#[ignore]
async fn source_restart_is_survived() {
    e2e_guard!();
    let mut job = Job::new("srcrestart", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 20).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    docker(&["restart", "pgsync-source"]);
    tokio::time::sleep(Duration::from_secs(4)).await;
    job.src = connect(&job.source_admin_url).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, Duration::from_secs(90))
        .await;
    assert!(p.log().contains("reconnecting"));
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
#[ignore]
async fn repeated_random_crashes_converge() {
    e2e_guard!();
    use rand::Rng;
    let mut job = Job::new("stress", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 200).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    let url = job.source_admin_url.clone();
    let schema = job.schema.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let writer = tokio::spawn(async move {
        let c = connect(&url).await;
        let mut i = 0i64;
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            i += 1;
            c.batch_execute(&format!(
                "BEGIN; UPDATE {schema}.accounts SET balance = balance + 1, name = left(coalesce(name,'') || 'x', 70) WHERE id = {}; \
                 INSERT INTO {schema}.events (account_id, kind, note) VALUES ({}, 'stress', 'i{i}'); \
                 DELETE FROM {schema}.events WHERE id = {}; COMMIT;",
                (i % 200) + 1,
                (i % 200) + 1,
                i * 3
            ))
            .await
            .unwrap();
            if i % 50 == 0 {
                c.batch_execute(&format!(
                    "UPDATE {schema}.events SET note = repeat('big', 50000) WHERE id % 97 = 0"
                ))
                .await
                .unwrap();
            }
        }
        i
    });
    let rounds: u32 = std::env::var("PGSYNC_E2E_CRASH_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    let mut rng = rand::rng();
    for round in 0..rounds {
        let ms = rng.random_range(100..2500);
        tokio::time::sleep(Duration::from_millis(ms)).await;
        if rng.random_bool(0.7) {
            p.kill();
        } else {
            let _ = p.terminate();
        }
        eprintln!("round {round}: killed after {ms} ms");
        p = job.spawn(&["run"]);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let n = writer.await.unwrap();
    eprintln!("writer produced {n} transactions");
    job.wait_converged(ACCOUNTS_TABLES, Duration::from_secs(120))
        .await;
    let (_, txns, _, _, _) = job.job_row().await.unwrap();
    eprintln!("target applied {txns} transactions");
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
#[ignore]
async fn large_table_snapshot() {
    e2e_guard!();
    let mut job = Job::new(
        "large",
        ACCOUNTS_DDL,
        &["accounts", "events"],
        "  max_parallel_copies: 2",
        "",
    )
    .await;
    let n: i64 = std::env::var("PGSYNC_E2E_LARGE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300_000);
    seed_accounts(&job.src, &job.schema, n).await; // 4n rows
    let started = std::time::Instant::now();
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", Duration::from_secs(600)).await;
    eprintln!("snapshot of {} rows took {:?}", n * 4, started.elapsed());
    job.wait_converged(ACCOUNTS_TABLES, Duration::from_secs(120))
        .await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn idle_slot_advances_over_unpublished_wal() {
    e2e_guard!();
    let mut job = Job::new("idle", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 5).await;
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    // WAL that carries nothing for this publication.
    job.src
        .batch_execute("CREATE TABLE IF NOT EXISTS public.unpublished_fill (g int, pad text); INSERT INTO public.unpublished_fill SELECT g, repeat('u', 500) FROM generate_series(1, 20000) g; DROP TABLE public.unpublished_fill")
        .await
        .unwrap();
    let head = job.source_wal_lsn().await;
    // The walsender pings after wal_sender_timeout/2 (15 s in compose); the reply must
    // carry the skipped position so the slot does not retain that WAL.
    let start = std::time::Instant::now();
    loop {
        let row = job
            .src
            .query_one("SELECT confirmed_flush_lsn >= $1::text::pg_lsn, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $2", &[&head, &job.slot])
            .await
            .unwrap();
        if row.get::<_, bool>(0) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(45),
            "slot stuck at {} while WAL head is {head}",
            row.get::<_, String>(1)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn published_but_unconfigured_table_stops_visibly() {
    e2e_guard!();
    // Only `accounts` is configured and published. After bootstrap an operator adds a second
    // table to the publication; a same-named table also exists on the target (never
    // snapshotted). pgsync must refuse to apply its changes instead of diverging silently.
    let mut job = Job::new("unconfigured", ACCOUNTS_DDL, &["accounts"], "", "").await;
    seed_accounts(&job.src, &job.schema, 10).await;
    let s = job.schema.clone();
    // `events` exists on the source (referenced by ACCOUNTS_DDL) but is not published yet.
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    job.tgt
        .batch_execute(&format!(
            "CREATE TABLE {s}.events (id bigserial PRIMARY KEY, account_id bigint NOT NULL, kind text NOT NULL, amount numeric(14,2), payload json, note text)"
        ))
        .await
        .unwrap();
    job.src
        .batch_execute(&format!(
            "ALTER PUBLICATION {} ADD TABLE {s}.events; \
             INSERT INTO {s}.events (account_id, kind, note) VALUES (1, 'leak', 'must not arrive'); \
             UPDATE {s}.accounts SET balance = balance + 1 WHERE id = 1;",
            job.publication
        ))
        .await
        .unwrap();
    job.wait_state("RESYNC_REQUIRED", T).await;
    assert_eq!(p.wait(), 4, "{}", p.log());
    let (_, _, _, _, err) = job.job_row().await.unwrap();
    let err = err.unwrap();
    assert!(
        err.contains(&format!("{s}.events")) && err.contains("not in replication.tables"),
        "{err}"
    );
    async fn count(job: &Job) -> i64 {
        let q = format!("SELECT count(*) FROM {}.events", job.schema);
        job.tgt.query_one(&q, &[]).await.unwrap().get(0)
    }
    assert_eq!(
        count(&job).await,
        0,
        "nothing may be applied to the unconfigured table"
    );
    // `run` keeps refusing: the change is in the slot's WAL regardless of later publication
    // edits (membership is resolved from the historic catalog).
    let (code, _) = job.run(&["run"]);
    assert_eq!(code, 4);
    assert_eq!(count(&job).await, 0);
    // Recovery: configure the table and re-bootstrap (the target copy is dropped and
    // recreated from a new snapshot, which now includes the row).
    job.write_config(&["accounts", "events"], "", "");
    let (code, out) = job.run(&["cleanup", "--yes"]);
    assert_eq!(code, 0, "{out}");
    let mut p = job.spawn(&["bootstrap", "--restart-snapshot"]);
    job.wait_state("STREAMING", T).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert!(count(&job).await >= 1);
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn bootstrap_recovers_from_interrupted_bootstrap() {
    e2e_guard!();
    // A bootstrap that died between recording its intent (job row INITIALIZING, slot owned)
    // and recording the snapshot may have left a slot behind. The next plain `bootstrap`
    // must drop that slot and proceed instead of refusing or orphaning it.
    let mut job = Job::new("interrupted", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    seed_accounts(&job.src, &job.schema, 10).await;
    let (code, _) = job.run(&["status"]); // creates the pgsync metadata tables
    assert_eq!(code, 0);
    job.tgt
        .execute(
            "INSERT INTO pgsync.job (job_id, state, source_system_id, source_dbname, slot, publication, slot_owned) \
             VALUES ($1, 'INITIALIZING', 'x', 'app', $2, $3, true)",
            &[&job.job_id, &job.slot, &job.publication],
        )
        .await
        .unwrap();
    job.src
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&job.slot],
        )
        .await
        .unwrap();
    let orphan_restart: String = job
        .src
        .query_one(
            "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&job.slot],
        )
        .await
        .unwrap()
        .get(0);
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", T).await;
    assert!(p.log().contains("interrupted bootstrap"), "{}", p.log());
    // The slot in use now is a new one (created with an exported snapshot), not the orphan.
    let now_restart: String = job
        .src
        .query_one(
            "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&job.slot],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(orphan_restart, now_restart);
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    // `cleanup` on an INITIALIZING leftover drops the owned slot too.
    job.tgt
        .execute(
            "UPDATE pgsync.job SET state = 'INITIALIZING' WHERE job_id = $1",
            &[&job.job_id],
        )
        .await
        .unwrap();
    let (code, out) = job.run(&["cleanup", "--yes"]);
    assert_eq!(code, 0, "{out}");
    assert!(!job.slot_exists().await);
    job.teardown().await;
}

#[tokio::test]
async fn snapshot_failure_drops_the_fresh_slot() {
    e2e_guard!();
    // A failed copy cannot be resumed, so the slot created for it must not be left
    // retaining WAL. The job stays SNAPSHOTTING with the reason, `run` refuses, and
    // `bootstrap --restart-snapshot` starts over.
    let mut job = Job::new(
        "snapfail",
        ACCOUNTS_DDL,
        &["accounts", "events"],
        "  snapshot_timeout: 1ms",
        "",
    )
    .await;
    seed_accounts(&job.src, &job.schema, 20_000).await;
    let (code, out) = job.run(&["bootstrap"]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("snapshot exceeded snapshot_timeout"), "{out}");
    assert_eq!(job.job_state().await.as_deref(), Some("SNAPSHOTTING"));
    let (_, _, _, _, err) = job.job_row().await.unwrap();
    let err = err.unwrap();
    assert!(err.contains("slot was dropped"), "{err}");
    assert!(
        !job.slot_exists().await,
        "slot must not retain WAL after a failed snapshot"
    );
    let (code, _) = job.run(&["run"]);
    assert_eq!(code, 3, "SNAPSHOTTING is not resumable");
    job.write_config(&["accounts", "events"], "", "");
    let mut p = job.spawn(&["bootstrap", "--restart-snapshot"]);
    job.wait_state("STREAMING", Duration::from_secs(120)).await;
    mixed_dml(&job, 1).await;
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}

#[tokio::test]
async fn inherited_target_timeouts_do_not_stall_copy_or_apply() {
    e2e_guard!();
    // Timeouts configured on the target database would cancel the copy (and long apply
    // transactions) on every attempt; pgsync's own sessions must reset them.
    let mut job = Job::new("timeouts", ACCOUNTS_DDL, &["accounts", "events"], "", "").await;
    let reset = "ALTER DATABASE app RESET statement_timeout; ALTER DATABASE app RESET idle_in_transaction_session_timeout";
    job.tgt.batch_execute(reset).await.unwrap();
    job.tgt
        .batch_execute("ALTER DATABASE app SET statement_timeout = '20ms'; ALTER DATABASE app SET idle_in_transaction_session_timeout = '20ms'")
        .await
        .unwrap();
    seed_accounts(&job.src, &job.schema, 20_000).await; // the COPY takes well over 20 ms
    let mut p = job.spawn(&["bootstrap"]);
    job.wait_state("STREAMING", Duration::from_secs(120)).await;
    let s = &job.schema;
    job.src
        .batch_execute(&format!(
            "INSERT INTO {s}.events (account_id, kind, note) SELECT (g % 100) + 1, 'bulk', repeat('x', 200) FROM generate_series(1, 20000) g"
        ))
        .await
        .unwrap();
    job.wait_converged(ACCOUNTS_TABLES, T).await;
    job.tgt.batch_execute(reset).await.unwrap();
    assert!(!p.log().contains("reconnecting"), "{}", p.log());
    assert_eq!(p.terminate(), 0);
    job.teardown().await;
}
