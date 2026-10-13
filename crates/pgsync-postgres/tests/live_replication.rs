//! Live protocol test against the Docker source. Skipped unless PGSYNC_TEST_SOURCE_URL is set.

use pgsync_core::checkpoint::Checkpoint;
use pgsync_core::config::SafetyConfig;
use pgsync_core::event::{Op, SourceEvent};
use pgsync_core::traits::{EventStream, Feedback, StreamFactory};
use pgsync_postgres::replication::conn::ReplicationConnection;
use pgsync_postgres::replication::slot::{
    create_slot_with_snapshot, drop_slot, identify_system, read_slot,
};
use pgsync_postgres::replication::stream::PgStreamFactory;
use pgsync_postgres::url::ConnectionInfo;
use std::time::Duration;

fn source() -> Option<(ConnectionInfo, SafetyConfig)> {
    let url = std::env::var("PGSYNC_TEST_SOURCE_URL").ok()?;
    let safety = SafetyConfig::default();
    Some((ConnectionInfo::parse(&url, &safety).unwrap(), safety))
}

#[tokio::test]
async fn replication_connection_streams_an_insert() {
    let Some((info, safety)) = source() else {
        eprintln!("skipped: PGSYNC_TEST_SOURCE_URL not set");
        return;
    };
    let timeout = Duration::from_secs(5);
    let slot = "pgsync_live_test";
    // Produce changes with an ordinary client as a role that may write (the replication
    // role is read-only by design). Clean up before the slot exists so the delete is not
    // part of the stream.
    let admin_url = std::env::var("PGSYNC_TEST_SOURCE_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433/app".to_owned());
    let admin = ConnectionInfo::parse(&admin_url, &safety).unwrap();
    let pg = pgsync_postgres::client::connect(&admin, &safety, timeout, "pgsync-test")
        .await
        .unwrap();
    pg.batch_execute("DELETE FROM public.users WHERE email = 'live-test@example.test'")
        .await
        .unwrap();

    let mut conn = ReplicationConnection::connect(&info, &safety, timeout)
        .await
        .unwrap();
    let sys = identify_system(&mut conn).await.unwrap();
    assert_eq!(sys.dbname.as_deref(), Some("app"));

    if read_slot(&mut conn, slot).await.unwrap().is_some() {
        drop_slot(&mut conn, slot).await.unwrap();
    }
    let created = create_slot_with_snapshot(&mut conn, slot).await.unwrap();
    assert!(!created.snapshot_name.is_empty());
    assert!(created.consistent_point.as_u64() > 0);
    conn.close().await;

    pg.batch_execute("INSERT INTO public.users (email, full_name) VALUES ('live-test@example.test', 'Live Test')")
        .await
        .unwrap();

    let mut cp = Checkpoint::new_job("t", &sys.system_id, "app", slot, "pgsync_demo");
    cp.consistent_point = Some(created.consistent_point);
    cp.commit_lsn = created.consistent_point;
    cp.committed_lsn = created.consistent_point;
    cp.acked_lsn = created.consistent_point;
    let mut factory = PgStreamFactory {
        info: info.clone(),
        safety: safety.clone(),
        timeout,
        slot: slot.into(),
        publication: "pgsync_demo".into(),
    };
    let (mut stream, wal_end) = factory.connect(&cp).await.unwrap();
    assert!(wal_end >= created.consistent_point);

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let ev = tokio::time::timeout_at(deadline, stream.next_event())
            .await
            .expect("event before deadline")
            .unwrap();
        if matches!(ev, SourceEvent::Keepalive { .. }) {
            continue;
        }
        let done = matches!(ev, SourceEvent::Commit { .. });
        seen.push(ev);
        if done {
            break;
        }
    }
    assert!(matches!(seen[0], SourceEvent::Begin { .. }), "{seen:?}");
    assert!(
        matches!(&seen[1], SourceEvent::Relation(r) if r.name == "users" && r.schema == "public"),
        "{seen:?}"
    );
    assert!(
        matches!(&seen[2], SourceEvent::Op(Op::Insert { new, .. }) if new.values.len() == 8),
        "{seen:?}"
    );
    let SourceEvent::Commit { end_lsn, .. } = seen[3] else {
        panic!()
    };
    stream
        .send_feedback(Feedback {
            written: end_lsn,
            flushed: end_lsn,
            applied: end_lsn,
            reply_requested: true,
        })
        .await
        .unwrap();
    // The server answers a reply request with a keepalive.
    let ev = tokio::time::timeout(Duration::from_secs(5), stream.next_event())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(ev, SourceEvent::Keepalive { .. }), "{ev:?}");
    stream.close().await;

    // Clean up: the slot's confirmed position should now reflect our feedback.
    let mut conn = ReplicationConnection::connect(&info, &safety, timeout)
        .await
        .unwrap();
    let s = read_slot(&mut conn, slot).await.unwrap().unwrap();
    assert_eq!(s.confirmed_flush_lsn, Some(end_lsn));
    drop_slot(&mut conn, slot).await.unwrap();
    pg.batch_execute("DELETE FROM public.users WHERE email = 'live-test@example.test'")
        .await
        .unwrap();
}
