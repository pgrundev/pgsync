-- Settings that tests change at runtime with ALTER SYSTEM must not be pinned on the
-- server command line: command-line options take precedence over postgresql.auto.conf.
ALTER SYSTEM SET max_slot_wal_keep_size = '1GB';
SELECT pg_reload_conf();
