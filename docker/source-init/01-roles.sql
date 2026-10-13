-- Least-privilege replication role used by PGSync.
-- REPLICATION lets it create logical slots and stream WAL; it has no DML rights.
CREATE ROLE pgsync WITH LOGIN REPLICATION PASSWORD 'pgsync';
GRANT CONNECT ON DATABASE app TO pgsync;
