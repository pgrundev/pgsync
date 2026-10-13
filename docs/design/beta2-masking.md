# PGSync Beta 2 — Deterministic data masking and schema safety (design proposal)

Status: proposal, nothing implemented. Written at the end of Beta 1.1 to confirm that the
engine can take a masking stage without touching transaction ordering, checkpoint atomicity
or bounded memory. Scope follows the Beta 2 requirements: deterministic field
transformations, identical masking in snapshot and CDC, FK and uniqueness preservation,
reuse of PGRun's masking policies, fail-closed on unknown sensitive columns, schema-change
detection, large-transaction spilling, versioned policies, publication safety.

## 1. Where masking goes

```
source ─pgoutput─▶ decoder ─▶ [Transform] ─▶ bounded channel ─▶ applier ─▶ target
                                   ▲
source ─COPY TO──▶ row parser ─────┘ (snapshot path, same Transform, same policy)
```

One new stage, `Transform`, applied to the `Op` model (`pgsync_core::event::Op`) **before**
the event enters the reader→applier channel. Reasons:

- The channel's byte accounting then counts masked values, so memory stays bounded by the
  same limits (a transform never enlarges a value beyond a configured bound; see §6).
- The applier, checkpoint and feedback logic do not change at all: a masked `Op` is just an
  `Op`. Ordering is preserved because the stage is a pure function on one event.
- The reader task stays single-threaded for a job; masking cost is CPU in the same task.
  If it ever dominates, the stage can be pipelined behind a second bounded channel without
  reordering (one producer, one consumer).

The snapshot path currently streams COPY bytes untouched. With a policy that applies to a
table, the copier switches to a row-aware path: `COPY ... TO STDOUT` in text format is
parsed per row into the same `Tuple` representation, transformed, re-encoded, and written
to `COPY ... FROM STDIN`. Tables without a policy keep the byte passthrough. Parsing COPY
text is bounded per row (one row at a time, `buffer_size_kb` chunks), so snapshot memory
stays flat.

## 2. Determinism and consistency between snapshot and CDC

Every transformation is a pure function `f(policy_version, table, column, value) -> value`
with no state and no randomness: HMAC-SHA-256 keyed by a per-job secret (never stored in
the target), truncated/formatted to the column's domain (`text`, `varchar(n)`, numeric
ranges, dates shifted by a keyed offset, emails rebuilt as `<hmac>@masked.invalid`, …).
Because the same function runs on the snapshot rows and on every later change, a row
masked during the copy and the same row updated later produce identical outputs, and an
UPDATE whose source value did not change produces the same masked value again.

Unchanged TOAST values (`Value::Unchanged`) are passed through untouched: the target still
holds the masked value from before, which is exactly what it must keep.

## 3. Foreign keys and uniqueness

- **Keys are never masked by default.** Primary keys and replica-identity columns are
  excluded from policies unless the policy explicitly opts in with a *key-preserving*
  transform (injective on the column domain, e.g. HMAC of an integer into a 64-bit
  integer, or a format-preserving encryption of a UUID). The same transform applied to a
  referencing column yields the same masked value, so FK relationships survive.
- Policies declare which columns participate in FK relationships (or pgsync derives them
  from `pg_constraint` at bootstrap); referencing and referenced columns must use the same
  transform and the same key, verified at preflight (fail if not).
- Uniqueness is preserved when a transform is injective. HMAC truncated to the target type
  is not strictly injective; the preflight computes the collision risk from the domain size
  and refuses transforms on unique columns when the output space is smaller than the
  input domain. Email masks include the HMAC of the whole address to stay unique.
- UPDATE/DELETE `WHERE` clauses are built from masked key values, so the applier finds the
  masked row. Nothing in the sink changes.

## 4. Policy model and reuse of PGRun's policies

PGRun already classifies columns (sensitive / internal / public) and holds masking rules.
pgsync consumes an exported policy file (versioned, content-hashed):

```yaml
policy:
  version: 7                         # monotonic
  classification_hash: sha256:...    # from PGRun
  default: fail                      # unknown column -> refuse (fail closed)
  tables:
    public.users:
      email:  {transform: email_hmac}
      name:   {transform: fake_name_hmac}
      ssn:    {transform: null}
      id:     {transform: keep}       # key
    public.orders:
      user_id: {transform: keep}
      note:    {transform: redact}
```

Every column of every replicated table must be listed (or matched by a table-level `*`
rule that is itself explicit). A column that appears in a `Relation` message but not in
the policy stops the job with `unsupported` before anything is applied (the same place
Beta 1.1 refuses unconfigured tables). This is the fail-closed guarantee: a column added on
the source after the policy was written cannot leak.

## 5. Versioning and rebuilds

The checkpoint row records `policy_version` and `policy_hash`. A Golden Copy is defined by
(source system id, slot, policy version). Changing the policy changes what every existing
row should look like, which CDC cannot retroactively fix; therefore:

- `run` with a different policy hash than the checkpoint refuses (`config` error) unless
  the new policy is marked as a *compatible extension* (only new columns/tables added,
  nothing changed for existing ones) — then the version is bumped in the checkpoint.
- Any other change requires `bootstrap --restart-snapshot`; the target is rebuilt under
  the new policy. The old copy remains readable until the new one is complete only if the
  operator points the job at a new target database (recommended: one target per policy
  version).

## 6. Bounded memory and large transactions

Transforms are per-value and allocate at most `max(len(value), bound)` where `bound` is
the maximum output size of the transform (known per transform). The channel accounts for
the transformed size. `max_message_bytes` keeps applying before decoding.

Large transactions (bigger than the target can comfortably hold open, or longer than the
operator allows) are a Beta 2 engine item independent of masking: protocol v2
(`streaming = on`) delivers in-progress transactions in chunks keyed by xid; the applier
stages chunks in a bounded disk spool (`spill_dir`, `spill_max_bytes`) and applies the
transaction on `StreamCommit`. Ordering is still by commit, the checkpoint still moves only
inside the applying transaction, and the spool is discarded on `StreamAbort` or crash
(the server redelivers from the confirmed position). Masking happens before spilling, so
the spool never contains raw values.

## 7. Schema-change detection

Beta 1.1 fails at apply time with the column name. Beta 2 adds a strict mode: on every
`Relation` message, compare (name, type oid, typmod, key flags) with the cached target
columns and the policy; any difference pauses the job *before* the transaction is opened
(`PAUSED`, reason recorded) rather than failing mid-apply. Optional allow-list for
additive-and-policied changes. DDL is still never replicated automatically.

## 8. Golden Copy publication safety

A target becomes publishable only when: the job is `STREAMING`, `policy_version` matches
the requested one, no column exists on the target outside the policy (checked against
`pg_attribute`), and a verification pass has sampled rows and confirmed that no value in a
masked column equals the source value (a cheap statistical check using the same keyed
function). PGRun orchestrates this; pgsync exposes it as `pgsync verify-policy` (read only).

## 9. What stays unchanged

Engine ordering, the one-transaction-per-transaction apply, checkpoint-with-data
atomicity, feedback after commit, slot continuity checks, bounded channel, status and
heartbeat, TLS policy. Masking is additive: a job without a policy behaves exactly as in
Beta 1.1 (and must say so loudly in `status`: `masking: none`).

## 10. Open questions for the Beta 2 kickoff

- Key management: where the per-job HMAC secret lives (PGRun secret store), rotation
  implies a rebuild.
- Which transforms PGRun's existing policies need that are not expressible as pure
  functions (e.g. "consistent fake name from a dictionary" is fine; "keep the first user
  with this email" is not).
- Whether `REPLICA IDENTITY FULL` tables can be masked at all (the WHERE clause compares
  every column; masked non-key columns must be reproduced exactly — they are, by
  determinism, but the cost is high).
