# Score correction 2026-09: SOK mismatch rejections and the Mesa fork window

## What happened

Two incidents cost block producers points that they did not lose through
their own fault. Both are visible in the mainnet coordinator database
(copy of 2026-09-23 21:00Z).

### 1. Valid submissions rejected with a SOK mismatch

From 2026-05-07 until 2026-09-02 15:48Z, the verifier rejected valid
submissions because their snark work failed the SOK check
(MinaProtocol/mina#19299). The number of affected BPs grew from 2 (May) to
41–51 per week (June–August) and 79–85 per week in the last two weeks
before the fork. The rejections stopped when the SOK waiver was deployed
(`TOLERATE_SOK_MISMATCH`, gitops-infrastructure#1791, live 2026-09-02
~16:05Z).

The error text depends on the verifier binary, not on the network:

| Verifier binary | `validation_error` |
|---|---|
| pre-fork (3.5.0 stop-slot and earlier) | `Transaction_snark.verify: Mismatched sok_message` |
| post-fork (4.0.0-mesa), mainnet included | `proof's sok message digest does not match the sok message` |

All mainnet rejections in this incident are pre-fork (see the pre-check
below). A search for only the post-fork text finds nothing, which is why
the problem was first reported as "since late August".

Effect in the current 90-day window: 77 BPs lost 30,249 batches in total.

### 2. Mesa hard-fork transition, 2026-09-03 15:08:48–17:48:48Z

The pre-fork chain stopped at height 548187 at approximately 14:50Z. The
new chain started at 18:00Z. Participation per batch
(submitters / BPs that scored):

| Batch (UTC) | Submitters | Scored | |
|---|---|---|---|
| 14:48:48–15:08:48 | 104 | 104 | normal |
| 15:08:48–15:28:48 | 46 | 46 | **excluded** |
| 15:28:48–15:48:48 | 45 | 45 | **excluded** |
| 15:48:48–16:08:48 | 9 | 9 | **excluded** |
| 16:08:48–16:28:48 | 21 | 21 | **excluded** |
| 16:28:48–16:48:48 | 22 | 21 | **excluded** |
| 16:48:48–17:08:48 | 27 | 26 | **excluded** |
| 17:08:48–17:28:48 | 33 | 33 | **excluded** |
| 17:28:48–17:48:48 | 31 | 31 | **excluded** |
| 17:48:48–18:08:48 | 106 | 61 | kept: upgraded nodes score again after 18:00 |
| 18:08:48–23:48:48 | 107–89 | 79–81 | kept, see below |

In the excluded batches, only BPs that kept the stopped pre-fork node
running submitted (the frozen block at 548187) and scored. BPs that
stopped the node to install the upgrade had nothing to submit.

From 18:08:48Z until approximately 23:48Z, 8–28 BPs per batch submitted
but did not score. All their rows are `(Pickles.verify dlog_check)`:
pre-fork blocks from nodes that were not upgraded yet, checked by the
post-fork verifier. That is on the operator's side, so these batches are
kept. Neither SOK text matches a `dlog_check` row (0 rows match both).

Query used for the table (on the database copy):

```sql
SELECT to_timestamp(b.batch_start_epoch) AT TIME ZONE 'UTC' s, to_timestamp(b.batch_end_epoch) AT TIME ZONE 'UTC' e,
       b.files_processed, count(ps.node_id) scorers
FROM bot_logs b LEFT JOIN points_summary ps ON ps.bot_log_id = b.id
WHERE b.batch_start_epoch BETWEEN extract(epoch FROM timestamptz '2026-09-03 13:00Z') AND extract(epoch FROM timestamptz '2026-09-04 02:00Z')
GROUP BY 1,2,3 ORDER BY 1;
```

## Correction

| Correction label | Task | Effect |
|---|---|---|
| `2026-09-mesa-fork` | `score-correction-exclude-batches` | The 8 fork batches no longer count for anyone (window 6480 → 6472 batches) |
| `2026-09-sok-mismatch` | `score-correction-credit-rejected` | 1 point for each batch in which a BP had a submission rejected with `Mismatched sok_message` and no point |

Dry run on the database copy, both corrections together:

| | now | corrected |
|---|---|---|
| BPs at ≥ 99% | 32 | 74 |
| BPs at ≥ 95% | 47 | 77 |
| BPs at ≥ 90% | 65 | 82 |

Full cycle on the copy (apply both, revert both): exclusion 1 s, credit
92 s, reverts 4 s and 1 s; afterwards `points`, `points_summary` and
`bot_logs.files_processed` are exactly as before.

### Decisions (for the maintainers)

1. **Credits skip the chain check.** A rejected row has no state hash, so
   the credit cannot check that the block was on the canonical chain.
   `--require-point-within-hours N` is an optional proxy: credit a batch
   only if the BP earned a real point within N hours of it. On the copy:
   no guard 34,880 credits, `N=2` 34,409 credits (−471, −1.4%). The
   threshold counts above and the BP that reported the problem are the
   same with both.
2. **The fork exclusion slightly lowers 8 BPs** that scored during the
   window but are below 100%, because (p−8)/(n−8) < p/n when p < n. The
   largest drop is 0.04 points; all 8 are BPs between 44% and 67%. The
   alternative is to credit every active BP in those batches instead of
   excluding them.
3. **`score_history` and payouts.** Rows already written keep the old
   scores, and mina-payout-reports reads `score_history` at a given time.
   Snapshots taken after 2026-05-07 must be recalculated separately if
   they were used.

## Runbook

The tasks ship in the coordinator image (this repository, version with
`uptime_service_validation/maintenance`). Deploying that image restarts the
coordinator; its `initdb` initContainer runs `invoke create-database`,
which also:

* builds `idx_submissions_submitted_at` online (see the README), and
* re-creates `cleanup_old_data()` with the NULL-safe `statehash` cleanup
  (`CREATE OR REPLACE FUNCTION` in `create_tables.sql`). Credit rows have
  `statehash_id = NULL`, which broke the old `NOT IN` version.

The image bump also brings every change since the deployed tag (v2.0.8
at the time of writing).

Commands below work in bash and zsh:

```sh
NS=delegation-program-validation
kubectl -n "$NS" get deploy,sts          # expect deployment.apps/delegation-program-verify-coordinator
kx() { kubectl -n "$NS" exec deploy/delegation-program-verify-coordinator -- "$@"; }
kx invoke --list | grep score-correction
```

1. **Pre-flight checks.** Run with `psql` as the coordinator user:

   ```sql
   -- the first --apply creates score_corrections in public
   SELECT has_schema_privilege(current_user, 'public', 'CREATE');                  -- expect: t
   -- the deploy updated cleanup_old_data (NULL-safe statehash cleanup)
   SELECT prosrc LIKE '%NOT EXISTS (SELECT 1 FROM points p%' FROM pg_proc WHERE proname = 'cleanup_old_data';  -- expect: t
   -- the index was built by the initContainer
   SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('idx_submissions_submitted_at');           -- expect: t
   -- only the pre-fork SOK text exists, none after the waiver
   SELECT date_trunc('week', submitted_at) wk,
          CASE WHEN validation_error LIKE '%Mismatched sok_message%' THEN 'pre-fork'
               WHEN validation_error LIKE '%sok message digest does not match%' THEN 'post-fork'
               ELSE left(validation_error, 60) END kind,
          count(*), max(submitted_at)
   FROM submissions WHERE validation_error ILIKE '%sok%' GROUP BY 1,2 ORDER BY 1,2;
   ```

   Expected for the last query: only `pre-fork` rows, the last one at
   2026-09-02 15:48:12. If `post-fork` rows exist, run step 4 a second
   time under the **same** `--correction` label with
   `--error "sok message digest does not match"`; a revert covers every
   row under one label.

2. **Dry runs.** Review the printed score change of every BP:

   ```sh
   kx invoke score-correction-exclude-batches --correction 2026-09-mesa-fork \
       --start 2026-09-03T15:08:48Z --end 2026-09-03T17:48:48Z \
       --reason "Mesa fork transition, old chain stopped at 548187"
   kx invoke score-correction-credit-rejected --correction 2026-09-sok-mismatch \
       --start 2026-05-01T00:00:00Z --end 2026-09-02T16:08:48Z \
       --error "Mismatched sok_message" \
       --reason "Verifier rejected valid snark work, MinaProtocol/mina#19299"
   ```

   Expected: 8 batches excluded, 232 per-batch points removed, and **no**
   "overlaps the period partially" warning (the bounds are real batch
   boundaries). Then approximately 34,880 credits to 77 BPs (the range
   includes batches older than the 90-day window). Add
   `--require-point-within-hours 2` if the maintainers choose the guard.

3. **Backup.** Wait for the hourly dump that starts **after** the dry runs
   (`delegation-program-db-dump`, minute 0) and note its key, for example
   `s3://o1labs-uptime-service-backend/uptime-db-dumps/mainnet-postgres-dump-<date>_<hour>00.sql.tar.gz`.
   Put the key in `--reason` in step 4, so the audit rows link to the
   restore point.

4. **Apply**, in this order (the credit never touches excluded batches):

   ```sh
   kx invoke score-correction-exclude-batches --correction 2026-09-mesa-fork \
       --start 2026-09-03T15:08:48Z --end 2026-09-03T17:48:48Z \
       --reason "Mesa fork transition; backup <dump key>" --apply
   kx invoke score-correction-credit-rejected --correction 2026-09-sok-mismatch \
       --start 2026-05-01T00:00:00Z --end 2026-09-02T16:08:48Z \
       --error "Mismatched sok_message" \
       --reason "MinaProtocol/mina#19299; backup <dump key>" --apply
   ```

5. **Check.** After the next batch (at most 20 minutes), the leaderboard
   shows the new scores. `kx invoke score-correction-list` shows what is
   applied.

6. **Revert** (if necessary), per label. The result is exact in any order:

   ```sh
   kx invoke score-correction-revert --correction 2026-09-sok-mismatch   # then add --apply
   kx invoke score-correction-revert --correction 2026-09-mesa-fork      # then add --apply
   ```
