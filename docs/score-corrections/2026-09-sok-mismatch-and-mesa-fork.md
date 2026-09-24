# Score correction 2026-09: SOK mismatch rejections and the Mesa fork window

## What happened

Two incidents cost block producers points that they did not lose through
their own fault. Both are visible in the mainnet coordinator database
(copy of 2026-09-23 21:00Z).

### 1. Valid submissions rejected with `Mismatched sok_message`

From 2026-05-07 until 2026-09-02 15:28Z, the verifier rejected submissions
with `Transaction_snark.verify: Mismatched sok_message`
(MinaProtocol/mina#19299). The number of affected BPs grew from 2 (May) to
41–51 per week (June–August) and 79–85 per week in the last two weeks
before the fork. The rejections stopped when the SOK waiver was deployed
(`TOLERATE_SOK_MISMATCH`, gitops-infrastructure#1791).

Note: the mainnet error text is `Mismatched sok_message`. The devnet text
is `sok message digest does not match`. A search for the devnet text finds
nothing on mainnet, which is why the problem was first reported as
"since late August".

Effect in the current 90-day window: 77 BPs lost 30,249 batches in total.

### 2. Mesa hard-fork transition, 2026-09-03 15:00–18:00Z

The pre-fork chain stopped at height 548187 at approximately 14:50Z. The
new chain started at 18:00Z. In between, only 9–46 of approximately 108
BPs submitted (the frozen block at 548187), and only they got points. BPs
that stopped the old node to install the upgrade got nothing. 8 batches
are affected.

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

No BP goes down across these thresholds and no BP goes above 100%.

The credit does not repeat the "on the canonical chain" check for the
rejected submission. When the waiver was validated against live mainnet
traffic, all 108 re-verified submissions had a valid state hash, so the
risk is low.

## Runbook

The tasks ship in the coordinator image (this repository, version with
`uptime_service_validation/maintenance`). Deploy that image first, then
run the tasks inside the coordinator pod, which has the `POSTGRES_*`
environment. The image bump also brings every change since the deployed
tag (v2.0.8 at the time of writing).

Check the namespace and deployment names before you start; the values
below are from `gitops-infrastructure/platform/coda-infra-east4/delegation-program-validation`.

```sh
NS=delegation-program-validation
X="kubectl -n $NS exec deploy/delegation-program-verify-coordinator --"
$X invoke --list | grep score-correction
```

1. **Backup.** Note the name of the latest hourly dump in
   `s3://o1labs-uptime-service-backend/uptime-db-dumps/`.

2. **Index (optional, recommended).** Makes the credit query faster:

   ```sh
   $X invoke add-submissions-index
   ```

3. **Fork window.** Dry run, review the printed score changes, then apply:

   ```sh
   $X invoke score-correction-exclude-batches --correction 2026-09-mesa-fork \
       --start 2026-09-03T15:00:00Z --end 2026-09-03T18:00:00Z \
       --reason "Mesa hard fork transition, old chain stopped at 548187"
   # same command with --apply
   ```

   Expected: 8 batches, 232 per-batch points removed.

4. **SOK rejections.** Dry run, review, then apply:

   ```sh
   $X invoke score-correction-credit-rejected --correction 2026-09-sok-mismatch \
       --start 2026-05-01T00:00:00Z --end 2026-09-03T00:00:00Z \
       --error "Mismatched sok_message" \
       --reason "Verifier rejected valid snark work, MinaProtocol/mina#19299"
   # same command with --apply
   ```

   Expected: approximately 34,900 credits to 77 BPs (the range includes
   batches older than the 90-day window). Takes approximately 1 minute.

5. **Check.** After the next batch (at most 20 minutes), the leaderboard
   shows the new scores. `invoke score-correction-list` shows what is
   applied.

6. **Revert** (if necessary), per label:

   ```sh
   $X invoke score-correction-revert --correction 2026-09-sok-mismatch   # then --apply
   $X invoke score-correction-revert --correction 2026-09-mesa-fork      # then --apply
   ```

## Not covered

* `score_history` rows written before the correction keep the old scores.
  The payout service reads `score_history` at a given time, so a snapshot
  taken before the correction must be recalculated separately.
