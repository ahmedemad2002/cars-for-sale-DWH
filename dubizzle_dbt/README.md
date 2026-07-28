# dubizzle_dbt — data tests & analytics view

dbt project for the cars-for-sale-DWH pipeline. It does two things:

1. **Owns the `cars_scd_analytics` Athena view** ([models/marts/cars_scd_analytics.sql](models/marts/cars_scd_analytics.sql)) — the BI layer consumed by Power BI, previously maintained by hand in the Athena console. `feature_*` columns are enumerated at compile time, so new features added by the pipeline's schema evolution are picked up automatically.
2. **Runs 30 data-quality checks** against the Gold SCD table after every daily pipeline run, via the `DbtRunner` Lambda (`Lambda Scripts/dbt-runner/`), with results in the daily digest email.

## Running locally

Requires `dbt-core` + `dbt-athena-community` and a `dubizzle_dbt` profile in `~/.dbt/profiles.yml` (type `athena`, region `eu-north-1`, schema `dubizzle`, an `s3_staging_dir` you can write to). **Never commit `profiles.yml`** — it's environment-specific and can leak account details.

```
dbt deps               # once, installs dbt_utils
dbt build              # analytics view + all 29 data tests
dbt source freshness   # staleness check on _ingestionDate
```

## Severity philosophy

- **error** — pipeline/SCD-integrity violations (duplicate active rows, closed rows without end dates, overlapping periods). These mean the merge logic is broken and must be fixed.
- **warn** — seller-input plausibility violations (900M km odometers, junk prices). Chronic low-level noise in the source data; flagged for awareness, doesn't redden the digest.

## Test inventory

### Source tests on `dubizzle.cars_scd` (models/staging/sources.yml)

| Test | Type | Severity |
|---|---|---|
| `id`, `title`, `status`, `first_seen_date`, `scd_valid_from` not null | not_null | error |
| `status` in (active, updated, deleted) | accepted_values | error |
| (`id`, `scd_valid_from`) unique | dbt_utils | error |
| `price` in [10k, 50M] EGP | accepted_range | warn |
| `year` in [1960, next year] | accepted_range | warn |
| `kilometers` in [0, 1.5M] | accepted_range | warn |
| `power (hp)` in (0, 1500] | accepted_range | warn |
| `engine capacity (cc)` in (0, 10000] | accepted_range | warn |
| freshness of `_ingestionDate` | source freshness | warn 30h / error 54h |

### Singular tests (tests/*.sql)

| Test | Rule | Severity |
|---|---|---|
| assert_active_rows_open | active ⇒ `scd_valid_to` null | error |
| assert_closed_rows_have_end_date | updated/deleted ⇒ `scd_valid_to` set | error |
| assert_scd_date_order | `scd_valid_from` ≤ `scd_valid_to` | error |
| assert_first_seen_before_valid_from | `first_seen_date` ≤ `scd_valid_from` | error |
| assert_one_active_row_per_id | ≤ 1 active row per listing | error |
| assert_no_future_valid_from | `scd_valid_from` ≤ today | error |
| assert_no_overlapping_scd_periods | closed periods per id don't overlap | error |
| assert_min_active_count | ≥ 500 active listings (mirrors `MIN_SILVER_ROWS`) | error |
| assert_no_ancient_dates | pipeline dates ≥ 2020-01-01 | error |
| assert_active_price_null_rate | < 10% of active rows missing price | warn |
| assert_km_plausible_for_year | km ≤ 60k × vehicle age | warn |
| assert_active_not_stale | no active listing > 365 days old | warn |

### Model tests on `cars_scd_analytics` (models/marts/marts.yml + tests/)

| Test | Rule | Severity |
|---|---|---|
| days_listed ≥ 0 | expression | error |
| price_segment in (Budget, Mid-range, Premium) | accepted_values | error |
| mileage_category in (Low/Medium/High Mileage) | accepted_values | error |
| assert_days_to_sell_only_for_deleted | populated iff closed-deleted row | error |
| assert_is_current_active_consistency | flag ⇔ active + open | error |

### Deliberately not ported from `Lambda Scripts/Test_gold_layer.py`

- **Required columns present** — dbt errors inherently when a tested column is missing.
- **Empty-column / feature-dtype checks** — Parquet dtype concerns invisible to SQL; still covered by `Test_gold_layer.py`, which remains for ad-hoc parquet-level runs.
- **Fully-duplicate rows** — an 82-column GROUP BY for marginal value over the (`id`, `scd_valid_from`) uniqueness test.

## Automated daily run

`Silver-to-Gold` async-invokes the `DbtRunner` Lambda (container image, see `Lambda Scripts/dbt-runner/`) after each successful Gold save. The runner executes `dbt build` + `dbt source freshness` and prints `DBT PASS/WARN/FAIL` lines plus a `DBT SUMMARY:` line to CloudWatch, which `EmailNotify` parses into the Data Quality section of the daily digest.

`dbt docs generate` works out of the box for a browsable catalog of models, columns, and tests.
