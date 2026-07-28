# CLAUDE.md

Operational notes for working in this repo. For architecture, pipeline stages, and schema, see [README.md](README.md) and [Raw Data/README.md](Raw%20Data/README.md) — don't duplicate that here; keep this file to things that aren't derivable from the code.

## Raw Data/ retention policy

Only two snapshots are kept committed at a time:
- The original **legacy** snapshot (`gold_layer_2026-04-05.*`, loose files at `Raw Data/` root, old naming convention).
- The **latest** dated snapshot (`Raw Data/YYYY-MM-DD/`).

When a new snapshot is added, prune the previous dated snapshot folder (not the legacy root one). Before removing a folder, pull its row count / unique ID / status breakdown stats and add them to the **Snapshot History** table in `Raw Data/README.md` so the history isn't lost, just the raw files.

## Never commit a raw CSV of a full gold snapshot

GitHub hard-blocks any pushed blob over 100MB. A full CSV export of the gold layer (132k+ rows × 82 columns) is well over that (~136MB uncompressed). Use `.parquet` (primary) and `.xlsx` (if a spreadsheet-friendly copy is wanted) for the full snapshot instead. CSV is fine only for the small `gold-layer-sample.csv` (a handful of representative rows).

If a large CSV ever ends up committed, replacing it in a later commit is not enough — the oversized blob still lives in the earlier commit's history and will still block the push. It has to be removed from the commit that introduced it (e.g. via interactive rebase), not just superseded.

## `local/` is gitignored scratch/WIP

Excluded from git (`.gitignore`): scratch notebooks, test data, Power BI files, ad-hoc configs. Don't reference it in the README files or treat its contents as authoritative/shipped.

## `dubizzle_dbt/` is a real, tracked project

It holds the data-quality tests and the `cars_scd_analytics` model (see its README). Only its artifacts (`target/`, `dbt_packages/`, `logs/`) are gitignored. **Never commit a `profiles.yml`** — it's environment-specific and can leak account details; the local one lives in `~/.dbt/`, and the Lambda runner uses the env-var-driven template at `Lambda Scripts/dbt-runner/profiles.yml` (that template is the one intentional exception, tracked via a `.gitignore` negation).

## Athena catalog facts (confirmed from the live account)

Database `dubizzle`, gold table `cars_scd` (over `s3://dubizzle-gold/OneBigTable`), views `cars_scd_analytics` (dbt-managed) and `cars_analytics`. Region `eu-north-1`. Athena staging for dbt: `s3://dubizzle-gold/athena-results/`. The live `cars_scd_analytics` definition is the source of truth over any docs: it computes `days_listed`/`days_to_sell` from `createdat` (not `first_seen_date`), and its segment labels are `Mid-range` / `Low Mileage` / `Medium Mileage` / `High Mileage`.
