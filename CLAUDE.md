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

## `local/` and `dubizzle_dbt/` are gitignored scratch/WIP

Both are excluded from git (`.gitignore`) and are working/experimental space:
- `local/` — scratch notebooks, test data, Power BI files, ad-hoc configs.
- `dubizzle_dbt/` — a scaffolded dbt project, still default boilerplate with no real models.

Don't reference either in the README files or treat their contents as authoritative/shipped — they're not part of the tracked project structure until they contain real, intentional work.
