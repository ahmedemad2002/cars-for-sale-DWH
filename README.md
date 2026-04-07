# 🚗 Used Car Market Intelligence Pipeline
### A serverless data lakehouse on AWS for tracking used car listings on Dubizzle Egypt

This project builds an end-to-end data pipeline that scrapes used car listings daily, tracks their full lifecycle using SCD Type-2, and makes them queryable via Athena for market intelligence — using **delisting events as a proxy for sales** to answer questions like: *which cars sell fastest, and at what price points?*

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Orchestration Layer                             │
│                   EventBridge (daily CRON trigger)                       │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────┐
│  Lambda: DubizzleScrapeDay                           │
│  • POST to search.olx.com.eg/_msearch                │
│  • Fetches newest + oldest sort orders               │
│  • Captures elite & featured listings                │
│  • Saves raw JSON → S3 Bronze                        │
└─────────────────────────┬────────────────────────────┘
                          │ async invoke
                          ▼
┌──────────────────────────────────────────────────────┐
│  Lambda: Bronze-to-Silver                            │
│  • Merges newest/oldest (newest wins on ID conflict) │
│  • Flattens formattedExtraFields → columns           │
│  • Expands Extra Features → feature_* bool columns   │
│  • Coerces types (Year, Price, Kilometers → Float64) │
│  • Outputs Parquet, partitioned by ingestion_date    │
│  • Sanity check gate: blocks Gold if rows < 500      │
└─────────────────────────┬────────────────────────────┘
                          │ async invoke (if gate passes)
                          ▼
┌──────────────────────────────────────────────────────┐
│  Lambda: Silver-to-Gold                              │
│  • Deduplicates Silver by id (newest updatedAt wins) │
│  • Applies SCD Type-2 merge into cars_scd.parquet    │
│    ├─ New ID          → insert (status=active)       │
│    ├─ updatedAt diff  → close old + open new version │
│    └─ Missing from    → close (status=deleted)       │
│         Silver                                       │
│  • Handles schema evolution (new feature_* columns)  │
│  • Overwrites single Gold Parquet file on S3         │
└─────────────────────────┬────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────┐
│  AWS Athena                                          │
│  • Queries directly over S3 Parquet                  │
│  • last_seen_date & days_listed computed on-the-fly  │
│  • Views for fast-seller / slow-seller analysis      │
└─────────────────────────┬────────────────────────────┘
                          │
                          ▼
               Power BI (reporting layer)
```

---

## Pipeline Stages

### Bronze — Raw Ingestion
The scraper hits Dubizzle's internal search API twice per day: once sorted `desc` (newest listings) and once sorted `asc` (oldest listings). This two-pass approach maximises coverage across the full inventory. Elite and featured listings — which appear in separate response buckets — are captured and merged in. Raw JSON is saved as-is to S3, preserving original fields for reprocessing if the transformation logic changes.

### Silver — Typed & Flattened
Bronze-to-Silver performs all structural transformation:

- **Deduplication**: On ID, newest wins — the two scrape passes can return the same listing.
- **Schema flattening**: `formattedExtraFields` is a variable-length list of `{name_l1, formattedValue_l1}` pairs. These are pivoted into individual columns. Extra Features (air conditioning, sunroof, etc.) become boolean `feature_*` columns — `NaN` means unknown, not absent.
- **Type coercion**: `Price`, `Year`, `Kilometers`, and other numeric fields are coerced from string with `pd.to_numeric(..., errors='coerce')` into `Float64` (nullable). `createdAt` / `updatedAt` Unix timestamps are converted to ISO-8601 strings.
- **Output**: Parquet partitioned by `ingestion_date=YYYY-MM-DD/`, registered as an Athena partition after write.

### Gold — SCD Type-2 History Table
The Gold layer maintains `cars_scd.parquet`, a single cumulative file that tracks the full history of every listing. The SCD merge logic:

| Scenario | Action |
|---|---|
| ID not in Gold | Insert new row: `status=active` |
| ID in Gold, `updatedAt` changed | Close old row (`status=updated`, `scd_valid_to=today`), insert new active row preserving `first_seen_date` |
| ID in Gold, `updatedAt` unchanged | No-op — leave existing row untouched |
| Active ID absent from Silver | Close row (`status=deleted`, `scd_valid_to=today`) |

Schema evolution is handled via `align_schemas()` — new `feature_*` columns appearing in Silver are backfilled as `False` in existing Gold rows; columns disappearing from Silver are filled `None`. SCD metadata columns (`first_seen_date`, `scd_valid_from`, `scd_valid_to`, `status`) are excluded from this alignment.

---

## Key Engineering Decisions

**SCD Type-2 over snapshot overwrite**
A simple daily overwrite would lose all history. SCD Type-2 lets us reconstruct the full timeline of any listing — when it appeared, when its price changed, and when it was delisted — enabling `days_listed` and `days_to_sell` calculations in Athena.

**Parquet over CSV for Athena**
CSV with `OpenCSVSerde` forces all columns to string, making numeric comparisons awkward and failing silently on special-character column names (e.g., `Power (hp)`). Parquet carries schema metadata natively, handles nullable numerics cleanly, and is significantly faster to scan in Athena.

**Sanity check gate before Gold**
If the scraper returns an abnormally low row count (default threshold: 500), the Bronze-to-Silver Lambda does not invoke Silver-to-Gold. A bad scrape day would otherwise look like a mass-delisting event, corrupting the SCD table with thousands of false `status=deleted` entries. The threshold is tunable via `MIN_SILVER_ROWS` env var.

**Silver deduplication before SCD merge**
The SCD loop builds `gold_active_map` once before iterating. If Silver itself contains duplicate IDs (same listing in both newest and oldest passes), the loop would insert two active rows for the same ID within a single run. Deduplicating Silver upstream (newest `updatedAt` wins) eliminates this class of bug cleanly.

**Direct Lambda chaining over Step Functions**
The pipeline is a strict linear chain with no branching, parallelism, or retry-state requirements. Direct async `InvocationType='Event'` invocations keep the architecture simple and avoid the overhead of defining a state machine for a three-step sequence.

**Delisting as sales signal**
Dubizzle does not expose a "sold" status. A listing disappearing from the API — captured as `status=deleted` in Gold — is the best available proxy for a sale. This is the core analytical assumption of the project; a configurable buffer rule to handle single-day scrape gaps vs. confirmed delistings is on the roadmap.

---

## Data Model

The Gold table (`cars_scd.parquet`) schema:

| Column | Type | Description |
|---|---|---|
| `id` | string | Dubizzle internal listing ID |
| `externalID` | string | Public-facing ID (used in URL) |
| `title` | string | Listing title |
| `Price` | Float64 | Listed price (EGP) |
| `Year` | Float64 | Model year |
| `Kilometers` | Float64 | Odometer reading |
| `Make` | string | Car manufacturer |
| `Model` | string | Car model |
| `Body Type` | string | e.g. Sedan, SUV |
| `Transmission` | string | Auto / Manual |
| `feature_*` | boolean | One column per extra feature; `NaN` = unknown |
| `updatedAt` | string | ISO-8601 timestamp of last listing update |
| `first_seen_date` | string | Date listing first appeared in the pipeline |
| `scd_valid_from` | string | Date this row version became active |
| `scd_valid_to` | string | Date this row version was closed (`null` if active) |
| `status` | string | `active` / `updated` / `deleted` |

`last_seen_date` and `days_listed` are computed on-the-fly in Athena rather than stored — keeping the Gold table append-only and avoiding recomputation on every write.

---

## Analytical Use Cases

Once the SCD table is built, Athena views answer questions like:

- **Fast sellers**: Active listings with `days_listed < N` before `status=deleted`
- **Slow / stale inventory**: Active listings with `first_seen_date` more than X days ago
- **Price sensitivity**: Do price-updated listings (`status=updated` with `Price` change) sell faster afterward?
- **Make/model velocity**: Which makes and models turn over fastest in the Egyptian market?
- **Seasonality**: Are there weekly or monthly patterns in listing volume or delisting rate?

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | AWS Lambda (Python 3.14) |
| Storage | AWS S3 (Bronze / Silver / Gold buckets) |
| Query | AWS Athena |
| Scheduling | Amazon EventBridge |
| Data format | Parquet (snappy compression) via `pyarrow` |
| Transformation | `pandas`, `boto3` |
| Reporting | Power BI (DirectQuery over Athena) |

---

## Repository Structure

```
├── Lambda Scripts/
│   ├── DubizzleScrapeDay.py            # Scraper: hits Dubizzle API, saves raw JSON to Bronze
│   ├── Bronze-to-Silver.py             # Transforms & types raw JSON → partitioned Parquet
│   └── Silver-to-Gold.py               # SCD Type-2 merge into Gold history table
├── Raw Data/                           # Local sample data (gitignored — too large to commit)
│   ├── [gold_layer_2026-04-05.parquet  # Snapshot of the parquet file saved in gold layer S3 bucket](./Raw%20Data/gold_layer_2026-04-05.parquet)
│   ├── [gold 2026-04-05.csv            # CSV file to show the data in Excel](./Raw%20Data/gold%202026-04-05.csv)
│   └── [gold 2026-04-05 sample.csv     # Sample of the CSV file to preview data without download](./Raw%20Data/gold%202026-04-05%20sample.csv)
└── README.md
```

Configuration (credentials, S3 paths, API headers/payloads) is managed via a `config.json` file stored in S3, keeping secrets out of the codebase. Lambda environment variables are used for inter-function references and tunable thresholds.

---

## Setup Notes

The pipeline runs entirely on AWS. To replicate it you will need:

- Three S3 buckets (Bronze, Silver, Gold) or a single bucket with prefix-based separation
- Three Lambda functions with the appropriate IAM roles (S3 read/write, Lambda invoke)
- A `config.json` uploaded to S3 with the API endpoint, request headers, and payload templates
- Environment variables set on each Lambda (bucket names, function names, `MIN_SILVER_ROWS`)
- An EventBridge rule triggering `DubizzleScrapeDay` on your desired schedule

---

## Roadmap

- [ ] Athena views for fast-seller and slow/unsold analysis
- [ ] `days_listed` and `days_to_sell` analytical columns via Athena views
- [ ] Price history table (append-only change log, separate from SCD)
- [ ] Scrape-gap buffer rule: require N consecutive missing days before marking a listing as `deleted`
- [ ] More granular SCD change detection (column-level diff beyond `updatedAt`)
- [ ] Power BI dashboard: listing velocity, price distribution, make/model heatmaps