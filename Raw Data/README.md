# Gold Layer Data Documentation

**Files:** `2026-07-05/gold-layer.parquet` / `gold-layer.xlsx`
**Generated:** 2026-07-05
**Total rows:** 132,951
**Total columns:** 82

This file is the Gold layer output of the `cars-for-sale-DWH` pipeline. Each row represents a **version** of a used car listing scraped from Dubizzle Egypt. A single listing can have multiple rows — one per state change — tracked using SCD Type-2 logic.

> **Naming convention note:** Snapshots are organized as `Raw Data/YYYY-MM-DD/` folders, with files named generically (`gold-layer.parquet`, `.xlsx`, `-sample.csv`) since the folder itself carries the date. The very first snapshot (`gold_layer_2026-04-05.*`) predates this convention and still lives loose at the `Raw Data/` root with the date embedded in the filename — it's kept as-is for history rather than renamed.

---

## What is SCD Type-2?

Instead of overwriting records when a listing changes, the pipeline keeps the full history. Every time a listing is updated or disappears, the old row is "closed" and a new one is opened. This lets you reconstruct what any listing looked like on any given date.

---

## Column Reference

### Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `id` | int64 | Dubizzle internal listing ID (unique per listing, not per row) |
| `externalID` | string | Public-facing ID used in the listing URL |
| `slug` | string | URL-friendly listing title |
| `_sourceURL` | string | Full URL to the listing on dubizzle.com.eg |

---

### Listing Content

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `title` | string | No | Listing title as written by the seller |
| `description` | string | No | Full listing description |
| `Brand` | string | No | Car manufacturer (e.g. Toyota, BMW) |
| `Model` | string | No | Car model name (e.g. Land Cruiser, 320) |
| `Version` | string | ~44% filled | Trim/variant name |
| `Body Type` | string | ~99% filled | e.g. Sedan, SUV, Hatchback, 4X4 |
| `Condition` | string | No | `Used` or `New` |
| `Color` | string | No | Exterior color |
| `Interior` | string | ~58% filled | e.g. Full Leather, Part Leather |
| `Source` | string | ~47% filled | Country of origin (e.g. Europe, Company Source) |
| `Number of doors` | string | ~60% filled | e.g. `4/5`, `2/3` |

---

### Pricing & Payment

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `Price` | Float64 | ~0% | Listed price in Egyptian Pounds (EGP) |
| `Price Type` | string | No | `Price` (fixed) or `Negotiable` |
| `Down Payment` | Float64 | ~99% empty | Installment down payment amount if applicable |
| `Display down payment` | string | ~85% empty | Formatted display string for down payment |
| `Payment Options` | string | ~17% empty | e.g. `Cash`, `Cash or Installments` |

---

### Vehicle Specs

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `Year` | Float64 | No | Model year |
| `Kilometers` | Float64 | No | Odometer reading |
| `Transmission Type` | string | No | `Automatic` or `Manual` |
| `Fuel Type` | string | No | e.g. Benzine, Diesel, Electric |
| `Air Conditioning` | string | ~43% empty | e.g. `Automatic air conditioning` |
| `Engine Capacity (CC)` | float64 | ~29% empty | Engine displacement in cubic centimetres |
| `Power (hp)` | float64 | ~67% empty | Engine power in horsepower |
| `Consumption (l/100 km)` | float64 | ~79% empty | Fuel consumption |
| `Number of seats` | float64 | ~54% empty | Seating capacity |
| `Number of Owners` | float64 | ~57% empty | Number of previous owners |

---

### Media & Extras

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `Virtual Tour` | string | ~33% empty | `Not Available` or link |
| `Video` | string | ~33% empty | `Not Available` or link |

---

### Feature Flags (`feature_*`)

Each `feature_*` column represents an optional car feature selected by the seller. There are **43 feature columns** in total.

| Value | Meaning |
|-------|---------|
| `True` | Seller confirmed this feature is present |
| `NaN` | Not mentioned — feature may or may not be present |

> **Note:** `NaN` is deliberately not filled with `False`. Absence of a feature in the listing does not mean the car lacks it — the seller may simply not have selected it.

**Full list of feature columns:**

`feature_Keyless Entry` · `feature_Power Steering` · `feature_Power Windows` · `feature_Steering Switches` · `feature_Keyless Start` · `feature_Touch Screen` · `feature_ABS` · `feature_Airbags` · `feature_Power Mirrors` · `feature_Power Seats` · `feature_Front Camera` · `feature_Rear View Camera` · `feature_EBD` · `feature_Parking Sensors` · `feature_ESP` · `feature_Front Speakers` · `feature_Rear speakers` · `feature_USB Charger` · `feature_Bluetooth System` · `feature_Premium Wheels/Rims` · `feature_Cruise Control` · `feature_Navigation System` · `feature_Power Locks` · `feature_Sunroof` · `feature_Fog Lights` · `feature_Roof Rack` · `feature_Daytime running lights` · `feature_Start/Stop system` · `feature_AM/FM Radio` · `feature_CD Player` · `feature_Cassette Player` · `feature_Cool Box` · `feature_DVD Player` · `feature_Heated seats` · `feature_Immobilizer Key` · `feature_Rear Seat Entertainment` · `feature_Alarm/Anti-Theft System` · `feature_Aux Audio In` · `feature_Off-Road Tyres` · `feature_Equipped for disabled` · `feature_Isofix` · `feature_Tyre pressure sensor` · `feature_Xenon Headlights`

---

### Timestamps

| Column | Type | Description |
|--------|------|-------------|
| `createdAt` | string (ISO-8601) | When the listing was originally created on Dubizzle |
| `updatedAt` | string (ISO-8601) | When the listing was last modified by the seller |
| `_ingestionDate` | string (YYYY-MM-DD) | The scrape date this row's data was captured |

---

### SCD Tracking Columns

These columns are added by the pipeline and are not part of the original listing data.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `first_seen_date` | string (YYYY-MM-DD) | No | Date the listing first appeared in the pipeline |
| `scd_valid_from` | string (YYYY-MM-DD) | No | Date this row version became active |
| `scd_valid_to` | string (YYYY-MM-DD) | Yes — null if currently active | Date this row version was closed |
| `status` | string | No | Current state of this row version (see below) |

#### Status Values

| Status | Meaning |
|--------|---------|
| `active` | Listing is currently live on Dubizzle |
| `updated` | Listing existed but changed (price, description, etc.); this row is the old version |
| `deleted` | Listing disappeared from Dubizzle — likely sold |

---

## Snapshot History

Raw Data snapshots are pulled periodically and committed for portfolio/demo purposes. Only the **latest** snapshot plus the original **legacy** snapshot are kept in the repo at any time — older ones are pruned to keep repo size manageable, but their stats are preserved here for a sense of the pipeline's growth over time.

| Snapshot | Rows | Unique IDs | Active | Updated | Deleted | Notes |
|---|---|---|---|---|---|---|
| 2026-04-05 | 39,574 | — | 10,871 | 28,703 combined¹ | 28,703 combined¹ | Initial sample. Root-level, legacy filename convention. Kept in repo. |
| 2026-04-16 | 51,231 | 30,521 | 11,504 | 17,938 | 21,789 | Pruned from repo 2026-07-05 — kept here for history only. |
| 2026-07-05 | 132,951 | 66,773 | 13,274 | 56,816 | 62,861 | Current snapshot. Added `Athena View.csv` analytics export. |

¹ The 2026-04-05 snapshot's original documentation only broke rows into active vs. closed (updated+deleted together), not split individually.

---

## Sample Records

See `2026-07-05/gold-layer-sample.csv` for 5 representative rows covering all three status values.

---

## Analytics View Export (`Athena View.csv`)

This file is a materialized export of an Athena view (`cars_scd_analytics`) built on top of the Gold SCD table, adding computed business-intelligence columns for reporting. Column names in this export are lowercased (Athena convention), unlike the Gold Parquet/CSV above.

| Column | Description |
|---|---|
| `last_seen_date` | Most recent date the listing was observed |
| `days_listed` | Days from listing creation (`createdAt` on Dubizzle) to `scd_valid_to` (or today, if still open) |
| `days_to_sell` | Same basis as `days_listed`, but only populated for `status = deleted` rows — isolates true sales-cycle time |
| `age_in_years` | Current year minus model `year` |
| `price_per_km` | `price / kilometers` — normalizes price by wear |
| `feature_count` | Count of `feature_*` columns present (true) on the listing |
| `has_premium_features` | Whether the listing has at least one of: sunroof, navigation system, touch screen |
| `has_safety_suite` | Whether the listing has all of: ABS, airbags, ESP, parking sensors |
| `price_segment` | `Budget` (< 500k EGP) / `Mid-range` (500k–1.5M) / `Premium` (> 1.5M) |
| `mileage_category` | `Low Mileage` (< 50k km) / `Medium Mileage` (50k–150k) / `High Mileage` (> 150k) |
| `is_current_active` | `True` if the listing is currently live (`status = active`, `scd_valid_to` is null) |

All other columns (`id`, `externalid`, `title`, `brand`, `model`, `year`, `price`, `kilometers`, `power_hp`, `engine_cc`, `body_type`, `transmission_type`, `fuel_type`, `color`, `condition`, `status`, `first_seen_date`, `scd_valid_from`, `scd_valid_to`, `createdat`, `updatedat`) mirror the Gold table fields described above, just lowercased.

---

## Key Caveats

- **Delisting ≠ guaranteed sale.** A listing disappearing from the scrape results is the best available proxy for a sale, since Dubizzle does not expose a "sold" status. Scraping gaps can also cause false delistings — a buffer rule requiring consecutive missing days before marking a listing as `deleted` is planned.
- **`NaN` in feature columns ≠ `False`.** Do not impute feature flags as absent without understanding the seller's data entry behaviour.
- **`has_premium_features` / `has_safety_suite` nulls ≠ `False`.** In the analytics export these come through as non-boolean (`object`) with substantial nulls (36,329 / 21,571 non-null out of 132,951) rather than clean booleans — consistent with three-valued logic (`AND`/`OR` over nullable `feature_*` inputs) propagating `NULL` when an input feature isn't known, rather than resolving to `False`.
- **Multiple rows per listing is expected.** Filter on `status = 'active'` for the current live snapshot, or use `scd_valid_from` / `scd_valid_to` to reconstruct the state at any historical date.
- **Prices are in Egyptian Pounds (EGP).** No currency conversion is applied.
