# 🎵 Spotify Data Engineering Project — Azure End-to-End Pipeline

> A modern data engineering project built on **Azure**, ingesting Spotify-like data from an **Azure SQL Database** into a **Data Lake (ADLS Gen2)** using **Azure Data Factory** with incremental loading and CDC tracking.

<img width="894" height="501" alt="image" src="https://github.com/user-attachments/assets/d4d382e8-7a35-428e-ae91-cf9ad05b2862" />

## 🛠️ Tech Stack

- **Azure SQL Database** — Source OLTP system
- **Azure Data Factory** — Pipeline orchestration
- **Azure Data Lake Storage Gen2** — Raw and processed data storage (bronze + silver)
- **Databricks (PySpark)** — Silver layer transformations
- **Delta Lake** — ACID-compliant storage format for silver tables
- **Parquet** — Storage format for bronze analytical workloads
- **JSON** — CDC watermark configuration files


---

## 📐 Architecture Overview

```
Azure SQL Database (Source)
        │
        ▼
Azure Data Factory (Orchestration)
  ├── Backfill (Full Load)
  └── Incremental Load (CDC-based)
        │
        ▼
Azure Data Lake Storage Gen2
  ├── bronze/ (Raw Layer)
  │     ├── DimArtist/
  │     ├── DimDate/
  │     ├── DimTrack/
  │     ├── DimUser/
  │     ├── FactStream/
  │     ├── DimArtist_cdc/
  │     ├── DimDate_cdc/
  │     ├── DimTrack_cdc/
  │     ├── DimUser_cdc/
  │     └── FactStream_cdc/
  └── silver/ (Cleaned Layer)
        ├── DimArtist/
        ├── DimDate/
        ├── DimTrack/
        ├── DimUser/
        └── FactStream/
        │
        ▼
Databricks (PySpark + Delta Lake)
  └── spotify.silver.*  ← Delta tables
```

---

## 🗄️ Source Data Model

The source is an **Azure SQL Database** (`SQLDB-SpotifyProject-BDV`) with a star schema modeling a music streaming platform:

| Table | Type | CDC Column | Description |
|---|---|---|---|
| `DimUser` | Dimension | `updated_at` | User profiles and subscription info |
| `DimArtist` | Dimension | `updated_at` | Artist metadata and genre |
| `DimTrack` | Dimension | `updated_at` | Track details linked to artists |
| `DimDate` | Dimension | `date` | Calendar date dimension |
| `FactStream` | Fact | `stream_timestamp` | Streaming events (user × track × date) |

---

## 🏗️ Bronze Layer — Raw Ingestion

The **bronze layer** is the first landing zone in the data lake. It stores raw data in **Parquet format** exactly as extracted from the SQL source, with no transformations applied.

### Storage Structure

All data lands in the `bronze` container of **ADLS Gen2** (`saspotifyprojectbdv`):

```
bronze/
├── DimArtist/          ← Parquet files (incremental loads)
├── DimArtist_cdc/      ← CDC config JSON (tracks last loaded timestamp)
├── DimDate/
├── DimDate_cdc/
├── DimTrack/
├── DimTrack_cdc/
├── DimUser/
├── DimUser_cdc/
├── FactStream/
└── FactStream_cdc/
```

Each `<Table>_cdc/` folder contains a `cdc.json` file that stores the **last successfully loaded CDC watermark** for that table, enabling stateful incremental loads.

---

## ⚙️ ADF Pipeline — `incremental_ingestion`

### Pipeline Overview

The pipeline `incremental_ingestion` runs a **ForEach** loop over a configurable array of tables, executing the following activities per table:

```
ForEachTable
  ├── Last_CDC         (Lookup)       → Reads last CDC timestamp from cdc.json
  ├── current          (Set Variable) → Captures current UTC timestamp
  ├── AzureSQLToLake   (Copy Data)    → Extracts new/changed rows to Parquet
  └── If IncrementalData (If Condition)
        ├── True  → MAX_CDC (Script) + update_last_cdc (Copy)
        └── False → DeleteEmptyFile (Delete)
```

### Key Design Decisions

**Backfilling:** The pipeline supports a `from_date` parameter per table. When provided, it overrides the CDC watermark and loads data from that date forward — enabling full historical backfills for any table.

**Incremental Loading:** On each run, the pipeline reads the last CDC timestamp from `cdc.json`, queries only rows where the CDC column is greater than that value, and writes a time-stamped Parquet file.

**Empty File Cleanup:** If a copy activity produces no data (nothing new to load), the empty Parquet file is automatically deleted, keeping the lake clean.

**CDC Watermark Update:** After a successful load, the pipeline queries `MAX(<cdc_col>)` from the source and overwrites `cdc.json` with the new high-watermark, ensuring the next run only picks up records beyond this point.

### Pipeline Parameters

```json
{
  "loop_input": [
    { "schema": "dbo", "table": "DimUser",     "cdc_col": "updated_at",       "from_date": "" },
    { "schema": "dbo", "table": "DimTrack",    "cdc_col": "updated_at",       "from_date": "" },
    { "schema": "dbo", "table": "DimDate",     "cdc_col": "date",             "from_date": "" },
    { "schema": "dbo", "table": "DimArtist",   "cdc_col": "updated_at",       "from_date": "" },
    { "schema": "dbo", "table": "FactStream",  "cdc_col": "stream_timestamp", "from_date": "" }
  ]
}
```

To trigger a backfill, set `from_date` to a specific date string (e.g. `"2024-01-01"`).

### Output File Naming

Each Parquet file written to the lake is named:

```
bronze/<TableName>/<TableName>_<utcNow()>.parquet
```

This ensures each run creates a uniquely timestamped file, preserving the full history of ingestion batches.

---

## 🔗 Linked Services & Datasets

| Resource | Type | Purpose |
|---|---|---|
| `AzureSQL_LS` | Azure SQL Database | Source connection for reads and MAX_CDC script |
| `AzureSQLDB_DS` | Azure SQL Dataset | Dynamic source dataset (schema/table from parameters) |
| `ADLSGen2_Parquet_LS` | ADLS Gen2 | Sink for Parquet data files |
| `ADLSGen2_JSON_DS` | ADLS Gen2 JSON | Read/write CDC watermark files |

---

## 🥈 Silver Layer — Transformations (Databricks / PySpark)

The **silver layer** reads raw Parquet files from bronze using **Auto Loader** (`cloudFiles` streaming), applies table-specific data quality and enrichment transformations, and writes clean **Delta tables** back to ADLS Gen2.

### Common Setup

All silver notebooks share the following imports and base configuration:

```python
from pyspark.sql.functions import (
    col, upper, lower, trim, when, regexp_replace,
    to_timestamp, hour, dayofweek, month, quarter,
    to_date, lit
)
from pyspark.sql.types import IntegerType

from utils.transformations import reusable

STORAGE = "saspotifyprojectbdv"
BRONZE  = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER  = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
```

All tables use the same write pattern:

```python
df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{SILVER}/<Table>/checkpoint") \
    .option("path",               f"{SILVER}/<Table>/data") \
    .trigger(once=True) \
    .toTable("spotify.silver.<Table>")
```

---

### 👤 DimUser

**Target:** `spotify.silver.DimUser` | **Dedup key:** `user_id`

| Column | Operation | Description |
|---|---|---|
| `user_name` | `upper(trim(col))` | Standardise to UPPERCASE and strip whitespace |
| `email` | `lower(trim(col))` | Normalise email to lowercase |
| `email` | null guard | Replace null / empty with `unknown@unknown.com` |
| `subscription_type` | `lower(trim(col))` | Normalise to lowercase (e.g. `FREE` → `free`) |
| `is_premium` | derived boolean | `True` when `subscription_type = 'premium'`, else `False` |
| `_rescued_data` | dropped | Remove Auto Loader rescue column |

```python
df_users = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{SILVER}/DimUser/schema") \
    .load(f"{BRONZE}/DimUser")

df_users = df_users.withColumn("user_name", upper(trim(col("user_name"))))
df_users = df_users.withColumn("email", lower(trim(col("email"))))
df_users = df_users.withColumn(
    "email",
    when(col("email").isNull() | (col("email") == ""), lit("unknown@unknown.com"))
    .otherwise(col("email"))
)
df_users = df_users.withColumn("subscription_type", lower(trim(col("subscription_type"))))
df_users = df_users.withColumn(
    "is_premium",
    when(col("subscription_type") == "premium", lit(True)).otherwise(lit(False))
)

df_users = reusable().dropColumns(df_users, ["_rescued_data"])
df_users = df_users.dropDuplicates(["user_id"])
```

---

### 🎤 DimArtist

**Target:** `spotify.silver.DimArtist` | **Dedup key:** `artist_id`

| Column | Operation | Description |
|---|---|---|
| `artist_name` | `upper(trim(col))` | Standardise to UPPERCASE and strip whitespace |
| `genre` | `lower(trim(col))` | Normalise genre to lowercase |
| `genre` | null guard | Replace null / empty with `unknown` |
| `country` | `upper(trim(col))` | Normalise to uppercase ISO code (e.g. `us` → `US`) |
| `_rescued_data` | dropped | Remove Auto Loader rescue column |

```python
df_artists = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{SILVER}/DimArtist/schema") \
    .load(f"{BRONZE}/DimArtist")

df_artists = df_artists.withColumn("artist_name", upper(trim(col("artist_name"))))
df_artists = df_artists.withColumn("genre", lower(trim(col("genre"))))
df_artists = df_artists.withColumn(
    "genre",
    when(col("genre").isNull() | (col("genre") == ""), lit("unknown"))
    .otherwise(col("genre"))
)
df_artists = df_artists.withColumn("country", upper(trim(col("country"))))

df_artists = reusable().dropColumns(df_artists, ["_rescued_data"])
df_artists = df_artists.dropDuplicates(["artist_id"])
```

---

### 🎵 DimTrack

**Target:** `spotify.silver.DimTrack` | **Dedup key:** `track_id`

| Column | Operation | Description |
|---|---|---|
| `track_name` | `regexp_replace` + `trim` | Replace hyphens with spaces, strip whitespace |
| `track_name` | null guard | Replace null / empty with `unknown` |
| `duration_flag` | derived string | `short` (<150 s) \| `medium` (150–299 s) \| `long` (≥300 s) |
| `tempo_category` | derived string | `slow` (<90 BPM) \| `moderate` (90–139) \| `fast` (≥140) |
| `is_explicit` | normalised boolean | Coerce `'1'`, `'true'`, `'yes'` → `True`, else `False` |
| `_rescued_data` | dropped | Remove Auto Loader rescue column |

```python
df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{SILVER}/DimTrack/schema") \
    .load(f"{BRONZE}/DimTrack")

df_track = df_track.withColumn("track_name", trim(regexp_replace(col("track_name"), "-", " ")))
df_track = df_track.withColumn(
    "track_name",
    when(col("track_name").isNull() | (col("track_name") == ""), lit("unknown"))
    .otherwise(col("track_name"))
)
df_track = df_track.withColumn(
    "duration_flag",
    when(col("duration_sec") < 150, lit("short"))
    .when((col("duration_sec") >= 150) & (col("duration_sec") < 300), lit("medium"))
    .when(col("duration_sec") >= 300, lit("long"))
    .otherwise(lit("unknown"))
)
df_track = df_track.withColumn(
    "tempo_category",
    when(col("bpm") < 90, lit("slow"))
    .when((col("bpm") >= 90) & (col("bpm") < 140), lit("moderate"))
    .when(col("bpm") >= 140, lit("fast"))
    .otherwise(lit("unknown"))
)
df_track = df_track.withColumn(
    "is_explicit",
    when(col("is_explicit").cast("string").isin("1", "true", "True", "yes"), lit(True))
    .otherwise(lit(False))
)

df_track = reusable().dropColumns(df_track, ["_rescued_data"])
df_track = df_track.dropDuplicates(["track_id"])
```

---

### 📅 DimDate

**Target:** `spotify.silver.DimDate` | **Dedup key:** `date_key`

| Column | Operation | Description |
|---|---|---|
| `date` | `to_date(col)` | Ensure `DateType` cast (source may deliver as string) |
| `quarter` | `quarter(col)` | Derive calendar quarter (1–4) |
| `month_num` | `month(col).cast(IntegerType)` | Ensure month is integer type |
| `is_weekend` | `dayofweek isin(1, 7)` | `True` for Sunday (1) and Saturday (7) |
| `season` | derived string | `winter` (12,1,2) \| `spring` (3,4,5) \| `summer` (6,7,8) \| `autumn` (9,10,11) |
| `_rescued_data` | dropped | Remove Auto Loader rescue column |

```python
df_date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{SILVER}/DimDate/schema") \
    .load(f"{BRONZE}/DimDate")

df_date = df_date.withColumn("date", to_date(col("date")))
df_date = df_date.withColumn("quarter", quarter(col("date")))
df_date = df_date.withColumn("month_num", month(col("date")).cast(IntegerType()))
df_date = df_date.withColumn(
    "is_weekend",
    when(dayofweek(col("date")).isin(1, 7), lit(True)).otherwise(lit(False))
)
df_date = df_date.withColumn(
    "season",
    when(col("month_num").isin(12, 1, 2), lit("winter"))
    .when(col("month_num").isin(3, 4, 5),  lit("spring"))
    .when(col("month_num").isin(6, 7, 8),  lit("summer"))
    .otherwise(lit("autumn"))
)

df_date = reusable().dropColumns(df_date, ["_rescued_data"])  # fixed: was overwriting df_track
df_date = df_date.dropDuplicates(["date_key"])
```

---

### ▶️ FactStream

**Target:** `spotify.silver.FactStream` | **Dedup:** none — all streaming events are retained

| Column | Operation | Description |
|---|---|---|
| `stream_timestamp` | `to_timestamp(col)` | Ensure `TimestampType` cast |
| `stream_hour` | `hour(col).cast(IntegerType)` | Derive hour of day (0–23) |
| `stream_day_of_week` | `dayofweek(col).cast(IntegerType)` | Derive day of week (1=Sun … 7=Sat) |
| `time_of_day` | derived string | `morning` (5–11) \| `afternoon` (12–16) \| `evening` (17–20) \| `night` (rest) |
| `ms_played` | `cast(IntegerType)` | Coerce to integer (source may deliver as float/string) |
| `seconds_played` | `ms_played / 1000` | Human-readable playback duration |
| orphan rows | `filter isNotNull` | Drop rows where `user_id`, `track_id`, or `date_key` is null |
| `_rescued_data` | dropped | Remove Auto Loader rescue column |

```python
df_fact = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{SILVER}/FactStream/schema") \
    .load(f"{BRONZE}/FactStream")

df_fact = df_fact.withColumn("stream_timestamp", to_timestamp(col("stream_timestamp")))
df_fact = df_fact.withColumn("stream_hour", hour(col("stream_timestamp")).cast(IntegerType()))
df_fact = df_fact.withColumn("stream_day_of_week", dayofweek(col("stream_timestamp")).cast(IntegerType()))
df_fact = df_fact.withColumn(
    "time_of_day",
    when((col("stream_hour") >= 5)  & (col("stream_hour") < 12), lit("morning"))
    .when((col("stream_hour") >= 12) & (col("stream_hour") < 17), lit("afternoon"))
    .when((col("stream_hour") >= 17) & (col("stream_hour") < 21), lit("evening"))
    .otherwise(lit("night"))
)
df_fact = df_fact.withColumn("ms_played", col("ms_played").cast(IntegerType()))
df_fact = df_fact.withColumn("seconds_played", (col("ms_played") / 1000).cast(IntegerType()))
df_fact = df_fact.filter(
    col("user_id").isNotNull() &
    col("track_id").isNotNull() &
    col("date_key").isNotNull()
)

df_fact = reusable().dropColumns(df_fact, ["_rescued_data"])
```

---

## 🚀 How to Run

### First Run (Backfill)

1. Open the `incremental_ingestion` pipeline in ADF
2. Set `from_date` to `""` (empty) — the pipeline will use whatever is in `cdc.json`, or you can set a specific start date per table
3. Click **Debug** or add a trigger

### Subsequent Runs (Incremental)

Leave all `from_date` values empty. The pipeline will automatically read the last CDC watermark from `cdc.json` and only load new records.

### Manual Backfill for a Specific Table

Update `from_date` for the desired table entry in `loop_input` with a date string like `"2024-01-01"`. This bypasses the CDC file and loads from that date forward.

---

## 📁 Project Structure

```
ADF-SpotifyProject-BDV/
├── pipeline/
│   └── incremental_ingestion.json
├── dataset/
│   ├── AzureSQLDB_DS.json
│   ├── ADLSGen2_Parquet_LS.json
│   └── ADLSGen2_JSON_DS.json
└── linkedService/
    ├── AzureSQL_LS.json
    └── ADLS_LS.json

spotify_dab/ (Databricks Asset Bundle)
├── src/
│   └── spotify_dab_etl/
│       └── transformations/
│           └── silver_Dimensions.py
├── resources/
│   └── spotify_dab_etl.pipeline.yml
└── databricks.yml
```

---

## 🛣️ Roadmap

- [x] **Bronze Layer** — Raw ingestion from SQL to ADLS Gen2 with CDC
- [x] **Silver Layer** — Cleaned and conformed data with data quality checks
- [x] **Gold Layer** — Aggregated business-ready tables and metrics


---

## 👤 Author

**Bruno Ducati Vazquez**
[GitHub](https://github.com/BrunoDucati-Vazquez)
