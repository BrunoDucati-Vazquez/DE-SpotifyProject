# 🎵 Spotify Data Engineering Project — Azure End-to-End Pipeline

> A modern data engineering project built on **Azure**, ingesting Spotify-like data from an **Azure SQL Database** into a **Data Lake (ADLS Gen2)** using **Azure Data Factory** with incremental loading and CDC tracking.

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
  └── bronze/ (Raw Layer)
        ├── DimArtist/
        ├── DimDate/
        ├── DimTrack/
        ├── DimUser/
        ├── FactStream/
        ├── DimArtist_cdc/
        ├── DimDate_cdc/
        ├── DimTrack_cdc/
        ├── DimUser_cdc/
        └── FactStream_cdc/
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
```

---

## 🛣️ Roadmap

- [x] **Bronze Layer** — Raw ingestion from SQL to ADLS Gen2 with CDC
- [ ] **Silver Layer** — Cleaned and conformed data with data quality checks
- [ ] **Gold Layer** — Aggregated business-ready tables and metrics
- [ ] **Semantic Layer / Reporting** — Power BI or Synapse Analytics integration

---

## 🛠️ Tech Stack

- **Azure SQL Database** — Source OLTP system
- **Azure Data Factory** — Pipeline orchestration
- **Azure Data Lake Storage Gen2** — Raw data storage (bronze)
- **Parquet** — Storage format for analytical workloads
- **JSON** — CDC watermark configuration files

---

## 👤 Author

**Bruno Ducati Vazquez**
[GitHub](https://github.com/BrunoDucati-Vazquez)
