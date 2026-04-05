# E-Hailing Lakehouse — Azure Databricks

A production-style end-to-end data lakehouse built on Azure Databricks,
processing 500 e-hailing trips across 5 Nigerian cities using Medallion
Architecture (Bronze → Silver → Gold).

## Architecture
Raw CSV (ADLS Gen2)
↓
[Bronze] Raw ingestion — 500 rows, no modifications, Delta table
↓
[Silver] PySpark cleaning — nulls filled, timestamps cast,
whitespace stripped, fare categories derived,
data quality flagged
↓
[Gold] 5 Spark SQL aggregation tables — business-ready
↓
ADLS Gen2 — all layers persisted as Delta tables

## Tech Stack

| Tool | Purpose |
|---|---|
| Azure Databricks | Notebook environment + Spark compute |
| PySpark | Bronze → Silver transformations |
| Spark SQL | Silver → Gold aggregations |
| Delta Lake | Persistent cross-notebook table format |
| ADLS Gen2 | Cloud storage for all layers |
| Azure Event Hubs | Real-time trip event streaming |

## Dataset

500 synthetic e-hailing trips · 15 drivers · 5 cities ·
Jan–Feb 2024 · 3 vehicle types · 4 payment methods

Includes realistic data quality issues: 92 cancellations,
13 rows with missing driver info, 9 missing fares,
leading whitespace in location columns.

## Gold Layer Results

| Table | Rows | Description |
|---|---|---|
| driver_performance | 15 | Revenue, ratings, cancellation rate per driver |
| city_summary | 5 | Revenue and avg fare per city |
| daily_trends | 42 | Day-by-day trip volume and revenue |
| payment_breakdown | 4 | Budget/standard/premium split per payment type |
| vehicle_performance | 3 | Sedan vs SUV vs bike comparison |

## How to Run

1. Upload `data/raw_trips.csv` to Databricks FileStore
2. Add ADLS Gen2 config cell to each notebook (see notebooks/)
3. Run notebooks in order: 01 → 02 → 03
4. Optionally run 04 for streaming simulation