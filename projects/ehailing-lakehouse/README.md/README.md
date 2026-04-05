# E-Hailing Lakehouse Project

A production-style end-to-end data lakehouse built on Azure Databricks,
processing 500 e-hailing trips across 5 Nigerian cities using the
Medallion Architecture (Bronze → Silver → Gold). Built to scale up.

## Architecture

[Bronze] Raw ingestion → [Silver] Cleaning & validation → [Gold] Business aggregations

- **Bronze**: 500 raw trips loaded as-is into a Delta table. No modifications.
- **Silver**: PySpark cleaning — nulls handled, timestamps cast, whitespace stripped,
  data quality flagged, fare categories derived.
- **Gold**: 5 Spark SQL aggregation tables — driver performance, city revenue,
  daily trends, payment breakdown, vehicle performance.

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

500 synthetic e-hailing trips across Lagos, Abuja, Port Harcourt,
Kano, and Ibadan. Jan–Feb 2024. 15 drivers, 3 vehicle types,
4 payment methods. Includes realistic data quality issues:
92 cancellations, 13 rows with missing driver info, 9 missing fares,
leading whitespace in location columns.


## Key Results

| Gold Table | Rows | Description |
|---|---|---|
| Driver Performance | 15 | Revenue, ratings, cancellation rate per driver |
| City Revenue | 5 | Total revenue and avg fare per city |
| Daily Trends | 42 | Day-by-day trip volume and revenue |
| Payment Breakdown | 4 | Budget/standard/premium split per payment type |
| Vehicle Performance | 3 | Sedan vs SUV vs bike comparison |

## How to Run

1. Upload `data/raw_trips.csv` to Databricks FileStore
2. Add ADLS Gen2 config cell to each notebook (see notebooks/)
3. Run notebooks in order: 01 → 02 → 03
4. Optionally run 04 for streaming simulation