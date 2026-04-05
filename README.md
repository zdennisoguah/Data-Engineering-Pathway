# Dennis Oguah — Data Engineering Portfolio

Data engineer building production pipelines and lakehouses on Azure.

**Stack:** Python · SQL · PySpark · Azure Databricks · Delta Lake ·
Azure Event Hubs · ADLS Gen2 · Git

---

## Projects

### E-Hailing Lakehouse (Azure Databricks + Delta Lake)
> [`/projects/ehailing-lakehouse`](./projects/ehailing-lakehouse)

End-to-end lakehouse processing 500+ e-hailing trips across 5 Nigerian
cities (Lagos, Abuja, Port Harcourt, Kano, Ibadan) using Medallion
Architecture on Azure Databricks.

- **Bronze** — Raw ingestion into ADLS Gen2 Delta tables
- **Silver** — PySpark cleaning: nulls, timestamps, data quality flags
- **Gold** — 5 Spark SQL aggregation tables: driver performance,
  city revenue, daily trends, payment breakdown, vehicle performance
- **Streaming** — Real-time trip events via Azure Event Hubs → Bronze

**Tools:** Azure Databricks · PySpark · Spark SQL · Delta Lake ·
ADLS Gen2 · Azure Event Hubs

---

## SQL

### Advanced Analytical Queries
> [`/sql/advanced-analysis`](./sql/advanced-analysis)

10 DE-grade SQL queries on e-hailing data using DuckDB — window
functions, CTEs, CASE WHEN pivots, running totals, and GROUP BY + HAVING.

---

## Streaming

### Azure Event Hubs Pipeline
> [`/streaming/event-hubs-pipeline`](./streaming/event-hubs-pipeline)

Python producer publishing simulated real-time trip events to Azure
Event Hubs, consumed in Databricks and written to Bronze Delta layer.

---

## Scripts

### Python Data Cleaning Pipeline
> [`/scripts`](./scripts)

Production-grade Python script with logging, error handling, and
relative paths — processes raw e-hailing CSV and outputs cleaned JSON.