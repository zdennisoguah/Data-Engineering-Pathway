# Hospital Operations Lakehouse

End-to-end healthcare data lakehouse processing 55,500 patient
admission records using Medallion Architecture, PII masking,
and Apache Airflow orchestration.

## Dataset
Kaggle Healthcare Dataset — 55,500 rows, 15 columns.
Source: kaggle.com/datasets/prasad22/healthcare-dataset

## Architecture
Bronze → Silver → Gold on local PySpark with Delta Lake.
Orchestrated via Apache Airflow DAG on Astronomer.

## What's new vs Project 1
- PII masking: patient names hashed with SHA256 at Silver layer
- Multi-source joins: patients + admissions joined at Gold layer
- Apache Airflow: Python-based DAG with 6 tasks, retries, validation
- 55,500 rows vs 500 — order of magnitude larger dataset

## Tech Stack
Python · PySpark 3.5.3 · Delta Spark 3.3.0 · Apache Airflow 2.9 ·
Astronomer · OrbStack · Delta Lake

## Pipeline
start → ingest_bronze → transform_silver → build_gold → validate → end

## Gold Tables
| Table | Rows | Description |
|---|---|---|
| admission_summary | 18 | Avg stay, abnormal rate per condition |
| hospital_revenue | 20 | Revenue per hospital |
| doctor_performance | 20 | Patients and billing per doctor |
| disease_trends | 24 | Cases per condition and age group |
| insurance_analysis | 5 | Claims and costs per insurer |

## PII Handling
Patient names are irreversibly hashed with SHA256 at the Silver layer.
Original names are dropped. Only anonymous patient_id flows to Gold.