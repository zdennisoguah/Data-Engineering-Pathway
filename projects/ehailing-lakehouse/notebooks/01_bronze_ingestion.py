# Databricks notebook source
# Bronze Layer — Raw Ingestion
# Rule: load everything as-is. No cleaning. No changes.

import pandas as pd
from pyspark.sql import SparkSession
import io

# Read raw data from Unity Catalog table
# In production this would be: spark.read.csv("abfss://container@storage.dfs.core.windows.net/raw/trips.csv")

bronze_df = spark.table("de_learning_workspace.default.raw_trips")

print(f"Bronze rows loaded: {bronze_df.count()}")
print(f"Columns: {len(bronze_df.columns)}")
bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

print("=" * 60)
print("BRONZE LAYER — DATA PROFILE REPORT")
print("=" * 60)

# Row and column count
print(f"\nTotal rows    : {bronze_df.count()}")
print(f"Total columns : {len(bronze_df.columns)}")

# Null counts per column
print("\nNull / empty counts per column:")
print("-" * 40)

null_counts = bronze_df.select([
    count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
    for c in bronze_df.columns
]).collect()[0].asDict()

for col_name, null_count in null_counts.items():
    if null_count > 0:
        pct = round((null_count / bronze_df.count()) * 100, 1)
        print(f"  {col_name:<25} {null_count:>4} nulls ({pct}%)")

# Trip status breakdown
print("\nTrip status breakdown:")
bronze_df.groupBy("trip_status").count().orderBy("count", ascending=False).show()

# City breakdown
print("Trips by city:")
bronze_df.groupBy("pickup_city").count().orderBy("count", ascending=False).show()

# Vehicle type breakdown
print("Trips by vehicle type:")
bronze_df.groupBy("vehicle_type").count().orderBy("count", ascending=False).show()

# Surge multiplier distribution
print("Surge multiplier distribution:")
bronze_df.groupBy("surge_multiplier").count().orderBy("surge_multiplier").show()

# COMMAND ----------

# Register bronze as a temp view for downstream use
bronze_df.write.mode("overwrite").saveAsTable("bronze_trips")

# Quick sanity check via SQL
result = spark.sql("""
    SELECT
        COUNT(*)                                    AS total_trips,
        COUNT(DISTINCT driver_id)                   AS unique_drivers,
        COUNT(DISTINCT pickup_city)                 AS cities_covered,
        COUNT(DISTINCT vehicle_type)                AS vehicle_types,
        SUM(CASE WHEN trip_status = 'completed'
            THEN 1 ELSE 0 END)                      AS completed_trips,
        SUM(CASE WHEN trip_status = 'cancelled'
            THEN 1 ELSE 0 END)                      AS cancelled_trips,
        SUM(CASE WHEN driver_id IS NULL
            OR driver_id = '' THEN 1 ELSE 0 END)   AS missing_driver_rows,
        SUM(CASE WHEN fare_amount IS NULL
            THEN 1 ELSE 0 END)                      AS missing_fare_rows
    FROM bronze_trips
""")

print("BRONZE SUMMARY")
print("=" * 60)
result.show(truncate=False)
print("Bronze layer complete. No data was modified.")
print("Raw data registered as temp view: bronze_trips")


# COMMAND ----------

# Config
storage_account_name = "delakehousezdennis"
storage_account_key  = "your_actual_key_here"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
ADLS_BASE = f"abfss://lakehouse@{storage_account_name}.dfs.core.windows.net"

# Write Bronze to ADLS Gen2
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{ADLS_BASE}/bronze/trips")

# Also save as Delta table for cross-notebook access
bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_trips")

print(f"Bronze written to ADLS Gen2: {ADLS_BASE}/bronze/trips")
print(f"Bronze rows: {bronze_df.count()}")