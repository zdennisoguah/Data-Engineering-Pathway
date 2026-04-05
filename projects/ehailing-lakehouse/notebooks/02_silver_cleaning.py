# Databricks notebook source
# Configure Databricks to access your ADLS Gen2 account
# Replace the values with your own storage account name and key

storage_account_name = "delakehousezdennis"  # your storage account name
storage_account_key  = "your_actual_key_here"  # from Azure Portal → Access keys

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
ADLS_BASE = f"abfss://lakehouse@{storage_account_name}.dfs.core.windows.net"
print("Config loaded")


# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, when, lit, avg,
    to_timestamp, coalesce
)
from pyspark.sql.functions import round as spark_round

# Start from Bronze temp view
bronze_df = spark.table("bronze_trips")

print(f"Starting Silver cleaning. Input rows: {bronze_df.count()}")

# COMMAND ----------

# Fix leading/trailing whitespace on location columns
silver_df = bronze_df \
    .withColumn("pickup_location", trim(col("pickup_location"))) \
    .withColumn("dropoff_location", trim(col("dropoff_location")))

# Verify whitespace fix worked
from pyspark.sql.functions import length
before = bronze_df.filter(length(col("pickup_location")) != length(trim(col("pickup_location")))).count()
after = silver_df.filter(length(col("pickup_location")) != length(trim(col("pickup_location")))).count()
print(f"Rows with leading/trailing spaces in pickup_location: before={before}, after={after}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Fill missing payment_type
silver_df = silver_df.fillna({"payment_type": "unknown"})

# Fill missing rating with 0.0
silver_df = silver_df.fillna({"rating": 0.0})

# Impute missing fare_amount with average of non-null fares
avg_fare = silver_df.select(avg("fare_amount")).collect()[0][0]
avg_fare_rounded = float(f"{avg_fare:.2f}")
print(f"Average fare for imputation: {avg_fare_rounded}")
silver_df = silver_df.fillna({"fare_amount": avg_fare_rounded})

# Confirm no more nulls in these columns
from pyspark.sql.functions import count, when
null_check = silver_df.select(
    count(when(col("payment_type").isNull(), 1)).alias("null_payment"),
    count(when(col("fare_amount").isNull(), 1)).alias("null_fare"),
    count(when(col("rating").isNull(), 1)).alias("null_rating")
).collect()[0]
print(f"Nulls after fill — payment: {null_check['null_payment']}, fare: {null_check['null_fare']}, rating: {null_check['null_rating']}")

# COMMAND ----------

# Cast string timestamps to proper timestamp type
silver_df = silver_df \
    .withColumn("request_time", to_timestamp(col("request_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("pickup_time", to_timestamp(col("pickup_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("dropoff_time", to_timestamp(col("dropoff_time"), "yyyy-MM-dd HH:mm:ss"))

print("Timestamp columns cast successfully")
silver_df.select("trip_id", "request_time", "pickup_time", "dropoff_time").show(5, truncate=False)

# COMMAND ----------

# Add fare_category
silver_df = silver_df.withColumn(
    "fare_category",
    when(col("fare_amount") < 1000, "budget")
    .when((col("fare_amount") >= 1000) & (col("fare_amount") <= 2000), "standard")
    .otherwise("premium")
)

# Add fare_in_usd
silver_df = silver_df.withColumn(
    "fare_in_usd",
    spark_round(col("fare_amount") / 1500, 2)
)

# Add data_quality flag
silver_df = silver_df.withColumn(
    "data_quality",
    when(
        col("driver_id").isNull() | (col("driver_id") == "") |
        col("driver_name").isNull() | (col("driver_name") == ""),
        "incomplete"
    ).otherwise("ok")
)

print("Derived columns added: fare_category, fare_in_usd, data_quality")
silver_df.select("trip_id", "fare_amount", "fare_category", "fare_in_usd", "data_quality").show(10)

# COMMAND ----------

# Register Silver as a temp view
silver_df.createOrReplaceTempView("silver_trips")

summary = spark.sql("""
    SELECT
        COUNT(*)                                                AS total_rows,
        COUNT(DISTINCT driver_id)                              AS unique_drivers,
        COUNT(DISTINCT pickup_city)                            AS cities,
        SUM(CASE WHEN trip_status = 'completed' THEN 1 END)   AS completed_trips,
        SUM(CASE WHEN trip_status = 'cancelled' THEN 1 END)   AS cancelled_trips,
        SUM(CASE WHEN data_quality = 'incomplete' THEN 1 END) AS incomplete_records,
        SUM(CASE WHEN fare_category = 'budget' THEN 1 END)    AS budget_trips,
        SUM(CASE WHEN fare_category = 'standard' THEN 1 END)  AS standard_trips,
        SUM(CASE WHEN fare_category = 'premium' THEN 1 END)   AS premium_trips,
        ROUND(AVG(fare_amount), 2)                            AS avg_fare_ngn,
        ROUND(AVG(fare_in_usd), 2)                            AS avg_fare_usd
    FROM silver_trips
""")

print("SILVER LAYER — CLEANING COMPLETE")
print("=" * 60)
summary.show(truncate=False)

# COMMAND ----------

# Write Silver layer as a Delta table — persists across notebooks
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_trips")

print("Silver layer written as Delta table: silver_trips")
print(f"Rows written: {spark.table('silver_trips').count()}")

# COMMAND ----------

# Write cell — bottom of notebook
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{ADLS_BASE}/silver/trips")

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_trips")

print(f"Silver written to ADLS Gen2: {ADLS_BASE}/silver/trips")
print(f"Silver rows: {silver_df.count()}")