# Databricks notebook source
# Config cell — top of notebook
storage_account_name = "delakehousezdennis"
storage_account_key  = "your_actual_key_here"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
ADLS_BASE = f"abfss://lakehouse@{storage_account_name}.dfs.core.windows.net"

# Read Silver from Delta table
silver_df = spark.table("silver_trips")
silver_df.createOrReplaceTempView("silver_trips")
print(f"Config loaded. Silver rows: {silver_df.count()}")

# COMMAND ----------

# Read Silver from Delta table — persists across notebooks and sessions
silver_df = spark.table("silver_trips")
count = silver_df.count()
print(f"Silver rows loaded from Delta table: {count}")

# Register as temp view for SQL queries in this session
silver_df.createOrReplaceTempView("silver_trips")
print("silver_trips registered as temp view for this session")

# COMMAND ----------

gold_drivers = spark.sql("""
    SELECT
        driver_id,
        driver_name,
        vehicle_type,
        COUNT(trip_id)                                          AS total_trips,
        SUM(CASE WHEN trip_status = 'completed' THEN 1 END)    AS completed_trips,
        SUM(CASE WHEN trip_status = 'cancelled' THEN 1 END)    AS cancelled_trips,
        ROUND(SUM(CASE WHEN trip_status = 'cancelled' THEN 1 END) * 100.0 / COUNT(*), 1)
                                                                AS cancellation_rate_pct,
        ROUND(SUM(fare_amount), 2)                             AS total_revenue_ngn,
        ROUND(SUM(fare_in_usd), 2)                             AS total_revenue_usd,
        ROUND(AVG(fare_amount), 2)                             AS avg_fare_ngn,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2)   AS avg_rating
    FROM silver_trips
    WHERE data_quality = 'ok'
    GROUP BY driver_id, driver_name, vehicle_type
    ORDER BY total_revenue_ngn DESC
""")

gold_drivers.createOrReplaceTempView("gold_driver_performance")
print(f"Gold Table 1 — Driver Performance: {gold_drivers.count()} rows")
gold_drivers.show(truncate=False)

# COMMAND ----------

gold_cities = spark.sql("""
    SELECT
        pickup_city                                             AS city,
        COUNT(trip_id)                                          AS total_trips,
        SUM(CASE WHEN trip_status = 'completed' THEN 1 END)    AS completed_trips,
        ROUND(SUM(CASE WHEN trip_status = 'completed'
            THEN fare_amount END), 2)                           AS total_revenue_ngn,
        ROUND(AVG(CASE WHEN trip_status = 'completed'
            THEN fare_amount END), 2)                           AS avg_fare_ngn,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2)   AS avg_rating,
        COUNT(DISTINCT driver_id)                              AS active_drivers
    FROM silver_trips
    GROUP BY pickup_city
    ORDER BY total_revenue_ngn DESC
""")

gold_cities.createOrReplaceTempView("gold_city_summary")
print(f"Gold Table 2 — City Revenue: {gold_cities.count()} rows")
gold_cities.show(truncate=False)

# COMMAND ----------

gold_daily = spark.sql("""
    SELECT
        DATE(request_time)                                      AS trip_date,
        COUNT(trip_id)                                          AS total_trips,
        SUM(CASE WHEN trip_status = 'completed' THEN 1 END)    AS completed_trips,
        SUM(CASE WHEN trip_status = 'cancelled' THEN 1 END)    AS cancelled_trips,
        ROUND(SUM(CASE WHEN trip_status = 'completed'
            THEN fare_amount END), 2)                           AS daily_revenue_ngn,
        ROUND(AVG(CASE WHEN trip_status = 'completed'
            THEN fare_amount END), 2)                           AS avg_fare_ngn,
        ROUND(AVG(surge_multiplier), 2)                        AS avg_surge
    FROM silver_trips
    GROUP BY DATE(request_time)
    ORDER BY trip_date
""")

gold_daily.createOrReplaceTempView("gold_daily_trends")
print(f"Gold Table 3 — Daily Trends: {gold_daily.count()} rows")
gold_daily.show(50, truncate=False)

# COMMAND ----------

gold_payment = spark.sql("""
    SELECT
        payment_type,
        COUNT(trip_id)                                          AS total_trips,
        COUNT(CASE WHEN fare_category = 'budget' THEN 1 END)   AS budget_trips,
        COUNT(CASE WHEN fare_category = 'standard' THEN 1 END) AS standard_trips,
        COUNT(CASE WHEN fare_category = 'premium' THEN 1 END)  AS premium_trips,
        ROUND(SUM(fare_amount), 2)                             AS total_revenue_ngn,
        ROUND(AVG(fare_amount), 2)                             AS avg_fare_ngn
    FROM silver_trips
    WHERE trip_status = 'completed'
    GROUP BY payment_type
    ORDER BY total_revenue_ngn DESC
""")

gold_payment.createOrReplaceTempView("gold_payment_breakdown")
print(f"Gold Table 4 — Payment Breakdown: {gold_payment.count()} rows")
gold_payment.show(truncate=False)

# COMMAND ----------

gold_vehicles = spark.sql("""
    SELECT
        vehicle_type,
        COUNT(trip_id)                                          AS total_trips,
        SUM(CASE WHEN trip_status = 'completed' THEN 1 END)    AS completed_trips,
        ROUND(AVG(distance_km), 2)                             AS avg_distance_km,
        ROUND(AVG(fare_amount), 2)                             AS avg_fare_ngn,
        ROUND(SUM(fare_amount), 2)                             AS total_revenue_ngn,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2)   AS avg_rating,
        ROUND(AVG(surge_multiplier), 2)                        AS avg_surge
    FROM silver_trips
    GROUP BY vehicle_type
    ORDER BY total_revenue_ngn DESC
""")

gold_vehicles.createOrReplaceTempView("gold_vehicle_performance")
print(f"Gold Table 5 — Vehicle Performance: {gold_vehicles.count()} rows")
gold_vehicles.show(truncate=False)

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER — ALL TABLES COMPLETE")
print("=" * 60)

tables = [
    ("gold_driver_performance", "Driver Performance"),
    ("gold_city_summary",       "City Revenue"),
    ("gold_daily_trends",       "Daily Trends"),
    ("gold_payment_breakdown",  "Payment Breakdown"),
    ("gold_vehicle_performance","Vehicle Performance"),
]

for view_name, label in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM {view_name}").collect()[0][0]
    print(f"  {label:<25} → {count} rows")

print("=" * 60)
print("Pipeline complete: Bronze → Silver → Gold")
print("All Gold tables registered as temp views")

# COMMAND ----------

# Write all Gold tables to ADLS — add this as the final cell
tables = [
    (gold_drivers,  "gold/driver_performance"),
    (gold_cities,   "gold/city_summary"),
    (gold_daily,    "gold/daily_trends"),
    (gold_payment,  "gold/payment_breakdown"),
    (gold_vehicles, "gold/vehicle_performance"),
]

for df, path in tables:
    df.write.format("delta").mode("overwrite") \
        .save(f"{ADLS_BASE}/{path}")
    print(f"Written: {ADLS_BASE}/{path} ({df.count()} rows)")

print("\nAll Gold tables written to ADLS Gen2")