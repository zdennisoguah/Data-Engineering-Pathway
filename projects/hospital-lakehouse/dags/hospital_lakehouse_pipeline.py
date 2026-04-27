from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import logging

# ── DAG DEFAULT ARGS ──────────────────────────────────────
default_args = {
    "owner"           : "dennis",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ── PATH CONFIG ───────────────────────────────────────────
PROJECT_BASE = "/usr/local/airflow/include"
DELTA_BASE   = f"{PROJECT_BASE}/delta"
DATA_PATH    = f"{PROJECT_BASE}/data/healthcare_raw.csv"

# ── TASK FUNCTIONS ────────────────────────────────────────

def run_bronze(**context):
    """Ingest raw CSV and write Bronze Delta tables."""
    logging.info("Starting Bronze ingestion")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, monotonically_increasing_id
    import os

    builder = SparkSession.builder \
        .appName("hospital-bronze-airflow") \
        .master("local[*]") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "8")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(f"{DELTA_BASE}/bronze/patients", exist_ok=True)
    os.makedirs(f"{DELTA_BASE}/bronze/admissions", exist_ok=True)

    raw_df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    raw_count = raw_df.count()
    logging.info(f"Raw rows loaded: {raw_count}")

    # Bronze patients
    bronze_patients = raw_df \
        .withColumn("admission_id", monotonically_increasing_id()) \
        .select(
            col("admission_id"),
            col("Name").alias("patient_name"),
            col("Age").alias("age"),
            col("Gender").alias("gender"),
            col("`Blood Type`").alias("blood_type")
        )

    bronze_patients.write.format("delta").mode("overwrite") \
        .save(f"{DELTA_BASE}/bronze/patients")

    patients_count = bronze_patients.count()
    logging.info(f"bronze_patients written: {patients_count} rows")

    # Bronze admissions
    bronze_admissions = raw_df \
        .withColumn("admission_id", monotonically_increasing_id()) \
        .select(
            col("admission_id"),
            col("`Medical Condition`").alias("medical_condition"),
            col("`Date of Admission`").alias("date_of_admission"),
            col("Doctor").alias("doctor"),
            col("Hospital").alias("hospital"),
            col("`Insurance Provider`").alias("insurance_provider"),
            col("`Billing Amount`").alias("billing_amount"),
            col("`Room Number`").alias("room_number"),
            col("`Admission Type`").alias("admission_type"),
            col("`Discharge Date`").alias("discharge_date"),
            col("Medication").alias("medication"),
            col("`Test Results`").alias("test_results")
        )

    bronze_admissions.write.format("delta").mode("overwrite") \
        .save(f"{DELTA_BASE}/bronze/admissions")

    admissions_count = bronze_admissions.count()
    logging.info(f"bronze_admissions written: {admissions_count} rows")

    logging.info("Bronze complete")
    spark.stop()

    return {
        "bronze_patients": patients_count,
        "bronze_admissions": admissions_count
    }


def run_silver(**context):
    """Clean Bronze data and write Silver Delta tables with PII masking."""
    logging.info("Starting Silver transformation")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        to_date,
        datediff,
        when,
        udf,
        round,
        upper,
        trim
    )
    from pyspark.sql.types import StringType
    import hashlib
    import os

    # Build Spark session
    builder = (
        SparkSession.builder
        .appName("hospital-silver-airflow")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "8")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Ensure folders exist
        os.makedirs(f"{DELTA_BASE}/silver/patients", exist_ok=True)
        os.makedirs(f"{DELTA_BASE}/silver/admissions", exist_ok=True)

        # Read Bronze layer
        bronze_patients = spark.read.format("delta").load(
            f"{DELTA_BASE}/bronze/patients"
        )

        bronze_admissions = spark.read.format("delta").load(
            f"{DELTA_BASE}/bronze/admissions"
        )

        logging.info(
            f"Bronze patients rows loaded: {bronze_patients.count()}"
        )
        logging.info(
            f"Bronze admissions rows loaded: {bronze_admissions.count()}"
        )

        # -----------------------------
        # PII HASHING FUNCTION
        # -----------------------------
        def hash_name(name):
            if name is None:
                return None

            cleaned = name.strip().lower()
            return hashlib.sha256(
                cleaned.encode()
            ).hexdigest()[:16]

        hash_udf = udf(hash_name, StringType())

        # -----------------------------
        # SILVER PATIENTS
        # -----------------------------
        silver_patients = (
            bronze_patients
            .withColumn("patient_id", hash_udf(col("patient_name")))
            .drop("patient_name")
            .withColumn("gender", upper(trim(col("gender"))))
            .withColumn("blood_type", upper(trim(col("blood_type"))))
            .withColumn(
                "age_group",
                when(col("age") < 18, "Paediatric (0-17)")
                .when(col("age") < 36, "Young Adult (18-35)")
                .when(col("age") < 61, "Adult (36-60)")
                .otherwise("Senior (61+)")
            )
            .select(
                "admission_id",
                "patient_id",
                "age",
                "age_group",
                "gender",
                "blood_type"
            )
        )

        silver_patients.write.format("delta") \
            .mode("overwrite") \
            .save(f"{DELTA_BASE}/silver/patients")

        patients_count = silver_patients.count()

        logging.info(
            f"silver_patients written: {patients_count} rows"
        )

        # -----------------------------
        # SILVER ADMISSIONS
        # -----------------------------
        silver_admissions = (
            bronze_admissions
            .withColumn(
                "date_of_admission",
                to_date(col("date_of_admission"), "yyyy-MM-dd")
            )
            .withColumn(
                "discharge_date",
                to_date(col("discharge_date"), "yyyy-MM-dd")
            )
            .withColumn(
                "length_of_stay_days",
                datediff(
                    col("discharge_date"),
                    col("date_of_admission")
                )
            )
            .withColumn(
                "admission_type",
                upper(trim(col("admission_type")))
            )
            .withColumn(
                "medical_condition",
                upper(trim(col("medical_condition")))
            )
            .withColumn(
                "test_results",
                upper(trim(col("test_results")))
            )
            .withColumn(
                "billing_amount",
                round(col("billing_amount"), 2)
            )
            .withColumn(
                "billing_category",
                when(col("billing_amount") < 5000, "low")
                .when(col("billing_amount") < 20000, "mid")
                .otherwise("high")
            )
            .withColumn(
                "data_quality",
                when(
                    col("length_of_stay_days").isNull() |
                    (col("length_of_stay_days") < 0) |
                    col("billing_amount").isNull(),
                    "investigate"
                ).otherwise("ok")
            )
        )

        silver_admissions.write.format("delta") \
            .mode("overwrite") \
            .save(f"{DELTA_BASE}/silver/admissions")

        admissions_count = silver_admissions.count()

        logging.info(
            f"silver_admissions written: {admissions_count} rows"
        )

        logging.info("Silver complete. PII masked.")

        return {
            "silver_patients": patients_count,
            "silver_admissions": admissions_count
        }

    finally:
        spark.stop()


def run_gold(**context):
    """Build Gold aggregation tables from Silver."""
    logging.info("Starting Gold aggregations")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    import os

    builder = SparkSession.builder \
        .appName("hospital-gold-airflow") \
        .master("local[*]") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "8")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for table in ["admission_summary", "hospital_revenue",
                  "doctor_performance", "disease_trends",
                  "insurance_analysis"]:
        os.makedirs(f"{DELTA_BASE}/gold/{table}", exist_ok=True)

    # Load Silver and join
    sp = spark.read.format("delta").load(f"{DELTA_BASE}/silver/patients")
    sa = spark.read.format("delta").load(f"{DELTA_BASE}/silver/admissions")
    joined = sa.join(sp, on="admission_id", how="left")
    joined.createOrReplaceTempView("hospital_data")

    gold_queries = {
        "admission_summary": """
            SELECT medical_condition, admission_type,
                COUNT(*) AS total_admissions,
                ROUND(AVG(length_of_stay_days), 1) AS avg_length_of_stay,
                ROUND(AVG(billing_amount), 2) AS avg_billing_amount,
                ROUND(COUNT(CASE WHEN test_results='ABNORMAL' THEN 1 END)
                    * 100.0 / COUNT(*), 1) AS abnormal_rate_pct
            FROM hospital_data
            GROUP BY medical_condition, admission_type
            ORDER BY total_admissions DESC
        """,
        "hospital_revenue": """
            SELECT hospital,
                COUNT(*) AS total_admissions,
                ROUND(SUM(billing_amount), 2) AS total_revenue,
                ROUND(AVG(billing_amount), 2) AS avg_billing_amount,
                ROUND(AVG(length_of_stay_days), 1) AS avg_length_of_stay
            FROM hospital_data
            GROUP BY hospital
            ORDER BY total_revenue DESC LIMIT 20
        """,
        "doctor_performance": """
            SELECT doctor,
                COUNT(*) AS total_patients,
                COUNT(DISTINCT medical_condition) AS conditions_treated,
                ROUND(AVG(billing_amount), 2) AS avg_billing_amount,
                ROUND(SUM(billing_amount), 2) AS total_revenue_generated
            FROM hospital_data
            GROUP BY doctor
            ORDER BY total_patients DESC LIMIT 20
        """,
        "disease_trends": """
            SELECT medical_condition, age_group,
                COUNT(*) AS total_cases,
                ROUND(AVG(length_of_stay_days), 1) AS avg_stay_days,
                ROUND(AVG(billing_amount), 2) AS avg_billing,
                ROUND(COUNT(CASE WHEN admission_type='EMERGENCY' THEN 1 END)
                    * 100.0 / COUNT(*), 1) AS emergency_rate_pct
            FROM hospital_data
            GROUP BY medical_condition, age_group
            ORDER BY medical_condition, total_cases DESC
        """,
        "insurance_analysis": """
            SELECT insurance_provider,
                COUNT(*) AS total_claims,
                ROUND(SUM(billing_amount), 2) AS total_billed,
                ROUND(AVG(billing_amount), 2) AS avg_claim_amount,
                ROUND(COUNT(CASE WHEN billing_category='high' THEN 1 END)
                    * 100.0 / COUNT(*), 1) AS high_cost_rate_pct
            FROM hospital_data
            GROUP BY insurance_provider
            ORDER BY total_billed DESC
        """
    }

    results = {}
    for table_name, query in gold_queries.items():
        df = spark.sql(query)
        df.write.format("delta").mode("overwrite") \
            .save(f"{DELTA_BASE}/gold/{table_name}")
        results[table_name] = df.count()
        logging.info(f"Gold {table_name}: {df.count()} rows")

    spark.stop()
    logging.info("Gold complete")
    return results


def validate_pipeline(**context):
    """Validate all Delta tables exist and have expected row counts."""
    logging.info("Validating pipeline output")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = SparkSession.builder \
        .appName("hospital-validate") \
        .master("local[*]") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    tables = {
        "bronze/patients"        : 55500,
        "bronze/admissions"      : 55500,
        "silver/patients"        : 55500,
        "silver/admissions"      : 55500,
        "gold/admission_summary" : 18,
        "gold/hospital_revenue"  : 20,
        "gold/doctor_performance": 20,
        "gold/disease_trends"    : 24,
        "gold/insurance_analysis": 5,
    }

    all_valid = True
    for path, expected in tables.items():
        actual = spark.read.format("delta") \
            .load(f"{DELTA_BASE}/{path}").count()
        status = "✅" if actual == expected else "⚠️"
        logging.info(f"{status} {path}: {actual} rows (expected {expected})")
        if actual != expected:
            all_valid = False

    spark.stop()

    if not all_valid:
        raise ValueError("Pipeline validation failed — row counts don't match")

    logging.info("All tables validated successfully")
    return "Pipeline validated"


# ── DAG DEFINITION ────────────────────────────────────────
with DAG(
    dag_id="hospital_lakehouse_pipeline",
    default_args=default_args,
    description="Hospital Operations Lakehouse — Bronze → Silver → Gold",
    schedule_interval="0 0 * * *",   # midnight every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "lakehouse", "pyspark", "delta"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze,
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=run_gold,
    )

    validate = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline,
    )

    end = EmptyOperator(task_id="end")

    # Dependency chain — each task waits for the previous to succeed
    start >> bronze >> silver >> gold >> validate >> end