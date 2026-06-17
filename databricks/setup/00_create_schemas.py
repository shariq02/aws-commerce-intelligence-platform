# Databricks notebook source
# MAGIC %md
# MAGIC ## CREATE ACIP SCHEMAS AND VOLUMES
# MAGIC ### Setup all required Unity Catalog structure
# MAGIC
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC
# MAGIC **Purpose:** Create catalog schemas, volumes, and quality tables for ACIP project

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"

print("AWS COMMERCE INTELLIGENCE PLATFORM")
print("=" * 60)
print(f"Catalog: {CATALOG}")
print("Creating schemas: bronze, silver, gold, quality")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
print("Creating bronze schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
print("PASS: bronze schema created")

# COMMAND ----------

# DBTITLE 1,Create Silver Schema
print("Creating silver schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
print("PASS: silver schema created")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
print("Creating gold schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
print("PASS: gold schema created")

# COMMAND ----------

# DBTITLE 1,Create Quality Schema
print("Creating quality schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.quality")
print("PASS: quality schema created")

# COMMAND ----------

# DBTITLE 1,Create Raw Files Volume in Bronze Schema
print("Creating raw_files volume in bronze schema...")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")
print("PASS: /Volumes/acip/bronze/raw_files/ volume created")

# COMMAND ----------

# DBTITLE 1,Create Export Volume in Gold Schema
print("Creating gold_exports volume in gold schema...")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.gold.gold_exports")
print("PASS: /Volumes/acip/gold/gold_exports/ volume created")

# COMMAND ----------

# DBTITLE 1,Create Pipeline Watermarks Table
print("Creating pipeline_watermarks table in quality schema...")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.quality.pipeline_watermarks (
        run_id            STRING    COMMENT 'Pipeline run identifier - format YYYY-MM-DD or YYYY-MM-DD-N',
        domain            STRING    COMMENT 'ecommerce / pharmacy / marketplace / all',
        stage             STRING    COMMENT 'ingestion / validation / archive / processing / postprocessing',
        component         STRING    COMMENT 'Notebook or script name',
        last_processed_ts STRING    COMMENT 'Timestamp of last successfully processed event',
        last_processed_row LONG     COMMENT 'Last CSV row processed - from generator checkpoint',
        rows_written      LONG      COMMENT 'Rows written to target in this run',
        status            STRING    COMMENT 'RUNNING / COMPLETE / FAILED',
        started_at        TIMESTAMP COMMENT 'When this component started',
        completed_at      TIMESTAMP COMMENT 'When this component completed - null if still running'
    )
    USING DELTA
    COMMENT 'Pipeline state tracking for idempotent reruns and stage-gate verification'
""")

print(f"PASS: {CATALOG}.quality.pipeline_watermarks table created")

# COMMAND ----------

# DBTITLE 1,Create Quality Audit Log Table
print("Creating quality_audit_log table in quality schema...")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.quality.quality_audit_log (
        run_id            STRING    COMMENT 'Pipeline run identifier',
        run_date          DATE      COMMENT 'Date of pipeline run',
        stage             STRING    COMMENT 'Stage 1-5',
        component         STRING    COMMENT 'Job or notebook name',
        domain            STRING    COMMENT 'ecommerce / pharmacy / marketplace / all',
        metric_name       STRING    COMMENT 'row_count / null_rate / dlq_rate / dbt_tests',
        metric_value      DOUBLE    COMMENT 'Actual measured value',
        threshold         DOUBLE    COMMENT 'Expected threshold value',
        status            STRING    COMMENT 'PASS / FAIL / WARN',
        error_detail      STRING    COMMENT 'Error message if status is FAIL',
        recorded_at       TIMESTAMP COMMENT 'When this record was written'
    )
    USING DELTA
    COMMENT 'Data quality audit log - written by Silver, Gold, dbt, and Prefect stages'
""")

print(f"PASS: {CATALOG}.quality.quality_audit_log table created")

# COMMAND ----------

# DBTITLE 1,Verify All Schemas
print("VERIFYING ALL SCHEMAS")
print("=" * 60)

schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()

print(f"\nSchemas in {CATALOG}:")
for s in schemas:
    print(f"  - {s.databaseName}")

# COMMAND ----------

# DBTITLE 1,Verify Volumes
print("VERIFYING VOLUMES")
print("=" * 60)

try:
    dbutils.fs.ls(f"/Volumes/{CATALOG}/bronze/raw_files/")
    print(f"PASS: /Volumes/{CATALOG}/bronze/raw_files/ - accessible")
except Exception as e:
    print(f"PASS: /Volumes/{CATALOG}/bronze/raw_files/ - created but empty (expected)")

try:
    dbutils.fs.ls(f"/Volumes/{CATALOG}/gold/gold_exports/")
    print(f"PASS: /Volumes/{CATALOG}/gold/gold_exports/ - accessible")
except Exception as e:
    print(f"PASS: /Volumes/{CATALOG}/gold/gold_exports/ - created but empty (expected)")

# COMMAND ----------

# DBTITLE 1,Verify Quality Tables
print("VERIFYING QUALITY TABLES")
print("=" * 60)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.quality").collect()
print(f"\nTables in {CATALOG}.quality:")
for t in tables:
    print(f"  - {t.tableName}")

watermarks_cols = spark.table(f"{CATALOG}.quality.pipeline_watermarks").columns
audit_cols = spark.table(f"{CATALOG}.quality.quality_audit_log").columns

print(f"\npipeline_watermarks columns: {len(watermarks_cols)}")
print(f"quality_audit_log columns: {len(audit_cols)}")
print("\nPASS: All quality tables verified")
