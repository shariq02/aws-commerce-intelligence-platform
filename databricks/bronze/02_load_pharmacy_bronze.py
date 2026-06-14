# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE - PHARMACY RAW LOAD
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Load raw Pharma Sales CSV files into Bronze Delta tables as-is
# MAGIC **Output:** acip.bronze.pharma_sales_hourly, acip.bronze.pharma_sales_daily,
# MAGIC             acip.bronze.pharma_sales_weekly, acip.bronze.pharma_sales_monthly

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files/pharma"

print(f"Source: {VOLUME_PATH}")
print(f"Target catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Load and Write Hourly Sales
print("Loading saleshourly.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/saleshourly.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
col_count = len(df.columns)
print(f"Rows: {row_count}, Columns: {col_count}")
print(f"Columns: {df.columns}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.pharma_sales_hourly")

print(f"PASS: {CATALOG}.{SCHEMA}.pharma_sales_hourly written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Daily Sales
print("Loading salesdaily.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/salesdaily.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.pharma_sales_daily")

print(f"PASS: {CATALOG}.{SCHEMA}.pharma_sales_daily written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Weekly Sales
print("Loading salesweekly.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/salesweekly.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.pharma_sales_weekly")

print(f"PASS: {CATALOG}.{SCHEMA}.pharma_sales_weekly written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Monthly Sales
print("Loading salesmonthly.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/salesmonthly.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.pharma_sales_monthly")

print(f"PASS: {CATALOG}.{SCHEMA}.pharma_sales_monthly written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Verify All Bronze Tables
print("Verifying all bronze pharmacy tables...")
print("=" * 60)

tables = [
    "pharma_sales_hourly",
    "pharma_sales_daily",
    "pharma_sales_weekly",
    "pharma_sales_monthly"
]

total_rows = 0
for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    total_rows += count
    print(f"  {CATALOG}.{SCHEMA}.{table}: {count} rows")

print(f"\nTotal rows across all tables: {total_rows}")
print("PASS: All pharmacy bronze tables verified")
