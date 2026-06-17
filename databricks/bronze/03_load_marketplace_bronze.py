# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE - MARKETPLACE RAW LOAD
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Load raw Olist seller CSV files into Bronze Delta tables as-is  
# MAGIC **Note:** Marketplace domain uses same Olist source files as ecommerce
# MAGIC           but creates separate Bronze tables for domain separation.  
# MAGIC **Output:** acip.bronze.marketplace_sellers,
# MAGIC             acip.bronze.marketplace_order_items,
# MAGIC             acip.bronze.marketplace_products, 
# MAGIC             acip.bronze.marketplace_geolocations 

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files/olist"

print(f"Source: {VOLUME_PATH}")
print(f"Target catalog: {CATALOG}.{SCHEMA}")
print("Note: Marketplace reuses Olist source files with domain-specific table names")

# COMMAND ----------

# DBTITLE 1,Load and Write Sellers
print("Loading olist_sellers_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_sellers_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")
print(f"Columns: {df.columns}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.marketplace_sellers")

print(f"PASS: {CATALOG}.{SCHEMA}.marketplace_sellers written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Order Items (Marketplace View)
print("Loading olist_order_items_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_order_items_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")
print(f"Columns: {df.columns}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.marketplace_order_items")

print(f"PASS: {CATALOG}.{SCHEMA}.marketplace_order_items written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Products (Marketplace View)
print("Loading olist_products_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_products_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.marketplace_products")

print(f"PASS: {CATALOG}.{SCHEMA}.marketplace_products written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Geolocation
print("Loading olist_geolocation_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_geolocation_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")
print(f"Columns: {df.columns}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.marketplace_geolocations")

print(f"PASS: {CATALOG}.{SCHEMA}.marketplace_geolocations written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Orders (Marketplace View)
print("Loading olist_orders_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_orders_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.marketplace_orders")

print(f"PASS: {CATALOG}.{SCHEMA}.marketplace_orders written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Verify All Bronze Tables
print("Verifying all bronze marketplace tables...")
print("=" * 60)

tables = [
    "marketplace_sellers",
    "marketplace_order_items",
    "marketplace_products",
    "marketplace_geolocations",
    "marketplace_orders"
]

total_rows = 0
for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    total_rows += count
    print(f"  {CATALOG}.{SCHEMA}.{table}: {count} rows")

print(f"\nTotal rows across all tables: {total_rows}")
print("PASS: All marketplace bronze tables verified")
