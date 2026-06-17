# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE - ECOMMERCE RAW LOAD
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Load raw Olist CSV files into Bronze Delta tables as-is  
# MAGIC **Output:** acip.bronze.orders, acip.bronze.order_items,  
# MAGIC             acip.bronze.order_payments, acip.bronze.customers,  
# MAGIC             acip.bronze.products, acip.bronze.order_reviews,   
# MAGIC             acip.bronze.category_translations

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/raw_files/olist"

print(f"Source: {VOLUME_PATH}")
print(f"Target catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Load and Write Orders
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
    .saveAsTable(f"{CATALOG}.{SCHEMA}.orders")

print(f"PASS: {CATALOG}.{SCHEMA}.orders written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Order Items
print("Loading olist_order_items_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_order_items_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.order_items")

print(f"PASS: {CATALOG}.{SCHEMA}.order_items written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Order Payments
print("Loading olist_order_payments_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.order_payments")

print(f"PASS: {CATALOG}.{SCHEMA}.order_payments written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Customers
print("Loading olist_customers_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_customers_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.customers")

print(f"PASS: {CATALOG}.{SCHEMA}.customers written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Products
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
    .saveAsTable(f"{CATALOG}.{SCHEMA}.products")

print(f"PASS: {CATALOG}.{SCHEMA}.products written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Order Reviews
print("Loading olist_order_reviews_dataset.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.order_reviews")

print(f"PASS: {CATALOG}.{SCHEMA}.order_reviews written - {row_count} rows")

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

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.sellers")

print(f"PASS: {CATALOG}.{SCHEMA}.sellers written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Load and Write Category Translations
print("Loading product_category_name_translation.csv...")

df = spark.read.csv(
    f"{VOLUME_PATH}/product_category_name_translation.csv",
    header=True,
    inferSchema=False
)

row_count = df.count()
print(f"Rows: {row_count}")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.category_translations")

print(f"PASS: {CATALOG}.{SCHEMA}.category_translations written - {row_count} rows")

# COMMAND ----------

# DBTITLE 1,Verify All Bronze Tables
print("Verifying all bronze ecommerce tables...")
print("=" * 60)

tables = [
    "orders", "order_items", "order_payments",
    "customers", "products", "order_reviews",
    "sellers", "category_translations"
]

total_rows = 0
for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    total_rows += count
    print(f"  {CATALOG}.{SCHEMA}.{table}: {count} rows")

print(f"\nTotal rows across all tables: {total_rows}")
print("PASS: All ecommerce bronze tables verified")
