# Databricks notebook source
# MAGIC %md
# MAGIC ## CREATE ACIP SCHEMAS AND VOLUMES
# MAGIC ### Setup all required Unity Catalog structure
# MAGIC
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC
# MAGIC **Purpose:** Create catalog schemas and volumes for ACIP project
# MAGIC
# MAGIC **Run Order:** STEP 0 - Run this FIRST before any processing
# MAGIC ```
# MAGIC 0. 00_create_schemas.py          (THIS SCRIPT - RUN FIRST)
# MAGIC 1. 00_normalise_ecommerce_csv.py
# MAGIC 2. 00_normalise_pharmacy_csv.py
# MAGIC 3. 00_normalise_marketplace_csv.py
# MAGIC 4. 01_load_ecommerce_bronze.py
# MAGIC 5. 02_load_pharmacy_bronze.py
# MAGIC 6. 03_load_marketplace_bronze.py
# MAGIC 7. 04_ecommerce_silver_transform.py
# MAGIC 8. 05_pharmacy_silver_transform.py
# MAGIC 9. 06_marketplace_silver_transform.py
# MAGIC ```

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
    files = dbutils.fs.ls(f"/Volumes/{CATALOG}/bronze/raw_files/")
    print(f"/Volumes/{CATALOG}/bronze/raw_files/ - accessible")
except Exception as e:
    print(f"/Volumes/{CATALOG}/bronze/raw_files/ - created but empty (expected)")

try:
    files = dbutils.fs.ls(f"/Volumes/{CATALOG}/gold/gold_exports/")
    print(f"/Volumes/{CATALOG}/gold/gold_exports/ - accessible")
except Exception as e:
    print(f"/Volumes/{CATALOG}/gold/gold_exports/ - created but empty (expected)")

# COMMAND ----------

# DBTITLE 1,Final Summary
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"\nCatalog: {CATALOG}")
print("\nSchemas created:")
print(f"  {CATALOG}.bronze   - Raw event data (Bronze Delta tables)")
print(f"  {CATALOG}.silver   - Cleaned typed data (Silver Delta tables)")
print(f"  {CATALOG}.gold     - Dimensional model (Gold Delta tables)")
print(f"  {CATALOG}.quality  - Data quality audit log")
print("\nVolumes created:")
print(f"  /Volumes/{CATALOG}/bronze/raw_files/   - Upload CSV files here")
print(f"  /Volumes/{CATALOG}/gold/gold_exports/  - Gold export files land here")
print("\nNEXT STEP: Upload CSV files to /Volumes/acip/bronze/raw_files/")
print("  - data/raw/olist/*.csv")
print("  - data/raw/pharma/*.csv")
