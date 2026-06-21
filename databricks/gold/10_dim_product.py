# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - DIM PRODUCT
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Build product dimension from pharmacy and marketplace payloads  
# MAGIC **Input:** acip.silver.events (pharmacy and marketplace domains)  
# MAGIC **Output:** acip.gold.dim_product (SCD1 - overwrite on change)
# MAGIC
# MAGIC **Fix applied (June 2026):**
# MAGIC   product_key was generated using F.abs(F.hash(product_id)) which produced
# MAGIC   1 hash collision (duplicate primary key). Fixed to use
# MAGIC   monotonically_increasing_id() which guarantees uniqueness.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.dim_product"

print("GOLD - DIM PRODUCT")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Products from Pharmacy Events
print("STEP 1: PARSE PRODUCTS FROM PHARMACY EVENTS")
print("=" * 70)

pharma_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("category_group", StringType(), True),
    StructField("atc_code", StringType(), True),
    StructField("drug_class", StringType(), True),
    StructField("is_prescription", BooleanType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

pharma_products = silver.filter(F.col("domain") == "pharmacy") \
    .withColumn("payload_parsed", F.from_json(F.col("payload"), pharma_schema)) \
    .select(
        F.col("payload_parsed.product_id").alias("product_id"),
        F.col("payload_parsed.category").alias("category"),
        F.col("payload_parsed.category_group").alias("category_group"),
        F.col("payload_parsed.atc_code").alias("atc_code"),
        F.col("payload_parsed.drug_class").alias("drug_class"),
        F.col("payload_parsed.is_prescription").alias("is_prescription"),
        F.lit("pharmacy").alias("domain")
    ) \
    .filter(F.col("product_id").isNotNull()) \
    .dropDuplicates(["product_id"])

print(f"Pharmacy products: {pharma_products.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Parse Products from Marketplace Events
print("STEP 2: PARSE PRODUCTS FROM MARKETPLACE EVENTS")
print("=" * 70)

market_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("category_group", StringType(), True),
])

market_products = silver.filter(
    F.col("domain") == "marketplace"
).filter(
    F.col("event_type") == "seller.order.dispatched"
).withColumn("payload_parsed", F.from_json(F.col("payload"), market_schema)) \
 .select(
    F.col("payload_parsed.product_id").alias("product_id"),
    F.col("payload_parsed.category").alias("category"),
    F.col("payload_parsed.category_group").alias("category_group"),
    F.lit(None).cast("string").alias("atc_code"),
    F.lit(None).cast("string").alias("drug_class"),
    F.lit(False).alias("is_prescription"),
    F.lit("marketplace").alias("domain")
).filter(F.col("product_id").isNotNull()) \
 .dropDuplicates(["product_id"])

print(f"Marketplace products: {market_products.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Union and Deduplicate
print("STEP 3: UNION AND DEDUPLICATE")
print("=" * 70)

all_products = pharma_products.union(market_products) \
    .dropDuplicates(["product_id"])

total = all_products.count()
print(f"Total unique products before key generation: {total:,}")

domain_dist = all_products.groupBy("domain").count().collect()
print("\nProducts per domain:")
for row in domain_dist:
    print(f"  {row['domain']}: {row['count']:,}")

# COMMAND ----------

# DBTITLE 1,STEP 4: Generate Surrogate Key
print("STEP 4: GENERATE SURROGATE KEY")
print("=" * 70)

# FIX: monotonically_increasing_id() guarantees uniqueness
# F.abs(F.hash(product_id)) had 1 hash collision causing duplicate product_key
# monotonically_increasing_id() produces unique 64-bit integers, no collisions possible
# Note: values are not sequential but are guaranteed unique within the dataset

all_products = all_products.withColumn(
    "product_key",
    F.monotonically_increasing_id()
)

# Verify no duplicates
dup_count = all_products.groupBy("product_key").count().filter(F.col("count") > 1).count()
print(f"Duplicate product_key count: {dup_count} (expected 0)")

display(all_products.limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 5: Write Gold Table
print("STEP 5: WRITE GOLD TABLE")
print("=" * 70)

all_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 6: Verify
print("STEP 6: VERIFY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()
null_product_key = df.filter(F.col("product_key").isNull()).count()
null_product_id = df.filter(F.col("product_id").isNull()).count()
dup_keys = df.groupBy("product_key").count().filter(F.col("count") > 1).count()

print(f"Total rows: {total:,}")
print(f"Null product_key: {null_product_key}")
print(f"Null product_id: {null_product_id}")
print(f"Duplicate product_key: {dup_keys}")

status = "PASS" if null_product_key == 0 and dup_keys == 0 else "FAIL"
print(f"\n{status}: dim_product verified")
