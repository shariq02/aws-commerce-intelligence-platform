# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - DIM GEOGRAPHY
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Build geography dimension from ecommerce and marketplace payloads  
# MAGIC **Input:** acip.silver.events (ecommerce domain)  
# MAGIC **Output:** acip.gold.dim_geography

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.dim_geography"

print("GOLD - DIM GEOGRAPHY")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Geography from Silver Events
print("STEP 1: PARSE GEOGRAPHY FROM SILVER EVENTS")
print("=" * 70)

payload_schema = StructType([
    StructField("region", StringType(), True),
    StructField("state_region", StringType(), True),
    StructField("customer_state", StringType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

ecommerce_events = silver.filter(F.col("domain") == "ecommerce") \
    .withColumn("payload_parsed", F.from_json(F.col("payload"), payload_schema)) \
    .select(
        F.col("payload_parsed.region").alias("region"),
        F.col("payload_parsed.state_region").alias("state_region"),
        F.col("payload_parsed.customer_state").alias("state")
    ) \
    .filter(F.col("region").isNotNull() & (F.trim(F.col("region")) != ""))

print(f"Ecommerce geography records: {ecommerce_events.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Deduplicate and Build Dimension
print("STEP 2: DEDUPLICATE AND BUILD DIMENSION")
print("=" * 70)

geo_df = ecommerce_events \
    .withColumn("city",
        F.trim(F.split(F.col("region"), "-").getItem(0))
    ) \
    .withColumn("state",
        F.trim(F.split(F.col("region"), "-").getItem(1))
    ) \
    .withColumn("country", F.lit("Brazil")) \
    .select("region", "city", "state", "state_region", "country") \
    .dropDuplicates(["region"]) \
    .filter(F.col("state_region").isNotNull())

geo_df = geo_df.withColumn(
    "geo_key",
    F.abs(F.hash(F.col("region"))).cast("long")
)

total = geo_df.count()
print(f"Unique geographies: {total:,}")

region_dist = geo_df.groupBy("state_region").count().collect()
print("\nState region distribution:")
for row in region_dist:
    pct = row["count"] / total * 100
    print(f"  {row['state_region']}: {row['count']:,} ({pct:.1f}%)")

display(geo_df.limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 3: Write Gold Table
print("STEP 3: WRITE GOLD TABLE")
print("=" * 70)

geo_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 4: Verify
print("STEP 4: VERIFY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
null_geo_key = df.filter(F.col("geo_key").isNull()).count()
null_region = df.filter(F.col("region").isNull()).count()

print(f"Total rows: {df.count():,}")
print(f"Null geo_key: {null_geo_key}")
print(f"Null region: {null_region}")
print(f"\nPASS: dim_geography verified" if null_geo_key == 0 else "\nFAIL: null geo_keys found")
