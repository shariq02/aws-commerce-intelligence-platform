# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - FACT SELLER PERFORMANCE
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Build marketplace seller performance fact table
# MAGIC **Input:** acip.silver.events (marketplace), acip.gold.dim_seller,
# MAGIC            acip.gold.dim_date, acip.gold.dim_geography
# MAGIC **Output:** acip.gold.fact_seller_performance
# MAGIC
# MAGIC **Fixes applied (June 2026):**
# MAGIC   1. performance_key: F.abs(F.hash(event_id)) had 10 hash collisions.
# MAGIC      Fixed to monotonically_increasing_id() which guarantees uniqueness.
# MAGIC   2. Streaming dispatch events were missing price, seller_tier,
# MAGIC      dispatch_time_days, freight_value, category, product_id from payload.
# MAGIC      Fixed in marketplace_generator.py -- all fields now present in payload.
# MAGIC   3. Added dispatch_time_days to payload_schema to pick up generator fix.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType
)

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.fact_seller_performance"

print("GOLD - FACT SELLER PERFORMANCE")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Marketplace Events from Silver
print("STEP 1: PARSE MARKETPLACE EVENTS FROM SILVER")
print("=" * 70)

payload_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_tier", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("seller_region", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("category_group", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("dispatch_time_mins", DoubleType(), True),
    # FIX: dispatch_time_days added -- was missing from original schema
    # Generator fix now includes this field in seller.order.dispatched payload
    StructField("dispatch_time_days", DoubleType(), True),
    StructField("sla_threshold_mins", IntegerType(), True),
    StructField("is_sla_breached", BooleanType(), True),
    StructField("dispatch_speed_bucket", StringType(), True),
    StructField("old_price", DoubleType(), True),
    StructField("new_price", DoubleType(), True),
    StructField("change_pct", DoubleType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

marketplace = silver.filter(F.col("domain") == "marketplace") \
    .withColumn("p", F.from_json(F.col("payload"), payload_schema)) \
    .select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("correlation_id"),
        F.col("occurred_at"),
        F.to_date(F.col("occurred_at")).alias("event_date"),
        F.col("p.seller_id").alias("seller_id"),
        F.col("p.seller_tier").alias("seller_tier"),
        F.col("p.seller_state").alias("seller_state"),
        F.col("p.seller_region").alias("seller_region"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.category").alias("category"),
        F.col("p.category_group").alias("category_group"),
        F.col("p.price").alias("price"),
        F.col("p.freight_value").alias("freight_value"),
        F.col("p.dispatch_time_mins").alias("dispatch_time_mins"),
        # FIX: derive dispatch_time_days from payload if present,
        # otherwise calculate from dispatch_time_mins
        F.coalesce(
            F.col("p.dispatch_time_days"),
            F.when(
                F.col("p.dispatch_time_mins").isNotNull(),
                F.round(F.col("p.dispatch_time_mins") / 1440.0, 4)
            )
        ).alias("dispatch_time_days"),
        F.col("p.sla_threshold_mins").alias("sla_threshold_mins"),
        F.col("p.is_sla_breached").alias("is_sla_breached"),
        F.col("p.dispatch_speed_bucket").alias("dispatch_speed_bucket"),
        F.col("p.old_price").alias("old_price"),
        F.col("p.new_price").alias("new_price"),
        F.col("p.change_pct").alias("change_pct"),
    ).filter(F.col("seller_id").isNotNull())

total = marketplace.count()
print(f"Marketplace events parsed: {total:,}")

event_dist = marketplace.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_dist:
    pct = row["count"] / total * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

# Check null price on dispatch events before proceeding
dispatch_total = marketplace.filter(F.col("event_type") == "seller.order.dispatched").count()
null_price_dispatch = marketplace.filter(
    (F.col("event_type") == "seller.order.dispatched") &
    F.col("price").isNull()
).count()
print(f"\nNull price on dispatch events: {null_price_dispatch:,} of {dispatch_total:,}")
if null_price_dispatch > 0:
    pct = null_price_dispatch / max(dispatch_total, 1) * 100
    print(f"  NOTE: {pct:.1f}% null price -- check marketplace_generator.py fix was applied")

# COMMAND ----------

# DBTITLE 1,STEP 2: Load Dimensions
print("STEP 2: LOAD DIMENSIONS")
print("=" * 70)

dim_seller = spark.table(f"{CATALOG}.gold.dim_seller") \
    .filter(F.col("is_current")) \
    .select("seller_key", "seller_id")

dim_date = spark.table(f"{CATALOG}.gold.dim_date") \
    .select("date_key", F.col("full_date").alias("event_date"))

print(f"dim_seller (current): {dim_seller.count():,}")
print(f"dim_date: {dim_date.count():,}")

date_range = dim_date.select(
    F.min("event_date").alias("min"),
    F.max("event_date").alias("max")
).collect()[0]
print(f"dim_date range: {date_range['min']} to {date_range['max']}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Join to Dimensions
print("STEP 3: JOIN TO DIMENSIONS")
print("=" * 70)

fact = marketplace \
    .join(dim_seller, on="seller_id", how="left") \
    .join(dim_date, on="event_date", how="left")

total = fact.count()
null_seller_key = fact.filter(F.col("seller_key").isNull()).count()
null_date_key = fact.filter(F.col("date_key").isNull()).count()

print(f"Fact rows: {total:,}")
print(f"Null seller_key: {null_seller_key:,} ({null_seller_key/total*100:.1f}%)")
print(f"Null date_key: {null_date_key:,} ({null_date_key/total*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Generate Surrogate Key and Select Final Columns
print("STEP 4: GENERATE SURROGATE KEY")
print("=" * 70)

# FIX: monotonically_increasing_id() guarantees uniqueness
# F.abs(F.hash(event_id)) had 10 hash collisions causing duplicate performance_keys
fact = fact.withColumn(
    "performance_key",
    F.monotonically_increasing_id()
).select(
    "performance_key",
    "event_id",
    "event_type",
    "correlation_id",
    "seller_key",
    "date_key",
    "seller_tier",
    "seller_state",
    "seller_region",
    "product_id",
    "category",
    "category_group",
    "price",
    "freight_value",
    "dispatch_time_mins",
    "dispatch_time_days",
    "sla_threshold_mins",
    "is_sla_breached",
    "dispatch_speed_bucket",
    "old_price",
    "new_price",
    "change_pct",
    "occurred_at"
)

dup_keys = fact.groupBy("performance_key").count().filter(F.col("count") > 1).count()
print(f"Duplicate performance_key: {dup_keys} (expected 0)")

# COMMAND ----------

# DBTITLE 1,STEP 5: Write Gold Table
print("STEP 5: WRITE GOLD TABLE")
print("=" * 70)

fact.write \
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

dispatch_df = df.filter(F.col("event_type") == "seller.order.dispatched")
dispatch_total = dispatch_df.count()

checks = {
    "null_performance_key":           df.filter(F.col("performance_key").isNull()).count(),
    "dup_performance_key":            df.groupBy("performance_key").count().filter(F.col("count") > 1).count(),
    "null_price_on_dispatch":         dispatch_df.filter(F.col("price").isNull()).count(),
    "null_seller_tier_on_dispatch":   dispatch_df.filter(F.col("seller_tier").isNull()).count(),
    "null_dispatch_time_days":        dispatch_df.filter(F.col("dispatch_time_days").isNull()).count(),
    "null_is_sla_breached":           dispatch_df.filter(F.col("is_sla_breached").isNull()).count(),
}

print(f"Total rows: {total:,}")
print(f"Dispatch events: {dispatch_total:,}")
for check_name, count in checks.items():
    status = "PASS" if count == 0 else "FAIL"
    print(f"  {status} {check_name}: {count:,}")

sla_breach_count = dispatch_df.filter(F.col("is_sla_breached") == True).count()
print(f"\nSLA breach rate: {sla_breach_count:,} of {dispatch_total:,} ({sla_breach_count/max(dispatch_total,1)*100:.1f}%)")

speed_dist = df.filter(
    F.col("dispatch_speed_bucket").isNotNull()
).groupBy("dispatch_speed_bucket").count().orderBy("dispatch_speed_bucket").collect()
print("\nDispatch speed distribution:")
for row in speed_dist:
    print(f"  {row['dispatch_speed_bucket']}: {row['count']:,}")

tier_dist = df.filter(
    F.col("seller_tier").isNotNull()
).groupBy("seller_tier").count().orderBy(F.col("count").desc()).collect()
print("\nSeller tier distribution:")
for row in tier_dist:
    print(f"  {row['seller_tier']}: {row['count']:,}")

status = "PASS" if checks["null_performance_key"] == 0 and checks["dup_performance_key"] == 0 else "FAIL"
print(f"\n{status}: fact_seller_performance verified")

display(df.select(
    "performance_key", "event_type", "seller_key",
    "seller_tier", "dispatch_time_days",
    "is_sla_breached", "dispatch_speed_bucket", "price"
).limit(5))
