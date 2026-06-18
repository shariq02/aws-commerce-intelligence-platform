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

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, LongType
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
        F.col("p.dispatch_time_days").alias("dispatch_time_days"),
        F.col("p.sla_threshold_mins").alias("sla_threshold_mins"),
        F.col("p.is_sla_breached").alias("is_sla_breached"),
        F.col("p.dispatch_speed_bucket").alias("dispatch_speed_bucket"),
        F.col("p.old_price").alias("old_price"),
        F.col("p.new_price").alias("new_price"),
        F.col("p.change_pct").alias("change_pct"),
    ).filter(F.col("seller_id").isNotNull())

print(f"Marketplace events parsed: {marketplace.count():,}")

event_dist = marketplace.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_dist:
    pct = row["count"] / marketplace.count() * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

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

# COMMAND ----------

# DBTITLE 1,STEP 3: Join to Dimensions
print("STEP 3: JOIN TO DIMENSIONS")
print("=" * 70)

fact = marketplace \
    .join(dim_seller, on="seller_id", how="left") \
    .join(dim_date, on="event_date", how="left") \
    .withColumn(
        "performance_key",
        F.abs(F.hash(F.col("event_id"))).cast("long")
    ) \
    .select(
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

total = fact.count()
null_seller_key = fact.filter(F.col("seller_key").isNull()).count()
print(f"Fact rows: {total:,}")
print(f"Null seller_key: {null_seller_key:,} ({null_seller_key/total*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Write Gold Table
print("STEP 4: WRITE GOLD TABLE")
print("=" * 70)

fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 5: Verify
print("STEP 5: VERIFY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()

null_perf_key = df.filter(F.col("performance_key").isNull()).count()
sla_breach_count = df.filter(
    F.col("is_sla_breached") == True
).count()
dispatch_events = df.filter(F.col("event_type") == "seller.order.dispatched").count()

print(f"Total rows: {total:,}")
print(f"Null performance_key: {null_perf_key}")
print(f"SLA breached: {sla_breach_count:,} of {dispatch_events:,} dispatches ({sla_breach_count/max(dispatch_events,1)*100:.1f}%)")

speed_dist = df.filter(
    F.col("dispatch_speed_bucket").isNotNull()
).groupBy("dispatch_speed_bucket").count().collect()
print("\nDispatch speed distribution:")
for row in speed_dist:
    print(f"  {row['dispatch_speed_bucket']}: {row['count']:,}")

print(f"\nPASS: fact_seller_performance verified" if null_perf_key == 0 else "\nFAIL")

display(df.select(
    "performance_key", "event_type", "seller_key",
    "seller_tier", "dispatch_time_days",
    "is_sla_breached", "dispatch_speed_bucket"
).limit(5))
