# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - FACT INVENTORY SNAPSHOTS
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Build pharmacy inventory fact table joining to dim_product and dim_date
# MAGIC **Input:** acip.silver.events (pharmacy), acip.gold.dim_product, acip.gold.dim_date
# MAGIC **Output:** acip.gold.fact_inventory_snapshots
# MAGIC
# MAGIC **Fixes applied (June 2026):**
# MAGIC   1. occurred_at format M/D/YYYY THH (e.g. 1/5/2014T08) was not parseable
# MAGIC      by TRY_CAST AS DATE. Now fixed in notebook 05 to ISO 8601 format.
# MAGIC      This notebook now uses to_date(occurred_at) directly which works
# MAGIC      for both ISO 8601 batch events and streaming events.
# MAGIC   2. 3,515 streaming events had null stock_level and reorder_threshold.
# MAGIC      Added defaults: stock_level=100, reorder_threshold=50.
# MAGIC   3. 70 streaming events had null is_prescription.
# MAGIC      Added default: is_prescription=False for unknown events.

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
TARGET_TABLE = f"{CATALOG}.gold.fact_inventory_snapshots"

# Defaults for streaming events with missing payload fields
DEFAULT_STOCK_LEVEL      = 100
DEFAULT_REORDER_THRESHOLD = 50
DEFAULT_IS_PRESCRIPTION  = False

print("GOLD - FACT INVENTORY SNAPSHOTS")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")
print(f"Defaults: stock_level={DEFAULT_STOCK_LEVEL}, reorder_threshold={DEFAULT_REORDER_THRESHOLD}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Pharmacy Events from Silver
print("STEP 1: PARSE PHARMACY EVENTS FROM SILVER")
print("=" * 70)

payload_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("category_group", StringType(), True),
    StructField("atc_code", StringType(), True),
    StructField("drug_class", StringType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("is_prescription", BooleanType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("weekday", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("time_of_day", StringType(), True),
    StructField("is_peak_hour", BooleanType(), True),
    StructField("stock_level", IntegerType(), True),
    StructField("reorder_threshold", IntegerType(), True),
    StructField("fill_time_mins", IntegerType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

pharmacy = silver.filter(F.col("domain") == "pharmacy") \
    .withColumn("p", F.from_json(F.col("payload"), payload_schema)) \
    .select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("correlation_id"),
        F.col("occurred_at"),
        # FIX: occurred_at is now ISO 8601 from notebook 05 fix
        # to_date() handles both "2014-01-05T08:00:00" and "2026-06-18T11:21:53"
        F.to_date(F.col("occurred_at")).alias("event_date"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.category").alias("category"),
        F.col("p.quantity").alias("quantity"),
        # FIX: default null is_prescription to False for streaming events
        F.coalesce(
            F.col("p.is_prescription"),
            F.lit(DEFAULT_IS_PRESCRIPTION)
        ).alias("is_prescription"),
        # FIX: default null stock_level to DEFAULT_STOCK_LEVEL for streaming events
        F.coalesce(
            F.col("p.stock_level"),
            F.lit(DEFAULT_STOCK_LEVEL)
        ).alias("stock_level"),
        # FIX: default null reorder_threshold to DEFAULT_REORDER_THRESHOLD
        F.coalesce(
            F.col("p.reorder_threshold"),
            F.lit(DEFAULT_REORDER_THRESHOLD)
        ).alias("reorder_threshold"),
        F.col("p.fill_time_mins").alias("fill_time_mins"),
        F.col("p.time_of_day").alias("time_of_day"),
        F.col("p.is_peak_hour").alias("is_peak_hour"),
        F.col("p.is_weekend").alias("is_weekend"),
        F.col("p.hour").alias("hour_of_day"),
    ).filter(F.col("product_id").isNotNull()) \
     .withColumn("days_of_supply",
        F.when(
            F.col("quantity").isNotNull() & (F.col("quantity") > 0),
            F.round(F.col("stock_level") / F.col("quantity"), 1)
        ).otherwise(F.lit(None))
    ) \
     .withColumn("stock_alert_level",
        F.when(F.col("stock_level") <= F.col("reorder_threshold") * 0.5, "critical")
         .when(F.col("stock_level") <= F.col("reorder_threshold"), "high")
         .when(F.col("stock_level") <= F.col("reorder_threshold") * 2, "medium")
         .otherwise("normal")
    )

total = pharmacy.count()
print(f"Pharmacy events parsed: {total:,}")

# Verify occurred_at is now parseable
null_event_date = pharmacy.filter(F.col("event_date").isNull()).count()
print(f"Null event_date (unparseable occurred_at): {null_event_date:,} (expected 0 or near 0)")

alert_dist = pharmacy.groupBy("stock_alert_level").count().collect()
print("\nStock alert level distribution:")
for row in alert_dist:
    pct = row["count"] / total * 100
    print(f"  {row['stock_alert_level']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 2: Load Dimensions
print("STEP 2: LOAD DIMENSIONS")
print("=" * 70)

dim_product = spark.table(f"{CATALOG}.gold.dim_product") \
    .filter(F.col("domain") == "pharmacy") \
    .select("product_key", "product_id")

dim_date = spark.table(f"{CATALOG}.gold.dim_date") \
    .select("date_key", F.col("full_date").alias("event_date"))

print(f"dim_product (pharmacy): {dim_product.count():,}")
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

fact = pharmacy \
    .join(dim_product, on="product_id", how="left") \
    .join(dim_date, on="event_date", how="left") \
    .withColumn(
        "snapshot_key",
        F.monotonically_increasing_id()
    ) \
    .select(
        "snapshot_key",
        "event_id",
        "event_type",
        "correlation_id",
        "product_key",
        "date_key",
        "quantity",
        "stock_level",
        "reorder_threshold",
        "days_of_supply",
        "stock_alert_level",
        "fill_time_mins",
        "is_prescription",
        "time_of_day",
        "is_peak_hour",
        "is_weekend",
        "hour_of_day",
        "occurred_at"
    )

total = fact.count()
null_product_key = fact.filter(F.col("product_key").isNull()).count()
null_date_key = fact.filter(F.col("date_key").isNull()).count()
null_stock = fact.filter(F.col("stock_level").isNull()).count()
null_reorder = fact.filter(F.col("reorder_threshold").isNull()).count()

print(f"Fact rows: {total:,}")
print(f"Null product_key: {null_product_key:,} ({null_product_key/total*100:.1f}%)")
print(f"Null date_key: {null_date_key:,} ({null_date_key/total*100:.1f}%) -- only if outside dim_date range")
print(f"Null stock_level: {null_stock:,} (expected 0 after fix)")
print(f"Null reorder_threshold: {null_reorder:,} (expected 0 after fix)")

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

checks = {
    "null_snapshot_key":    df.filter(F.col("snapshot_key").isNull()).count(),
    "dup_snapshot_key":     df.groupBy("snapshot_key").count().filter(F.col("count") > 1).count(),
    "null_quantity":        df.filter(F.col("quantity").isNull()).count(),
    "null_stock_level":     df.filter(F.col("stock_level").isNull()).count(),
    "null_reorder":         df.filter(F.col("reorder_threshold").isNull()).count(),
    "null_is_prescription": df.filter(F.col("is_prescription").isNull()).count(),
    "null_date_key":        df.filter(F.col("date_key").isNull()).count(),
}

print(f"Total rows: {total:,}")
for check_name, count in checks.items():
    status = "PASS" if count == 0 else ("WARN" if check_name == "null_date_key" else "FAIL")
    print(f"  {status} {check_name}: {count:,}")

time_dist = df.groupBy("time_of_day").count().collect()
print("\nTime of day distribution:")
for row in time_dist:
    pct = row["count"] / total * 100
    print(f"  {row['time_of_day']}: {row['count']:,} ({pct:.1f}%)")

status = "PASS" if checks["null_snapshot_key"] == 0 and checks["dup_snapshot_key"] == 0 \
         and checks["null_stock_level"] == 0 else "FAIL"
print(f"\n{status}: fact_inventory_snapshots verified")

display(df.select(
    "snapshot_key", "product_key", "date_key",
    "quantity", "stock_level", "reorder_threshold",
    "stock_alert_level", "is_prescription"
).limit(5))
