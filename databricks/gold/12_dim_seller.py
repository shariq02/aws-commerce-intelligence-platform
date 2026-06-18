# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - DIM SELLER (SCD2)
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Build seller dimension with SCD2 on seller_tier changes
# MAGIC **Input:** acip.silver.events (marketplace domain)
# MAGIC **Output:** acip.gold.dim_seller (SCD2)
# MAGIC **SCD2 Logic:** Tracks seller_tier changes over time
# MAGIC **Rollback:** RESTORE TABLE acip.gold.dim_seller TO VERSION AS OF N

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.dim_seller"

print("GOLD - DIM SELLER (SCD2)")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Sellers from Silver Events
print("STEP 1: PARSE SELLERS FROM SILVER EVENTS")
print("=" * 70)

payload_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_tier", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("seller_region", StringType(), True),
    StructField("total_orders", DoubleType(), True),
    StructField("total_revenue", DoubleType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

sellers_raw = silver.filter(
    F.col("domain") == "marketplace"
).filter(
    F.col("event_type") == "listing.created"
).withColumn("payload_parsed", F.from_json(F.col("payload"), payload_schema)) \
 .select(
    F.col("payload_parsed.seller_id").alias("seller_id"),
    F.col("payload_parsed.seller_tier").alias("seller_tier"),
    F.col("payload_parsed.seller_city").alias("seller_city"),
    F.col("payload_parsed.seller_state").alias("seller_state"),
    F.col("payload_parsed.seller_region").alias("seller_region"),
    F.col("payload_parsed.total_orders").alias("total_orders"),
    F.col("payload_parsed.total_revenue").alias("total_revenue"),
    F.to_date(F.col("occurred_at")).alias("event_date")
).filter(F.col("seller_id").isNotNull())

print(f"Raw seller records: {sellers_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Get Latest Record Per Seller
print("STEP 2: GET LATEST RECORD PER SELLER")
print("=" * 70)

window = Window.partitionBy("seller_id").orderBy(F.col("event_date").desc())

sellers_latest = sellers_raw \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"Unique sellers: {sellers_latest.count():,}")

tier_dist = sellers_latest.groupBy("seller_tier").count().collect()
print("\nSeller tier distribution:")
for row in tier_dist:
    pct = row["count"] / sellers_latest.count() * 100
    print(f"  {row['seller_tier']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 3: Apply SCD2 Logic
print("STEP 3: APPLY SCD2 LOGIC")
print("=" * 70)

if not spark.catalog.tableExists(TARGET_TABLE):
    print("First run - inserting all records as current")

    dim_seller = sellers_latest.select(
        F.abs(F.hash(
            F.col("seller_id"),
            F.col("seller_tier"),
            F.col("event_date").cast("string")
        )).cast("long").alias("seller_key"),
        F.col("seller_id"),
        F.col("seller_tier"),
        F.col("seller_city"),
        F.col("seller_state"),
        F.col("seller_region"),
        F.col("total_orders"),
        F.col("total_revenue"),
        F.col("event_date").alias("effective_date"),
        F.lit("9999-12-31").cast("date").alias("expiry_date"),
        F.lit(True).alias("is_current")
    )

    dim_seller.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_TABLE)

else:
    print("Subsequent run - applying SCD2 MERGE")

    sellers_latest.createOrReplaceTempView("sellers_latest_view")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING (
            SELECT
                ABS(HASH(seller_id, seller_tier, CAST(event_date AS STRING))) AS seller_key,
                seller_id, seller_tier, seller_city, seller_state,
                seller_region, total_orders, total_revenue, event_date AS effective_date
            FROM sellers_latest_view
        ) AS source
        ON target.seller_id = source.seller_id AND target.is_current = TRUE
        WHEN MATCHED AND target.seller_tier != source.seller_tier THEN
            UPDATE SET target.expiry_date = CURRENT_DATE(), target.is_current = FALSE
        WHEN NOT MATCHED THEN
            INSERT (seller_key, seller_id, seller_tier, seller_city, seller_state,
                    seller_region, total_orders, total_revenue,
                    effective_date, expiry_date, is_current)
            VALUES (source.seller_key, source.seller_id, source.seller_tier,
                    source.seller_city, source.seller_state, source.seller_region,
                    source.total_orders, source.total_revenue,
                    source.effective_date, DATE('9999-12-31'), TRUE)
    """)

written = spark.table(TARGET_TABLE).count()
current_count = spark.table(TARGET_TABLE).filter(F.col("is_current")).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} total rows, {current_count:,} current")

# COMMAND ----------

# DBTITLE 1,STEP 4: Verify SCD2 Integrity
print("STEP 4: VERIFY SCD2 INTEGRITY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()
current = df.filter(F.col("is_current")).count()
null_key = df.filter(F.col("seller_key").isNull()).count()

print(f"Total rows:  {total:,}")
print(f"Current:     {current:,}")
print(f"Historical:  {total - current:,}")
print(f"Null keys:   {null_key}")

tier_dist = df.filter(F.col("is_current")).groupBy("seller_tier").count().collect()
print("\nCurrent seller tier distribution:")
for row in tier_dist:
    pct = row["count"] / current * 100
    print(f"  {row['seller_tier']}: {row['count']:,} ({pct:.1f}%)")

print(f"\nPASS: dim_seller SCD2 verified" if null_key == 0 else "\nFAIL: null keys found")

display(df.limit(5))
