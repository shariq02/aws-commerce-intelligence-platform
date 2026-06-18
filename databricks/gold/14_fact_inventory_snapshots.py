# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - FACT INVENTORY SNAPSHOTS
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Build pharmacy inventory fact table joining to dim_product and dim_date
# MAGIC **Input:** acip.silver.events (pharmacy), acip.gold.dim_product, acip.gold.dim_date
# MAGIC **Output:** acip.gold.fact_inventory_snapshots

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

print("GOLD - FACT INVENTORY SNAPSHOTS")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

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
        F.to_date(F.substring(F.col("occurred_at"), 1, 10)).alias("event_date"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.category").alias("category"),
        F.col("p.quantity").alias("quantity"),
        F.col("p.is_prescription").alias("is_prescription"),
        F.col("p.stock_level").alias("stock_level"),
        F.col("p.reorder_threshold").alias("reorder_threshold"),
        F.col("p.fill_time_mins").alias("fill_time_mins"),
        F.col("p.time_of_day").alias("time_of_day"),
        F.col("p.is_peak_hour").alias("is_peak_hour"),
        F.col("p.is_weekend").alias("is_weekend"),
        F.col("p.hour").alias("hour_of_day"),
    ).filter(F.col("product_id").isNotNull()) \
     .withColumn("days_of_supply",
        F.when(F.col("quantity") > 0,
            F.round(F.col("stock_level") / F.col("quantity"), 1)
        ).otherwise(F.lit(None))
    ) \
     .withColumn("stock_alert_level",
        F.when(F.col("stock_level") <= F.col("reorder_threshold") * 0.5, "critical")
         .when(F.col("stock_level") <= F.col("reorder_threshold"), "high")
         .when(F.col("stock_level") <= F.col("reorder_threshold") * 2, "medium")
         .otherwise("normal")
    )

print(f"Pharmacy events parsed: {pharmacy.count():,}")

alert_dist = pharmacy.groupBy("stock_alert_level").count().collect()
print("\nStock alert level distribution:")
for row in alert_dist:
    pct = row["count"] / pharmacy.count() * 100
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

# COMMAND ----------

# DBTITLE 1,STEP 3: Join to Dimensions
print("STEP 3: JOIN TO DIMENSIONS")
print("=" * 70)

fact = pharmacy \
    .join(dim_product, on="product_id", how="left") \
    .join(dim_date, on="event_date", how="left") \
    .withColumn(
        "snapshot_key",
        F.abs(F.hash(F.col("event_id"))).cast("long")
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
print(f"Fact rows: {total:,}")
print(f"Null product_key: {null_product_key:,} ({null_product_key/total*100:.1f}%)")

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

null_snapshot_key = df.filter(F.col("snapshot_key").isNull()).count()
null_quantity = df.filter(F.col("quantity").isNull()).count()

print(f"Total rows: {total:,}")
print(f"Null snapshot_key: {null_snapshot_key}")
print(f"Null quantity: {null_quantity}")

time_dist = df.groupBy("time_of_day").count().collect()
print("\nTime of day distribution:")
for row in time_dist:
    pct = row["count"] / total * 100
    print(f"  {row['time_of_day']}: {row['count']:,} ({pct:.1f}%)")

print(f"\nPASS: fact_inventory_snapshots verified" if null_snapshot_key == 0 else "\nFAIL")

display(df.select(
    "snapshot_key", "product_key", "date_key",
    "quantity", "stock_level", "stock_alert_level", "days_of_supply"
).limit(5))
