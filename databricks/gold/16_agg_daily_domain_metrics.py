# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - AGG DAILY DOMAIN METRICS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Daily event count and value aggregates per domain  
# MAGIC **Input:** acip.silver.events (all domains)  
# MAGIC **Output:** acip.gold.agg_daily_domain_metrics

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.agg_daily_domain_metrics"

print("GOLD - AGG DAILY DOMAIN METRICS")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Extract Value from Payload per Domain
print("STEP 1: EXTRACT VALUE FROM PAYLOAD")
print("=" * 70)

amount_schema = StructType([
    StructField("total_amount", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("price", DoubleType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

silver_with_value = silver \
    .withColumn("p", F.from_json(F.col("payload"), amount_schema)) \
    .withColumn("event_value",
        F.coalesce(
            F.col("p.total_amount"),
            F.col("p.quantity"),
            F.col("p.price"),
            F.lit(0.0)
        )
    ) \
    .withColumn("metric_date",
        F.to_date(F.substring(F.col("occurred_at"), 1, 10))
    ) \
    .filter(F.col("metric_date").isNotNull())

print(f"Silver events with value: {silver_with_value.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Aggregate by Domain and Date
print("STEP 2: AGGREGATE BY DOMAIN AND DATE")
print("=" * 70)

agg = silver_with_value.groupBy("metric_date", "domain", "event_type").agg(
    F.count("event_id").alias("event_count"),
    F.sum("event_value").alias("total_value"),
    F.avg("event_value").alias("avg_value"),
    F.max("event_value").alias("max_value")
).withColumn("updated_at", F.current_timestamp())

total = agg.count()
print(f"Aggregation rows: {total:,}")

domain_dist = agg.groupBy("domain").agg(
    F.sum("event_count").alias("total_events")
).collect()
print("\nTotal events per domain:")
for row in domain_dist:
    print(f"  {row['domain']}: {row['total_events']:,}")

display(agg.orderBy("metric_date", "domain").limit(10))

# COMMAND ----------

# DBTITLE 1,STEP 3: Write Gold Table
print("STEP 3: WRITE GOLD TABLE")
print("=" * 70)

agg.write \
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
print(f"Total rows: {df.count():,}")

date_range = df.agg(
    F.min("metric_date").alias("min_date"),
    F.max("metric_date").alias("max_date")
).collect()[0]
print(f"Date range: {date_range['min_date']} to {date_range['max_date']}")

domain_summary = df.groupBy("domain").agg(
    F.sum("event_count").alias("total_events"),
    F.countDistinct("metric_date").alias("distinct_dates")
).collect()
print("\nSummary per domain:")
for row in domain_summary:
    print(f"  {row['domain']}: {row['total_events']:,} events across {row['distinct_dates']:,} dates")

print("\nPASS: agg_daily_domain_metrics verified")
