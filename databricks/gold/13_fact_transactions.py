# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - FACT TRANSACTIONS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Build ecommerce fact table joining to dim_customer, dim_date, dim_geography  
# MAGIC **Input:** acip.silver.events (ecommerce), acip.gold.dim_customer,
# MAGIC            acip.gold.dim_date, acip.gold.dim_geography  
# MAGIC **Output:** acip.gold.fact_transactions

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
TARGET_TABLE = f"{CATALOG}.gold.fact_transactions"

print("GOLD - FACT TRANSACTIONS")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Ecommerce Events from Silver
print("STEP 1: PARSE ECOMMERCE EVENTS FROM SILVER")
print("=" * 70)

payload_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("order_value_tier", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state_region", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("is_installment", BooleanType(), True),
    StructField("max_installments", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("is_multi_item", BooleanType(), True),
    StructField("is_multi_seller", BooleanType(), True),
    StructField("primary_seller_id", StringType(), True),
    StructField("fulfilment_time_mins", DoubleType(), True),
    StructField("fulfilment_time_days", DoubleType(), True),
    StructField("fulfilment_bucket", StringType(), True),
    StructField("delivery_on_time", BooleanType(), True),
    StructField("avg_review_score", DoubleType(), True),
    StructField("review_sentiment", StringType(), True),
    StructField("has_negative_review", BooleanType(), True),
    StructField("return_reason", StringType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

ecommerce = silver.filter(F.col("domain") == "ecommerce") \
    .withColumn("p", F.from_json(F.col("payload"), payload_schema)) \
    .select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("correlation_id").alias("order_id"),
        F.col("occurred_at"),
        F.to_date(F.col("occurred_at")).alias("event_date"),
        F.col("p.customer_id").alias("customer_id"),
        F.col("p.customer_segment").alias("customer_segment"),
        F.col("p.order_value_tier").alias("order_value_tier"),
        F.col("p.region").alias("region"),
        F.col("p.total_amount").alias("total_amount"),
        F.col("p.payment_method").alias("payment_method"),
        F.col("p.is_installment").alias("is_installment"),
        F.col("p.max_installments").alias("max_installments"),
        F.col("p.order_status").alias("order_status"),
        F.col("p.item_count").alias("item_count"),
        F.col("p.is_multi_item").alias("is_multi_item"),
        F.col("p.is_multi_seller").alias("is_multi_seller"),
        F.col("p.fulfilment_time_mins").alias("fulfilment_time_mins"),
        F.col("p.fulfilment_time_days").alias("fulfilment_time_days"),
        F.col("p.fulfilment_bucket").alias("fulfilment_bucket"),
        F.col("p.delivery_on_time").alias("delivery_on_time"),
        F.col("p.avg_review_score").alias("avg_review_score"),
        F.col("p.review_sentiment").alias("review_sentiment"),
        F.col("p.has_negative_review").alias("has_negative_review"),
        F.col("p.return_reason").alias("return_reason"),
    ).filter(F.col("customer_id").isNotNull())

print(f"Ecommerce events parsed: {ecommerce.count():,}")

event_dist = ecommerce.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_dist:
    pct = row["count"] / ecommerce.count() * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 2: Load Dimensions
print("STEP 2: LOAD DIMENSIONS")
print("=" * 70)

dim_customer = spark.table(f"{CATALOG}.gold.dim_customer") \
    .filter(F.col("is_current")) \
    .select("customer_key", "customer_id", "customer_segment")

dim_date = spark.table(f"{CATALOG}.gold.dim_date") \
    .select("date_key", "full_date")

dim_geo = spark.table(f"{CATALOG}.gold.dim_geography") \
    .select("geo_key", "region")

print(f"dim_customer (current): {dim_customer.count():,}")
print(f"dim_date: {dim_date.count():,}")
print(f"dim_geography: {dim_geo.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Join to Dimensions
print("STEP 3: JOIN TO DIMENSIONS")
print("=" * 70)

fact = ecommerce \
    .join(
        dim_customer.select("customer_key", "customer_id"),
        on="customer_id", how="left"
    ) \
    .join(
        dim_date.select(F.col("full_date").alias("event_date"), "date_key"),
        on="event_date", how="left"
    ) \
    .join(
        dim_geo.select("geo_key", "region"),
        on="region", how="left"
    ) \
    .withColumn(
        "transaction_key",
        F.abs(F.hash(F.col("event_id"))).cast("long")
    ) \
    .select(
        "transaction_key",
        "event_id",
        "event_type",
        "order_id",
        "customer_key",
        "date_key",
        "geo_key",
        "total_amount",
        "payment_method",
        "is_installment",
        "max_installments",
        "order_status",
        "item_count",
        "is_multi_item",
        "is_multi_seller",
        "fulfilment_time_mins",
        "fulfilment_time_days",
        "fulfilment_bucket",
        "delivery_on_time",
        "avg_review_score",
        "review_sentiment",
        "has_negative_review",
        "return_reason",
        "occurred_at"
    )

total = fact.count()
null_customer_key = fact.filter(F.col("customer_key").isNull()).count()
null_date_key = fact.filter(F.col("date_key").isNull()).count()

print(f"Fact rows: {total:,}")
print(f"Null customer_key: {null_customer_key:,} ({null_customer_key/total*100:.1f}%)")
print(f"Null date_key: {null_date_key:,} ({null_date_key/total*100:.1f}%)")

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
    "null_transaction_key": df.filter(F.col("transaction_key").isNull()).count(),
    "null_event_id":        df.filter(F.col("event_id").isNull()).count(),
    "null_total_amount":    df.filter(F.col("total_amount").isNull()).count(),
}

print(f"Total rows: {total:,}")
for check, count in checks.items():
    status = "PASS" if count == 0 else "FAIL"
    print(f"  {status} {check}: {count:,}")

event_dist = df.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_dist:
    pct = row["count"] / total * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

fulfilment_dist = df.filter(
    F.col("fulfilment_bucket").isNotNull()
).groupBy("fulfilment_bucket").count().collect()
print("\nFulfilment bucket distribution:")
for row in fulfilment_dist:
    print(f"  {row['fulfilment_bucket']}: {row['count']:,}")

display(df.select(
    "transaction_key", "event_type", "order_id",
    "customer_key", "date_key", "total_amount",
    "fulfilment_bucket", "order_status"
).limit(5))
