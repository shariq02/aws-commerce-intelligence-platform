# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - ECOMMERCE TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Transform Bronze ecommerce tables to event envelope schema
# MAGIC **Input:** acip.bronze.orders, order_items, order_payments, customers,
# MAGIC            products, order_reviews, category_translations
# MAGIC **Output:** acip.silver.ecommerce_events

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.ecommerce_events"

print(f"Source: {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load Bronze Tables
print("Loading bronze tables...")

orders = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.orders")
items = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.order_items")
payments = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.order_payments")
customers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.customers")
products = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.products")
reviews = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.order_reviews")
translations = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.category_translations")

print(f"PASS: orders - {orders.count()} rows")
print(f"PASS: items - {items.count()} rows")
print(f"PASS: payments - {payments.count()} rows")
print(f"PASS: customers - {customers.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Payment Aggregates
print("Building payment aggregates...")

payment_totals = payments.groupBy("order_id").agg(
    F.sum(F.col("payment_value").cast("double")).alias("total_amount"),
    F.first("payment_type").alias("payment_method")
)

print(f"PASS: payment_totals - {payment_totals.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Product Category Mapping
print("Building product category mapping...")

products_translated = products.join(
    translations,
    on="product_category_name",
    how="left"
).select(
    "product_id",
    F.coalesce(
        F.col("product_category_name_english"),
        F.col("product_category_name"),
        F.lit("other")
    ).alias("category")
)

print(f"PASS: products_translated - {products_translated.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Items Per Order
print("Building items per order...")

items_enriched = items.join(
    products_translated, on="product_id", how="left"
)

items_per_order = items_enriched.groupBy("order_id").agg(
    F.count("order_item_id").alias("item_count"),
    F.sum(F.col("price").cast("double")).alias("items_total"),
    F.collect_list("category").alias("categories"),
    F.first("seller_id").alias("primary_seller_id")
)

print(f"PASS: items_per_order - {items_per_order.count()} rows")

# COMMAND ----------

# DBTITLE 1,Enrich Orders
print("Enriching orders with all dimensions...")

orders_enriched = orders \
    .join(
        customers.select("customer_id", "customer_city", "customer_state"),
        on="customer_id", how="left"
    ) \
    .join(payment_totals, on="order_id", how="left") \
    .join(items_per_order, on="order_id", how="left") \
    .join(
        reviews.select("order_id", "review_score"),
        on="order_id", how="left"
    )

orders_enriched = orders_enriched.withColumn(
    "total_amount",
    F.coalesce(F.col("total_amount").cast("double"), F.lit(0.0))
)

quantiles = orders_enriched.approxQuantile("total_amount", [0.2, 0.5], 0.01)
p20 = float(quantiles[0])
p50 = float(quantiles[1])

orders_enriched = orders_enriched.withColumn(
    "customer_segment",
    F.when(F.col("total_amount") >= p50, "premium")
     .when(F.col("total_amount") >= p20, "standard")
     .otherwise("new")
).withColumn(
    "region",
    F.concat_ws("-", F.col("customer_city"), F.col("customer_state"))
).withColumn(
    "order_status_clean",
    F.when(F.col("order_status") == "delivered", "completed")
     .when(F.col("order_status").isin("shipped", "processing", "approved"), "pending")
     .when(F.col("order_status").isin("canceled", "unavailable"), "failed")
     .otherwise(F.col("order_status"))
)

print(f"PASS: orders enriched - {orders_enriched.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - order.placed
print("Building order.placed events...")

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

placed_events = orders_enriched.select(
    uuid_udf().alias("event_id"),
    F.lit("order.placed").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.to_timestamp("order_purchase_timestamp").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_segment"),
    F.col("region"),
    F.col("total_amount"),
    F.coalesce(F.col("payment_method"), F.lit("unknown")).alias("payment_method"),
    F.col("order_status_clean").alias("order_status"),
    F.col("item_count"),
    F.lit(None).cast("string").alias("fulfilment_time_mins"),
    F.lit(None).cast("string").alias("return_reason"),
    F.lit(None).cast("string").alias("review_score_raw")
).filter(
    F.col("order_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"PASS: order.placed - {placed_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - order.fulfilled
print("Building order.fulfilled events...")

fulfilled_events = orders_enriched.filter(
    F.col("order_delivered_customer_date").isNotNull()
).select(
    uuid_udf().alias("event_id"),
    F.lit("order.fulfilled").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.to_timestamp("order_delivered_customer_date").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_segment"),
    F.col("region"),
    F.col("total_amount"),
    F.coalesce(F.col("payment_method"), F.lit("unknown")).alias("payment_method"),
    F.lit("completed").alias("order_status"),
    F.col("item_count"),
    F.round(
        (F.unix_timestamp("order_delivered_customer_date") -
         F.unix_timestamp("order_purchase_timestamp")) / 60, 0
    ).cast("string").alias("fulfilment_time_mins"),
    F.lit(None).cast("string").alias("return_reason"),
    F.lit(None).cast("string").alias("review_score_raw")
).filter(
    F.col("order_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"PASS: order.fulfilled - {fulfilled_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - order.returned
print("Building order.returned events...")

returned_events = orders_enriched.filter(
    F.col("review_score").cast("int") <= 2
).filter(
    F.col("review_score").isNotNull()
).select(
    uuid_udf().alias("event_id"),
    F.lit("order.returned").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.to_timestamp("order_purchase_timestamp").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_segment"),
    F.col("region"),
    F.col("total_amount"),
    F.coalesce(F.col("payment_method"), F.lit("unknown")).alias("payment_method"),
    F.lit("returned").alias("order_status"),
    F.col("item_count"),
    F.lit(None).cast("string").alias("fulfilment_time_mins"),
    F.lit("low_review_score").alias("return_reason"),
    F.col("review_score").alias("review_score_raw")
).filter(
    F.col("order_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"PASS: order.returned - {returned_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Union All Events and Deduplicate
print("Combining and deduplicating events...")

all_events = placed_events.union(fulfilled_events).union(returned_events)

all_events = all_events.dropDuplicates(["correlation_id", "event_type"])

total = all_events.count()
print(f"Total silver events after deduplication: {total}")

# COMMAND ----------

# DBTITLE 1,Write Silver Delta Table
print(f"Writing to {TARGET_TABLE}...")

all_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"PASS: {TARGET_TABLE} written - {total} rows")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
print("Running data quality checks...")
print("=" * 60)

df = spark.table(TARGET_TABLE)
total = df.count()
null_event_id = df.filter(F.col("event_id").isNull()).count()
null_occurred_at = df.filter(F.col("occurred_at").isNull()).count()
null_correlation_id = df.filter(F.col("correlation_id").isNull()).count()
null_domain = df.filter(F.col("domain").isNull()).count()

event_type_dist = df.groupBy("event_type").count().collect()
segment_dist = df.groupBy("customer_segment").count().collect()

print(f"Total rows: {total}")
print(f"Null event_id: {null_event_id}")
print(f"Null occurred_at: {null_occurred_at}")
print(f"Null correlation_id: {null_correlation_id}")
print(f"Null domain: {null_domain}")

print("\nEvent type distribution:")
for row in event_type_dist:
    print(f"  {row['event_type']}: {row['count']}")

print("\nCustomer segment distribution:")
for row in segment_dist:
    print(f"  {row['customer_segment']}: {row['count']}")

if null_event_id == 0 and null_occurred_at == 0 and null_domain == 0:
    print("\nPASS: All quality checks passed")
else:
    print("\nFAIL: Quality issues detected")
