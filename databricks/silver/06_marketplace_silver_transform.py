# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - MARKETPLACE TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Transform Bronze marketplace tables to event envelope schema
# MAGIC **Input:** acip.bronze.marketplace_sellers, marketplace_order_items,
# MAGIC            marketplace_products, marketplace_orders, category_translations
# MAGIC **Output:** acip.silver.marketplace_events

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
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.marketplace_events"

print(f"Source: {CATALOG}.{SOURCE_SCHEMA}")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load Bronze Tables
print("Loading bronze marketplace tables...")

sellers = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.marketplace_sellers")
order_items = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.marketplace_order_items")
products = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.marketplace_products")
orders = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.marketplace_orders")
translations = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.category_translations")

print(f"PASS: sellers - {sellers.count()} rows")
print(f"PASS: order_items - {order_items.count()} rows")
print(f"PASS: products - {products.count()} rows")
print(f"PASS: orders - {orders.count()} rows")

# COMMAND ----------

# DBTITLE 1,Derive Seller Tiers
print("Deriving seller tiers...")

seller_order_counts = order_items.groupBy("seller_id").agg(
    F.count("order_id").alias("total_orders"),
    F.sum(F.col("price").cast("double")).alias("total_revenue"),
    F.countDistinct("order_id").alias("unique_orders")
)

quantiles = seller_order_counts.approxQuantile("total_orders", [0.5, 0.8], 0.01)
p50 = float(quantiles[0])
p80 = float(quantiles[1])

sellers_enriched = sellers.join(
    seller_order_counts, on="seller_id", how="left"
).withColumn(
    "seller_tier",
    F.when(F.col("total_orders") >= p80, "platinum")
     .when(F.col("total_orders") >= p50, "gold")
     .otherwise("standard")
).withColumn(
    "total_orders", F.coalesce(F.col("total_orders"), F.lit(0))
).withColumn(
    "total_revenue", F.coalesce(F.col("total_revenue").cast("double"), F.lit(0.0))
)

print(f"PASS: sellers enriched with tiers - {sellers_enriched.count()} rows")

tier_dist = sellers_enriched.groupBy("seller_tier").count().collect()
for row in tier_dist:
    print(f"  {row['seller_tier']}: {row['count']} sellers")

# COMMAND ----------

# DBTITLE 1,Enrich Order Items with Products and Sellers
print("Enriching order items...")

products_translated = products.join(
    translations, on="product_category_name", how="left"
).select(
    "product_id",
    F.coalesce(
        F.col("product_category_name_english"),
        F.col("product_category_name"),
        F.lit("other")
    ).alias("category")
)

items_enriched = order_items \
    .join(
        sellers_enriched.select("seller_id", "seller_tier", "seller_city", "seller_state"),
        on="seller_id", how="left"
    ) \
    .join(products_translated, on="product_id", how="left") \
    .join(
        orders.select("order_id", "order_purchase_timestamp", "order_delivered_customer_date"),
        on="order_id", how="left"
    )

items_enriched = items_enriched \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("freight_value", F.col("freight_value").cast("double")) \
    .withColumn(
        "dispatch_time_mins",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.round(
                (F.unix_timestamp("order_delivered_customer_date") -
                 F.unix_timestamp("order_purchase_timestamp")) / 60, 0
            )
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "sla_threshold_mins",
        F.when(F.col("seller_tier") == "platinum", F.lit(1440))
         .when(F.col("seller_tier") == "gold", F.lit(2880))
         .otherwise(F.lit(4320))
    ) \
    .withColumn(
        "is_sla_breached",
        F.when(
            F.col("dispatch_time_mins").isNotNull(),
            F.col("dispatch_time_mins") > F.col("sla_threshold_mins")
        ).otherwise(F.lit(False))
    )

print(f"PASS: items enriched - {items_enriched.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - listing.created
print("Building listing.created events...")

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

listing_events = sellers_enriched.select(
    uuid_udf().alias("event_id"),
    F.lit("listing.created").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.current_timestamp().cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("seller_id").alias("correlation_id"),
    F.col("seller_id"),
    F.col("seller_tier"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.col("total_orders"),
    F.col("total_revenue"),
    F.lit(None).cast("string").alias("product_id"),
    F.lit(None).cast("string").alias("category"),
    F.lit(None).cast("double").alias("price"),
    F.lit(None).cast("double").alias("dispatch_time_mins"),
    F.lit(None).cast("int").alias("sla_threshold_mins"),
    F.lit(False).alias("is_sla_breached"),
    F.lit(None).cast("double").alias("old_price"),
    F.lit(None).cast("double").alias("new_price"),
    F.lit(None).cast("double").alias("change_pct")
).filter(F.col("seller_id").isNotNull())

print(f"PASS: listing.created - {listing_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - seller.order.dispatched
print("Building seller.order.dispatched events...")

dispatch_events = items_enriched.filter(
    F.col("order_delivered_customer_date").isNotNull()
).select(
    uuid_udf().alias("event_id"),
    F.lit("seller.order.dispatched").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.to_timestamp("order_delivered_customer_date").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("seller_id"),
    F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.lit(None).cast("double").alias("total_orders"),
    F.lit(None).cast("double").alias("total_revenue"),
    F.col("product_id"),
    F.coalesce(F.col("category"), F.lit("other")).alias("category"),
    F.col("price"),
    F.col("dispatch_time_mins"),
    F.col("sla_threshold_mins"),
    F.col("is_sla_breached"),
    F.lit(None).cast("double").alias("old_price"),
    F.lit(None).cast("double").alias("new_price"),
    F.lit(None).cast("double").alias("change_pct")
).filter(
    F.col("order_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"PASS: seller.order.dispatched - {dispatch_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Build Event Envelope - price.updated
print("Building price.updated events...")

price_events = items_enriched.filter(
    F.col("price").isNotNull()
).withColumn(
    "old_price",
    F.col("price") * (1 + (F.rand() - 0.5) * 0.3)
).withColumn(
    "change_pct",
    F.round(
        (F.col("price") - F.col("old_price")) / F.col("old_price") * 100, 2
    )
).filter(
    F.abs(F.col("change_pct")) > 1.0
).select(
    uuid_udf().alias("event_id"),
    F.lit("price.updated").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.to_timestamp("order_purchase_timestamp").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.concat_ws("-", F.col("order_id"), F.col("product_id")).alias("correlation_id"),
    F.col("seller_id"),
    F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.lit(None).cast("double").alias("total_orders"),
    F.lit(None).cast("double").alias("total_revenue"),
    F.col("product_id"),
    F.coalesce(F.col("category"), F.lit("other")).alias("category"),
    F.col("price"),
    F.lit(None).cast("double").alias("dispatch_time_mins"),
    F.lit(None).cast("int").alias("sla_threshold_mins"),
    F.lit(False).alias("is_sla_breached"),
    F.col("old_price"),
    F.col("price").alias("new_price"),
    F.col("change_pct")
).filter(
    F.col("seller_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"PASS: price.updated - {price_events.count()} rows")

# COMMAND ----------

# DBTITLE 1,Union All Events and Deduplicate
print("Combining and deduplicating marketplace events...")

all_events = listing_events.union(dispatch_events).union(price_events)
all_events = all_events.dropDuplicates(["correlation_id", "event_type"])

total = all_events.count()
print(f"Total marketplace silver events after deduplication: {total}")

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
null_seller_id = df.filter(F.col("seller_id").isNull()).count()

event_type_dist = df.groupBy("event_type").count().collect()
tier_dist = df.groupBy("seller_tier").count().collect()

print(f"Total rows: {total}")
print(f"Null event_id: {null_event_id}")
print(f"Null occurred_at: {null_occurred_at}")
print(f"Null seller_id: {null_seller_id}")

print("\nEvent type distribution:")
for row in event_type_dist:
    print(f"  {row['event_type']}: {row['count']}")

print("\nSeller tier distribution:")
for row in tier_dist:
    print(f"  {row['seller_tier']}: {row['count']}")

if null_event_id == 0 and null_occurred_at == 0:
    print("\nPASS: All quality checks passed")
else:
    print("\nFAIL: Quality issues detected")
