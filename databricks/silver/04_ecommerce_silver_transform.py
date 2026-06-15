# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - ECOMMERCE TRANSFORMATION
# MAGIC ### Bronze to Silver with Full Feature Extraction
# MAGIC
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Transform raw Olist Bronze tables into enriched Silver event layer
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - acip.bronze.orders
# MAGIC - acip.bronze.order_items
# MAGIC - acip.bronze.order_payments
# MAGIC - acip.bronze.customers
# MAGIC - acip.bronze.products
# MAGIC - acip.bronze.order_reviews
# MAGIC - acip.bronze.category_translations
# MAGIC
# MAGIC **Output Table:** acip.silver.ecommerce_events
# MAGIC
# MAGIC **Run Order:**
# MAGIC ```
# MAGIC 1. databricks/bronze/01_load_ecommerce_bronze.py
# MAGIC 2. databricks/silver/04_ecommerce_silver_transform.py  (THIS SCRIPT)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
import uuid

# COMMAND ----------

# DBTITLE 1,Initialize
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SOURCE = "bronze"
TARGET = "silver"
TARGET_TABLE = f"{CATALOG}.{TARGET}.ecommerce_events"

print("ECOMMERCE SILVER TRANSFORMATION")
print("=" * 70)
print(f"Source: {CATALOG}.{SOURCE}")
print(f"Target: {TARGET_TABLE}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Bronze Tables
print("STEP 1: LOADING BRONZE TABLES")
print("=" * 70)

orders = spark.table(f"{CATALOG}.{SOURCE}.orders")
items = spark.table(f"{CATALOG}.{SOURCE}.order_items")
payments = spark.table(f"{CATALOG}.{SOURCE}.order_payments")
customers = spark.table(f"{CATALOG}.{SOURCE}.customers")
products = spark.table(f"{CATALOG}.{SOURCE}.products")
reviews = spark.table(f"{CATALOG}.{SOURCE}.order_reviews")
translations = spark.table(f"{CATALOG}.{SOURCE}.category_translations")

print(f"orders:               {orders.count():,} rows")
print(f"order_items:          {items.count():,} rows")
print(f"order_payments:       {payments.count():,} rows")
print(f"customers:            {customers.count():,} rows")
print(f"products:             {products.count():,} rows")
print(f"order_reviews:        {reviews.count():,} rows")
print(f"category_translations:{translations.count():,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean and Type Orders
print("STEP 2: CLEAN AND TYPE ORDERS")
print("=" * 70)

orders_clean = orders \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("customer_id").isNotNull() & (F.trim(F.col("customer_id")) != "")) \
    .withColumn("order_id", F.trim(F.col("order_id"))) \
    .withColumn("customer_id", F.trim(F.col("customer_id"))) \
    .withColumn("order_status", F.lower(F.trim(F.col("order_status")))) \
    .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_approved_ts", F.to_timestamp("order_approved_at")) \
    .withColumn("order_delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date")) \
    .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date")) \
    .withColumn("order_status_clean",
        F.when(F.col("order_status") == "delivered", "completed")
         .when(F.col("order_status").isin("shipped", "processing", "approved", "created", "invoiced"), "pending")
         .when(F.col("order_status").isin("canceled", "unavailable"), "failed")
         .otherwise(F.col("order_status"))
    ) \
    .withColumn("is_delivered", F.col("order_delivered_customer_ts").isNotNull()) \
    .withColumn("is_cancelled",
        F.col("order_status").isin("canceled", "unavailable")
    )

total_orders = orders_clean.count()
delivered = orders_clean.filter(F.col("is_delivered")).count()
cancelled = orders_clean.filter(F.col("is_cancelled")).count()

print(f"Total valid orders:   {total_orders:,}")
print(f"Delivered:            {delivered:,} ({delivered/total_orders*100:.1f}%)")
print(f"Cancelled:            {cancelled:,} ({cancelled/total_orders*100:.1f}%)")

display(orders_clean.select(
    "order_id", "order_status", "order_status_clean",
    "is_delivered", "is_cancelled", "order_purchase_ts"
).limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 3: Build Payment Aggregates
print("STEP 3: BUILD PAYMENT AGGREGATES")
print("=" * 70)

payments_clean = payments \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("payment_value").isNotNull()) \
    .filter(F.col("payment_value").cast("double") > 0) \
    .withColumn("payment_value", F.col("payment_value").cast("double")) \
    .withColumn("payment_installments", F.col("payment_installments").cast("int")) \
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))

payment_totals = payments_clean.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_amount"),
    F.max("payment_installments").alias("max_installments"),
    F.count("payment_sequential").alias("payment_parts"),
    F.first("payment_type").alias("payment_method"),
    F.collect_list("payment_type").alias("all_payment_types")
).withColumn(
    "is_installment",
    F.col("max_installments") > 1
).withColumn(
    "payment_method_clean",
    F.when(F.col("payment_method") == "credit_card", "credit_card")
     .when(F.col("payment_method") == "boleto", "boleto")
     .when(F.col("payment_method") == "voucher", "voucher")
     .when(F.col("payment_method") == "debit_card", "debit_card")
     .otherwise("other")
)

print(f"Orders with payments: {payment_totals.count():,}")

payment_method_dist = payments_clean.groupBy("payment_type").count().collect()
print("\nPayment method distribution:")
for row in payment_method_dist:
    pct = row["count"] / payments_clean.count() * 100
    print(f"  {row['payment_type']}: {row['count']:,} ({pct:.1f}%)")

display(payment_totals.select(
    "order_id", "total_amount", "payment_method_clean",
    "max_installments", "is_installment"
).limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 4: Build Product Category Mapping
print("STEP 4: BUILD PRODUCT CATEGORY MAPPING")
print("=" * 70)

products_clean = products \
    .filter(F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != "")) \
    .withColumn("product_id", F.trim(F.col("product_id"))) \
    .withColumn("product_weight_g", F.col("product_weight_g").cast("double")) \
    .withColumn("product_photos_qty", F.col("product_photos_qty").cast("int"))

products_translated = products_clean.join(
    translations.withColumnRenamed(
        "product_category_name_english", "category_english"
    ),
    on="product_category_name",
    how="left"
).select(
    "product_id",
    F.coalesce(
        F.col("category_english"),
        F.col("product_category_name"),
        F.lit("other")
    ).alias("category"),
    "product_weight_g",
    "product_photos_qty"
).withColumn(
    "category_group",
    F.when(F.col("category").isin(
        "bed_bath_table", "furniture_decor", "housewares",
        "home_confort", "kitchen_dining_laundry_garden_furniture"
    ), "home_living")
     .when(F.col("category").isin(
        "sports_leisure", "fashion_bags_accessories",
        "fashion_shoes", "fashion_male_clothing"
    ), "fashion_sports")
     .when(F.col("category").isin(
        "computers_accessories", "telephony", "electronics",
        "computers", "tablets_printing_image"
    ), "electronics")
     .when(F.col("category").isin(
        "health_beauty", "perfumery", "diapers_and_hygiene"
    ), "health_beauty")
     .otherwise("other")
)

print(f"Products with categories: {products_translated.count():,}")

category_dist = products_translated.groupBy("category_group").count().collect()
print("\nCategory group distribution:")
for row in category_dist:
    pct = row["count"] / products_translated.count() * 100
    print(f"  {row['category_group']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 5: Build Order Items Aggregates
print("STEP 5: BUILD ORDER ITEMS AGGREGATES")
print("=" * 70)

items_clean = items \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("product_id").isNotNull()) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("freight_value", F.col("freight_value").cast("double")) \
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))

items_enriched = items_clean.join(
    products_translated.select("product_id", "category", "category_group"),
    on="product_id",
    how="left"
)

items_per_order = items_enriched.groupBy("order_id").agg(
    F.count("order_item_id").alias("item_count"),
    F.sum("price").alias("items_subtotal"),
    F.sum("freight_value").alias("total_freight"),
    F.avg("price").alias("avg_item_price"),
    F.max("price").alias("max_item_price"),
    F.min("price").alias("min_item_price"),
    F.collect_list("category").alias("categories_list"),
    F.collect_set("category_group").alias("category_groups"),
    F.countDistinct("seller_id").alias("seller_count"),
    F.first("seller_id").alias("primary_seller_id")
).withColumn(
    "is_multi_seller",
    F.col("seller_count") > 1
).withColumn(
    "is_multi_item",
    F.col("item_count") > 1
)

print(f"Orders with items: {items_per_order.count():,}")
multi_seller = items_per_order.filter(F.col("is_multi_seller")).count()
print(f"Multi-seller orders: {multi_seller:,} ({multi_seller/items_per_order.count()*100:.1f}%)")

display(items_per_order.select(
    "order_id", "item_count", "items_subtotal",
    "seller_count", "is_multi_seller"
).limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 6: Clean Customers
print("STEP 6: CLEAN CUSTOMERS")
print("=" * 70)

customers_clean = customers \
    .filter(F.col("customer_id").isNotNull() & (F.trim(F.col("customer_id")) != "")) \
    .withColumn("customer_id", F.trim(F.col("customer_id"))) \
    .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id"))) \
    .withColumn("customer_city", F.initcap(F.lower(F.trim(F.col("customer_city"))))) \
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state")))) \
    .withColumn("customer_zip_prefix", F.trim(F.col("customer_zip_code_prefix"))) \
    .withColumn("region",
        F.concat_ws("-", F.col("customer_city"), F.col("customer_state"))
    ) \
    .withColumn("state_region",
        F.when(F.col("customer_state").isin("SP", "RJ", "MG", "ES"), "southeast")
         .when(F.col("customer_state").isin("RS", "SC", "PR"), "south")
         .when(F.col("customer_state").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), "northeast")
         .when(F.col("customer_state").isin("PA", "AM", "RO", "AC", "AP", "RR", "TO"), "north")
         .when(F.col("customer_state").isin("MT", "MS", "GO", "DF"), "center_west")
         .otherwise("unknown")
    )

print(f"Valid customers: {customers_clean.count():,}")

state_dist = customers_clean.groupBy("customer_state").count().orderBy(
    F.col("count").desc()
).limit(5).collect()
print("\nTop 5 states by customer count:")
for row in state_dist:
    pct = row["count"] / customers_clean.count() * 100
    print(f"  {row['customer_state']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 7: Clean Reviews
print("STEP 7: CLEAN REVIEWS")
print("=" * 70)

reviews_clean = reviews \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("review_score").isNotNull()) \
    .withColumn("review_score", F.col("review_score").cast("int")) \
    .filter(F.col("review_score").between(1, 5)) \
    .withColumn("review_sentiment",
        F.when(F.col("review_score") >= 4, "positive")
         .when(F.col("review_score") == 3, "neutral")
         .otherwise("negative")
    ) \
    .withColumn("is_negative_review", F.col("review_score") <= 2) \
    .withColumn("has_review_comment",
        F.col("review_comment_message").isNotNull() &
        (F.trim(F.col("review_comment_message")) != "")
    )

reviews_per_order = reviews_clean.groupBy("order_id").agg(
    F.avg("review_score").alias("avg_review_score"),
    F.min("review_score").alias("min_review_score"),
    F.first("review_sentiment").alias("review_sentiment"),
    F.max(F.col("is_negative_review").cast("int")).alias("has_negative_review_int")
).withColumn(
    "has_negative_review",
    F.col("has_negative_review_int") == 1
).drop("has_negative_review_int")

print(f"Orders with reviews: {reviews_per_order.count():,}")

sentiment_dist = reviews_clean.groupBy("review_sentiment").count().collect()
print("\nReview sentiment distribution:")
for row in sentiment_dist:
    pct = row["count"] / reviews_clean.count() * 100
    print(f"  {row['review_sentiment']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 8: Enrich Orders - Full Join
print("STEP 8: ENRICH ORDERS - FULL JOIN")
print("=" * 70)

orders_enriched = orders_clean \
    .join(
        customers_clean.select(
            "customer_id", "customer_unique_id",
            "customer_city", "customer_state",
            "region", "state_region", "customer_zip_prefix"
        ),
        on="customer_id", how="left"
    ) \
    .join(payment_totals, on="order_id", how="left") \
    .join(items_per_order, on="order_id", how="left") \
    .join(reviews_per_order, on="order_id", how="left")

orders_enriched = orders_enriched \
    .withColumn("total_amount",
        F.coalesce(F.col("total_amount").cast("double"), F.lit(0.0))
    ) \
    .withColumn("item_count",
        F.coalesce(F.col("item_count").cast("int"), F.lit(1))
    )

quantiles = orders_enriched.approxQuantile("total_amount", [0.2, 0.5, 0.8], 0.01)
p20, p50, p80 = float(quantiles[0]), float(quantiles[1]), float(quantiles[2])

orders_enriched = orders_enriched \
    .withColumn("customer_segment",
        F.when(F.col("total_amount") >= p80, "premium")
         .when(F.col("total_amount") >= p50, "standard")
         .when(F.col("total_amount") >= p20, "occasional")
         .otherwise("new")
    ) \
    .withColumn("order_value_tier",
        F.when(F.col("total_amount") >= 500, "high_value")
         .when(F.col("total_amount") >= 100, "mid_value")
         .when(F.col("total_amount") >= 20, "low_value")
         .otherwise("micro")
    ) \
    .withColumn("fulfilment_time_mins",
        F.when(
            F.col("order_delivered_customer_ts").isNotNull() &
            F.col("order_purchase_ts").isNotNull(),
            F.round(
                (F.unix_timestamp("order_delivered_customer_ts") -
                 F.unix_timestamp("order_purchase_ts")) / 60, 0
            )
        ).otherwise(F.lit(None))
    ) \
    .withColumn("fulfilment_time_days",
        F.when(F.col("fulfilment_time_mins").isNotNull(),
            F.round(F.col("fulfilment_time_mins") / 1440, 1)
        ).otherwise(F.lit(None))
    ) \
    .withColumn("fulfilment_bucket",
        F.when(F.col("fulfilment_time_days") <= 3, "express")
         .when(F.col("fulfilment_time_days") <= 7, "standard")
         .when(F.col("fulfilment_time_days") <= 14, "slow")
         .when(F.col("fulfilment_time_days").isNotNull(), "very_slow")
         .otherwise("unknown")
    ) \
    .withColumn("delivery_on_time",
        F.when(
            F.col("order_delivered_customer_ts").isNotNull() &
            F.col("order_estimated_delivery_ts").isNotNull(),
            F.col("order_delivered_customer_ts") <= F.col("order_estimated_delivery_ts")
        ).otherwise(F.lit(None))
    ) \
    .withColumn("carrier_delay_mins",
        F.when(
            F.col("order_delivered_carrier_ts").isNotNull() &
            F.col("order_approved_ts").isNotNull(),
            F.round(
                (F.unix_timestamp("order_delivered_carrier_ts") -
                 F.unix_timestamp("order_approved_ts")) / 60, 0
            )
        ).otherwise(F.lit(None))
    )

print(f"Enriched orders: {orders_enriched.count():,}")

segment_dist = orders_enriched.groupBy("customer_segment").count().collect()
print("\nCustomer segment distribution:")
for row in segment_dist:
    pct = row["count"] / orders_enriched.count() * 100
    print(f"  {row['customer_segment']}: {row['count']:,} ({pct:.1f}%)")

tier_dist = orders_enriched.groupBy("order_value_tier").count().collect()
print("\nOrder value tier distribution:")
for row in tier_dist:
    pct = row["count"] / orders_enriched.count() * 100
    print(f"  {row['order_value_tier']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 9: Build Event Envelope - order.placed
print("STEP 9: BUILD EVENT ENVELOPE - order.placed")
print("=" * 70)

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

placed_events = orders_enriched.select(
    uuid_udf().alias("event_id"),
    F.lit("order.placed").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_unique_id"),
    F.col("customer_segment"),
    F.col("order_value_tier"),
    F.col("region"),
    F.col("state_region"),
    F.col("customer_state"),
    F.col("total_amount"),
    F.col("payment_method_clean").alias("payment_method"),
    F.col("is_installment"),
    F.col("max_installments"),
    F.col("order_status_clean").alias("order_status"),
    F.col("item_count"),
    F.col("is_multi_item"),
    F.col("is_multi_seller"),
    F.col("primary_seller_id"),
    F.lit(None).cast("double").alias("fulfilment_time_mins"),
    F.lit(None).cast("double").alias("fulfilment_time_days"),
    F.lit(None).cast("string").alias("fulfilment_bucket"),
    F.lit(None).cast("boolean").alias("delivery_on_time"),
    F.lit(None).cast("double").alias("avg_review_score"),
    F.lit(None).cast("string").alias("review_sentiment"),
    F.lit(None).cast("boolean").alias("has_negative_review"),
    F.lit(None).cast("string").alias("return_reason")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

placed_count = placed_events.count()
print(f"order.placed events: {placed_count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 10: Build Event Envelope - order.fulfilled
print("STEP 10: BUILD EVENT ENVELOPE - order.fulfilled")
print("=" * 70)

fulfilled_events = orders_enriched.filter(
    F.col("order_delivered_customer_ts").isNotNull()
).select(
    uuid_udf().alias("event_id"),
    F.lit("order.fulfilled").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_delivered_customer_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_unique_id"),
    F.col("customer_segment"),
    F.col("order_value_tier"),
    F.col("region"),
    F.col("state_region"),
    F.col("customer_state"),
    F.col("total_amount"),
    F.col("payment_method_clean").alias("payment_method"),
    F.col("is_installment"),
    F.col("max_installments"),
    F.lit("completed").alias("order_status"),
    F.col("item_count"),
    F.col("is_multi_item"),
    F.col("is_multi_seller"),
    F.col("primary_seller_id"),
    F.col("fulfilment_time_mins"),
    F.col("fulfilment_time_days"),
    F.col("fulfilment_bucket"),
    F.col("delivery_on_time"),
    F.col("avg_review_score"),
    F.col("review_sentiment"),
    F.col("has_negative_review"),
    F.lit(None).cast("string").alias("return_reason")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

fulfilled_count = fulfilled_events.count()
print(f"order.fulfilled events: {fulfilled_count:,}")

fulfilment_dist = fulfilled_events.groupBy("fulfilment_bucket").count().collect()
print("\nFulfilment bucket distribution:")
for row in fulfilment_dist:
    pct = row["count"] / fulfilled_count * 100
    print(f"  {row['fulfilment_bucket']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 11: Build Event Envelope - order.returned
print("STEP 11: BUILD EVENT ENVELOPE - order.returned")
print("=" * 70)

returned_events = orders_enriched.filter(
    F.col("has_negative_review") == True
).select(
    uuid_udf().alias("event_id"),
    F.lit("order.returned").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("customer_id"),
    F.col("customer_unique_id"),
    F.col("customer_segment"),
    F.col("order_value_tier"),
    F.col("region"),
    F.col("state_region"),
    F.col("customer_state"),
    F.col("total_amount"),
    F.col("payment_method_clean").alias("payment_method"),
    F.col("is_installment"),
    F.col("max_installments"),
    F.lit("returned").alias("order_status"),
    F.col("item_count"),
    F.col("is_multi_item"),
    F.col("is_multi_seller"),
    F.col("primary_seller_id"),
    F.lit(None).cast("double").alias("fulfilment_time_mins"),
    F.lit(None).cast("double").alias("fulfilment_time_days"),
    F.lit(None).cast("string").alias("fulfilment_bucket"),
    F.lit(None).cast("boolean").alias("delivery_on_time"),
    F.col("avg_review_score"),
    F.col("review_sentiment"),
    F.col("has_negative_review"),
    F.lit("low_review_score").alias("return_reason")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

returned_count = returned_events.count()
print(f"order.returned events: {returned_count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 12: Union All Events and Deduplicate
print("STEP 12: UNION AND DEDUPLICATE")
print("=" * 70)

all_events = placed_events.union(fulfilled_events).union(returned_events)

before_dedup = all_events.count()
all_events = all_events.dropDuplicates(["correlation_id", "event_type"])
after_dedup = all_events.count()

print(f"Before deduplication: {before_dedup:,}")
print(f"After deduplication:  {after_dedup:,}")
print(f"Duplicates removed:   {before_dedup - after_dedup:,}")

event_type_dist = all_events.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_type_dist:
    pct = row["count"] / after_dedup * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 13: Write Silver Delta Table
print("STEP 13: WRITE SILVER DELTA TABLE")
print("=" * 70)

all_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 14: Data Quality Validation
print("STEP 14: DATA QUALITY VALIDATION")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()

checks = {
    "null_event_id":        df.filter(F.col("event_id").isNull()).count(),
    "null_event_type":      df.filter(F.col("event_type").isNull()).count(),
    "null_domain":          df.filter(F.col("domain").isNull()).count(),
    "null_occurred_at":     df.filter(F.col("occurred_at").isNull()).count(),
    "null_correlation_id":  df.filter(F.col("correlation_id").isNull()).count(),
    "null_customer_id":     df.filter(F.col("customer_id").isNull()).count(),
    "null_total_amount":    df.filter(F.col("total_amount").isNull()).count(),
    "zero_total_amount":    df.filter(F.col("total_amount") == 0).count(),
}

print(f"\nTotal rows: {total:,}")
print(f"Columns:    {len(df.columns)}")
print("\nNull checks:")
all_passed = True
for check, count in checks.items():
    pct = count / total * 100
    status = "PASS" if count == 0 else "WARN"
    if count > 0:
        all_passed = False
    print(f"  {status} {check}: {count:,} ({pct:.2f}%)")

print(f"\nOverall quality: {'PASS' if all_passed else 'WARN - review above'}")

display(df.select(
    "event_id", "event_type", "domain", "occurred_at",
    "correlation_id", "customer_segment", "order_value_tier",
    "total_amount", "fulfilment_time_days", "fulfilment_bucket"
).limit(10))
