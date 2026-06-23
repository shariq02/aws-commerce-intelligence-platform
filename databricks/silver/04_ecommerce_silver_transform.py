# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - ECOMMERCE TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Transform Bronze ecommerce tables to universal silver.events schema  
# MAGIC **Input:** acip.bronze.orders, order_items, order_payments, customers,
# MAGIC            products, order_reviews, category_translations  
# MAGIC **Output:** acip.silver.events (mode=OVERWRITE - resets silver.events entirely)  
# MAGIC **Rollback:** If this notebook fails, rerun it - overwrite is idempotent

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
SOURCE = "bronze"
TARGET_TABLE = f"{CATALOG}.silver.events"
RUN_ID = "manual"

print("ECOMMERCE SILVER TRANSFORMATION")
print("=" * 70)
print(f"Source: {CATALOG}.{SOURCE}")
print(f"Target: {TARGET_TABLE}")
print(f"Mode: OVERWRITE - resets silver.events entirely")
print(f"Run ID: {RUN_ID}")
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

print(f"orders:                {orders.count():,} rows")
print(f"order_items:           {items.count():,} rows")
print(f"order_payments:        {payments.count():,} rows")
print(f"customers:             {customers.count():,} rows")
print(f"products:              {products.count():,} rows")
print(f"order_reviews:         {reviews.count():,} rows")
print(f"category_translations: {translations.count():,} rows")

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
    .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_estimated_delivery_ts", F.to_timestamp("order_estimated_delivery_date")) \
    .withColumn("order_approved_ts", F.to_timestamp("order_approved_at")) \
    .withColumn("order_delivered_carrier_ts", F.to_timestamp("order_delivered_carrier_date")) \
    .withColumn("order_status_clean",
        F.when(F.col("order_status") == "delivered", "completed")
         .when(F.col("order_status").isin("shipped", "processing", "approved", "created", "invoiced"), "pending")
         .when(F.col("order_status").isin("canceled", "unavailable"), "failed")
         .otherwise(F.col("order_status"))
    )

total = orders_clean.count()
delivered = orders_clean.filter(F.col("order_delivered_customer_ts").isNotNull()).count()
cancelled = orders_clean.filter(F.col("order_status").isin("canceled", "unavailable")).count()

print(f"Total valid orders: {total:,}")
print(f"Delivered:          {delivered:,} ({delivered/total*100:.1f}%)")
print(f"Cancelled:          {cancelled:,} ({cancelled/total*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 3: Build Payment Aggregates
print("STEP 3: BUILD PAYMENT AGGREGATES")
print("=" * 70)

payments_clean = payments \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("payment_value").isNotNull()) \
    .filter(F.col("payment_value").cast("double") > 0) \
    .withColumn("payment_value", F.col("payment_value").cast("double")) \
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))

payment_totals = payments_clean.groupBy("order_id").agg(
    F.round(F.sum("payment_value"), 2).alias("total_amount"),
    F.max("payment_installments").alias("max_installments"),
    F.first("payment_type").alias("payment_method")
).withColumn(
    "is_installment", F.col("max_installments") > 1
).withColumn(
    "payment_method_clean",
    F.when(F.col("payment_method") == "credit_card", "credit_card")
     .when(F.col("payment_method") == "boleto", "boleto")
     .when(F.col("payment_method") == "voucher", "voucher")
     .when(F.col("payment_method") == "debit_card", "debit_card")
     .otherwise("other")
)

print(f"Orders with payments: {payment_totals.count():,}")

method_dist = payments_clean.groupBy("payment_type").count().collect()
print("\nPayment method distribution:")
for row in method_dist:
    pct = row["count"] / payments_clean.count() * 100
    print(f"  {row['payment_type']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Build Product Category Mapping
print("STEP 4: BUILD PRODUCT CATEGORY MAPPING")
print("=" * 70)

products_translated = products \
    .filter(F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != "")) \
    .join(
        translations.withColumnRenamed("product_category_name_english", "category_english"),
        on="product_category_name", how="left"
    ).select(
        "product_id",
        F.coalesce(
            F.col("category_english"),
            F.col("product_category_name"),
            F.lit("other")
        ).alias("category")
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

# COMMAND ----------

# DBTITLE 1,STEP 5: Build Order Items Aggregates
print("STEP 5: BUILD ORDER ITEMS AGGREGATES")
print("=" * 70)

items_enriched = items \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("product_id").isNotNull()) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("freight_value", F.col("freight_value").cast("double")) \
    .join(
        products_translated.select("product_id", "category", "category_group"),
        on="product_id", how="left"
    )

items_per_order = items_enriched.groupBy("order_id").agg(
    F.count("order_item_id").alias("item_count"),
    F.countDistinct("seller_id").alias("seller_count"),
    F.first("seller_id").alias("primary_seller_id")
).withColumn("is_multi_seller", F.col("seller_count") > 1) \
 .withColumn("is_multi_item", F.col("item_count") > 1)

print(f"Orders with items: {items_per_order.count():,}")

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

# COMMAND ----------

# DBTITLE 1,STEP 7: Clean Reviews
print("STEP 7: CLEAN REVIEWS")
print("=" * 70)

reviews_clean = reviews \
    .filter(F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")) \
    .filter(F.col("review_score").isNotNull()) \
    .withColumn("review_score", F.expr("try_cast(review_score as int)")) \
    .filter(F.col("review_score").isNotNull()) \
    .filter(F.col("review_score").between(1, 5)) \
    .withColumn("review_sentiment",
        F.when(F.col("review_score") >= 4, "positive")
         .when(F.col("review_score") == 3, "neutral")
         .otherwise("negative")
    ) \
    .withColumn("is_negative_review", F.col("review_score") <= 2)

reviews_per_order = reviews_clean.groupBy("order_id").agg(
    F.avg("review_score").alias("avg_review_score"),
    F.first("review_sentiment").alias("review_sentiment"),
    F.max(F.col("is_negative_review").cast("int")).alias("has_negative_review_int")
).withColumn(
    "has_negative_review", F.col("has_negative_review_int") == 1
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
            "customer_state", "region", "state_region"
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

# COMMAND ----------

# DBTITLE 1,STEP 9: Build Universal Event Envelope - order.placed
print("STEP 9: BUILD EVENT ENVELOPE - order.placed")
print("=" * 70)

uuid_udf = F.expr("uuid()")

placed_events = orders_enriched.select(
    uuid_udf.alias("event_id"),
    F.lit("order.placed").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.to_json(F.struct(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_segment"),
        F.col("order_value_tier"),
        F.col("region"),
        F.col("state_region"),
        F.col("customer_state"),
        F.col("total_amount"),
        F.coalesce(F.col("payment_method_clean"), F.lit("unknown")).alias("payment_method"),
        F.coalesce(F.col("is_installment"), F.lit(False)).alias("is_installment"),
        F.coalesce(F.col("max_installments"), F.lit(1)).alias("max_installments"),
        F.col("order_status_clean").alias("order_status"),
        F.col("item_count"),
        F.coalesce(F.col("is_multi_item"), F.lit(False)).alias("is_multi_item"),
        F.coalesce(F.col("is_multi_seller"), F.lit(False)).alias("is_multi_seller"),
        F.col("primary_seller_id"),
        F.lit(None).cast("double").alias("fulfilment_time_mins"),
        F.lit(None).cast("double").alias("fulfilment_time_days"),
        F.lit(None).cast("string").alias("fulfilment_bucket"),
        F.lit(None).cast("boolean").alias("delivery_on_time"),
        F.lit(None).cast("double").alias("avg_review_score"),
        F.lit(None).cast("string").alias("review_sentiment"),
        F.lit(None).cast("boolean").alias("has_negative_review"),
        F.lit(None).cast("string").alias("return_reason")
    )).alias("payload")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

placed_count = placed_events.count()
print(f"order.placed events: {placed_count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 10: Build Universal Event Envelope - order.fulfilled
print("STEP 10: BUILD EVENT ENVELOPE - order.fulfilled")
print("=" * 70)

fulfilled_events = orders_enriched.filter(
    F.col("order_delivered_customer_ts").isNotNull()
).select(
    uuid_udf.alias("event_id"),
    F.lit("order.fulfilled").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_delivered_customer_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.to_json(F.struct(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_segment"),
        F.col("order_value_tier"),
        F.col("region"),
        F.col("state_region"),
        F.col("customer_state"),
        F.col("total_amount"),
        F.coalesce(F.col("payment_method_clean"), F.lit("unknown")).alias("payment_method"),
        F.coalesce(F.col("is_installment"), F.lit(False)).alias("is_installment"),
        F.coalesce(F.col("max_installments"), F.lit(1)).alias("max_installments"),
        F.lit("completed").alias("order_status"),
        F.col("item_count"),
        F.coalesce(F.col("is_multi_item"), F.lit(False)).alias("is_multi_item"),
        F.coalesce(F.col("is_multi_seller"), F.lit(False)).alias("is_multi_seller"),
        F.col("primary_seller_id"),
        F.col("fulfilment_time_mins"),
        F.col("fulfilment_time_days"),
        F.col("fulfilment_bucket"),
        F.col("delivery_on_time"),
        F.col("avg_review_score"),
        F.col("review_sentiment"),
        F.col("has_negative_review"),
        F.lit(None).cast("string").alias("return_reason")
    )).alias("payload")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

fulfilled_count = fulfilled_events.count()
print(f"order.fulfilled events: {fulfilled_count:,}")

fulfilment_dist = fulfilled_events.select(
    F.get_json_object(F.col("payload"), "$.fulfilment_bucket").alias("bucket")
).groupBy("bucket").count().collect()
print("\nFulfilment bucket distribution:")
for row in fulfilment_dist:
    pct = row["count"] / fulfilled_count * 100
    print(f"  {row['bucket']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 11: Build Universal Event Envelope - order.returned
print("STEP 11: BUILD EVENT ENVELOPE - order.returned")
print("=" * 70)

returned_events = orders_enriched.filter(
    F.col("has_negative_review") == True
).select(
    uuid_udf.alias("event_id"),
    F.lit("order.returned").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("ecommerce").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.to_json(F.struct(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_segment"),
        F.col("order_value_tier"),
        F.col("region"),
        F.col("state_region"),
        F.col("customer_state"),
        F.col("total_amount"),
        F.coalesce(F.col("payment_method_clean"), F.lit("unknown")).alias("payment_method"),
        F.coalesce(F.col("is_installment"), F.lit(False)).alias("is_installment"),
        F.coalesce(F.col("max_installments"), F.lit(1)).alias("max_installments"),
        F.lit("returned").alias("order_status"),
        F.col("item_count"),
        F.coalesce(F.col("is_multi_item"), F.lit(False)).alias("is_multi_item"),
        F.coalesce(F.col("is_multi_seller"), F.lit(False)).alias("is_multi_seller"),
        F.col("primary_seller_id"),
        F.lit(None).cast("double").alias("fulfilment_time_mins"),
        F.lit(None).cast("double").alias("fulfilment_time_days"),
        F.lit(None).cast("string").alias("fulfilment_bucket"),
        F.lit(None).cast("boolean").alias("delivery_on_time"),
        F.col("avg_review_score"),
        F.col("review_sentiment"),
        F.col("has_negative_review"),
        F.lit("low_review_score").alias("return_reason")
    )).alias("payload")
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

event_dist = all_events.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_dist:
    pct = row["count"] / after_dedup * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 13: Write silver.events - OVERWRITE
print("STEP 13: WRITE silver.events - OVERWRITE")
print("=" * 70)
print("NOTE: This resets silver.events entirely.")
print("If this fails, simply rerun this notebook.")

all_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"\nPASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 14: Write Watermark
print("STEP 14: WRITE WATERMARK")
print("=" * 70)

spark.sql(f"""
    INSERT INTO {CATALOG}.quality.pipeline_watermarks VALUES (
        '{RUN_ID}',
        'ecommerce',
        'processing',
        '04_ecommerce_silver_transform',
        current_timestamp(),
        null,
        {written},
        'COMPLETE',
        current_timestamp(),
        current_timestamp()
    )
""")

print(f"PASS: Watermark written for run_id={RUN_ID} domain=ecommerce")

# COMMAND ----------

# DBTITLE 1,STEP 15: Data Quality Checks
print("STEP 15: DATA QUALITY CHECKS")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()

checks = {
    "null_event_id":       df.filter(F.col("event_id").isNull()).count(),
    "null_event_type":     df.filter(F.col("event_type").isNull()).count(),
    "null_domain":         df.filter(F.col("domain").isNull()).count(),
    "null_occurred_at":    df.filter(F.col("occurred_at").isNull()).count(),
    "null_correlation_id": df.filter(F.col("correlation_id").isNull()).count(),
    "null_payload":        df.filter(F.col("payload").isNull()).count(),
}

print(f"Total rows: {total:,}")
print(f"Columns:    {len(df.columns)}")
print("\nNull checks:")
all_passed = True
for check, count in checks.items():
    pct = count / total * 100
    status = "PASS" if count == 0 else "FAIL"
    if count > 0:
        all_passed = False
    print(f"  {status} {check}: {count:,} ({pct:.2f}%)")

print(f"\nOverall: {'PASS' if all_passed else 'FAIL - review above'}")

display(df.select(
    "event_id", "event_type", "domain",
    "occurred_at", "correlation_id", "payload"
).limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 16: Write Flat silver.ecommerce_orders
print("STEP 16: WRITE FLAT silver.ecommerce_orders")
print("=" * 70)
print("Parallel flat materialisation of silver.events ecommerce rows.")
print("silver.events is untouched. This table serves Power BI and Grafana directly.")
print("Gold notebooks continue to read from silver.events -- no Gold changes needed.")

FLAT_TARGET = f"{CATALOG}.silver.ecommerce_orders"

ecommerce_flat = all_events.select(
    F.col("event_id"),
    F.col("event_type"),
    F.col("domain"),
    F.col("occurred_at"),
    F.col("correlation_id").alias("order_id"),
    F.get_json_object(F.col("payload"), "$.customer_id").alias("customer_id"),
    F.get_json_object(F.col("payload"), "$.customer_unique_id").alias("customer_unique_id"),
    F.get_json_object(F.col("payload"), "$.customer_segment").alias("customer_segment"),
    F.get_json_object(F.col("payload"), "$.order_value_tier").alias("order_value_tier"),
    F.get_json_object(F.col("payload"), "$.region").alias("region"),
    F.get_json_object(F.col("payload"), "$.state_region").alias("state_region"),
    F.get_json_object(F.col("payload"), "$.customer_state").alias("customer_state"),
    F.get_json_object(F.col("payload"), "$.total_amount").cast("double").alias("total_amount"),
    F.get_json_object(F.col("payload"), "$.payment_method").alias("payment_method"),
    F.get_json_object(F.col("payload"), "$.is_installment").cast("boolean").alias("is_installment"),
    F.get_json_object(F.col("payload"), "$.max_installments").cast("int").alias("max_installments"),
    F.get_json_object(F.col("payload"), "$.order_status").alias("order_status"),
    F.get_json_object(F.col("payload"), "$.item_count").cast("int").alias("item_count"),
    F.get_json_object(F.col("payload"), "$.is_multi_item").cast("boolean").alias("is_multi_item"),
    F.get_json_object(F.col("payload"), "$.is_multi_seller").cast("boolean").alias("is_multi_seller"),
    F.get_json_object(F.col("payload"), "$.primary_seller_id").alias("primary_seller_id"),
    F.get_json_object(F.col("payload"), "$.fulfilment_time_mins").cast("double").alias("fulfilment_time_mins"),
    F.get_json_object(F.col("payload"), "$.fulfilment_time_days").cast("double").alias("fulfilment_time_days"),
    F.get_json_object(F.col("payload"), "$.fulfilment_bucket").alias("fulfilment_bucket"),
    F.get_json_object(F.col("payload"), "$.delivery_on_time").cast("boolean").alias("delivery_on_time"),
    F.get_json_object(F.col("payload"), "$.avg_review_score").cast("double").alias("avg_review_score"),
    F.get_json_object(F.col("payload"), "$.review_sentiment").alias("review_sentiment"),
    F.get_json_object(F.col("payload"), "$.has_negative_review").cast("boolean").alias("has_negative_review"),
    F.get_json_object(F.col("payload"), "$.return_reason").alias("return_reason"),
)

ecommerce_flat.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FLAT_TARGET)

flat_count = spark.table(FLAT_TARGET).count()
print(f"\nPASS: {FLAT_TARGET} written - {flat_count:,} rows")

flat_checks = {
    "null_event_id":    spark.table(FLAT_TARGET).filter(F.col("event_id").isNull()).count(),
    "null_order_id":    spark.table(FLAT_TARGET).filter(F.col("order_id").isNull()).count(),
    "null_total_amount": spark.table(FLAT_TARGET).filter(F.col("total_amount").isNull()).count(),
    "null_customer_id": spark.table(FLAT_TARGET).filter(F.col("customer_id").isNull()).count(),
}
for check, count in flat_checks.items():
    status = "PASS" if count == 0 else "WARN"
    print(f"  {status} {check}: {count:,}")

display(spark.table(FLAT_TARGET).limit(3))
