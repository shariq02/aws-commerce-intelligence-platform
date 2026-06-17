# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - MARKETPLACE TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Transform Bronze marketplace tables to universal silver.events schema  
# MAGIC **Input:** acip.bronze.marketplace_sellers, marketplace_order_items,
# MAGIC            marketplace_products, marketplace_orders, category_translations  
# MAGIC **Output:** acip.silver.events (mode=APPEND - adds marketplace rows)  
# MAGIC **Rollback:** If this fails, rerun notebook 04 first (overwrite), then rerun 05 and this

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

print("MARKETPLACE SILVER TRANSFORMATION")
print("=" * 70)
print(f"Source: {CATALOG}.{SOURCE}")
print(f"Target: {TARGET_TABLE}")
print(f"Mode: APPEND - adds marketplace rows to existing silver.events")
print(f"Run ID: {RUN_ID}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Bronze Tables
print("STEP 1: LOADING BRONZE TABLES")
print("=" * 70)

sellers = spark.table(f"{CATALOG}.{SOURCE}.marketplace_sellers")
order_items = spark.table(f"{CATALOG}.{SOURCE}.marketplace_order_items")
products = spark.table(f"{CATALOG}.{SOURCE}.marketplace_products")
orders = spark.table(f"{CATALOG}.{SOURCE}.marketplace_orders")
translations = spark.table(f"{CATALOG}.{SOURCE}.category_translations")

print(f"marketplace_sellers:     {sellers.count():,} rows")
print(f"marketplace_order_items: {order_items.count():,} rows")
print(f"marketplace_products:    {products.count():,} rows")
print(f"marketplace_orders:      {orders.count():,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean Sellers
print("STEP 2: CLEAN SELLERS")
print("=" * 70)

sellers_clean = sellers \
    .filter(F.col("seller_id").isNotNull() & (F.trim(F.col("seller_id")) != "")) \
    .withColumn("seller_id", F.trim(F.col("seller_id"))) \
    .withColumn("seller_city", F.initcap(F.lower(F.trim(F.col("seller_city"))))) \
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state")))) \
    .withColumn("seller_region",
        F.when(F.col("seller_state").isin("SP", "RJ", "MG", "ES"), "southeast")
         .when(F.col("seller_state").isin("RS", "SC", "PR"), "south")
         .when(F.col("seller_state").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), "northeast")
         .when(F.col("seller_state").isin("PA", "AM", "RO", "AC", "AP", "RR", "TO"), "north")
         .when(F.col("seller_state").isin("MT", "MS", "GO", "DF"), "center_west")
         .otherwise("unknown")
    )

print(f"Valid sellers: {sellers_clean.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 3: Build Product Category Mapping
print("STEP 3: BUILD PRODUCT CATEGORY MAPPING")
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

# DBTITLE 1,STEP 4: Compute Seller Performance Metrics
print("STEP 4: COMPUTE SELLER PERFORMANCE METRICS")
print("=" * 70)

order_items_clean = order_items \
    .filter(F.col("seller_id").isNotNull()) \
    .filter(F.col("order_id").isNotNull()) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("freight_value", F.col("freight_value").cast("double")) \
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))

seller_metrics = order_items_clean.groupBy("seller_id").agg(
    F.count("order_id").alias("total_orders"),
    F.sum("price").alias("total_revenue"),
    F.countDistinct("product_id").alias("unique_products")
)

quantiles_orders = seller_metrics.approxQuantile("total_orders", [0.5, 0.8], 0.01)
p50_orders = float(quantiles_orders[0])
p80_orders = float(quantiles_orders[1])

quantiles_revenue = seller_metrics.approxQuantile("total_revenue", [0.5, 0.8], 0.01)
p50_revenue = float(quantiles_revenue[0])
p80_revenue = float(quantiles_revenue[1])

seller_metrics = seller_metrics \
    .withColumn("seller_tier",
        F.when(
            (F.col("total_orders") >= p80_orders) &
            (F.col("total_revenue") >= p80_revenue), "platinum"
        ).when(
            (F.col("total_orders") >= p50_orders) |
            (F.col("total_revenue") >= p50_revenue), "gold"
        ).otherwise("standard")
    )

sellers_enriched = sellers_clean.join(seller_metrics, on="seller_id", how="left") \
    .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0))) \
    .withColumn("total_revenue", F.coalesce(F.col("total_revenue").cast("double"), F.lit(0.0))) \
    .withColumn("seller_tier", F.coalesce(F.col("seller_tier"), F.lit("standard")))

tier_dist = sellers_enriched.groupBy("seller_tier").count().collect()
print(f"Sellers enriched: {sellers_enriched.count():,}")
print("\nSeller tier distribution:")
for row in tier_dist:
    pct = row["count"] / sellers_enriched.count() * 100
    print(f"  {row['seller_tier']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 5: Enrich Order Items
print("STEP 5: ENRICH ORDER ITEMS")
print("=" * 70)

orders_clean = orders \
    .filter(F.col("order_id").isNotNull()) \
    .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date"))

items_enriched = order_items_clean \
    .join(
        sellers_enriched.select(
            "seller_id", "seller_tier", "seller_city",
            "seller_state", "seller_region"
        ),
        on="seller_id", how="left"
    ) \
    .join(
        products_translated.select("product_id", "category", "category_group"),
        on="product_id", how="left"
    ) \
    .join(
        orders_clean.select(
            "order_id", "order_purchase_ts", "order_delivered_customer_ts"
        ),
        on="order_id", how="left"
    ) \
    .withColumn("dispatch_time_mins",
        F.when(
            F.col("order_delivered_customer_ts").isNotNull() &
            F.col("order_purchase_ts").isNotNull(),
            F.round(
                (F.unix_timestamp("order_delivered_customer_ts") -
                 F.unix_timestamp("order_purchase_ts")) / 60, 0
            )
        ).otherwise(F.lit(None))
    ) \
    .withColumn("sla_threshold_mins",
        F.when(F.col("seller_tier") == "platinum", F.lit(1440))
         .when(F.col("seller_tier") == "gold", F.lit(2880))
         .otherwise(F.lit(4320))
    ) \
    .withColumn("is_sla_breached",
        F.when(
            F.col("dispatch_time_mins").isNotNull(),
            F.col("dispatch_time_mins") > F.col("sla_threshold_mins")
        ).otherwise(F.lit(False))
    ) \
    .withColumn("dispatch_time_days",
        F.when(F.col("dispatch_time_mins").isNotNull(),
            F.round(F.col("dispatch_time_mins") / 1440, 1)
        ).otherwise(F.lit(None))
    ) \
    .withColumn("dispatch_speed_bucket",
        F.when(F.col("dispatch_time_days") <= 3, "fast")
         .when(F.col("dispatch_time_days") <= 7, "normal")
         .when(F.col("dispatch_time_days") <= 14, "slow")
         .when(F.col("dispatch_time_days").isNotNull(), "very_slow")
         .otherwise("unknown")
    )

print(f"Items enriched: {items_enriched.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 6: Build Event Envelope - listing.created
print("STEP 6: BUILD EVENT ENVELOPE - listing.created")
print("=" * 70)

uuid_udf = F.expr("uuid()")

listing_events = sellers_enriched.select(
    uuid_udf.alias("event_id"),
    F.lit("listing.created").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.current_timestamp().cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("seller_id").alias("correlation_id"),
    F.to_json(F.struct(
        F.col("seller_id"),
        F.col("seller_tier"),
        F.col("seller_city"),
        F.col("seller_state"),
        F.col("seller_region"),
        F.col("total_orders"),
        F.col("total_revenue"),
        F.coalesce(F.col("unique_products"), F.lit(0)).alias("unique_products"),
        F.lit(None).cast("string").alias("product_id"),
        F.lit(None).cast("string").alias("category"),
        F.lit(None).cast("string").alias("category_group"),
        F.lit(None).cast("double").alias("price"),
        F.lit(None).cast("double").alias("freight_value"),
        F.lit(None).cast("double").alias("dispatch_time_mins"),
        F.lit(None).cast("double").alias("dispatch_time_days"),
        F.lit(None).cast("int").alias("sla_threshold_mins"),
        F.lit(False).alias("is_sla_breached"),
        F.lit(None).cast("string").alias("dispatch_speed_bucket"),
        F.lit(None).cast("double").alias("old_price"),
        F.lit(None).cast("double").alias("new_price"),
        F.lit(None).cast("double").alias("change_pct")
    )).alias("payload")
).filter(F.col("correlation_id").isNotNull())

print(f"listing.created events: {listing_events.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 7: Build Event Envelope - seller.order.dispatched
print("STEP 7: BUILD EVENT ENVELOPE - seller.order.dispatched")
print("=" * 70)

dispatch_events = items_enriched.filter(
    F.col("order_delivered_customer_ts").isNotNull()
).select(
    uuid_udf.alias("event_id"),
    F.lit("seller.order.dispatched").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_delivered_customer_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.to_json(F.struct(
        F.col("seller_id"),
        F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
        F.col("seller_city"),
        F.col("seller_state"),
        F.col("seller_region"),
        F.lit(None).cast("double").alias("total_orders"),
        F.lit(None).cast("double").alias("total_revenue"),
        F.lit(None).cast("long").alias("unique_products"),
        F.col("product_id"),
        F.coalesce(F.col("category"), F.lit("other")).alias("category"),
        F.coalesce(F.col("category_group"), F.lit("other")).alias("category_group"),
        F.col("price"),
        F.col("freight_value"),
        F.col("dispatch_time_mins"),
        F.col("dispatch_time_days"),
        F.col("sla_threshold_mins"),
        F.col("is_sla_breached"),
        F.col("dispatch_speed_bucket"),
        F.lit(None).cast("double").alias("old_price"),
        F.lit(None).cast("double").alias("new_price"),
        F.lit(None).cast("double").alias("change_pct")
    )).alias("payload")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

dispatch_count = dispatch_events.count()
sla_breached = items_enriched.filter(F.col("is_sla_breached")).count()
print(f"seller.order.dispatched events: {dispatch_count:,}")
print(f"SLA breached: {sla_breached:,} ({sla_breached/max(dispatch_count,1)*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 8: Build Event Envelope - price.updated
print("STEP 8: BUILD EVENT ENVELOPE - price.updated")
print("=" * 70)

price_events = items_enriched.filter(
    F.col("price").isNotNull() &
    F.col("order_purchase_ts").isNotNull()
).withColumn(
    "old_price", F.round(F.col("price") * (1 + (F.rand() - 0.5) * 0.4), 2)
).withColumn(
    "change_pct",
    F.round((F.col("price") - F.col("old_price")) / F.col("old_price") * 100, 2)
).filter(F.abs(F.col("change_pct")) >= 2.0).select(
    uuid_udf.alias("event_id"),
    F.lit("price.updated").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.concat_ws("-", F.col("order_id"), F.col("product_id")).alias("correlation_id"),
    F.to_json(F.struct(
        F.col("seller_id"),
        F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
        F.col("seller_city"),
        F.col("seller_state"),
        F.col("seller_region"),
        F.lit(None).cast("double").alias("total_orders"),
        F.lit(None).cast("double").alias("total_revenue"),
        F.lit(None).cast("long").alias("unique_products"),
        F.col("product_id"),
        F.coalesce(F.col("category"), F.lit("other")).alias("category"),
        F.coalesce(F.col("category_group"), F.lit("other")).alias("category_group"),
        F.col("price"),
        F.col("freight_value"),
        F.lit(None).cast("double").alias("dispatch_time_mins"),
        F.lit(None).cast("double").alias("dispatch_time_days"),
        F.lit(None).cast("int").alias("sla_threshold_mins"),
        F.lit(False).alias("is_sla_breached"),
        F.lit(None).cast("string").alias("dispatch_speed_bucket"),
        F.col("old_price"),
        F.col("price").alias("new_price"),
        F.col("change_pct")
    )).alias("payload")
).filter(
    F.col("correlation_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

print(f"price.updated events: {price_events.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 9: Union All Events and Deduplicate
print("STEP 9: UNION AND DEDUPLICATE")
print("=" * 70)

all_events = listing_events.union(dispatch_events).union(price_events)
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

# DBTITLE 1,STEP 10: Append to silver.events
print("STEP 10: APPEND TO silver.events")
print("=" * 70)
print("NOTE: Mode=APPEND adds marketplace rows.")
print("If this fails, rerun notebook 04 first, then rerun 05 and this.")

all_events.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TARGET_TABLE)

total = spark.table(TARGET_TABLE).count()
print(f"\nPASS: {TARGET_TABLE} total rows after append - {total:,}")

# COMMAND ----------

# DBTITLE 1,STEP 11: Write Watermark
print("STEP 11: WRITE WATERMARK")
print("=" * 70)

spark.sql(f"""
    INSERT INTO {CATALOG}.quality.pipeline_watermarks VALUES (
        '{RUN_ID}',
        'marketplace',
        'processing',
        '06_marketplace_silver_transform',
        current_timestamp(),
        null,
        {after_dedup},
        'COMPLETE',
        current_timestamp(),
        current_timestamp()
    )
""")

print(f"PASS: Watermark written for run_id={RUN_ID} domain=marketplace")

# COMMAND ----------

# DBTITLE 1,STEP 12: Data Quality Checks
print("STEP 12: DATA QUALITY CHECKS")
print("=" * 70)

df = spark.table(TARGET_TABLE).filter(F.col("domain") == "marketplace")
total = df.count()

checks = {
    "null_event_id":       df.filter(F.col("event_id").isNull()).count(),
    "null_occurred_at":    df.filter(F.col("occurred_at").isNull()).count(),
    "null_correlation_id": df.filter(F.col("correlation_id").isNull()).count(),
    "null_payload":        df.filter(F.col("payload").isNull()).count(),
}

print(f"Marketplace rows in silver.events: {total:,}")
print("\nNull checks (marketplace rows only):")
all_passed = True
for check, count in checks.items():
    pct = count / total * 100 if total > 0 else 0
    status = "PASS" if count == 0 else "FAIL"
    if count > 0:
        all_passed = False
    print(f"  {status} {check}: {count:,} ({pct:.2f}%)")

print(f"\nOverall: {'PASS' if all_passed else 'FAIL - review above'}")

display(df.select("event_id", "event_type", "domain", "occurred_at", "correlation_id", "payload").limit(5))
