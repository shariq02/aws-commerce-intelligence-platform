# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - MARKETPLACE TRANSFORMATION
# MAGIC ### Bronze to Silver with Full Feature Extraction
# MAGIC
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Transform raw Olist seller Bronze tables into enriched Silver event layer
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - acip.bronze.marketplace_sellers
# MAGIC - acip.bronze.marketplace_order_items
# MAGIC - acip.bronze.marketplace_products
# MAGIC - acip.bronze.marketplace_orders
# MAGIC - acip.bronze.category_translations
# MAGIC
# MAGIC **Output Table:** acip.silver.marketplace_events
# MAGIC
# MAGIC **Run Order:**
# MAGIC ```
# MAGIC 1. databricks/bronze/03_load_marketplace_bronze.py
# MAGIC 2. databricks/silver/06_marketplace_silver_transform.py  (THIS SCRIPT)
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
TARGET_TABLE = f"{CATALOG}.{TARGET}.marketplace_events"

print("MARKETPLACE SILVER TRANSFORMATION")
print("=" * 70)
print(f"Source: {CATALOG}.{SOURCE}")
print(f"Target: {TARGET_TABLE}")
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
print(f"category_translations:   {translations.count():,} rows")
print(f"\nSellers columns: {sellers.columns}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean Sellers
print("STEP 2: CLEAN SELLERS")
print("=" * 70)

sellers_clean = sellers \
    .filter(F.col("seller_id").isNotNull() & (F.trim(F.col("seller_id")) != "")) \
    .withColumn("seller_id", F.trim(F.col("seller_id"))) \
    .withColumn("seller_city", F.initcap(F.lower(F.trim(F.col("seller_city"))))) \
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state")))) \
    .withColumn("seller_zip_prefix", F.trim(F.col("seller_zip_code_prefix"))) \
    .withColumn("seller_region",
        F.when(F.col("seller_state").isin("SP", "RJ", "MG", "ES"), "southeast")
         .when(F.col("seller_state").isin("RS", "SC", "PR"), "south")
         .when(F.col("seller_state").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), "northeast")
         .when(F.col("seller_state").isin("PA", "AM", "RO", "AC", "AP", "RR", "TO"), "north")
         .when(F.col("seller_state").isin("MT", "MS", "GO", "DF"), "center_west")
         .otherwise("unknown")
    )

print(f"Valid sellers: {sellers_clean.count():,}")

state_dist = sellers_clean.groupBy("seller_state").count().orderBy(
    F.col("count").desc()
).limit(5).collect()
print("\nTop 5 seller states:")
for row in state_dist:
    pct = row["count"] / sellers_clean.count() * 100
    print(f"  {row['seller_state']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 3: Build Product Category Mapping
print("STEP 3: BUILD PRODUCT CATEGORY MAPPING")
print("=" * 70)

products_clean = products \
    .filter(F.col("product_id").isNotNull() & (F.trim(F.col("product_id")) != "")) \
    .withColumn("product_id", F.trim(F.col("product_id"))) \
    .withColumn("product_weight_g", F.col("product_weight_g").cast("double")) \
    .withColumn("product_photos_qty", F.col("product_photos_qty").cast("int")) \
    .withColumn("product_name_length", F.col("product_name_length").cast("int")) \
    .withColumn("product_description_length", F.col("product_description_length").cast("int"))

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
    "product_photos_qty",
    "product_name_length"
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
) \
.withColumn("is_heavy_product", F.col("product_weight_g") > 5000) \
.withColumn("has_good_listing",
    F.col("product_photos_qty") >= 3
)

print(f"Products with categories: {products_translated.count():,}")

category_dist = products_translated.groupBy("category_group").count().collect()
print("\nCategory group distribution:")
for row in category_dist:
    pct = row["count"] / products_translated.count() * 100
    print(f"  {row['category_group']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Compute Seller Performance Metrics
print("STEP 4: COMPUTE SELLER PERFORMANCE METRICS")
print("=" * 70)

order_items_clean = order_items \
    .filter(F.col("seller_id").isNotNull() & (F.trim(F.col("seller_id")) != "")) \
    .filter(F.col("order_id").isNotNull()) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("freight_value", F.col("freight_value").cast("double")) \
    .withColumn("order_item_id", F.col("order_item_id").cast("int")) \
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))

seller_metrics = order_items_clean.groupBy("seller_id").agg(
    F.count("order_id").alias("total_orders"),
    F.countDistinct("order_id").alias("unique_orders"),
    F.sum("price").alias("total_revenue"),
    F.avg("price").alias("avg_order_value"),
    F.max("price").alias("max_order_value"),
    F.min("price").alias("min_order_value"),
    F.sum("freight_value").alias("total_freight_charged"),
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
    ) \
    .withColumn("is_high_volume",
        F.col("total_orders") >= p80_orders
    ) \
    .withColumn("is_high_revenue",
        F.col("total_revenue") >= p80_revenue
    ) \
    .withColumn("avg_freight_per_order",
        F.round(F.col("total_freight_charged") / F.col("total_orders"), 2)
    )

sellers_enriched = sellers_clean.join(
    seller_metrics, on="seller_id", how="left"
).withColumn(
    "total_orders", F.coalesce(F.col("total_orders"), F.lit(0))
).withColumn(
    "total_revenue", F.coalesce(F.col("total_revenue").cast("double"), F.lit(0.0))
).withColumn(
    "seller_tier", F.coalesce(F.col("seller_tier"), F.lit("standard"))
)

print(f"Sellers with metrics: {sellers_enriched.count():,}")
print(f"\nSeller tier thresholds:")
print(f"  Orders: P50={p50_orders:.0f}, P80={p80_orders:.0f}")
print(f"  Revenue: P50=BRL {p50_revenue:.2f}, P80=BRL {p80_revenue:.2f}")

tier_dist = sellers_enriched.groupBy("seller_tier").count().collect()
print("\nSeller tier distribution:")
for row in tier_dist:
    pct = row["count"] / sellers_enriched.count() * 100
    print(f"  {row['seller_tier']}: {row['count']:,} ({pct:.1f}%)")

display(sellers_enriched.select(
    "seller_id", "seller_state", "seller_tier",
    "total_orders", "total_revenue", "unique_products"
).limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 5: Enrich Order Items
print("STEP 5: ENRICH ORDER ITEMS")
print("=" * 70)

orders_clean = orders \
    .filter(F.col("order_id").isNotNull()) \
    .withColumn("order_id", F.trim(F.col("order_id"))) \
    .withColumn("order_purchase_ts", F.to_timestamp("order_purchase_timestamp")) \
    .withColumn("order_delivered_customer_ts", F.to_timestamp("order_delivered_customer_date")) \
    .withColumn("order_approved_ts", F.to_timestamp("order_approved_at"))

items_enriched = order_items_clean \
    .join(
        sellers_enriched.select(
            "seller_id", "seller_tier", "seller_city",
            "seller_state", "seller_region", "total_orders",
            "total_revenue", "is_high_volume", "is_high_revenue"
        ),
        on="seller_id", how="left"
    ) \
    .join(
        products_translated.select(
            "product_id", "category", "category_group",
            "product_weight_g", "is_heavy_product", "has_good_listing"
        ),
        on="product_id", how="left"
    ) \
    .join(
        orders_clean.select(
            "order_id", "order_purchase_ts",
            "order_delivered_customer_ts", "order_approved_ts"
        ),
        on="order_id", how="left"
    )

items_enriched = items_enriched \
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
    ) \
    .withColumn("freight_ratio",
        F.when(F.col("price") > 0,
            F.round(F.col("freight_value") / F.col("price"), 4)
        ).otherwise(F.lit(None))
    ) \
    .withColumn("is_high_freight",
        F.col("freight_ratio") > 0.3
    )

print(f"Enriched order items: {items_enriched.count():,}")

sla_breach_count = items_enriched.filter(F.col("is_sla_breached")).count()
sla_total = items_enriched.filter(F.col("dispatch_time_mins").isNotNull()).count()
print(f"\nSLA breach rate: {sla_breach_count:,}/{sla_total:,} ({sla_breach_count/max(sla_total,1)*100:.1f}%)")

dispatch_dist = items_enriched.groupBy("dispatch_speed_bucket").count().collect()
print("\nDispatch speed distribution:")
for row in dispatch_dist:
    pct = row["count"] / items_enriched.count() * 100
    print(f"  {row['dispatch_speed_bucket']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 6: Build Event Envelope - listing.created
print("STEP 6: BUILD EVENT ENVELOPE - listing.created")
print("=" * 70)

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
    F.col("seller_region"),
    F.col("total_orders"),
    F.col("total_revenue"),
    F.col("unique_products"),
    F.col("is_high_volume"),
    F.col("is_high_revenue"),
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
    F.lit(None).cast("double").alias("change_pct"),
    F.lit(None).cast("boolean").alias("is_heavy_product"),
    F.lit(None).cast("boolean").alias("is_high_freight")
).filter(F.col("seller_id").isNotNull())

listing_count = listing_events.count()
print(f"listing.created events: {listing_count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 7: Build Event Envelope - seller.order.dispatched
print("STEP 7: BUILD EVENT ENVELOPE - seller.order.dispatched")
print("=" * 70)

dispatch_events = items_enriched.filter(
    F.col("order_delivered_customer_ts").isNotNull()
).select(
    uuid_udf().alias("event_id"),
    F.lit("seller.order.dispatched").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_delivered_customer_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.col("order_id").alias("correlation_id"),
    F.col("seller_id"),
    F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.col("seller_region"),
    F.lit(None).cast("double").alias("total_orders"),
    F.lit(None).cast("double").alias("total_revenue"),
    F.lit(None).cast("long").alias("unique_products"),
    F.col("is_high_volume"),
    F.col("is_high_revenue"),
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
    F.lit(None).cast("double").alias("change_pct"),
    F.col("is_heavy_product"),
    F.col("is_high_freight")
).filter(
    F.col("seller_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

dispatch_count = dispatch_events.count()
sla_breached = dispatch_events.filter(F.col("is_sla_breached")).count()
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
    "old_price",
    F.round(F.col("price") * (1 + (F.rand() - 0.5) * 0.4), 2)
).withColumn(
    "change_pct",
    F.round(
        (F.col("price") - F.col("old_price")) / F.col("old_price") * 100, 2
    )
).filter(
    F.abs(F.col("change_pct")) >= 2.0
).select(
    uuid_udf().alias("event_id"),
    F.lit("price.updated").alias("event_type"),
    F.lit("1.0").alias("event_version"),
    F.lit("marketplace").alias("domain"),
    F.lit("olist-replay").alias("source_system"),
    F.col("order_purchase_ts").cast("string").alias("occurred_at"),
    F.current_timestamp().cast("string").alias("ingested_at"),
    F.concat_ws("-", F.col("order_id"), F.col("product_id")).alias("correlation_id"),
    F.col("seller_id"),
    F.coalesce(F.col("seller_tier"), F.lit("standard")).alias("seller_tier"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.col("seller_region"),
    F.lit(None).cast("double").alias("total_orders"),
    F.lit(None).cast("double").alias("total_revenue"),
    F.lit(None).cast("long").alias("unique_products"),
    F.col("is_high_volume"),
    F.col("is_high_revenue"),
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
    F.col("change_pct"),
    F.col("is_heavy_product"),
    F.col("is_high_freight")
).withColumn("price_direction",
    F.when(F.col("change_pct") > 0, "increase").otherwise("decrease")
).withColumn("is_significant_change",
    F.abs(F.col("change_pct")) >= 20.0
).filter(
    F.col("seller_id").isNotNull() &
    F.col("occurred_at").isNotNull()
)

price_count = price_events.count()
significant = price_events.filter(F.col("is_significant_change")).count()
print(f"price.updated events: {price_count:,}")
print(f"Significant changes (>20%): {significant:,} ({significant/max(price_count,1)*100:.1f}%)")

direction_dist = price_events.groupBy("price_direction").count().collect()
print("\nPrice direction distribution:")
for row in direction_dist:
    pct = row["count"] / price_count * 100
    print(f"  {row['price_direction']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 9: Union All Events and Deduplicate
print("STEP 9: UNION AND DEDUPLICATE")
print("=" * 70)

listing_base = listing_events.drop("price_direction", "is_significant_change") \
    if "price_direction" in listing_events.columns else listing_events
dispatch_base = dispatch_events.drop("price_direction", "is_significant_change") \
    if "price_direction" in dispatch_events.columns else dispatch_events
price_base = price_events.drop("price_direction", "is_significant_change")

all_events = listing_base.union(dispatch_base).union(price_base)

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

tier_dist = all_events.groupBy("seller_tier").count().collect()
print("\nSeller tier distribution:")
for row in tier_dist:
    pct = row["count"] / after_dedup * 100
    print(f"  {row['seller_tier']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 10: Write Silver Delta Table
print("STEP 10: WRITE SILVER DELTA TABLE")
print("=" * 70)

all_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 11: Data Quality Validation
print("STEP 11: DATA QUALITY VALIDATION")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()

checks = {
    "null_event_id":       df.filter(F.col("event_id").isNull()).count(),
    "null_event_type":     df.filter(F.col("event_type").isNull()).count(),
    "null_domain":         df.filter(F.col("domain").isNull()).count(),
    "null_occurred_at":    df.filter(F.col("occurred_at").isNull()).count(),
    "null_correlation_id": df.filter(F.col("correlation_id").isNull()).count(),
    "null_seller_id":      df.filter(F.col("seller_id").isNull()).count(),
    "null_seller_tier":    df.filter(F.col("seller_tier").isNull()).count(),
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
    "event_id", "event_type", "seller_id", "seller_tier",
    "category", "dispatch_time_days", "is_sla_breached",
    "dispatch_speed_bucket", "occurred_at"
).limit(10))
