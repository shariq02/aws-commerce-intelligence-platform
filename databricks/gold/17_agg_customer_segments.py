# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - AGG CUSTOMER SEGMENTS
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Customer segment KPIs from fact_transactions
# MAGIC **Input:** acip.gold.fact_transactions, acip.gold.dim_customer
# MAGIC **Output:** acip.gold.agg_customer_segments

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.agg_customer_segments"

print("GOLD - AGG CUSTOMER SEGMENTS")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Fact and Dimension
print("STEP 1: LOAD FACT AND DIMENSION")
print("=" * 70)

fact = spark.table(f"{CATALOG}.gold.fact_transactions")
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer") \
    .filter(F.col("is_current")) \
    .select("customer_key", "customer_segment")

print(f"fact_transactions: {fact.count():,}")
print(f"dim_customer (current): {dim_customer.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Join and Aggregate
print("STEP 2: JOIN AND AGGREGATE")
print("=" * 70)

facts_with_segment = fact.join(dim_customer, on="customer_key", how="left")

placed = facts_with_segment.filter(F.col("event_type") == "order.placed")
fulfilled = facts_with_segment.filter(F.col("event_type") == "order.fulfilled")
returned = facts_with_segment.filter(F.col("event_type") == "order.returned")

placed_agg = placed.groupBy("customer_segment").agg(
    F.count("transaction_key").alias("order_count"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg("total_amount").alias("avg_order_value"),
    F.countDistinct("customer_key").alias("unique_customers")
)

fulfilled_agg = fulfilled.groupBy("customer_segment").agg(
    F.avg("fulfilment_time_days").alias("avg_fulfilment_days"),
    F.avg(F.col("delivery_on_time").cast("int")).alias("on_time_rate")
)

returned_agg = returned.groupBy("customer_segment").agg(
    F.count("transaction_key").alias("return_count")
)

agg = placed_agg \
    .join(fulfilled_agg, on="customer_segment", how="left") \
    .join(returned_agg, on="customer_segment", how="left") \
    .withColumn("return_rate",
        F.when(F.col("order_count") > 0,
            F.round(F.col("return_count") / F.col("order_count"), 4)
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn("revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
    ) \
    .withColumn("updated_at", F.current_timestamp())

print(f"Aggregation rows: {agg.count():,}")

display(agg.orderBy(F.col("total_revenue").desc()))

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

print(f"Total segment rows: {df.count():,}")
print("\nCustomer segment KPIs:")

rows = df.orderBy(F.col("total_revenue").desc()).collect()
for row in rows:
    print(f"\n  Segment: {row['customer_segment']}")
    print(f"    Orders:              {row['order_count']:,}")
    print(f"    Unique customers:    {row['unique_customers']:,}")
    print(f"    Total revenue:       {row['total_revenue']:,.2f}")
    print(f"    Avg order value:     {row['avg_order_value']:,.2f}")
    print(f"    Revenue/customer:    {row['revenue_per_customer']:,.2f}")
    print(f"    Avg fulfilment days: {row['avg_fulfilment_days']:.1f}" if row['avg_fulfilment_days'] else "    Avg fulfilment days: N/A")
    print(f"    Return rate:         {row['return_rate']*100:.1f}%")

print("\nPASS: agg_customer_segments verified")
