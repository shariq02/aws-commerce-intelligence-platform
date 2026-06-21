# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - DIM CUSTOMER (SCD2)
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Build customer dimension with SCD2 on customer_segment changes
# MAGIC **Input:** acip.silver.events (ecommerce domain)
# MAGIC **Output:** acip.gold.dim_customer (SCD2)
# MAGIC **SCD2 Logic:** Tracks customer_segment changes over time
# MAGIC **Rollback:** RESTORE TABLE acip.gold.dim_customer TO VERSION AS OF N
# MAGIC
# MAGIC **Fix applied (June 2026):**
# MAGIC   customer_key was generated using F.abs(F.hash(customer_id, segment, date))
# MAGIC   which produced 2 hash collisions (duplicate primary keys).
# MAGIC   Fixed to use monotonically_increasing_id() which guarantees uniqueness.

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.dim_customer"

print("GOLD - DIM CUSTOMER (SCD2)")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Parse Customers from Silver Events
print("STEP 1: PARSE CUSTOMERS FROM SILVER EVENTS")
print("=" * 70)

payload_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state_region", StringType(), True),
    StructField("customer_state", StringType(), True),
])

silver = spark.table(f"{CATALOG}.silver.events")

customers_raw = silver.filter(
    F.col("domain") == "ecommerce"
).filter(
    F.col("event_type") == "order.placed"
).withColumn("payload_parsed", F.from_json(F.col("payload"), payload_schema)) \
 .select(
    F.col("payload_parsed.customer_id").alias("customer_id"),
    F.col("payload_parsed.customer_unique_id").alias("customer_unique_id"),
    F.col("payload_parsed.customer_segment").alias("customer_segment"),
    F.col("payload_parsed.region").alias("region"),
    F.col("payload_parsed.state_region").alias("state_region"),
    F.col("payload_parsed.customer_state").alias("customer_state"),
    F.to_date(F.col("occurred_at")).alias("event_date")
).filter(F.col("customer_id").isNotNull())

print(f"Raw customer records from silver: {customers_raw.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Get Latest Record Per Customer
print("STEP 2: GET LATEST RECORD PER CUSTOMER")
print("=" * 70)

window = Window.partitionBy("customer_id").orderBy(F.col("event_date").desc())

customers_latest = customers_raw \
    .withColumn("row_num", F.row_number().over(window)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"Unique customers: {customers_latest.count():,}")

segment_dist = customers_latest.groupBy("customer_segment").count().collect()
print("\nCustomer segment distribution:")
for row in segment_dist:
    pct = row["count"] / customers_latest.count() * 100
    print(f"  {row['customer_segment']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 3: Apply SCD2 Logic
print("STEP 3: APPLY SCD2 LOGIC")
print("=" * 70)

if not spark.catalog.tableExists(TARGET_TABLE):
    print("First run - inserting all records as current")

    # FIX: monotonically_increasing_id() guarantees uniqueness
    # F.abs(F.hash(customer_id, segment, date)) produced 2 hash collisions
    dim_customer = customers_latest \
        .withColumn("customer_key", F.monotonically_increasing_id()) \
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_segment",
            "region",
            "state_region",
            "customer_state",
            F.col("event_date").alias("effective_date"),
            F.lit("9999-12-31").cast("date").alias("expiry_date"),
            F.lit(True).alias("is_current")
        )

    # Verify no duplicates before writing
    dup_count = dim_customer.groupBy("customer_key").count().filter(F.col("count") > 1).count()
    print(f"Duplicate customer_key before write: {dup_count} (expected 0)")

    dim_customer.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_TABLE)

else:
    print("Subsequent run - applying SCD2 MERGE")
    customers_latest.createOrReplaceTempView("customers_latest_view")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING (
            SELECT
                customer_id,
                customer_unique_id,
                customer_segment,
                region,
                state_region,
                customer_state,
                event_date AS effective_date
            FROM customers_latest_view
        ) AS source
        ON target.customer_id = source.customer_id
           AND target.is_current = TRUE
        WHEN MATCHED AND target.customer_segment != source.customer_segment THEN
            UPDATE SET target.expiry_date = CURRENT_DATE(), target.is_current = FALSE
        WHEN NOT MATCHED THEN
            INSERT (customer_key, customer_id, customer_unique_id, customer_segment,
                    region, state_region, customer_state,
                    effective_date, expiry_date, is_current)
            VALUES (monotonically_increasing_id(), source.customer_id,
                    source.customer_unique_id, source.customer_segment,
                    source.region, source.state_region, source.customer_state,
                    source.effective_date, DATE('9999-12-31'), TRUE)
    """)

written = spark.table(TARGET_TABLE).count()
current_count = spark.table(TARGET_TABLE).filter(F.col("is_current")).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} total rows, {current_count:,} current")

# COMMAND ----------

# DBTITLE 1,STEP 4: Verify SCD2 Integrity
print("STEP 4: VERIFY SCD2 INTEGRITY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()
current = df.filter(F.col("is_current")).count()
historical = df.filter(~F.col("is_current")).count()
null_key = df.filter(F.col("customer_key").isNull()).count()
dup_keys = df.groupBy("customer_key").count().filter(F.col("count") > 1).count()

print(f"Total rows:        {total:,}")
print(f"Current:           {current:,}")
print(f"Historical:        {historical:,}")
print(f"Null keys:         {null_key}")
print(f"Duplicate keys:    {dup_keys}")

segment_dist = df.filter(F.col("is_current")).groupBy("customer_segment").count().collect()
print("\nCurrent customer segment distribution:")
for row in segment_dist:
    pct = row["count"] / current * 100
    print(f"  {row['customer_segment']}: {row['count']:,} ({pct:.1f}%)")

status = "PASS" if null_key == 0 and dup_keys == 0 else "FAIL"
print(f"\n{status}: dim_customer SCD2 verified")

display(df.limit(5))
