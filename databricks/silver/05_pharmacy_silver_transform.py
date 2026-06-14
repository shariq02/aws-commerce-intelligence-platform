# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - PHARMACY TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Transform Bronze pharmacy tables to event envelope schema
# MAGIC **Input:** acip.bronze.pharma_sales_hourly
# MAGIC **Output:** acip.silver.pharmacy_events

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
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.pharmacy_events"

print(f"Source: {CATALOG}.{SOURCE_SCHEMA}.pharma_sales_hourly")
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Load Bronze Pharmacy Table
print("Loading bronze pharmacy tables...")

hourly = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.pharma_sales_hourly")
daily = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.pharma_sales_daily")

print(f"PASS: pharma_sales_hourly - {hourly.count()} rows")
print(f"PASS: pharma_sales_daily - {daily.count()} rows")
print(f"Columns: {hourly.columns}")

# COMMAND ----------

# DBTITLE 1,Inspect Pharma Drug Columns
print("Drug category columns in hourly dataset:")

drug_cols = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]

drug_category_map = {
    "M01AB": "anti_inflammatory_acetic_acid",
    "M01AE": "anti_inflammatory_propionic_acid",
    "N02BA": "analgesic_salicylic_acid",
    "N02BE": "analgesic_anilide",
    "N05B": "anxiolytic",
    "N05C": "hypnotic_sedative",
    "R03": "respiratory_obstructive",
    "R06": "antihistamine"
}

for col_name in drug_cols:
    sample = hourly.select(F.col(col_name).cast("double")).agg(
        F.avg(F.col(col_name).cast("double")).alias("avg"),
        F.max(F.col(col_name).cast("double")).alias("max")
    ).collect()[0]
    print(f"  {col_name} ({drug_category_map[col_name]}): avg={sample['avg']:.2f}, max={sample['max']:.2f}")

# COMMAND ----------

# DBTITLE 1,Unpivot Drug Columns to Row Per Drug Per Hour
print("Unpivoting drug columns to long format...")

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())

unpivoted_frames = []

for drug_col, category in drug_category_map.items():
    drug_events = hourly.select(
        uuid_udf().alias("event_id"),
        F.lit("prescription.filled").alias("event_type"),
        F.lit("1.0").alias("event_version"),
        F.lit("pharmacy").alias("domain"),
        F.lit("pharma-sales-replay").alias("source_system"),
        F.concat_ws(
            "T",
            F.col("datum"),
            F.lpad(F.col("Hour"), 2, "0")
        ).alias("occurred_at"),
        F.current_timestamp().cast("string").alias("ingested_at"),
        F.concat_ws(
            "-",
            F.col("datum"),
            F.lit(drug_col),
            F.col("Hour")
        ).alias("correlation_id"),
        F.lit(drug_col).alias("product_id"),
        F.lit(category).alias("category"),
        F.col(drug_col).cast("double").alias("quantity"),
        F.col("Year").cast("int").alias("year"),
        F.col("Month").cast("int").alias("month"),
        F.col("Hour").cast("int").alias("hour"),
        F.col("weekday_name").alias("weekday")
    ).filter(
        F.col(drug_col).cast("double") > 0
    ).filter(
        F.col("datum").isNotNull()
    )
    unpivoted_frames.append(drug_events)

all_drug_events = unpivoted_frames[0]
for frame in unpivoted_frames[1:]:
    all_drug_events = all_drug_events.union(frame)

total_before_dedup = all_drug_events.count()
print(f"PASS: unpivoted events before deduplication - {total_before_dedup} rows")

# COMMAND ----------

# DBTITLE 1,Add Inventory and Stock Level Columns
print("Adding stock level simulation columns...")

all_drug_events = all_drug_events \
    .withColumn(
        "stock_level",
        (F.rand() * 500 + 50).cast("int")
    ) \
    .withColumn(
        "reorder_threshold",
        F.lit(50)
    ) \
    .withColumn(
        "days_of_supply",
        (F.col("stock_level") / F.greatest(F.col("quantity"), F.lit(1))).cast("int")
    ) \
    .withColumn(
        "is_prescription",
        F.when(F.col("category").isin(
            "anxiolytic", "hypnotic_sedative", "respiratory_obstructive"
        ), True).otherwise(False)
    ) \
    .withColumn(
        "fill_time_mins",
        (F.rand() * 30 + 5).cast("int")
    )

# COMMAND ----------

# DBTITLE 1,Deduplicate
print("Deduplicating events...")

all_drug_events = all_drug_events.dropDuplicates(["correlation_id", "event_type"])

total = all_drug_events.count()
print(f"Total pharmacy silver events after deduplication: {total}")

# COMMAND ----------

# DBTITLE 1,Write Silver Delta Table
print(f"Writing to {TARGET_TABLE}...")

all_drug_events.write \
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
null_product_id = df.filter(F.col("product_id").isNull()).count()
null_quantity = df.filter(F.col("quantity").isNull()).count()

category_dist = df.groupBy("category").count().collect()

print(f"Total rows: {total}")
print(f"Null event_id: {null_event_id}")
print(f"Null occurred_at: {null_occurred_at}")
print(f"Null product_id: {null_product_id}")
print(f"Null quantity: {null_quantity}")

print("\nCategory distribution:")
for row in category_dist:
    print(f"  {row['category']}: {row['count']}")

if null_event_id == 0 and null_occurred_at == 0 and null_product_id == 0:
    print("\nPASS: All quality checks passed")
else:
    print("\nFAIL: Quality issues detected")
