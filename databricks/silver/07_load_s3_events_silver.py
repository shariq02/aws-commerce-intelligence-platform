# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - LOAD S3 BRONZE EVENTS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Load Flink S3 Bronze JSON events into universal silver.events schema  
# MAGIC **Input:** /Volumes/acip/bronze/raw_files/s3_events/ (JSON files from Flink)  
# MAGIC **Output:** acip.silver.events (mode=APPEND - adds streaming events)  
# MAGIC **Rollback:** If this fails, simply rerun - APPEND is safe to retry

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType
)

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.silver.events"
S3_EVENTS_VOLUME = f"/Volumes/{CATALOG}/bronze/raw_files/s3_events/"
RUN_ID = "manual"

print("S3 BRONZE EVENTS SILVER LOAD")
print("=" * 70)
print(f"Source: {S3_EVENTS_VOLUME}")
print(f"Target: {TARGET_TABLE}")
print(f"Mode: APPEND - adds streaming events to silver.events")
print(f"Run ID: {RUN_ID}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Check S3 Events Volume
print("STEP 1: CHECK S3 EVENTS VOLUME")
print("=" * 70)

try:
    files = dbutils.fs.ls(S3_EVENTS_VOLUME)
    print(f"Files found in volume: {len(files)}")
    for f in files[:10]:
        print(f"  {f.name} ({f.size/1024:.1f} KB)")
    if len(files) > 10:
        print(f"  ... and {len(files) - 10} more files")
except Exception as e:
    print(f"WARN: Could not list volume: {e}")
    print("Volume may be empty - check if S3 Bronze files have been uploaded")
    raise

# COMMAND ----------

# DBTITLE 1,STEP 2: Load JSON Files from Volume
print("STEP 2: LOAD JSON FILES FROM VOLUME")
print("=" * 70)

raw_df = spark.read \
    .option("multiline", "false") \
    .json(S3_EVENTS_VOLUME)

total_raw = raw_df.count()
print(f"Total raw records loaded: {total_raw:,}")
print(f"Schema inferred columns: {raw_df.columns}")

display(raw_df.limit(3))

# COMMAND ----------

# DBTITLE 1,STEP 3: Validate Envelope Fields
print("STEP 3: VALIDATE ENVELOPE FIELDS")
print("=" * 70)

REQUIRED_FIELDS = [
    "event_id", "event_type", "event_version",
    "domain", "source_system", "occurred_at",
    "ingested_at", "correlation_id", "payload"
]

missing_fields = [f for f in REQUIRED_FIELDS if f not in raw_df.columns]
if missing_fields:
    raise ValueError(f"S3 events missing required envelope fields: {missing_fields}")

print(f"PASS: All {len(REQUIRED_FIELDS)} envelope fields present")

null_counts = {}
for field in REQUIRED_FIELDS:
    null_count = raw_df.filter(F.col(field).isNull()).count()
    null_counts[field] = null_count
    status = "PASS" if null_count == 0 else "WARN"
    pct = null_count / total_raw * 100 if total_raw > 0 else 0
    print(f"  {status} null_{field}: {null_count:,} ({pct:.2f}%)")

domain_dist = raw_df.groupBy("domain").count().collect()
print("\nDomain distribution in S3 events:")
for row in domain_dist:
    pct = row["count"] / total_raw * 100
    print(f"  {row['domain']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Normalise to Universal Schema
print("STEP 4: NORMALISE TO UNIVERSAL SCHEMA")
print("=" * 70)

events_df = raw_df \
    .filter(F.col("event_id").isNotNull()) \
    .filter(F.col("domain").isin("ecommerce", "pharmacy", "marketplace")) \
    .filter(F.col("occurred_at").isNotNull()) \
    .filter(F.col("correlation_id").isNotNull()) \
    .withColumn("event_id", F.trim(F.col("event_id"))) \
    .withColumn("event_type", F.trim(F.col("event_type"))) \
    .withColumn("event_version", F.coalesce(F.col("event_version"), F.lit("1.0"))) \
    .withColumn("domain", F.lower(F.trim(F.col("domain")))) \
    .withColumn("source_system",
        F.coalesce(F.col("source_system"), F.lit("flink-streaming"))
    ) \
    .withColumn("payload",
        F.when(F.col("payload").cast("string").isNotNull(),
            F.to_json(F.col("payload"))
        ).otherwise(F.lit("{}"))
    ) \
    .select(
        "event_id",
        "event_type",
        "event_version",
        "domain",
        "source_system",
        "occurred_at",
        "ingested_at",
        "correlation_id",
        "payload"
    )

before_dedup = events_df.count()
events_df = events_df.dropDuplicates(["event_id"])
after_dedup = events_df.count()

print(f"Records before dedup: {before_dedup:,}")
print(f"Records after dedup:  {after_dedup:,}")
print(f"Duplicates removed:   {before_dedup - after_dedup:,}")

event_type_dist = events_df.groupBy("event_type").count().collect()
print("\nEvent type distribution:")
for row in event_type_dist:
    pct = row["count"] / after_dedup * 100
    print(f"  {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 5: Append to silver.events
print("STEP 5: APPEND TO silver.events")
print("=" * 70)
print("NOTE: Mode=APPEND adds S3 streaming events.")
print("Safe to rerun - duplicate event_ids will be handled by Gold deduplication.")

events_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TARGET_TABLE)

total = spark.table(TARGET_TABLE).count()
print(f"\nPASS: {TARGET_TABLE} total rows after S3 events append - {total:,}")

# COMMAND ----------

# DBTITLE 1,STEP 6: Write Watermark
print("STEP 6: WRITE WATERMARK")
print("=" * 70)

spark.sql(f"""
    INSERT INTO {CATALOG}.quality.pipeline_watermarks VALUES (
        '{RUN_ID}',
        'all',
        'processing',
        '07_load_s3_events_silver',
        current_timestamp(),
        null,
        {after_dedup},
        'COMPLETE',
        current_timestamp(),
        current_timestamp()
    )
""")

print(f"PASS: Watermark written for run_id={RUN_ID} domain=all (S3 events)")

# COMMAND ----------

# DBTITLE 1,STEP 7: Verify silver.events Domain Distribution
print("STEP 7: VERIFY silver.events DOMAIN DISTRIBUTION")
print("=" * 70)

df = spark.table(TARGET_TABLE)
total = df.count()

domain_dist = df.groupBy("domain").count().collect()
print(f"Total rows in silver.events: {total:,}")
print("\nDomain distribution:")
for row in domain_dist:
    pct = row["count"] / total * 100
    print(f"  {row['domain']}: {row['count']:,} ({pct:.1f}%)")

source_dist = df.groupBy("source_system").count().collect()
print("\nSource system distribution:")
for row in source_dist:
    pct = row["count"] / total * 100
    print(f"  {row['source_system']}: {row['count']:,} ({pct:.1f}%)")

null_payload = df.filter(F.col("payload").isNull()).count()
print(f"\nNull payload rows: {null_payload:,}")
print(f"\nOverall: {'PASS' if null_payload == 0 else 'WARN - null payloads found'}")
