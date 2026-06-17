# Databricks notebook source
# MAGIC %md
# MAGIC ## SILVER - PHARMACY TRANSFORMATION
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Transform Bronze pharmacy tables to universal silver.events schema  
# MAGIC **Input:** acip.bronze.pharma_sales_hourly  
# MAGIC **Output:** acip.silver.events (mode=APPEND - adds pharmacy rows)  
# MAGIC **Rollback:** If this fails, rerun notebook 04 first (overwrite), then rerun this

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

print("PHARMACY SILVER TRANSFORMATION")
print("=" * 70)
print(f"Source: {CATALOG}.{SOURCE}.pharma_sales_hourly")
print(f"Target: {TARGET_TABLE}")
print(f"Mode: APPEND - adds pharmacy rows to existing silver.events")
print(f"Run ID: {RUN_ID}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Bronze Table
print("STEP 1: LOADING BRONZE TABLE")
print("=" * 70)

hourly = spark.table(f"{CATALOG}.{SOURCE}.pharma_sales_hourly")

print(f"pharma_sales_hourly: {hourly.count():,} rows")
print(f"Columns: {hourly.columns}")

display(hourly.limit(3))

# COMMAND ----------

# DBTITLE 1,STEP 2: Define Drug Category Mapping
print("STEP 2: DEFINE DRUG CATEGORY MAPPING")
print("=" * 70)

DRUG_COLS = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]

DRUG_MAP = {
    "M01AB": {"category": "anti_inflammatory_acetic_acid",  "category_group": "anti_inflammatory", "is_prescription": True,  "atc_code": "M01AB", "drug_class": "NSAID"},
    "M01AE": {"category": "anti_inflammatory_propionic_acid","category_group": "anti_inflammatory", "is_prescription": False, "atc_code": "M01AE", "drug_class": "NSAID"},
    "N02BA": {"category": "analgesic_salicylic_acid",        "category_group": "analgesic",         "is_prescription": False, "atc_code": "N02BA", "drug_class": "salicylate"},
    "N02BE": {"category": "analgesic_anilide",               "category_group": "analgesic",         "is_prescription": False, "atc_code": "N02BE", "drug_class": "anilide"},
    "N05B":  {"category": "anxiolytic",                      "category_group": "psychoactive",      "is_prescription": True,  "atc_code": "N05B",  "drug_class": "benzodiazepine"},
    "N05C":  {"category": "hypnotic_sedative",               "category_group": "psychoactive",      "is_prescription": True,  "atc_code": "N05C",  "drug_class": "sedative"},
    "R03":   {"category": "respiratory_obstructive",         "category_group": "respiratory",       "is_prescription": True,  "atc_code": "R03",   "drug_class": "bronchodilator"},
    "R06":   {"category": "antihistamine",                   "category_group": "respiratory",       "is_prescription": False, "atc_code": "R06",   "drug_class": "antihistamine"},
}

print("Drug ATC code mapping:")
for code, info in DRUG_MAP.items():
    rx = "Rx" if info["is_prescription"] else "OTC"
    print(f"  {code}: {info['category']} [{rx}]")

# COMMAND ----------

# DBTITLE 1,STEP 3: Clean Hourly Data
print("STEP 3: CLEAN HOURLY DATA")
print("=" * 70)

hourly_clean = hourly \
    .filter(F.col("datum").isNotNull() & (F.trim(F.col("datum")) != "")) \
    .withColumn("datum", F.trim(F.col("datum"))) \
    .withColumn("year", F.col("Year").cast("int")) \
    .withColumn("month", F.col("Month").cast("int")) \
    .withColumn("hour", F.col("Hour").cast("int")) \
    .withColumn("weekday", F.trim(F.col("weekday_name"))) \
    .withColumn("is_weekend", F.col("weekday").isin("Saturday", "Sunday")) \
    .withColumn("time_of_day",
        F.when(F.col("hour").between(6, 11), "morning")
         .when(F.col("hour").between(12, 17), "afternoon")
         .when(F.col("hour").between(18, 22), "evening")
         .otherwise("night")
    ) \
    .withColumn("is_business_hours",
        F.col("hour").between(9, 18) & ~F.col("is_weekend")
    ) \
    .withColumn("is_peak_hour",
        F.col("hour").between(10, 12) | F.col("hour").between(16, 19)
    )

print(f"Valid hourly records: {hourly_clean.count():,}")

time_dist = hourly_clean.groupBy("time_of_day").count().collect()
print("\nTime of day distribution:")
for row in time_dist:
    pct = row["count"] / hourly_clean.count() * 100
    print(f"  {row['time_of_day']}: {row['count']:,} ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,STEP 4: Inspect Drug Statistics
print("STEP 4: INSPECT DRUG STATISTICS")
print("=" * 70)

print("Sales volume statistics per drug:")
for col_name in DRUG_COLS:
    stats = hourly_clean.select(
        F.count(F.when(F.col(col_name).cast("double") > 0, True)).alias("non_zero"),
        F.avg(F.col(col_name).cast("double")).alias("avg"),
        F.max(F.col(col_name).cast("double")).alias("max")
    ).collect()[0]
    pct = stats["non_zero"] / hourly_clean.count() * 100
    print(f"  {col_name}: non_zero={stats['non_zero']:,} ({pct:.1f}%) avg={stats['avg']:.2f} max={stats['max']:.2f}")

# COMMAND ----------

# DBTITLE 1,STEP 5: Unpivot Drugs and Build Universal Event Envelope
print("STEP 5: UNPIVOT DRUGS AND BUILD EVENT ENVELOPE")
print("=" * 70)

uuid_udf = F.expr("uuid()")

all_drug_events = None

for drug_col in DRUG_COLS:
    info = DRUG_MAP[drug_col]

    drug_events = hourly_clean.filter(
        F.col(drug_col).cast("double").isNotNull() &
        (F.col(drug_col).cast("double") > 0)
    ).select(
        uuid_udf.alias("event_id"),
        F.lit("prescription.filled").alias("event_type"),
        F.lit("1.0").alias("event_version"),
        F.lit("pharmacy").alias("domain"),
        F.lit("pharma-sales-replay").alias("source_system"),
        F.concat_ws("T",
            F.col("datum"),
            F.lpad(F.col("hour").cast("string"), 2, "0")
        ).alias("occurred_at"),
        F.current_timestamp().cast("string").alias("ingested_at"),
        F.concat_ws("-",
            F.col("datum"),
            F.lit(drug_col),
            F.col("hour").cast("string")
        ).alias("correlation_id"),
        F.to_json(F.struct(
            F.lit(drug_col).alias("product_id"),
            F.lit(info["category"]).alias("category"),
            F.lit(info["category_group"]).alias("category_group"),
            F.lit(info["atc_code"]).alias("atc_code"),
            F.lit(info["drug_class"]).alias("drug_class"),
            F.col(drug_col).cast("double").alias("quantity"),
            F.lit(info["is_prescription"]).alias("is_prescription"),
            F.col("year"),
            F.col("month"),
            F.col("hour"),
            F.col("weekday"),
            F.col("is_weekend"),
            F.col("time_of_day"),
            F.col("is_business_hours"),
            F.col("is_peak_hour"),
            F.round(F.rand() * 500 + 50, 0).cast("int").alias("stock_level"),
            F.lit(50).alias("reorder_threshold"),
            F.round(F.rand() * 30 + 5, 0).cast("int").alias("fill_time_mins")
        )).alias("payload")
    ).filter(
        F.col("correlation_id").isNotNull() &
        F.col("occurred_at").isNotNull()
    )

    if all_drug_events is None:
        all_drug_events = drug_events
    else:
        all_drug_events = all_drug_events.union(drug_events)

    count = drug_events.count()
    print(f"  {drug_col} ({info['category']}): {count:,} events")

# COMMAND ----------

# DBTITLE 1,STEP 6: Deduplicate
print("STEP 6: DEDUPLICATE")
print("=" * 70)

before_dedup = all_drug_events.count()
all_drug_events = all_drug_events.dropDuplicates(["correlation_id", "event_type"])
after_dedup = all_drug_events.count()

print(f"Before deduplication: {before_dedup:,}")
print(f"After deduplication:  {after_dedup:,}")
print(f"Duplicates removed:   {before_dedup - after_dedup:,}")

# COMMAND ----------

# DBTITLE 1,STEP 7: Append to silver.events
print("STEP 7: APPEND TO silver.events")
print("=" * 70)
print("NOTE: Mode=APPEND adds pharmacy rows.")
print("If this fails, rerun notebook 04 first, then rerun this.")

all_drug_events.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TARGET_TABLE)

total = spark.table(TARGET_TABLE).count()
print(f"\nPASS: {TARGET_TABLE} total rows after append - {total:,}")

# COMMAND ----------

# DBTITLE 1,STEP 8: Write Watermark
print("STEP 8: WRITE WATERMARK")
print("=" * 70)

spark.sql(f"""
    INSERT INTO {CATALOG}.quality.pipeline_watermarks VALUES (
        '{RUN_ID}',
        'pharmacy',
        'processing',
        '05_pharmacy_silver_transform',
        current_timestamp(),
        null,
        {after_dedup},
        'COMPLETE',
        current_timestamp(),
        current_timestamp()
    )
""")

print(f"PASS: Watermark written for run_id={RUN_ID} domain=pharmacy")

# COMMAND ----------

# DBTITLE 1,STEP 9: Data Quality Checks
print("STEP 9: DATA QUALITY CHECKS")
print("=" * 70)

df = spark.table(TARGET_TABLE).filter(F.col("domain") == "pharmacy")
total = df.count()

checks = {
    "null_event_id":       df.filter(F.col("event_id").isNull()).count(),
    "null_occurred_at":    df.filter(F.col("occurred_at").isNull()).count(),
    "null_correlation_id": df.filter(F.col("correlation_id").isNull()).count(),
    "null_payload":        df.filter(F.col("payload").isNull()).count(),
}

print(f"Pharmacy rows in silver.events: {total:,}")
print("\nNull checks (pharmacy rows only):")
all_passed = True
for check, count in checks.items():
    pct = count / total * 100 if total > 0 else 0
    status = "PASS" if count == 0 else "FAIL"
    if count > 0:
        all_passed = False
    print(f"  {status} {check}: {count:,} ({pct:.2f}%)")

print(f"\nOverall: {'PASS' if all_passed else 'FAIL - review above'}")

display(df.select("event_id", "event_type", "domain", "occurred_at", "correlation_id", "payload").limit(5))
