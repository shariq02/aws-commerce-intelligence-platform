# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD - DIM DATE
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Generate date dimension covering Olist date range 2016-2020  
# MAGIC **Input:** Generated - no source table needed  
# MAGIC **Output:** acip.gold.dim_date

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
TARGET_TABLE = f"{CATALOG}.gold.dim_date"

print("GOLD - DIM DATE")
print("=" * 70)
print(f"Target: {TARGET_TABLE}")

current_version = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE}").first()["version"] \
    if spark.catalog.tableExists(TARGET_TABLE) else None
if current_version is not None:
    print(f"ROLLBACK IF NEEDED: RESTORE TABLE {TARGET_TABLE} TO VERSION AS OF {current_version}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Generate Date Spine 2016-2020
print("STEP 1: GENERATE DATE SPINE 2016-2020")
print("=" * 70)

date_df = spark.sql("""
    SELECT sequence(
        to_date('2016-01-01'),
        to_date('2020-12-31'),
        interval 1 day
    ) AS date_array
""").select(F.explode(F.col("date_array")).alias("full_date"))

date_df = date_df \
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int")) \
    .withColumn("year", F.year("full_date")) \
    .withColumn("quarter", F.quarter("full_date")) \
    .withColumn("month", F.month("full_date")) \
    .withColumn("month_name", F.date_format("full_date", "MMMM")) \
    .withColumn("week_of_year", F.weekofyear("full_date")) \
    .withColumn("day_of_month", F.dayofmonth("full_date")) \
    .withColumn("day_of_week", F.dayofweek("full_date")) \
    .withColumn("day_name", F.date_format("full_date", "EEEE")) \
    .withColumn("is_weekend",
        F.col("day_of_week").isin(1, 7)
    ) \
    .withColumn("quarter_label",
        F.concat(F.col("year").cast("string"), F.lit("-Q"), F.col("quarter").cast("string"))
    ) \
    .withColumn("month_label",
        F.date_format("full_date", "yyyy-MM")
    )

total = date_df.count()
print(f"Date spine generated: {total:,} rows")
print(f"Range: 2016-01-01 to 2020-12-31")

display(date_df.limit(5))

# COMMAND ----------

# DBTITLE 1,STEP 2: Write Gold Table
print("STEP 2: WRITE GOLD TABLE")
print("=" * 70)

date_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

written = spark.table(TARGET_TABLE).count()
print(f"PASS: {TARGET_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 3: Verify
print("STEP 3: VERIFY")
print("=" * 70)

df = spark.table(TARGET_TABLE)
print(f"Total rows: {df.count():,}")
print(f"Columns: {df.columns}")

weekend_count = df.filter(F.col("is_weekend")).count()
print(f"Weekend days: {weekend_count:,} ({weekend_count/df.count()*100:.1f}%)")

year_dist = df.groupBy("year").count().orderBy("year").collect()
print("\nRows per year:")
for row in year_dist:
    print(f"  {row['year']}: {row['count']:,} days")

print("\nPASS: dim_date verified")
