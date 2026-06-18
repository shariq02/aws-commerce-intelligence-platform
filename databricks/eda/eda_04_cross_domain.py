# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA-04: CROSS-DOMAIN EXPLORATORY DATA ANALYSIS
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Cross-domain comparison and data quality analysis
# MAGIC **Input:** acip.gold.agg_daily_domain_metrics, all fact tables, acip.silver.events
# MAGIC **Output:** acip.eda.cross_domain_summary

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
EDA_TABLE = f"{CATALOG}.eda.cross_domain_summary"

sns.set_theme(style="whitegrid", palette="muted")

print("EDA-04: CROSS-DOMAIN")
print("=" * 70)
print(f"Output: {EDA_TABLE}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load All Gold Tables
print("STEP 1: LOAD ALL GOLD TABLES")
print("=" * 70)

agg_daily = spark.table(f"{CATALOG}.gold.agg_daily_domain_metrics")
fact_transactions = spark.table(f"{CATALOG}.gold.fact_transactions")
fact_inventory = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")
fact_seller = spark.table(f"{CATALOG}.gold.fact_seller_performance")
silver = spark.table(f"{CATALOG}.silver.events")

tables = {
    "dim_date":                  spark.table(f"{CATALOG}.gold.dim_date"),
    "dim_geography":             spark.table(f"{CATALOG}.gold.dim_geography"),
    "dim_product":               spark.table(f"{CATALOG}.gold.dim_product"),
    "dim_customer":              spark.table(f"{CATALOG}.gold.dim_customer"),
    "dim_seller":                spark.table(f"{CATALOG}.gold.dim_seller"),
    "fact_transactions":         fact_transactions,
    "fact_inventory_snapshots":  fact_inventory,
    "fact_seller_performance":   fact_seller,
    "agg_daily_domain_metrics":  agg_daily,
    "agg_customer_segments":     spark.table(f"{CATALOG}.gold.agg_customer_segments"),
}

print("Gold table row counts:")
table_counts = {}
for name, df in tables.items():
    count = df.count()
    table_counts[name] = count
    print(f"  {name}: {count:,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Daily Event Volume Comparison
print("STEP 2: DAILY EVENT VOLUME COMPARISON")
print("=" * 70)

daily_df = agg_daily.groupBy("metric_date", "domain").agg(
    F.sum("event_count").alias("total_events"),
    F.sum("total_value").alias("total_value")
).filter(
    F.col("metric_date").isNotNull()
).orderBy("metric_date").toPandas()

domain_totals = daily_df.groupby("domain")["total_events"].sum()
print("Total events per domain:")
for domain, total in domain_totals.items():
    print(f"  {domain}: {total:,}")

pivot_events = daily_df.pivot(
    index="metric_date", columns="domain", values="total_events"
).fillna(0)

fig, axes = plt.subplots(2, 1, figsize=(16, 10))

domain_colors = {"ecommerce": "#4C72B0", "pharmacy": "#55A868", "marketplace": "#DD8452"}

for domain in pivot_events.columns:
    color = domain_colors.get(domain, "#8172B2")
    axes[0].plot(pivot_events.index, pivot_events[domain],
                 label=domain, color=color, linewidth=1.5)

axes[0].set_title("Daily Event Volume by Domain")
axes[0].set_xlabel("Date")
axes[0].set_ylabel("Event Count")
axes[0].legend()
axes[0].tick_params(axis="x", rotation=45)

pivot_value = daily_df.pivot(
    index="metric_date", columns="domain", values="total_value"
).fillna(0)

for domain in pivot_value.columns:
    color = domain_colors.get(domain, "#8172B2")
    axes[1].plot(pivot_value.index, pivot_value[domain],
                 label=domain, color=color, linewidth=1.5)

axes[1].set_title("Daily Value by Domain")
axes[1].set_xlabel("Date")
axes[1].set_ylabel("Value")
axes[1].legend()
axes[1].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("/tmp/cross_domain_daily.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 3: Event Type Breakdown per Domain
print("STEP 3: EVENT TYPE BREAKDOWN PER DOMAIN")
print("=" * 70)

event_type_df = silver.groupBy("domain", "event_type").count().orderBy(
    "domain", F.col("count").desc()
).toPandas()

print("Event type distribution per domain:")
for domain in event_type_df["domain"].unique():
    domain_rows = event_type_df[event_type_df["domain"] == domain]
    domain_total = domain_rows["count"].sum()
    print(f"\n  {domain.upper()} ({domain_total:,} total):")
    for _, row in domain_rows.iterrows():
        pct = row["count"] / domain_total * 100
        print(f"    {row['event_type']}: {row['count']:,} ({pct:.1f}%)")

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
for idx, domain in enumerate(["ecommerce", "pharmacy", "marketplace"]):
    domain_data = event_type_df[event_type_df["domain"] == domain]
    axes[idx].pie(domain_data["count"],
                  labels=[e.replace(".", "\n") for e in domain_data["event_type"]],
                  autopct="%1.1f%%",
                  colors=sns.color_palette("muted", len(domain_data)))
    axes[idx].set_title(f"{domain.capitalize()} Event Types")

plt.tight_layout()
plt.savefig("/tmp/cross_domain_events.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 4: Source System Distribution (Batch vs Streaming)
print("STEP 4: SOURCE SYSTEM DISTRIBUTION")
print("=" * 70)

source_df = silver.groupBy("domain", "source_system").count().toPandas()

total_events = source_df["count"].sum()
print("Source system distribution across all domains:")

batch_sources = ["olist-replay", "pharma-sales-replay"]
streaming_sources = ["ecommerce-simulator", "pharmacy-simulator", "marketplace-simulator"]

batch_total = source_df[source_df["source_system"].isin(batch_sources)]["count"].sum()
streaming_total = source_df[source_df["source_system"].isin(streaming_sources)]["count"].sum()

print(f"\n  BATCH PATH:     {batch_total:,} events ({batch_total/total_events*100:.1f}%)")
print(f"  STREAMING PATH: {streaming_total:,} events ({streaming_total/total_events*100:.1f}%)")

print("\nBy source system:")
for _, row in source_df.sort_values("count", ascending=False).iterrows():
    pct = row["count"] / total_events * 100
    print(f"  {row['source_system']} ({row['domain']}): {row['count']:,} ({pct:.1f}%)")

fig, ax = plt.subplots(figsize=(10, 5))
path_data = pd.DataFrame({
    "path": ["Batch (CSV)", "Streaming (Flink)"],
    "count": [batch_total, streaming_total]
})
ax.pie(path_data["count"], labels=path_data["path"],
       autopct="%1.1f%%", colors=["#4C72B0", "#DD8452"])
ax.set_title("Data Path Distribution: Batch vs Streaming")
plt.tight_layout()
plt.savefig("/tmp/cross_domain_sources.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 5: Data Completeness Report
print("STEP 5: DATA COMPLETENESS REPORT")
print("=" * 70)

print("Gold table null rate analysis:")
print(f"\n{'Table':<35} {'Column':<25} {'Null Count':>12} {'Null Rate':>10}")
print("-" * 85)

key_checks = [
    (fact_transactions, "fact_transactions", ["customer_key", "date_key", "geo_key", "total_amount"]),
    (fact_inventory, "fact_inventory_snapshots", ["product_key", "date_key", "quantity", "stock_level"]),
    (fact_seller, "fact_seller_performance", ["seller_key", "date_key", "seller_tier"]),
]

completeness_data = []
for df, table_name, columns in key_checks:
    total = df.count()
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_rate = null_count / total * 100
        status = "PASS" if null_rate == 0 else "WARN" if null_rate < 5 else "FAIL"
        print(f"{table_name:<35} {col:<25} {null_count:>12,} {null_rate:>9.2f}% {status}")
        completeness_data.append({
            "table_name": table_name,
            "column_name": col,
            "null_count": null_count,
            "null_rate_pct": null_rate,
            "status": status
        })

# COMMAND ----------

# DBTITLE 1,STEP 6: Row Count Summary
print("STEP 6: ROW COUNT SUMMARY")
print("=" * 70)

print(f"\n{'Table':<40} {'Row Count':>12}")
print("-" * 55)
for name, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{name:<40} {count:>12,}")

print(f"\n{'Silver events'::<40} {silver.count():>12,}")

# COMMAND ----------

# DBTITLE 1,STEP 7: Write EDA Summary Table
print("STEP 7: WRITE EDA SUMMARY TABLE")
print("=" * 70)

domain_summary = agg_daily.groupBy("domain").agg(
    F.sum("event_count").alias("total_events"),
    F.sum("total_value").alias("total_value"),
    F.countDistinct("metric_date").alias("active_dates"),
    F.avg("event_count").alias("avg_daily_events")
)

source_summary = silver.groupBy("domain").agg(
    F.countDistinct("source_system").alias("source_systems"),
    F.count("event_id").alias("silver_event_count")
)

summary_df = domain_summary.join(source_summary, on="domain", how="left") \
    .withColumn("updated_at", F.current_timestamp())

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EDA_TABLE)

written = spark.table(EDA_TABLE).count()
print(f"PASS: {EDA_TABLE} written - {written:,} rows")

display(spark.table(EDA_TABLE))

# COMMAND ----------

# DBTITLE 1,STEP 8: Key Insights
print("STEP 8: KEY INSIGHTS")
print("=" * 70)

total_gold_rows = sum(table_counts.values())
silver_total = silver.count()

print("CROSS-DOMAIN EDA KEY FINDINGS")
print("-" * 50)
print(f"1. Total Gold table rows: {total_gold_rows:,}")
print(f"2. Total Silver events:   {silver_total:,}")
print(f"3. Batch path events:     {batch_total:,} ({batch_total/silver_total*100:.1f}%)")
print(f"4. Streaming path events: {streaming_total:,} ({streaming_total/silver_total*100:.1f}%)")
print(f"5. Domains covered:       3 (ecommerce, pharmacy, marketplace)")
print(f"6. Gold tables built:     {len(table_counts)}")
print(f"7. Data quality: {sum(1 for d in completeness_data if d['status']=='PASS')}/{len(completeness_data)} key column checks passed")
print(f"8. Date range covered:    2016-09-04 to 2020-09-03 (batch) + 2026-06-18 (streaming)")
