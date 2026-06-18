# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA-02: PHARMACY EXPLORATORY DATA ANALYSIS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Statistical analysis of pharmacy domain Gold tables  
# MAGIC **Input:** acip.gold.fact_inventory_snapshots, dim_product  
# MAGIC **Output:** acip.eda.pharmacy_summary

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
EDA_TABLE = f"{CATALOG}.eda.pharmacy_summary"

sns.set_theme(style="whitegrid", palette="muted")

print("EDA-02: PHARMACY")
print("=" * 70)
print(f"Output: {EDA_TABLE}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Gold Tables
print("STEP 1: LOAD GOLD TABLES")
print("=" * 70)

fact = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")
dim_product = spark.table(f"{CATALOG}.gold.dim_product").filter(F.col("domain") == "pharmacy")

print(f"fact_inventory_snapshots: {fact.count():,} rows")
print(f"dim_product (pharmacy):   {dim_product.count():,} rows")
print(f"Columns: {fact.columns}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Drug Category Demand Analysis
print("STEP 2: DRUG CATEGORY DEMAND ANALYSIS")
print("=" * 70)

fact_with_product = fact.join(
    dim_product.select("product_key", "product_id", "category", "atc_code", "drug_class",
                       F.col("is_prescription").alias("product_is_prescription")),
    on="product_key", how="left"
)

category_df = fact_with_product.groupBy("category", "product_is_prescription").agg(
    F.sum("quantity").alias("total_quantity"),
    F.avg("quantity").alias("avg_quantity"),
    F.count("snapshot_key").alias("transaction_count"),
    F.avg("fill_time_mins").alias("avg_fill_time")
).orderBy(F.col("total_quantity").desc()).toPandas()

print("Drug category demand:")
for _, row in category_df.iterrows():
    rx = "Rx" if row["product_is_prescription"] else "OTC"
    print(f"  [{rx}] {row['category']}: {row['total_quantity']:,.0f} units | avg {row['avg_quantity']:.2f}/record | fill {row['avg_fill_time']:.0f} mins")

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

colors = ["#C44E52" if rx else "#4C72B0" for rx in category_df["product_is_prescription"]]
axes[0].barh(category_df["category"][::-1], category_df["total_quantity"][::-1], color=colors[::-1])
axes[0].set_title("Total Sales Quantity by Drug Category\n(Red=Prescription, Blue=OTC)")
axes[0].set_xlabel("Total Quantity")

axes[1].bar(category_df["category"], category_df["avg_fill_time"],
            color=["#C44E52" if rx else "#4C72B0" for rx in category_df["product_is_prescription"]])
axes[1].set_title("Average Fill Time by Drug Category (mins)")
axes[1].set_ylabel("Fill Time (mins)")
axes[1].tick_params(axis="x", rotation=45)

plt.tight_layout()
plt.savefig("/tmp/pharmacy_category.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 3: Prescription vs OTC Comparison
print("STEP 3: PRESCRIPTION VS OTC COMPARISON")
print("=" * 70)

rx_otc = fact_with_product.filter(F.col("product_is_prescription").isNotNull()).groupBy("product_is_prescription").agg(
    F.sum("quantity").alias("total_quantity"),
    F.count("snapshot_key").alias("transaction_count"),
    F.avg("fill_time_mins").alias("avg_fill_time"),
    F.avg("stock_level").alias("avg_stock_level"),
    F.avg("days_of_supply").alias("avg_days_of_supply")
).toPandas()

for _, row in rx_otc.iterrows():
    label = "PRESCRIPTION (Rx)" if row["product_is_prescription"] else "OTC"
    print(f"\n{label}:")
    print(f"  Total quantity sold:  {row['total_quantity']:,.0f}")
    print(f"  Transaction count:    {row['transaction_count']:,.0f}")
    print(f"  Avg fill time:        {row['avg_fill_time']:.1f} mins")
    print(f"  Avg stock level:      {row['avg_stock_level']:.0f} units")
    print(f"  Avg days of supply:   {row['avg_days_of_supply']:.1f} days")

fig, axes = plt.subplots(1, 3, figsize=(16, 5))

labels = ["OTC", "Prescription"]
rx_otc_sorted = rx_otc.sort_values("product_is_prescription")

axes[0].pie(rx_otc_sorted["total_quantity"], labels=labels,
            autopct="%1.1f%%", colors=["#4C72B0", "#C44E52"])
axes[0].set_title("Sales Volume: Rx vs OTC")

axes[1].bar(labels, rx_otc_sorted["avg_fill_time"], color=["#4C72B0", "#C44E52"])
axes[1].set_title("Avg Fill Time: Rx vs OTC")
axes[1].set_ylabel("Minutes")

axes[2].bar(labels, rx_otc_sorted["avg_days_of_supply"], color=["#4C72B0", "#C44E52"])
axes[2].set_title("Avg Days of Supply: Rx vs OTC")
axes[2].set_ylabel("Days")

plt.tight_layout()
plt.savefig("/tmp/pharmacy_rx_otc.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 4: Hourly Demand Heatmap
print("STEP 4: HOURLY DEMAND HEATMAP")
print("=" * 70)

hourly_df = fact.filter(
    F.col("hour_of_day").isNotNull() &
    F.col("is_weekend").isNotNull()
).groupBy("hour_of_day", "is_weekend").agg(
    F.sum("quantity").alias("total_quantity")
).toPandas()

hourly_df["day_type"] = hourly_df["is_weekend"].map({True: "Weekend", False: "Weekday"})

pivot = hourly_df.pivot(index="hour_of_day", columns="day_type", values="total_quantity").fillna(0)

print("Peak hours by day type:")
for col in pivot.columns:
    peak_hour = pivot[col].idxmax()
    print(f"  {col}: peak at hour {peak_hour} ({pivot[col][peak_hour]:,.0f} units)")

fig, ax = plt.subplots(figsize=(12, 6))
pivot.plot(ax=ax, marker="o", linewidth=2)
ax.set_title("Hourly Drug Sales Quantity: Weekday vs Weekend")
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Total Quantity Sold")
ax.set_xticks(range(0, 24))
ax.legend(title="Day Type")
plt.tight_layout()
plt.savefig("/tmp/pharmacy_hourly.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 5: Stock Alert Level Analysis
print("STEP 5: STOCK ALERT LEVEL ANALYSIS")
print("=" * 70)

alert_df = fact.groupBy("stock_alert_level").agg(
    F.count("snapshot_key").alias("count"),
    F.avg("stock_level").alias("avg_stock"),
    F.avg("days_of_supply").alias("avg_days_supply")
).orderBy("count").toPandas()

total = alert_df["count"].sum()
print("Stock alert level distribution:")
for _, row in alert_df.iterrows():
    pct = row["count"] / total * 100
    print(f"  {row['stock_alert_level']}: {row['count']:,} ({pct:.1f}%) | avg stock {row['avg_stock']:.0f} | avg days supply {row['avg_days_supply']:.1f}")

alert_colors = {
    "normal": "#55A868", "medium": "#DD8452",
    "high": "#C44E52", "critical": "#8B0000"
}
colors = [alert_colors.get(a, "#4C72B0") for a in alert_df["stock_alert_level"]]

fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(alert_df["stock_alert_level"], alert_df["count"], color=colors)
ax.set_title("Stock Alert Level Distribution")
ax.set_xlabel("Count")
plt.tight_layout()
plt.savefig("/tmp/pharmacy_alerts.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 6: Days of Supply Distribution
print("STEP 6: DAYS OF SUPPLY DISTRIBUTION")
print("=" * 70)

dos_df = fact.filter(F.col("days_of_supply").isNotNull()).select("days_of_supply").toPandas()

print("Days of supply statistics:")
print(f"  Mean:   {dos_df['days_of_supply'].mean():.1f} days")
print(f"  Median: {dos_df['days_of_supply'].median():.1f} days")
print(f"  P75:    {dos_df['days_of_supply'].quantile(0.75):.1f} days")
print(f"  P90:    {dos_df['days_of_supply'].quantile(0.90):.1f} days")

critical = (dos_df["days_of_supply"] < 3).sum()
high_risk = ((dos_df["days_of_supply"] >= 3) & (dos_df["days_of_supply"] < 7)).sum()
print(f"\nCritical risk (<3 days): {critical:,} ({critical/len(dos_df)*100:.1f}%)")
print(f"High risk (3-7 days):    {high_risk:,} ({high_risk/len(dos_df)*100:.1f}%)")

fig, ax = plt.subplots(figsize=(12, 5))
ax.hist(dos_df["days_of_supply"].clip(upper=100), bins=50, color="#4C72B0", edgecolor="white")
ax.axvline(x=3, color="red", linestyle="--", label="Critical threshold (3 days)")
ax.axvline(x=7, color="orange", linestyle="--", label="High risk threshold (7 days)")
ax.set_title("Days of Supply Distribution")
ax.set_xlabel("Days of Supply")
ax.set_ylabel("Count")
ax.legend()
plt.tight_layout()
plt.savefig("/tmp/pharmacy_dos.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 7: Write EDA Summary Table
print("STEP 7: WRITE EDA SUMMARY TABLE")
print("=" * 70)

summary_df = spark.createDataFrame(
    category_df.rename(columns={
        "total_quantity": "total_quantity",
        "avg_quantity": "avg_quantity_per_record",
        "transaction_count": "transaction_count",
        "avg_fill_time": "avg_fill_time_mins"
    })
)

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EDA_TABLE)

written = spark.table(EDA_TABLE).count()
print(f"PASS: {EDA_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 8: Key Insights
print("STEP 8: KEY INSIGHTS")
print("=" * 70)

top_category = category_df.iloc[0]
rx_total = rx_otc_sorted[rx_otc_sorted["product_is_prescription"] == True]["total_quantity"].values[0]
otc_total = rx_otc_sorted[rx_otc_sorted["product_is_prescription"] == False]["total_quantity"].values[0]
total_quantity = rx_total + otc_total

print("PHARMACY EDA KEY FINDINGS")
print("-" * 50)
print(f"1. Total drug transactions: {fact.count():,}")
print(f"2. Top selling category: {top_category['category']} ({top_category['total_quantity']:,.0f} units)")
print(f"3. Prescription drugs: {rx_total/total_quantity*100:.1f}% of total volume")
print(f"4. OTC drugs: {otc_total/total_quantity*100:.1f}% of total volume")
print(f"5. Critical stock situations: {critical:,} records ({critical/len(dos_df)*100:.1f}%)")
print(f"6. Avg prescription fill time: {rx_otc_sorted[rx_otc_sorted['product_is_prescription']==True]['avg_fill_time'].values[0]:.0f} mins vs OTC {rx_otc_sorted[rx_otc_sorted['product_is_prescription']==False]['avg_fill_time'].values[0]:.0f} mins")
