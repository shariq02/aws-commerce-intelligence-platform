# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA-03: MARKETPLACE EXPLORATORY DATA ANALYSIS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Statistical analysis of marketplace domain Gold tables  
# MAGIC **Input:** acip.gold.fact_seller_performance, dim_seller  
# MAGIC **Output:** acip.eda.marketplace_summary

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
EDA_TABLE = f"{CATALOG}.eda.marketplace_summary"

sns.set_theme(style="whitegrid", palette="muted")

print("EDA-03: MARKETPLACE")
print("=" * 70)
print(f"Output: {EDA_TABLE}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Gold Tables
print("STEP 1: LOAD GOLD TABLES")
print("=" * 70)

fact = spark.table(f"{CATALOG}.gold.fact_seller_performance")
dim_seller = spark.table(f"{CATALOG}.gold.dim_seller").filter(F.col("is_current"))

dispatch = fact.filter(F.col("event_type") == "seller.order.dispatched")
price_events = fact.filter(F.col("event_type") == "price.updated")
listings = fact.filter(F.col("event_type") == "listing.created")

print(f"fact_seller_performance:    {fact.count():,} rows")
print(f"  seller.order.dispatched:  {dispatch.count():,}")
print(f"  price.updated:            {price_events.count():,}")
print(f"  listing.created:          {listings.count():,}")
print(f"dim_seller (current):       {dim_seller.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: SLA Breach Rate by Seller Tier
print("STEP 2: SLA BREACH RATE BY SELLER TIER")
print("=" * 70)

sla_by_tier = dispatch.filter(F.col("seller_tier").isNotNull()).groupBy("seller_tier").agg(
    F.count("performance_key").alias("total_dispatches"),
    F.sum(F.col("is_sla_breached").cast("int")).alias("breached_count"),
    F.avg(F.col("is_sla_breached").cast("int")).alias("breach_rate"),
    F.avg("dispatch_time_mins").alias("avg_dispatch_mins"),
    F.avg("dispatch_time_days").alias("avg_dispatch_days")
).orderBy("breach_rate").toPandas()

print("SLA breach rate by seller tier:")
for _, row in sla_by_tier.iterrows():
    print(f"  {row['seller_tier'].upper()}: {row['breach_rate']*100:.1f}% breach rate | {row['total_dispatches']:,.0f} dispatches | avg {row['avg_dispatch_days']:.1f} days")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

tier_colors = {"standard": "#4C72B0", "gold": "#DD8452", "platinum": "#C44E52"}
colors = [tier_colors.get(t, "#8172B2") for t in sla_by_tier["seller_tier"]]

axes[0].bar(sla_by_tier["seller_tier"], sla_by_tier["breach_rate"] * 100, color=colors)
axes[0].set_title("SLA Breach Rate by Seller Tier (%)")
axes[0].set_ylabel("Breach Rate (%)")
axes[0].axhline(y=20, color="red", linestyle="--", label="Alert threshold (20%)")
axes[0].legend()

axes[1].bar(sla_by_tier["seller_tier"], sla_by_tier["avg_dispatch_days"], color=colors)
axes[1].set_title("Average Dispatch Time by Seller Tier (days)")
axes[1].set_ylabel("Days")

plt.tight_layout()
plt.savefig("/tmp/marketplace_sla.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 3: Dispatch Speed Distribution
print("STEP 3: DISPATCH SPEED DISTRIBUTION")
print("=" * 70)

speed_df = dispatch.filter(
    F.col("dispatch_speed_bucket").isNotNull()
).groupBy("dispatch_speed_bucket", "seller_tier").agg(
    F.count("performance_key").alias("count")
).toPandas()

overall_speed = dispatch.filter(
    F.col("dispatch_speed_bucket").isNotNull()
).groupBy("dispatch_speed_bucket").agg(
    F.count("performance_key").alias("count")
).orderBy("count").toPandas()

total = overall_speed["count"].sum()
print("Overall dispatch speed distribution:")
for _, row in overall_speed.iterrows():
    pct = row["count"] / total * 100
    print(f"  {row['dispatch_speed_bucket']}: {row['count']:,} ({pct:.1f}%)")

speed_pivot = speed_df.pivot(
    index="dispatch_speed_bucket",
    columns="seller_tier",
    values="count"
).fillna(0)

speed_order = ["fast", "normal", "slow", "very_slow"]
speed_pivot = speed_pivot.reindex([s for s in speed_order if s in speed_pivot.index])

fig, ax = plt.subplots(figsize=(12, 6))
speed_pivot.plot(kind="bar", ax=ax, color=[tier_colors.get(c, "#8172B2") for c in speed_pivot.columns])
ax.set_title("Dispatch Speed Distribution by Seller Tier")
ax.set_xlabel("Dispatch Speed Bucket")
ax.set_ylabel("Count")
ax.tick_params(axis="x", rotation=0)
plt.tight_layout()
plt.savefig("/tmp/marketplace_speed.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 4: Price Volatility Analysis
print("STEP 4: PRICE VOLATILITY ANALYSIS")
print("=" * 70)

price_df = price_events.filter(F.col("change_pct").isNotNull()).select("change_pct", "seller_tier").toPandas()

print("Price change percentage statistics:")
print(f"  Mean:    {price_df['change_pct'].mean():.2f}%")
print(f"  Median:  {price_df['change_pct'].median():.2f}%")
print(f"  Std:     {price_df['change_pct'].std():.2f}%")
print(f"  Min:     {price_df['change_pct'].min():.2f}%")
print(f"  Max:     {price_df['change_pct'].max():.2f}%")

increases = (price_df["change_pct"] > 0).sum()
decreases = (price_df["change_pct"] < 0).sum()
significant = (price_df["change_pct"].abs() >= 20).sum()

print(f"\nPrice increases: {increases:,} ({increases/len(price_df)*100:.1f}%)")
print(f"Price decreases: {decreases:,} ({decreases/len(price_df)*100:.1f}%)")
print(f"Significant changes (>=20%): {significant:,} ({significant/len(price_df)*100:.1f}%)")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].hist(price_df["change_pct"].clip(-50, 50), bins=50, color="#4C72B0", edgecolor="white")
axes[0].axvline(x=0, color="black", linestyle="--")
axes[0].set_title("Price Change Distribution (clipped at ±50%)")
axes[0].set_xlabel("Price Change (%)")
axes[0].set_ylabel("Count")

price_by_tier = price_df.groupby("seller_tier")["change_pct"].mean().reset_index()
axes[1].bar(price_by_tier["seller_tier"], price_by_tier["change_pct"].abs(),
            color=[tier_colors.get(t, "#8172B2") for t in price_by_tier["seller_tier"]])
axes[1].set_title("Avg Absolute Price Change by Seller Tier (%)")
axes[1].set_ylabel("Avg |Change| (%)")

plt.tight_layout()
plt.savefig("/tmp/marketplace_price.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 5: Seller Revenue Concentration
print("STEP 5: SELLER REVENUE CONCENTRATION")
print("=" * 70)

seller_revenue = dispatch.filter(
    F.col("price").isNotNull() & F.col("seller_key").isNotNull()
).groupBy("seller_key", "seller_tier").agg(
    F.sum("price").alias("total_revenue"),
    F.count("performance_key").alias("dispatch_count")
).orderBy(F.col("total_revenue").desc())

total_sellers = seller_revenue.count()
total_revenue_all = seller_revenue.agg(F.sum("total_revenue")).collect()[0][0]

top_10pct = int(total_sellers * 0.1)
top_sellers_revenue = seller_revenue.limit(top_10pct).agg(F.sum("total_revenue")).collect()[0][0]

print(f"Total sellers: {total_sellers:,}")
print(f"Top 10% sellers ({top_10pct:,}): {top_sellers_revenue/total_revenue_all*100:.1f}% of revenue")

tier_revenue = dispatch.filter(
    F.col("seller_tier").isNotNull() & F.col("price").isNotNull()
).groupBy("seller_tier").agg(
    F.sum("price").alias("total_revenue"),
    F.countDistinct("seller_key").alias("seller_count")
).toPandas()

print("\nRevenue by seller tier:")
total_rev = tier_revenue["total_revenue"].sum()
for _, row in tier_revenue.iterrows():
    pct = row["total_revenue"] / total_rev * 100
    print(f"  {row['seller_tier']}: BRL {row['total_revenue']:,.0f} ({pct:.1f}%) | {row['seller_count']:,} sellers")

fig, ax = plt.subplots(figsize=(10, 5))
ax.pie(tier_revenue["total_revenue"],
       labels=tier_revenue["seller_tier"],
       autopct="%1.1f%%",
       colors=[tier_colors.get(t, "#8172B2") for t in tier_revenue["seller_tier"]])
ax.set_title("Revenue Share by Seller Tier")
plt.tight_layout()
plt.savefig("/tmp/marketplace_revenue.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 6: Freight Ratio Distribution
print("STEP 6: FREIGHT RATIO DISTRIBUTION")
print("=" * 70)

freight_df = dispatch.filter(
    F.col("price").isNotNull() &
    F.col("freight_value").isNotNull() &
    (F.col("price") > 0)
).withColumn(
    "freight_ratio", F.col("freight_value") / F.col("price")
).select("freight_ratio", "seller_tier").toPandas()

print("Freight ratio statistics:")
print(f"  Mean:   {freight_df['freight_ratio'].mean()*100:.1f}%")
print(f"  Median: {freight_df['freight_ratio'].median()*100:.1f}%")
print(f"  P75:    {freight_df['freight_ratio'].quantile(0.75)*100:.1f}%")
print(f"  P90:    {freight_df['freight_ratio'].quantile(0.90)*100:.1f}%")

high_freight = (freight_df["freight_ratio"] > 0.3).sum()
print(f"\nHigh freight ratio (>30% of order value): {high_freight:,} ({high_freight/len(freight_df)*100:.1f}%)")

fig, ax = plt.subplots(figsize=(12, 5))
ax.hist(freight_df["freight_ratio"].clip(0, 2), bins=50, color="#4C72B0", edgecolor="white")
ax.axvline(x=0.3, color="red", linestyle="--", label="High freight threshold (30%)")
ax.set_title("Freight Ratio Distribution (freight / order value)")
ax.set_xlabel("Freight Ratio")
ax.set_ylabel("Count")
ax.legend()
plt.tight_layout()
plt.savefig("/tmp/marketplace_freight.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 7: Write EDA Summary Table
print("STEP 7: WRITE EDA SUMMARY TABLE")
print("=" * 70)

summary_df = spark.createDataFrame(sla_by_tier)

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

top_breach_tier = sla_by_tier.sort_values("breach_rate", ascending=False).iloc[0]

print("MARKETPLACE EDA KEY FINDINGS")
print("-" * 50)
print(f"1. Total marketplace events: {fact.count():,}")
print(f"2. Total dispatch events: {dispatch.count():,}")
print(f"3. Overall SLA breach rate: {sla_by_tier['breach_rate'].mean()*100:.1f}%")
print(f"4. Highest breach tier: {top_breach_tier['seller_tier']} ({top_breach_tier['breach_rate']*100:.1f}%)")
print(f"5. Price increases vs decreases: {increases:,} vs {decreases:,}")
print(f"6. Top 10% sellers control {top_sellers_revenue/total_revenue_all*100:.1f}% of revenue")
print(f"7. High freight ratio orders: {high_freight:,} ({high_freight/len(freight_df)*100:.1f}%)")
print(f"8. Significant price changes (>=20%): {significant:,} events")
print("\nNOTE: High SLA breach rate may indicate SLA thresholds need calibration")
print("      or that dispatch time calculation includes weekends/holidays.")
