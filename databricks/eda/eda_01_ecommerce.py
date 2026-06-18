# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA-01: ECOMMERCE EXPLORATORY DATA ANALYSIS
# MAGIC **AWS Commerce Intelligence Platform**  
# MAGIC **Author:** Sharique Mohammad  
# MAGIC **Date:** June 2026  
# MAGIC **Purpose:** Statistical analysis of ecommerce domain Gold tables  
# MAGIC **Input:** acip.gold.fact_transactions, dim_customer, dim_geography, agg_customer_segments  
# MAGIC **Output:** acip.eda.ecommerce_summary

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
EDA_TABLE = f"{CATALOG}.eda.ecommerce_summary"

sns.set_theme(style="whitegrid", palette="muted")

print("EDA-01: ECOMMERCE")
print("=" * 70)
print(f"Output: {EDA_TABLE}")

# COMMAND ----------

# DBTITLE 1,STEP 1: Load Gold Tables
print("STEP 1: LOAD GOLD TABLES")
print("=" * 70)

fact = spark.table(f"{CATALOG}.gold.fact_transactions")
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer").filter(F.col("is_current"))
dim_geo = spark.table(f"{CATALOG}.gold.dim_geography")
agg_segments = spark.table(f"{CATALOG}.gold.agg_customer_segments")

placed = fact.filter(F.col("event_type") == "order.placed")
fulfilled = fact.filter(F.col("event_type") == "order.fulfilled")
returned = fact.filter(F.col("event_type") == "order.returned")

print(f"fact_transactions:     {fact.count():,} rows")
print(f"  order.placed:        {placed.count():,}")
print(f"  order.fulfilled:     {fulfilled.count():,}")
print(f"  order.returned:      {returned.count():,}")
print(f"dim_customer (current):{dim_customer.count():,}")
print(f"dim_geography:         {dim_geo.count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Order Value Distribution
print("STEP 2: ORDER VALUE DISTRIBUTION")
print("=" * 70)

value_stats = placed.select("total_amount").toPandas()

print("Order value percentiles:")
for p in [25, 50, 75, 90, 95, 99]:
    val = value_stats["total_amount"].quantile(p/100)
    print(f"  P{p}: BRL {val:.2f}")

print(f"Mean:   BRL {value_stats['total_amount'].mean():.2f}")
print(f"Std:    BRL {value_stats['total_amount'].std():.2f}")
print(f"Min:    BRL {value_stats['total_amount'].min():.2f}")
print(f"Max:    BRL {value_stats['total_amount'].max():.2f}")

outlier_threshold = value_stats["total_amount"].quantile(0.99)
outliers = value_stats[value_stats["total_amount"] > outlier_threshold]
print(f"\nOutliers (above P99 = BRL {outlier_threshold:.2f}): {len(outliers):,} orders ({len(outliers)/len(value_stats)*100:.1f}%)")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].hist(value_stats["total_amount"].clip(upper=1000), bins=50, color="#4C72B0", edgecolor="white")
axes[0].set_title("Order Value Distribution (capped at BRL 1000)")
axes[0].set_xlabel("Order Value (BRL)")
axes[0].set_ylabel("Count")

axes[1].hist(value_stats["total_amount"].clip(upper=500), bins=50, color="#55A868", edgecolor="white")
axes[1].set_title("Order Value Distribution (capped at BRL 500)")
axes[1].set_xlabel("Order Value (BRL)")
axes[1].set_ylabel("Count")

plt.tight_layout()
plt.savefig("/tmp/ecommerce_order_value.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 3: Order Status and Fulfilment Bucket Distribution
print("STEP 3: ORDER STATUS AND FULFILMENT BUCKET DISTRIBUTION")
print("=" * 70)

status_dist = placed.filter(F.col("order_status").isNotNull()).groupBy("order_status").count().orderBy(F.col("count").desc()).toPandas()
print("Order status distribution:")
for _, row in status_dist.iterrows():
    pct = row["count"] / placed.count() * 100
    print(f"  {row['order_status']}: {row['count']:,} ({pct:.1f}%)")

bucket_dist = fulfilled.filter(
    F.col("fulfilment_bucket").isNotNull()
).groupBy("fulfilment_bucket").count().orderBy("count").toPandas()

print("\nFulfilment bucket distribution:")
for _, row in bucket_dist.iterrows():
    pct = row["count"] / fulfilled.count() * 100
    print(f"  {row['fulfilment_bucket']}: {row['count']:,} ({pct:.1f}%)")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].barh(status_dist["order_status"], status_dist["count"], color="#4C72B0")
axes[0].set_title("Order Status Distribution")
axes[0].set_xlabel("Count")

colors = {"express": "#55A868", "standard": "#4C72B0", "slow": "#DD8452", "very_slow": "#C44E52"}
bucket_colors = [colors.get(b, "#8172B2") for b in bucket_dist["fulfilment_bucket"]]
axes[1].barh(bucket_dist["fulfilment_bucket"], bucket_dist["count"], color=bucket_colors)
axes[1].set_title("Fulfilment Bucket Distribution")
axes[1].set_xlabel("Count")

plt.tight_layout()
plt.savefig("/tmp/ecommerce_status_bucket.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 4: Customer Segment Analysis
print("STEP 4: CUSTOMER SEGMENT ANALYSIS")
print("=" * 70)

segment_df = agg_segments.toPandas()

print("Customer segment KPIs:")
for _, row in segment_df.sort_values("total_revenue", ascending=False).iterrows():
    print(f"\n  {row['customer_segment'].upper()}")
    print(f"    Orders:           {row['order_count']:,.0f}")
    print(f"    Unique customers: {row['unique_customers']:,.0f}")
    print(f"    Total revenue:    BRL {row['total_revenue']:,.2f}")
    print(f"    Avg order value:  BRL {row['avg_order_value']:,.2f}")
    print(f"    Return rate:      {row['return_rate']*100:.1f}%")
    print(f"    Avg fulfilment:   {row['avg_fulfilment_days']:.1f} days" if row['avg_fulfilment_days'] else f"    Avg fulfilment:   N/A")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

segment_order = segment_df.sort_values("total_revenue", ascending=False)

axes[0, 0].bar(segment_order["customer_segment"], segment_order["total_revenue"], color="#4C72B0")
axes[0, 0].set_title("Total Revenue by Segment")
axes[0, 0].set_ylabel("Revenue (BRL)")
axes[0, 0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))

axes[0, 1].bar(segment_order["customer_segment"], segment_order["avg_order_value"], color="#55A868")
axes[0, 1].set_title("Average Order Value by Segment")
axes[0, 1].set_ylabel("Avg Order Value (BRL)")

axes[1, 0].bar(segment_order["customer_segment"], segment_order["return_rate"] * 100, color="#DD8452")
axes[1, 0].set_title("Return Rate by Segment (%)")
axes[1, 0].set_ylabel("Return Rate (%)")

axes[1, 1].bar(
    segment_order["customer_segment"],
    segment_order["avg_fulfilment_days"].fillna(0),
    color="#8172B2"
)
axes[1, 1].set_title("Avg Fulfilment Days by Segment")
axes[1, 1].set_ylabel("Days")

plt.tight_layout()
plt.savefig("/tmp/ecommerce_segments.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 5: Payment Method Analysis
print("STEP 5: PAYMENT METHOD ANALYSIS")
print("=" * 70)

payment_df = placed.groupBy("payment_method").agg(
    F.count("transaction_key").alias("order_count"),
    F.avg("total_amount").alias("avg_order_value"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg(F.col("is_installment").cast("int")).alias("installment_rate")
).orderBy(F.col("order_count").desc()).toPandas()

print("Payment method breakdown:")
for _, row in payment_df.iterrows():
    pct = row["order_count"] / placed.count() * 100
    print(f"  {row['payment_method']}: {row['order_count']:,.0f} orders ({pct:.1f}%) | avg BRL {row['avg_order_value']:.2f} | installment rate {row['installment_rate']*100:.1f}%")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].pie(payment_df["order_count"], labels=payment_df["payment_method"],
            autopct="%1.1f%%", colors=sns.color_palette("muted"))
axes[0].set_title("Order Count by Payment Method")

axes[1].bar(payment_df["payment_method"], payment_df["avg_order_value"], color="#4C72B0")
axes[1].set_title("Average Order Value by Payment Method")
axes[1].set_ylabel("Avg Order Value (BRL)")

plt.tight_layout()
plt.savefig("/tmp/ecommerce_payment.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 6: Regional Distribution
print("STEP 6: REGIONAL DISTRIBUTION")
print("=" * 70)

placed_with_geo = placed.join(
    dim_geo.select("geo_key", "state_region", "state"),
    on="geo_key", how="left"
)

state_region_df = placed_with_geo.filter(F.col("state_region").isNotNull()).groupBy("state_region").agg(
    F.count("transaction_key").alias("order_count"),
    F.sum("total_amount").alias("total_revenue")
).orderBy(F.col("order_count").desc()).toPandas()

print("Orders by state region:")
for _, row in state_region_df.iterrows():
    pct = row["order_count"] / placed.count() * 100
    print(f"  {row['state_region']}: {row['order_count']:,} orders ({pct:.1f}%) | BRL {row['total_revenue']:,.0f}")

top_states = placed_with_geo.filter(F.col("state").isNotNull()).groupBy("state").agg(
    F.count("transaction_key").alias("order_count")
).orderBy(F.col("order_count").desc()).limit(10).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].barh(state_region_df["state_region"], state_region_df["order_count"], color="#4C72B0")
axes[0].set_title("Orders by State Region")
axes[0].set_xlabel("Order Count")

axes[1].barh(top_states["state"][::-1], top_states["order_count"][::-1], color="#55A868")
axes[1].set_title("Top 10 States by Order Count")
axes[1].set_xlabel("Order Count")

plt.tight_layout()
plt.savefig("/tmp/ecommerce_regional.png", dpi=100, bbox_inches="tight")
plt.show()

# COMMAND ----------

# DBTITLE 1,STEP 7: Delivery On-Time Rate
print("STEP 7: DELIVERY ON-TIME RATE")
print("=" * 70)

on_time_df = fulfilled.filter(F.col("delivery_on_time").isNotNull())
on_time_count = on_time_df.filter(F.col("delivery_on_time")).count()
total_with_data = on_time_df.count()
on_time_rate = on_time_count / total_with_data * 100

print(f"On-time deliveries:  {on_time_count:,} of {total_with_data:,} ({on_time_rate:.1f}%)")
print(f"Late deliveries:     {total_with_data - on_time_count:,} ({100 - on_time_rate:.1f}%)")

on_time_by_segment = fulfilled.filter(
    F.col("delivery_on_time").isNotNull()
).join(
    dim_customer.select("customer_key", "customer_segment"),
    on="customer_key", how="left"
).groupBy("customer_segment").agg(
    F.avg(F.col("delivery_on_time").cast("int")).alias("on_time_rate"),
    F.count("transaction_key").alias("order_count")
).toPandas()

print("\nOn-time rate by customer segment:")
for _, row in on_time_by_segment.iterrows():
    print(f"  {row['customer_segment']}: {row['on_time_rate']*100:.1f}%")

display(on_time_by_segment)

# COMMAND ----------

# DBTITLE 1,STEP 8: Write EDA Summary Table
print("STEP 8: WRITE EDA SUMMARY TABLE")
print("=" * 70)

total_placed = placed.count()
total_fulfilled = fulfilled.count()
total_returned = returned.count()

total_revenue = placed.agg(F.sum("total_amount")).collect()[0][0]
avg_order_value = placed.agg(F.avg("total_amount")).collect()[0][0]
avg_fulfilment_days = fulfilled.agg(F.avg("fulfilment_time_days")).collect()[0][0]
on_time_rate_overall = on_time_rate / 100

summary_data = []
for _, row in segment_df.iterrows():
    summary_data.append({
        "metric_group": "customer_segment",
        "dimension": row["customer_segment"],
        "order_count": int(row["order_count"]),
        "unique_customers": int(row["unique_customers"]),
        "total_revenue": float(row["total_revenue"]),
        "avg_order_value": float(row["avg_order_value"]),
        "return_rate": float(row["return_rate"]),
        "avg_fulfilment_days": float(row["avg_fulfilment_days"]) if row["avg_fulfilment_days"] else None,
    })

summary_df = spark.createDataFrame(pd.DataFrame(summary_data))

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(EDA_TABLE)

written = spark.table(EDA_TABLE).count()
print(f"PASS: {EDA_TABLE} written - {written:,} rows")

# COMMAND ----------

# DBTITLE 1,STEP 9: Key Insights
print("STEP 9: KEY INSIGHTS")
print("=" * 70)

print("ECOMMERCE EDA KEY FINDINGS")
print("-" * 50)
print(f"1. Total orders analysed: {total_placed:,}")
print(f"2. Total revenue: BRL {total_revenue:,.2f}")
print(f"3. Average order value: BRL {avg_order_value:.2f}")
print(f"4. Fulfilment rate: {total_fulfilled/total_placed*100:.1f}% of orders fulfilled")
print(f"5. Return rate: {total_returned/total_placed*100:.1f}% of orders returned")
print(f"6. Average fulfilment time: {avg_fulfilment_days:.1f} days")
print(f"7. On-time delivery rate: {on_time_rate:.1f}%")
print(f"8. Premium segment drives {segment_df[segment_df['customer_segment']=='premium']['total_revenue'].values[0]/total_revenue*100:.1f}% of revenue")
