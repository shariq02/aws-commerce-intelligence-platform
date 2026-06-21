# Databricks notebook source
# MAGIC %md
# MAGIC ## GOLD LAYER DATA QUALITY VALIDATION
# MAGIC **AWS Commerce Intelligence Platform**
# MAGIC **Author:** Sharique Mohammad
# MAGIC **Date:** June 2026
# MAGIC **Purpose:** Comprehensive data quality gate between Gold layer and dbt
# MAGIC **Run after:** 17_agg_customer_segments.py
# MAGIC **Run before:** dbt run
# MAGIC **Output:** PASS/WARN/FAIL per check with final gate decision

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configuration
spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"

print("GOLD LAYER DATA QUALITY VALIDATION")
print("=" * 70)
print(f"Catalog: {CATALOG}")
print(f"Run at:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Validation Framework

results = []

def check(name, status, detail, hard_fail=True):
    """
    Register a check result.
    status: PASS, WARN, FAIL
    hard_fail: if True and status=FAIL, blocks dbt gate
    """
    label = "HARD_FAIL" if (status == "FAIL" and hard_fail) else status
    results.append({
        "check": name,
        "status": label,
        "detail": detail
    })
    symbol = "PASS" if status == "PASS" else ("WARN" if status == "WARN" else "FAIL")
    print(f"  [{symbol}] {name}: {detail}")

def count_blank(df, col_name):
    """Count blank/whitespace strings that are not null."""
    return df.filter(
        F.col(col_name).isNotNull() &
        (F.trim(F.col(col_name)) == "")
    ).count()

def count_placeholder(df, col_name):
    """Count placeholder strings: -, N/A, NA, none, null, ., unknown."""
    placeholders = ["-", "N/A", "NA", "none", "null", "NULL", ".", "unknown", "UNKNOWN", "n/a"]
    return df.filter(
        F.col(col_name).isNotNull() &
        F.trim(F.col(col_name)).isin(placeholders)
    ).count()

def count_negative(df, col_name):
    """Count negative values in a numeric column."""
    return df.filter(F.col(col_name).isNotNull() & (F.col(col_name) < 0)).count()

def null_rate(count, total):
    return round(count / max(total, 1) * 100, 2)

# COMMAND ----------

# DBTITLE 1,SECTION 1: ROW COUNT ASSERTIONS
print("\nSECTION 1: ROW COUNT ASSERTIONS")
print("=" * 70)

MIN_ROWS = {
    "dim_date":                 1800,
    "dim_geography":            4000,
    "dim_product":              30000,
    "dim_customer":             90000,
    "dim_seller":               3000,
    "fact_transactions":        200000,
    "fact_inventory_snapshots": 60000,
    "fact_seller_performance":  180000,
    "agg_daily_domain_metrics": 3000,
    "agg_customer_segments":    4,
}

for table, min_rows in MIN_ROWS.items():
    count = spark.table(f"{CATALOG}.gold.{table}").count()
    if count >= min_rows:
        check(f"row_count.{table}", "PASS", f"{count:,} rows (min {min_rows:,})")
    else:
        check(f"row_count.{table}", "FAIL", f"{count:,} rows below minimum {min_rows:,}", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 2: PRIMARY KEY INTEGRITY
print("\nSECTION 2: PRIMARY KEY INTEGRITY")
print("=" * 70)

PRIMARY_KEYS = {
    "dim_date":                 "date_key",
    "dim_geography":            "geo_key",
    "dim_product":              "product_key",
    "dim_customer":             "customer_key",
    "dim_seller":               "seller_key",
    "fact_transactions":        "transaction_key",
    "fact_inventory_snapshots": "snapshot_key",
    "fact_seller_performance":  "performance_key",
}

for table, pk in PRIMARY_KEYS.items():
    df = spark.table(f"{CATALOG}.gold.{table}")
    total = df.count()
    null_pk = df.filter(F.col(pk).isNull()).count()
    dup_pk = df.groupBy(pk).count().filter(F.col("count") > 1).count()

    if null_pk == 0:
        check(f"pk_null.{table}.{pk}", "PASS", "No null primary keys")
    else:
        check(f"pk_null.{table}.{pk}", "FAIL", f"{null_pk:,} null primary keys ({null_rate(null_pk, total)}%)", hard_fail=True)

    if dup_pk == 0:
        check(f"pk_dup.{table}.{pk}", "PASS", "No duplicate primary keys")
    else:
        check(f"pk_dup.{table}.{pk}", "FAIL", f"{dup_pk:,} duplicate primary key values", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 3: DATE KEY COVERAGE
print("\nSECTION 3: DATE KEY COVERAGE")
print("=" * 70)

dim_date_range = spark.sql(f"""
    SELECT MIN(full_date) as min_date, MAX(full_date) as max_date
    FROM {CATALOG}.gold.dim_date
""").collect()[0]
dim_min = dim_date_range["min_date"]
dim_max = dim_date_range["max_date"]
print(f"  dim_date range: {dim_min} to {dim_max}")

FACT_DATE_TABLES = {
    "fact_transactions":        ("date_key", "occurred_at"),
    "fact_inventory_snapshots": ("date_key", "occurred_at"),
    "fact_seller_performance":  ("date_key", "occurred_at"),
}

for table, (date_key_col, occurred_col) in FACT_DATE_TABLES.items():
    df = spark.table(f"{CATALOG}.gold.{table}")
    total = df.count()
    null_dk = df.filter(F.col(date_key_col).isNull()).count()
    null_rate_pct = null_rate(null_dk, total)

    if null_rate_pct == 0:
        check(f"date_key_coverage.{table}", "PASS", f"0 null date_keys")
    elif null_rate_pct < 1:
        check(f"date_key_coverage.{table}", "WARN", f"{null_dk:,} null date_keys ({null_rate_pct}%) -- streaming events outside dim_date range", hard_fail=False)
    elif null_rate_pct < 5:
        check(f"date_key_coverage.{table}", "WARN", f"{null_dk:,} null date_keys ({null_rate_pct}%) -- check dim_date range", hard_fail=False)
    else:
        check(f"date_key_coverage.{table}", "FAIL", f"{null_dk:,} null date_keys ({null_rate_pct}%) -- dim_date range likely too narrow", hard_fail=True)

# Check dim_date covers all fact event dates
for table in ["fact_transactions", "fact_seller_performance"]:
    df = spark.table(f"{CATALOG}.gold.{table}")
    event_range = df.select(
        F.min(F.to_date(F.col("occurred_at"))).alias("min_event"),
        F.max(F.to_date(F.col("occurred_at"))).alias("max_event")
    ).collect()[0]
    min_event = event_range["min_event"]
    max_event = event_range["max_event"]

    if max_event and max_event > dim_max:
        check(f"dim_date_range.{table}", "FAIL",
              f"Events up to {max_event} but dim_date only covers to {dim_max} -- extend dim_date", hard_fail=True)
    elif min_event and min_event < dim_min:
        check(f"dim_date_range.{table}", "WARN",
              f"Events from {min_event} but dim_date starts at {dim_min}", hard_fail=False)
    else:
        check(f"dim_date_range.{table}", "PASS", f"Event dates {min_event} to {max_event} within dim_date range")

# Check pharmacy occurred_at parseability
print("\n  Checking pharmacy occurred_at parseability...")
pharma_df = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")
total_pharma = pharma_df.count()
unparseable = pharma_df.filter(
    F.to_date(F.col("occurred_at")).isNull() &
    F.col("occurred_at").isNotNull()
).count()
unparseable_rate = null_rate(unparseable, total_pharma)

if unparseable == 0:
    check("occurred_at_parseable.fact_inventory_snapshots", "PASS", "All occurred_at values parseable")
elif unparseable_rate < 5:
    check("occurred_at_parseable.fact_inventory_snapshots", "WARN",
          f"{unparseable:,} unparseable occurred_at ({unparseable_rate}%)", hard_fail=False)
else:
    check("occurred_at_parseable.fact_inventory_snapshots", "FAIL",
          f"{unparseable:,} unparseable occurred_at ({unparseable_rate}%) -- fix occurred_at format in notebook 14", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 4: REFERENTIAL INTEGRITY
print("\nSECTION 4: REFERENTIAL INTEGRITY")
print("=" * 70)

ri_checks = [
    ("fact_transactions",        "customer_key", "dim_customer",  "customer_key"),
    ("fact_transactions",        "geo_key",       "dim_geography", "geo_key"),
    ("fact_seller_performance",  "seller_key",    "dim_seller",    "seller_key"),
    ("fact_inventory_snapshots", "product_key",   "dim_product",   "product_key"),
]

for fact_table, fk_col, dim_table, dim_pk in ri_checks:
    orphans = spark.sql(f"""
        SELECT COUNT(*) as orphans
        FROM {CATALOG}.gold.{fact_table} f
        LEFT JOIN {CATALOG}.gold.{dim_table} d ON f.{fk_col} = d.{dim_pk}
        WHERE d.{dim_pk} IS NULL AND f.{fk_col} IS NOT NULL
    """).collect()[0]["orphans"]

    if orphans == 0:
        check(f"ri.{fact_table}.{fk_col}", "PASS", "No orphan foreign keys")
    else:
        check(f"ri.{fact_table}.{fk_col}", "FAIL", f"{orphans:,} orphan keys -- referential integrity broken", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 5: NULL CHECKS ON CRITICAL COLUMNS
print("\nSECTION 5: NULL CHECKS ON CRITICAL COLUMNS")
print("=" * 70)

# fact_transactions critical columns
ft = spark.table(f"{CATALOG}.gold.fact_transactions")
ft_total = ft.count()

for col_name in ["total_amount", "payment_method", "order_status"]:
    null_count = ft.filter(F.col(col_name).isNull()).count()
    rate = null_rate(null_count, ft_total)
    if null_count == 0:
        check(f"null.fact_transactions.{col_name}", "PASS", "No nulls")
    elif rate < 1:
        check(f"null.fact_transactions.{col_name}", "WARN", f"{null_count:,} nulls ({rate}%) -- likely streaming events", hard_fail=False)
    else:
        check(f"null.fact_transactions.{col_name}", "FAIL", f"{null_count:,} nulls ({rate}%)", hard_fail=True)

# fact_inventory_snapshots critical columns
fi = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")
fi_total = fi.count()

for col_name in ["stock_level", "reorder_threshold", "is_prescription"]:
    null_count = fi.filter(F.col(col_name).isNull()).count()
    rate = null_rate(null_count, fi_total)
    if null_count == 0:
        check(f"null.fact_inventory_snapshots.{col_name}", "PASS", "No nulls")
    elif rate < 0.2:
        check(f"null.fact_inventory_snapshots.{col_name}", "WARN", f"{null_count:,} nulls ({rate}%) -- review payload parsing", hard_fail=False)
    else:
        check(f"null.fact_inventory_snapshots.{col_name}", "FAIL", f"{null_count:,} nulls ({rate}%)", hard_fail=True)

# fact_seller_performance -- nulls acceptable on listing.created only
fp = spark.table(f"{CATALOG}.gold.fact_seller_performance")
fp_total = fp.count()

dispatch_events = fp.filter(F.col("event_type") == "seller.order.dispatched")
dispatch_total = dispatch_events.count()

for col_name in ["price", "is_sla_breached", "seller_tier"]:
    null_on_dispatch = dispatch_events.filter(F.col(col_name).isNull()).count()
    rate = null_rate(null_on_dispatch, dispatch_total)
    if null_on_dispatch == 0:
        check(f"null.fact_seller_performance.{col_name} (dispatch only)", "PASS", "No nulls on dispatch events")
    elif rate < 1:
        check(f"null.fact_seller_performance.{col_name} (dispatch only)", "WARN", f"{null_on_dispatch:,} nulls on dispatch events ({rate}%)", hard_fail=False)
    else:
        check(f"null.fact_seller_performance.{col_name} (dispatch only)", "FAIL", f"{null_on_dispatch:,} nulls on dispatch events ({rate}%)", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 6: STRING QUALITY CHECKS
print("\nSECTION 6: STRING QUALITY CHECKS")
print("=" * 70)

STRING_CHECKS = [
    ("fact_transactions",        ["order_status", "payment_method", "fulfilment_bucket"]),
    ("fact_inventory_snapshots", ["stock_alert_level", "time_of_day"]),
    ("fact_seller_performance",  ["seller_tier", "dispatch_speed_bucket", "category"]),
    ("dim_customer",             ["customer_segment", "customer_state"]),
    ("dim_seller",               ["seller_tier", "seller_state"]),
    ("dim_product",              ["category", "domain"]),
]

for table, cols in STRING_CHECKS:
    df = spark.table(f"{CATALOG}.gold.{table}")
    total = df.count()
    for col_name in cols:
        blank = count_blank(df, col_name)
        placeholder = count_placeholder(df, col_name)

        if blank == 0 and placeholder == 0:
            check(f"string_quality.{table}.{col_name}", "PASS", "No blank or placeholder values")
        else:
            issues = []
            if blank > 0:
                issues.append(f"{blank:,} blank/whitespace")
            if placeholder > 0:
                issues.append(f"{placeholder:,} placeholders (-/N/A/null/etc)")
            rate = null_rate(blank + placeholder, total)
            if rate < 1:
                check(f"string_quality.{table}.{col_name}", "WARN", " | ".join(issues), hard_fail=False)
            else:
                check(f"string_quality.{table}.{col_name}", "FAIL", " | ".join(issues), hard_fail=True)

# Leading/trailing whitespace check on ID columns
ID_COLS = [
    ("dim_customer", "customer_id"),
    ("dim_seller",   "seller_id"),
    ("dim_product",  "product_id"),
]

for table, col_name in ID_COLS:
    df = spark.table(f"{CATALOG}.gold.{table}")
    whitespace_count = df.filter(
        F.col(col_name).isNotNull() &
        (F.col(col_name) != F.trim(F.col(col_name)))
    ).count()
    if whitespace_count == 0:
        check(f"whitespace.{table}.{col_name}", "PASS", "No leading/trailing whitespace")
    else:
        check(f"whitespace.{table}.{col_name}", "FAIL", f"{whitespace_count:,} values with leading/trailing whitespace", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 7: NUMERIC QUALITY CHECKS
print("\nSECTION 7: NUMERIC QUALITY CHECKS")
print("=" * 70)

# Negative value checks
NEGATIVE_CHECKS = [
    ("fact_transactions",        "total_amount"),
    ("fact_inventory_snapshots", "stock_level"),
    ("fact_inventory_snapshots", "quantity"),
    ("fact_inventory_snapshots", "reorder_threshold"),
    ("fact_seller_performance",  "price"),
    ("fact_seller_performance",  "freight_value"),
    ("fact_seller_performance",  "dispatch_time_days"),
]

for table, col_name in NEGATIVE_CHECKS:
    df = spark.table(f"{CATALOG}.gold.{table}")
    neg_count = count_negative(df, col_name)
    if neg_count == 0:
        check(f"negative.{table}.{col_name}", "PASS", "No negative values")
    else:
        check(f"negative.{table}.{col_name}", "FAIL", f"{neg_count:,} negative values", hard_fail=True)

# Unrealistic value checks
print("\n  Checking unrealistic values...")

# Price > 100,000
high_price = spark.table(f"{CATALOG}.gold.fact_seller_performance") \
    .filter(F.col("price") > 100000).count()
if high_price == 0:
    check("unrealistic.fact_seller_performance.price", "PASS", "No price > 100,000")
else:
    check("unrealistic.fact_seller_performance.price", "WARN", f"{high_price:,} rows with price > 100,000", hard_fail=False)

# Dispatch time > 365 days
high_dispatch = spark.table(f"{CATALOG}.gold.fact_seller_performance") \
    .filter(F.col("dispatch_time_days").isNotNull() & (F.col("dispatch_time_days") > 365)).count()
if high_dispatch == 0:
    check("unrealistic.fact_seller_performance.dispatch_time_days", "PASS", "No dispatch_time_days > 365")
else:
    check("unrealistic.fact_seller_performance.dispatch_time_days", "WARN", f"{high_dispatch:,} rows with dispatch_time_days > 365", hard_fail=False)

# fill_time_mins > 1440 (more than 24 hours)
high_fill = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots") \
    .filter(F.col("fill_time_mins").isNotNull() & (F.col("fill_time_mins") > 1440)).count()
if high_fill == 0:
    check("unrealistic.fact_inventory_snapshots.fill_time_mins", "PASS", "No fill_time_mins > 1440")
else:
    check("unrealistic.fact_inventory_snapshots.fill_time_mins", "WARN", f"{high_fill:,} rows with fill_time_mins > 1440 mins", hard_fail=False)

# total_amount = 0
zero_amount = spark.table(f"{CATALOG}.gold.fact_transactions") \
    .filter(F.col("total_amount") == 0).count()
if zero_amount == 0:
    check("unrealistic.fact_transactions.total_amount_zero", "PASS", "No zero total_amount")
else:
    check("unrealistic.fact_transactions.total_amount_zero", "WARN", f"{zero_amount:,} rows with total_amount = 0", hard_fail=False)

# COMMAND ----------

# DBTITLE 1,SECTION 8: CATEGORICAL / ENUM CHECKS
print("\nSECTION 8: CATEGORICAL AND ENUM CHECKS")
print("=" * 70)

ENUM_CHECKS = [
    ("fact_transactions",        "order_status",
     ["delivered", "shipped", "canceled", "invoiced", "processing",
      "unavailable", "approved", "created"]),

    ("fact_inventory_snapshots", "stock_alert_level",
     ["normal", "medium", "high", "critical"]),

    ("fact_inventory_snapshots", "time_of_day",
     ["morning", "afternoon", "evening", "night"]),

    ("fact_seller_performance",  "seller_tier",
     ["standard", "gold", "platinum"]),

    ("fact_seller_performance",  "event_type",
     ["seller.order.dispatched", "listing.created", "price.updated"]),

    ("agg_daily_domain_metrics", "domain",
     ["ecommerce", "pharmacy", "marketplace"]),

    ("dim_product",              "domain",
     ["ecommerce", "pharmacy", "marketplace"]),
]

for table, col_name, valid_values in ENUM_CHECKS:
    df = spark.table(f"{CATALOG}.gold.{table}")
    invalid = df.filter(
        F.col(col_name).isNotNull() &
        ~F.col(col_name).isin(valid_values)
    ).count()
    if invalid == 0:
        check(f"enum.{table}.{col_name}", "PASS", f"All values in {valid_values}")
    else:
        sample = df.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).isin(valid_values)
        ).select(col_name).distinct().limit(5).collect()
        sample_vals = [r[col_name] for r in sample]
        check(f"enum.{table}.{col_name}", "WARN",
              f"{invalid:,} unexpected values: {sample_vals}", hard_fail=False)

# Domain cross-event check -- pharmacy events should not appear in fact_transactions
print("\n  Checking domain event type cross-contamination...")
pharmacy_in_ecommerce = spark.table(f"{CATALOG}.gold.fact_transactions") \
    .filter(F.col("event_type").isin(["prescription.filled", "prescription.submitted", "inventory.updated"])) \
    .count()
if pharmacy_in_ecommerce == 0:
    check("domain_contamination.fact_transactions", "PASS", "No pharmacy events in fact_transactions")
else:
    check("domain_contamination.fact_transactions", "FAIL",
          f"{pharmacy_in_ecommerce:,} pharmacy events found in fact_transactions", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 9: CROSS-COLUMN CONSISTENCY CHECKS
print("\nSECTION 9: CROSS-COLUMN CONSISTENCY CHECKS")
print("=" * 70)

# Stock alert level vs actual stock levels
fi = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")

# critical: stock_level <= reorder_threshold * 0.5
critical_wrong = fi.filter(
    (F.col("stock_alert_level") == "critical") &
    (F.col("stock_level") > F.col("reorder_threshold") * 0.5) &
    F.col("stock_level").isNotNull() &
    F.col("reorder_threshold").isNotNull()
).count()

if critical_wrong == 0:
    check("consistency.stock_alert_level_critical", "PASS", "Critical alert matches stock thresholds")
else:
    check("consistency.stock_alert_level_critical", "WARN",
          f"{critical_wrong:,} rows marked critical but stock above critical threshold", hard_fail=False)

# SLA breach flag vs actual dispatch time
fp = spark.table(f"{CATALOG}.gold.fact_seller_performance") \
    .filter(F.col("event_type") == "seller.order.dispatched")

breach_inconsistent = fp.filter(
    F.col("is_sla_breached").isNotNull() &
    F.col("dispatch_time_mins").isNotNull() &
    F.col("sla_threshold_mins").isNotNull() &
    (
        ((F.col("is_sla_breached") == True) & (F.col("dispatch_time_mins") <= F.col("sla_threshold_mins"))) |
        ((F.col("is_sla_breached") == False) & (F.col("dispatch_time_mins") > F.col("sla_threshold_mins")))
    )
).count()

dispatch_total = fp.count()
breach_rate = null_rate(breach_inconsistent, dispatch_total)

if breach_inconsistent == 0:
    check("consistency.sla_breach_flag", "PASS", "SLA breach flag consistent with dispatch times")
elif breach_rate < 1:
    check("consistency.sla_breach_flag", "WARN",
          f"{breach_inconsistent:,} rows where breach flag disagrees with dispatch time ({breach_rate}%)", hard_fail=False)
else:
    check("consistency.sla_breach_flag", "FAIL",
          f"{breach_inconsistent:,} inconsistent SLA breach flags ({breach_rate}%)", hard_fail=True)

# is_weekend vs day_of_week consistency in dim_date
dim_date = spark.table(f"{CATALOG}.gold.dim_date")
weekend_inconsistent = dim_date.filter(
    ((F.col("is_weekend") == True) & ~F.col("day_of_week").isin(1, 7)) |
    ((F.col("is_weekend") == False) & F.col("day_of_week").isin(1, 7))
).count()

if weekend_inconsistent == 0:
    check("consistency.dim_date.is_weekend_vs_day_of_week", "PASS", "is_weekend consistent with day_of_week")
else:
    check("consistency.dim_date.is_weekend_vs_day_of_week", "FAIL",
          f"{weekend_inconsistent:,} rows where is_weekend disagrees with day_of_week", hard_fail=True)

# SCD2 effective_date vs expiry_date
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer")
inverted_scd2 = dim_customer.filter(
    F.col("effective_date").isNotNull() &
    F.col("expiry_date").isNotNull() &
    (F.col("effective_date") >= F.col("expiry_date"))
).count()

if inverted_scd2 == 0:
    check("consistency.dim_customer.scd2_dates", "PASS", "effective_date < expiry_date for all rows")
else:
    check("consistency.dim_customer.scd2_dates", "FAIL",
          f"{inverted_scd2:,} rows where effective_date >= expiry_date", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 10: DUPLICATE EVENT CHECKS
print("\nSECTION 10: DUPLICATE EVENT CHECKS")
print("=" * 70)

# Duplicate order_id for order.placed in fact_transactions
dup_orders = spark.sql(f"""
    SELECT COUNT(*) as dup_count
    FROM (
        SELECT order_id, COUNT(*) as cnt
        FROM {CATALOG}.gold.fact_transactions
        WHERE event_type = 'order.placed'
        AND order_id IS NOT NULL
        GROUP BY order_id
        HAVING COUNT(*) > 1
    )
""").collect()[0]["dup_count"]

if dup_orders == 0:
    check("duplicates.fact_transactions.order_placed", "PASS", "No duplicate order.placed events per order_id")
else:
    check("duplicates.fact_transactions.order_placed", "WARN",
          f"{dup_orders:,} order_ids with multiple order.placed events", hard_fail=False)

# Duplicate snapshot_key in fact_inventory_snapshots
dup_snapshots = spark.sql(f"""
    SELECT COUNT(*) as dup_count
    FROM (
        SELECT snapshot_key, COUNT(*) as cnt
        FROM {CATALOG}.gold.fact_inventory_snapshots
        GROUP BY snapshot_key
        HAVING COUNT(*) > 1
    )
""").collect()[0]["dup_count"]

if dup_snapshots == 0:
    check("duplicates.fact_inventory_snapshots.snapshot_key", "PASS", "No duplicate snapshot keys")
else:
    check("duplicates.fact_inventory_snapshots.snapshot_key", "FAIL",
          f"{dup_snapshots:,} duplicate snapshot keys", hard_fail=True)

# COMMAND ----------

# DBTITLE 1,SECTION 11: DOMAIN AND COMPLETENESS CHECKS
print("\nSECTION 11: DOMAIN AND COMPLETENESS CHECKS")
print("=" * 70)

# All 3 domains in silver.events
silver_domains = [r["domain"] for r in
    spark.table(f"{CATALOG}.silver.events")
    .select("domain").distinct().collect()]

for domain in ["ecommerce", "pharmacy", "marketplace"]:
    if domain in silver_domains:
        check(f"domain_present.silver.{domain}", "PASS", f"{domain} domain present in silver.events")
    else:
        check(f"domain_present.silver.{domain}", "FAIL", f"{domain} domain MISSING from silver.events", hard_fail=True)

# All 3 domains in agg_daily_domain_metrics
agg_domains = [r["domain"] for r in
    spark.table(f"{CATALOG}.gold.agg_daily_domain_metrics")
    .select("domain").distinct().collect()]

for domain in ["ecommerce", "pharmacy", "marketplace"]:
    if domain in agg_domains:
        check(f"domain_present.agg_daily.{domain}", "PASS", f"{domain} present in agg_daily_domain_metrics")
    else:
        check(f"domain_present.agg_daily.{domain}", "FAIL", f"{domain} MISSING from agg_daily_domain_metrics", hard_fail=True)

# All 4 customer segments present
segments = [r["customer_segment"] for r in
    spark.table(f"{CATALOG}.gold.agg_customer_segments")
    .select("customer_segment").collect()]

for seg in ["premium", "standard", "occasional", "new"]:
    if seg in segments:
        check(f"segment_present.{seg}", "PASS", f"{seg} segment present")
    else:
        check(f"segment_present.{seg}", "FAIL", f"{seg} segment MISSING from agg_customer_segments", hard_fail=True)

# All 8 ATC drug codes in fact_inventory_snapshots
expected_atc = ["M01AB", "M01AE", "N02BA", "N02BE", "N05B", "N05C", "R03", "R06"]
actual_product_ids = [r["product_id"] for r in
    spark.sql(f"""
        SELECT DISTINCT product_id
        FROM {CATALOG}.gold.fact_inventory_snapshots
        WHERE product_id IS NOT NULL
    """).collect()]

for atc in expected_atc:
    if any(atc in pid for pid in actual_product_ids):
        check(f"atc_present.{atc}", "PASS", f"ATC code {atc} present")
    else:
        check(f"atc_present.{atc}", "WARN", f"ATC code {atc} not found in fact_inventory_snapshots", hard_fail=False)

# SCD2 check -- warn if zero historical rows
customer_historical = spark.table(f"{CATALOG}.gold.dim_customer") \
    .filter(F.col("is_current") == False).count()

if customer_historical == 0:
    check("scd2.dim_customer.historical_rows", "WARN",
          "Zero historical rows -- SCD2 correct but source data has no updates (synthetic data limitation)", hard_fail=False)
else:
    check("scd2.dim_customer.historical_rows", "PASS", f"{customer_historical:,} historical rows present")

seller_historical = spark.table(f"{CATALOG}.gold.dim_seller") \
    .filter(F.col("is_current") == False).count()

if seller_historical == 0:
    check("scd2.dim_seller.historical_rows", "WARN",
          "Zero historical rows -- SCD2 correct but source data has no updates (synthetic data limitation)", hard_fail=False)
else:
    check("scd2.dim_seller.historical_rows", "PASS", f"{seller_historical:,} historical rows present")

# COMMAND ----------

# DBTITLE 1,SECTION 12: DISTRIBUTION SANITY CHECKS
print("\nSECTION 12: DISTRIBUTION SANITY CHECKS")
print("=" * 70)

# SLA breach rate should be between 0% and 100% -- warn if > 80%
fp = spark.table(f"{CATALOG}.gold.fact_seller_performance") \
    .filter(F.col("event_type") == "seller.order.dispatched")
total_dispatch = fp.count()
total_breached = fp.filter(F.col("is_sla_breached") == True).count()
sla_rate = null_rate(total_breached, total_dispatch)

if sla_rate <= 80:
    check("distribution.sla_breach_rate", "PASS", f"SLA breach rate: {sla_rate}%")
else:
    check("distribution.sla_breach_rate", "WARN",
          f"SLA breach rate {sla_rate}% is very high -- may indicate SLA threshold calibration issue", hard_fail=False)

# Stock alert distribution -- if > 50% critical, warn
fi = spark.table(f"{CATALOG}.gold.fact_inventory_snapshots")
total_fi = fi.count()
critical_count = fi.filter(F.col("stock_alert_level") == "critical").count()
critical_rate = null_rate(critical_count, total_fi)

if critical_rate <= 50:
    check("distribution.critical_stock_rate", "PASS", f"Critical stock rate: {critical_rate}%")
else:
    check("distribution.critical_stock_rate", "WARN",
          f"Critical stock rate {critical_rate}% is very high -- review stock_level generation", hard_fail=False)

# Row distribution across domains in silver
silver = spark.table(f"{CATALOG}.silver.events")
silver_total = silver.count()
domain_dist = silver.groupBy("domain").count().collect()

print("\n  Silver domain distribution:")
for row in domain_dist:
    pct = null_rate(row["count"], silver_total)
    print(f"    {row['domain']}: {row['count']:,} ({pct}%)")
    if pct < 1:
        check(f"distribution.silver.{row['domain']}", "WARN",
              f"Only {pct}% of silver events -- domain may be underrepresented", hard_fail=False)
    else:
        check(f"distribution.silver.{row['domain']}", "PASS", f"{pct}% of silver events")

# COMMAND ----------

# DBTITLE 1,FINAL GATE - PASS / WARN / FAIL SUMMARY
print("\n" + "=" * 70)
print("FINAL VALIDATION SUMMARY")
print("=" * 70)

hard_fails = [r for r in results if r["status"] == "HARD_FAIL"]
warns = [r for r in results if r["status"] == "WARN"]
passes = [r for r in results if r["status"] == "PASS"]

print(f"\nPASS:      {len(passes)}")
print(f"WARN:      {len(warns)}")
print(f"HARD_FAIL: {len(hard_fails)}")
print(f"Total:     {len(results)}")

if warns:
    print(f"\nWARNINGS ({len(warns)}) -- review but may proceed:")
    for r in warns:
        print(f"  WARN  {r['check']}: {r['detail']}")

if hard_fails:
    print(f"\nHARD FAILURES ({len(hard_fails)}) -- DO NOT PROCEED TO DBT:")
    for r in hard_fails:
        print(f"  FAIL  {r['check']}: {r['detail']}")

print("\n" + "=" * 70)

if hard_fails:
    print("GATE: FAIL -- Fix all HARD_FAIL issues above before running dbt")
    print("=" * 70)
    raise Exception(f"Gold validation failed: {len(hard_fails)} hard failures. See output above.")
elif warns:
    print("GATE: PASS WITH WARNINGS -- Review warnings above, then proceed to dbt")
    print("=" * 70)
else:
    print("GATE: PASS -- All checks passed. Safe to proceed to dbt.")
    print("=" * 70)
