"""
ACIP POSTGRES TYPE FIXER
Converts TEXT columns to correct types after bulk load
Covers acip_gold, acip_quality, acip_dbt_marts schemas
Pattern adapted from genomics fix_postgres_types_fast.py
"""

import psycopg2
import time
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

print("=" * 70)
print("ACIP POSTGRES TYPE FIXER")
print("=" * 70)
print(f"Database: {POSTGRES_DB}")

# ---------------------------------------------------------------------------
# Schema type definitions
# Derived from actual gold/quality/dbt_marts column schemas
# ---------------------------------------------------------------------------

TYPE_MAP = {
    "acip_gold": {
        "dim_date": {
            "BOOLEAN": ["is_weekend"],
            "INT": ["date_key", "year", "quarter", "month", "week_of_year",
                    "day_of_month", "day_of_week"],
        },
        "dim_geography": {
            "BIGINT": ["geo_key"],
        },
        "dim_product": {
            "BOOLEAN": ["is_prescription"],
            "BIGINT": ["product_key"],
        },
        "dim_customer": {
            "BOOLEAN": ["is_current"],
            "BIGINT": ["customer_key"],
            "DATE": ["effective_date", "expiry_date"],
        },
        "dim_seller": {
            "BOOLEAN": ["is_current"],
            "BIGINT": ["seller_key"],
            "DOUBLE PRECISION": ["total_orders", "total_revenue"],
            "DATE": ["effective_date", "expiry_date"],
        },
        "fact_transactions": {
            "BOOLEAN": ["is_installment", "is_multi_item", "is_multi_seller",
                        "delivery_on_time", "has_negative_review"],
            "BIGINT": ["transaction_key", "customer_key", "geo_key"],
            "INT": ["date_key", "max_installments", "item_count"],
            "DOUBLE PRECISION": ["total_amount", "fulfilment_time_mins",
                                  "fulfilment_time_days", "avg_review_score"],
        },
        "fact_inventory_snapshots": {
            "BOOLEAN": ["is_prescription", "is_peak_hour", "is_weekend"],
            "BIGINT": ["snapshot_key", "product_key"],
            "INT": ["date_key", "stock_level", "reorder_threshold",
                    "fill_time_mins", "hour_of_day"],
            "DOUBLE PRECISION": ["quantity", "days_of_supply"],
        },
        "fact_seller_performance": {
            "BOOLEAN": ["is_sla_breached"],
            "BIGINT": ["performance_key", "seller_key"],
            "INT": ["date_key", "sla_threshold_mins"],
            "DOUBLE PRECISION": ["price", "freight_value", "dispatch_time_mins",
                                  "dispatch_time_days", "old_price", "new_price",
                                  "change_pct"],
        },
        "agg_daily_domain_metrics": {
            "DATE": ["metric_date"],
            "BIGINT": ["unique_customers", "unique_products"],
            "INT": ["event_count"],
            "DOUBLE PRECISION": ["total_value"],
        },
        "agg_customer_segments": {
            "BIGINT": ["customer_key"],
            "INT": ["order_count"],
            "DOUBLE PRECISION": ["total_spent", "avg_order_value",
                                  "lifetime_value"],
        },
    },
    "acip_quality": {
        "pipeline_watermarks": {
            "BIGINT": ["watermark_id"],
            "INT": ["records_processed"],
        },
        "quality_audit_log": {
            "BIGINT": ["audit_id"],
            "INT": ["records_checked", "records_failed"],
            "BOOLEAN": ["passed"],
        },
    },
    "acip_dbt_marts": {
        "mart_ecommerce_orders": {
            "BOOLEAN": ["is_installment", "is_multi_item", "is_multi_seller",
                        "delivery_on_time", "has_negative_review"],
            "BIGINT": ["transaction_key"],
            "INT": ["max_installments", "item_count", "year", "month",
                    "quarter", "day_of_week"],
            "DOUBLE PRECISION": ["total_amount", "net_revenue",
                                  "effective_payment_amount",
                                  "fulfilment_time_mins", "fulfilment_time_days",
                                  "avg_review_score"],
            "DATE": ["full_date"],
            "BOOLEAN_EXTRA": ["is_weekend"],
        },
        "mart_pharmacy_dispensing": {
            "BOOLEAN": ["is_prescription", "is_peak_hour", "is_weekend",
                        "is_below_reorder", "requires_action"],
            "BIGINT": ["snapshot_key"],
            "INT": ["stock_level", "reorder_threshold", "fill_time_mins",
                    "hour_of_day", "year", "month", "quarter", "day_of_week",
                    "stock_buffer"],
            "DOUBLE PRECISION": ["quantity", "days_of_supply"],
            "DATE": ["full_date"],
        },
        "mart_marketplace_performance": {
            "BOOLEAN": ["is_sla_breached", "is_weekend"],
            "BIGINT": ["performance_key"],
            "INT": ["sla_threshold_mins", "year", "month", "quarter",
                    "day_of_week"],
            "DOUBLE PRECISION": ["price", "freight_value", "dispatch_time_mins",
                                  "dispatch_time_days", "old_price", "new_price",
                                  "change_pct", "sla_overrun_days",
                                  "gross_revenue", "net_revenue"],
            "DATE": ["full_date"],
        },
        "mart_cross_domain_summary": {
            "INT": ["year", "month", "ecommerce_transactions",
                    "ecommerce_returns", "ecommerce_on_time_deliveries",
                    "pharmacy_dispensing_events", "pharmacy_critical_stock_events",
                    "pharmacy_rx_events", "marketplace_dispatch_events",
                    "marketplace_sla_breaches"],
            "DOUBLE PRECISION": ["ecommerce_net_revenue", "ecommerce_avg_order_value",
                                  "ecommerce_avg_review_score",
                                  "pharmacy_avg_fill_time",
                                  "pharmacy_avg_days_of_supply",
                                  "marketplace_gross_revenue",
                                  "marketplace_net_revenue",
                                  "marketplace_avg_dispatch_days"],
        },
    },
}


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def get_current_type(cur, pg_schema, table_name, col):
    cur.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
    """, (pg_schema, table_name, col))
    result = cur.fetchone()
    return result[0] if result else None


def alter_column(cur, conn, pg_schema, table_name, col, pg_type):
    if pg_type == "BOOLEAN":
        sql = f"""
            ALTER TABLE {pg_schema}."{table_name}"
            ALTER COLUMN "{col}" TYPE BOOLEAN
            USING CASE
                WHEN LOWER(TRIM("{col}"::TEXT)) IN ('true', 't', '1', 'yes') THEN TRUE
                WHEN LOWER(TRIM("{col}"::TEXT)) IN ('false', 'f', '0', 'no', '') THEN FALSE
                ELSE NULL
            END
        """
    elif pg_type == "INT":
        sql = f"""
            ALTER TABLE {pg_schema}."{table_name}"
            ALTER COLUMN "{col}" TYPE INTEGER
            USING CASE
                WHEN "{col}"::TEXT ~ '^-?[0-9]+$' THEN "{col}"::INTEGER
                WHEN "{col}"::TEXT ~ '^-?[0-9]+\\.0+$' THEN "{col}"::DOUBLE PRECISION::INTEGER
                ELSE NULL
            END
        """
    elif pg_type == "BIGINT":
        sql = f"""
            ALTER TABLE {pg_schema}."{table_name}"
            ALTER COLUMN "{col}" TYPE BIGINT
            USING CASE
                WHEN "{col}"::TEXT ~ '^-?[0-9]+$' THEN "{col}"::BIGINT
                ELSE NULL
            END
        """
    elif pg_type == "DOUBLE PRECISION":
        sql = f"""
            ALTER TABLE {pg_schema}."{table_name}"
            ALTER COLUMN "{col}" TYPE DOUBLE PRECISION
            USING CASE
                WHEN "{col}"::TEXT ~ '^-?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$'
                THEN "{col}"::DOUBLE PRECISION
                ELSE NULL
            END
        """
    elif pg_type == "DATE":
        sql = f"""
            ALTER TABLE {pg_schema}."{table_name}"
            ALTER COLUMN "{col}" TYPE DATE
            USING CASE
                WHEN "{col}"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                THEN "{col}"::DATE
                ELSE NULL
            END
        """
    else:
        return False

    cur.execute(sql)
    conn.commit()
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    stats = {"tables": 0, "converted": 0, "skipped": 0, "failed": 0}

    for pg_schema, tables in TYPE_MAP.items():
        print(f"\n{'=' * 70}")
        print(f"SCHEMA: {pg_schema}")
        print("=" * 70)

        for table_name, type_groups in tables.items():
            print(f"\n  {table_name}:")
            t_start = time.time()

            try:
                cur.execute(f'SELECT COUNT(*) FROM {pg_schema}."{table_name}"')
                row_count = cur.fetchone()[0]
                print(f"    Rows: {row_count:,}")
            except Exception:
                conn.rollback()
                print(f"    SKIP: table not found in PostgreSQL")
                continue

            table_converted = 0
            table_skipped = 0
            table_failed = 0

            for pg_type, columns in type_groups.items():
                actual_type = pg_type.replace("_EXTRA", "")
                for col in columns:
                    current = get_current_type(cur, pg_schema, table_name, col)
                    if current is None:
                        print(f"    SKIP {col}: column not found")
                        table_skipped += 1
                        continue
                    if current.upper() in (actual_type.upper(), actual_type.replace(" ", "").upper()):
                        table_skipped += 1
                        continue

                    print(f"    CONVERT {col}: {current} -> {actual_type}...", end="", flush=True)
                    t0 = time.time()
                    try:
                        alter_column(cur, conn, pg_schema, table_name, col, actual_type)
                        print(f" OK ({time.time()-t0:.1f}s)")
                        table_converted += 1
                    except Exception as e:
                        conn.rollback()
                        print(f" FAIL: {str(e)[:100]}")
                        table_failed += 1

            stats["tables"] += 1
            stats["converted"] += table_converted
            stats["skipped"] += table_skipped
            stats["failed"] += table_failed
            print(f"    Done in {time.time()-t_start:.1f}s -- converted={table_converted} skipped={table_skipped} failed={table_failed}")

    cur.close()
    conn.close()

    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"Tables processed: {stats['tables']}")
    print(f"Columns converted: {stats['converted']}")
    print(f"Columns skipped:   {stats['skipped']}")
    print(f"Columns failed:    {stats['failed']}")
    print("=" * 70)

    if stats["failed"] > 0:
        print("\nWARNING: Some columns failed to convert")
        sys.exit(1)
    else:
        print("\nSUCCESS: All types fixed")
        sys.exit(0)


if __name__ == "__main__":
    main()
