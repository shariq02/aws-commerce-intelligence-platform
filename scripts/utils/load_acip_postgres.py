"""
ACIP POSTGRES LOADER
Loads gold, quality, dbt_marts CSVs into PostgreSQL
Schema auto-detected from CSV headers
Three-tier load: psycopg2 copy_expert -> chunked insert
Pattern adapted from genomics load_postgres_hybrid_psql.py
"""

import os
import csv
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "172.31.32.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "acip")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "0940")

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "acip_export"
CHECKPOINT_FILE = DATA_DIR / ".postgres_checkpoint.json"

CHUNK_SIZE = 50_000

# Maps ACIP schema name to PostgreSQL target schema
SCHEMA_MAP = {
    "gold": "acip_gold",
    "quality": "acip_quality",
    "dbt_marts": "acip_dbt_marts",
}

print("=" * 70)
print("ACIP POSTGRES LOADER")
print("=" * 70)
print(f"Database: {POSTGRES_DB}")
print(f"Schemas:  {', '.join(SCHEMA_MAP.values())}")


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def get_csv_info(csv_file):
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        row_count = sum(1 for _ in reader)
    size_mb = csv_file.stat().st_size / (1024 * 1024)
    return headers, row_count, size_mb


def get_csv_hash(csv_file, sample_size=10_000):
    h = hashlib.md5()
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            h.update(",".join(row).encode("utf-8"))
    return h.hexdigest()


def build_column_ddl(headers):
    return ", ".join(f'"{col}" TEXT' for col in headers)


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


def run_sql(sql):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()


def table_exists(pg_schema, table_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (pg_schema, table_name))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count > 0


def get_table_count(pg_schema, table_name):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM {pg_schema}."{table_name}"')
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Load strategies
# ---------------------------------------------------------------------------

def load_via_copy_expert(csv_file, pg_schema, table_name):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    copy_sql = (
        f'COPY {pg_schema}."{table_name}" '
        f"FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"
    )
    t0 = time.time()
    with open(csv_file, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)
    print(f"  Loaded in {time.time() - t0:.1f}s")
    cur.close()
    conn.close()


def load_via_chunked(csv_file, pg_schema, table_name, headers):
    print(f"  Falling back to chunked insert ({CHUNK_SIZE:,} rows/chunk)...")
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    cols = ", ".join(f'"{h}"' for h in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    insert_sql = f'INSERT INTO {pg_schema}."{table_name}" ({cols}) VALUES ({placeholders})'

    total = 0
    chunk_n = 0
    t0 = time.time()

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= CHUNK_SIZE:
                chunk_n += 1
                cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                print(f"    Chunk {chunk_n}: {total:,} rows ({time.time()-t0:.1f}s)")
                batch = []
        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            total += len(batch)

    cur.close()
    conn.close()
    return total


# ---------------------------------------------------------------------------
# Per-table loader
# ---------------------------------------------------------------------------

def load_table(csv_file, pg_schema, table_name, checkpoint):
    checkpoint_key = f"{pg_schema}.{table_name}"
    t_start = datetime.now()

    print(f"\n  {table_name}:")
    print(f"    Start: {t_start.strftime('%Y-%m-%d %H:%M:%S')}")

    if not csv_file.exists():
        print(f"    SKIP: CSV not found")
        return None

    headers, csv_rows, csv_size_mb = get_csv_info(csv_file)
    csv_hash = get_csv_hash(csv_file)

    print(f"    CSV: {csv_rows:,} rows, {len(headers)} cols, {csv_size_mb:.1f} MB")

    prev = checkpoint.get(checkpoint_key, {})

    if table_exists(pg_schema, table_name):
        table_rows = get_table_count(pg_schema, table_name)
        print(f"    Existing: {table_rows:,} rows")

        if table_rows == csv_rows and csv_hash == prev.get("hash"):
            print(f"    Status: UNCHANGED -- skip")
            return True

        print(f"    Dropping and rebuilding...")
        run_sql(f'DROP TABLE IF EXISTS {pg_schema}."{table_name}"')

    col_ddl = build_column_ddl(headers)
    run_sql(f'CREATE TABLE {pg_schema}."{table_name}" ({col_ddl})')

    loaded_ok = False

    print(f"    Loading via copy_expert...")
    try:
        load_via_copy_expert(csv_file, pg_schema, table_name)
        loaded_ok = True
    except Exception as e:
        print(f"    copy_expert failed: {str(e)[:150]}")
        try:
            run_sql(f'DROP TABLE IF EXISTS {pg_schema}."{table_name}"')
            run_sql(f'CREATE TABLE {pg_schema}."{table_name}" ({col_ddl})')
            load_via_chunked(csv_file, pg_schema, table_name, headers)
            loaded_ok = True
        except Exception as e2:
            print(f"    Chunked insert failed: {str(e2)[:150]}")

    if not loaded_ok:
        print(f"    Status: FAILED")
        return False

    final_count = get_table_count(pg_schema, table_name)
    t_end = datetime.now()
    duration = (t_end - t_start).total_seconds()

    print(f"    Loaded:   {final_count:,} rows")
    print(f"    Duration: {duration:.1f}s")

    if final_count != csv_rows:
        print(f"    WARNING: Row count mismatch -- CSV={csv_rows:,} PG={final_count:,}")
        return False

    checkpoint[checkpoint_key] = {
        "rows": csv_rows,
        "columns": len(headers),
        "hash": csv_hash,
    }
    save_checkpoint(checkpoint)
    print(f"    Status: OK")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not POSTGRES_PASSWORD:
        print("\nERROR: POSTGRES_PASSWORD not found in .env")
        return

    # Create PostgreSQL schemas
    for pg_schema in SCHEMA_MAP.values():
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {pg_schema}")
        print(f"Schema ready: {pg_schema}")

    checkpoint = load_checkpoint()
    print(f"\nPreviously loaded: {len(checkpoint)} tables")

    overall_start = datetime.now()
    results = {}

    for acip_schema, pg_schema in SCHEMA_MAP.items():
        schema_dir = DATA_DIR / acip_schema

        if not schema_dir.exists():
            print(f"\n{acip_schema}: No data directory found -- skipping")
            continue

        csv_files = sorted(schema_dir.glob("*.csv"))

        if not csv_files:
            print(f"\n{acip_schema}: No CSV files found -- skipping")
            continue

        print(f"\n{'=' * 70}")
        print(f"LOADING: {acip_schema} -> {pg_schema} ({len(csv_files)} tables)")
        print("=" * 70)

        for csv_file in csv_files:
            table_name = csv_file.stem
            success = load_table(csv_file, pg_schema, table_name, checkpoint)
            if success is not None:
                results[f"{pg_schema}.{table_name}"] = success

    total_duration = (datetime.now() - overall_start).total_seconds()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total time: {total_duration:.1f}s ({total_duration/60:.1f} min)")

    successful = [k for k, v in results.items() if v]
    failed = [k for k, v in results.items() if not v]

    print(f"\nSuccessful: {len(successful)}")
    for k in successful:
        print(f"  OK  {k}")

    if failed:
        print(f"\nFailed: {len(failed)}")
        for k in failed:
            print(f"  FAIL {k}")

    print("\n" + "=" * 70)
    if not failed:
        print("NEXT STEP: Run scripts/fix_acip_postgres_types.py")
    else:
        print("Fix errors above and re-run")
    print("=" * 70)


if __name__ == "__main__":
    main()
