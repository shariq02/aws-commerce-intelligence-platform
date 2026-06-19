# Databricks notebook source
# MAGIC %md
# MAGIC #### ACIP EXPORT - GOLD, QUALITY, DBT_MARTS TO VOLUMES
# MAGIC Exports all tables from acip.gold, acip.quality, acip.dbt_marts
# MAGIC Uses smart change detection - only exports changed tables

# COMMAND ----------

from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()

CATALOG = "acip"
spark.sql(f"USE CATALOG {CATALOG}")

SCHEMAS_TO_EXPORT = ["gold", "quality", "dbt_marts"]

# dbt intermediate and staging views - exclude from export
SKIP_TABLES = {
    "stg_customers",
    "stg_transactions",
    "stg_inventory_snapshots",
    "stg_seller_performance",
    "int_ecommerce_orders",
    "int_pharmacy_dispensing",
    "int_marketplace_performance",
}

VOLUME_NAME = "acip_exports"

print("ACIP TABLE EXPORT")
print("=" * 70)
print(f"Catalog: {CATALOG}")
print(f"Schemas: {', '.join(SCHEMAS_TO_EXPORT)}")
print(f"Volume:  {VOLUME_NAME}")

# COMMAND ----------

# DBTITLE 1,Create Export Volumes

for schema in SCHEMAS_TO_EXPORT:
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{VOLUME_NAME}")
        print(f"Volume ready: {CATALOG}.{schema}.{VOLUME_NAME}")
    except Exception as e:
        print(f"Volume check {schema}: {e}")

# COMMAND ----------

# DBTITLE 1,Auto-Discover Tables Per Schema

def get_tables(schema):
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    result = []
    for t in tables:
        name = t.tableName
        if name in SKIP_TABLES:
            print(f"  SKIP: {schema}.{name} (excluded view)")
            continue
        result.append(name)
    return result

schema_tables = {}
for schema in SCHEMAS_TO_EXPORT:
    tables = get_tables(schema)
    schema_tables[schema] = tables
    print(f"\n{schema}: {len(tables)} tables")
    for t in tables:
        print(f"  - {t}")

# COMMAND ----------

# DBTITLE 1,Load Previous Export Metadata

def load_metadata(schema):
    checkpoint_path = f"/Volumes/{CATALOG}/{schema}/{VOLUME_NAME}/.export_metadata.json"
    try:
        content = dbutils.fs.head(checkpoint_path)
        return json.loads(content)
    except Exception:
        return {}

all_metadata = {}
for schema in SCHEMAS_TO_EXPORT:
    all_metadata[schema] = load_metadata(schema)
    print(f"{schema}: {len(all_metadata[schema])} tables in metadata")

# COMMAND ----------

# DBTITLE 1,Check Current Table States

def get_table_state(schema, table_name):
    try:
        df = spark.table(f"{CATALOG}.{schema}.{table_name}")
        return {
            "rows": df.count(),
            "columns": len(df.columns)
        }
    except Exception as e:
        return {"error": str(e)}

current_states = {}
for schema in SCHEMAS_TO_EXPORT:
    current_states[schema] = {}
    print(f"\n{schema.upper()}:")
    for table in schema_tables[schema]:
        state = get_table_state(schema, table)
        current_states[schema][table] = state
        if "error" not in state:
            print(f"  {table}: {state['rows']:,} rows, {state['columns']} cols")
        else:
            print(f"  {table}: ERROR - {state['error'][:80]}")

# COMMAND ----------

# DBTITLE 1,Identify Changed Tables

tables_to_export = {}
for schema in SCHEMAS_TO_EXPORT:
    tables_to_export[schema] = []
    prev_meta = all_metadata[schema]
    print(f"\n{schema.upper()}:")
    for table, state in current_states[schema].items():
        if "error" in state:
            print(f"  {table}: SKIP (error)")
            continue
        if table not in prev_meta:
            print(f"  {table}: EXPORT (new)")
            tables_to_export[schema].append(table)
        elif state["rows"] != prev_meta[table].get("rows"):
            print(f"  {table}: EXPORT (rows changed: {prev_meta[table].get('rows')} -> {state['rows']:,})")
            tables_to_export[schema].append(table)
        elif state["columns"] != prev_meta[table].get("columns"):
            print(f"  {table}: EXPORT (columns changed)")
            tables_to_export[schema].append(table)
        else:
            print(f"  {table}: SKIP (unchanged)")

total_to_export = sum(len(v) for v in tables_to_export.values())
print(f"\nTotal tables to export: {total_to_export}")

# COMMAND ----------

# DBTITLE 1,Export Changed Tables

export_results = {}

for schema in SCHEMAS_TO_EXPORT:
    export_results[schema] = {}
    volume_path = f"/Volumes/{CATALOG}/{schema}/{VOLUME_NAME}/"

    if not tables_to_export[schema]:
        print(f"\n{schema.upper()}: Nothing to export")
        continue

    print(f"\n{schema.upper()} EXPORT")
    print("=" * 70)

    for table in tables_to_export[schema]:
        output_path = f"{volume_path}{table}"
        state = current_states[schema][table]

        print(f"\n{table}:")
        print(f"  Rows:    {state['rows']:,}")
        print(f"  Columns: {state['columns']}")
        print(f"  Output:  {output_path}")

        try:
            df = spark.table(f"{CATALOG}.{schema}.{table}")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            files = dbutils.fs.ls(output_path)
            csv_files = [f for f in files if f.path.endswith(".csv")]

            if csv_files:
                size_mb = csv_files[0].size / (1024 * 1024)
                print(f"  File:    {csv_files[0].name} ({size_mb:.2f} MB)")
                export_results[schema][table] = {"success": True, "size_mb": size_mb, "file": csv_files[0].name}
            else:
                print(f"  ERROR: No CSV file created")
                export_results[schema][table] = {"success": False, "error": "No CSV file"}

        except Exception as e:
            print(f"  ERROR: {str(e)[:100]}")
            export_results[schema][table] = {"success": False, "error": str(e)[:100]}

# COMMAND ----------

# DBTITLE 1,Update Metadata

for schema in SCHEMAS_TO_EXPORT:
    new_metadata = all_metadata[schema].copy()
    checkpoint_path = f"/Volumes/{CATALOG}/{schema}/{VOLUME_NAME}/.export_metadata.json"

    for table in tables_to_export[schema]:
        if export_results[schema].get(table, {}).get("success"):
            new_metadata[table] = current_states[schema][table].copy()
            new_metadata[table]["file"] = export_results[schema][table]["file"]
            new_metadata[table]["size_mb"] = export_results[schema][table]["size_mb"]

    dbutils.fs.put(checkpoint_path, json.dumps(new_metadata, indent=2), overwrite=True)
    print(f"{schema}: Metadata saved ({len(new_metadata)} tables)")

# COMMAND ----------

# DBTITLE 1,Export Summary

print("\n" + "=" * 70)
print("EXPORT SUMMARY")
print("=" * 70)

for schema in SCHEMAS_TO_EXPORT:
    successful = [t for t, r in export_results[schema].items() if r.get("success")]
    failed = [t for t, r in export_results[schema].items() if not r.get("success")]
    total_mb = sum(export_results[schema][t]["size_mb"] for t in successful)

    print(f"\n{schema.upper()}: {len(successful)} exported, {len(failed)} failed ({total_mb:.2f} MB)")
    for t in successful:
        print(f"  OK  {t} ({export_results[schema][t]['size_mb']:.2f} MB)")
    for t in failed:
        print(f"  FAIL {t}: {export_results[schema][t].get('error', '')[:80]}")

print("\n" + "=" * 70)
print("NEXT STEP: Run scripts/download_acip_tables.py locally")
print("=" * 70)
