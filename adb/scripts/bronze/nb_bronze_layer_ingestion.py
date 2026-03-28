# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Bronze Layer Ingestion — CAD · PLM · ERP
# MAGIC **Layer**: Bronze (raw / as-landed)
# MAGIC
# MAGIC **Sources**
# MAGIC | Domain | Table | Format | ADLS Path |
# MAGIC |--------|-------|--------|-----------|
# MAGIC | CAD | cad_metadata | CSV (as-is) | bronze/cad/ |
# MAGIC | PLM | plm_revisions | Delta | bronze/plm/revisions/ |
# MAGIC | PLM | plm_bom | Delta | bronze/plm/bom/ |
# MAGIC | ERP | erp_cost | Delta | bronze/erp/cost/ |
# MAGIC
# MAGIC **Target**: Unity Catalog → `<catalog>.bronze_plm`
# MAGIC
# MAGIC **Pattern**: Read from ADLS → add audit columns → write as Delta to Unity Catalog
# MAGIC
# MAGIC > Run this notebook via a Databricks Workflow triggered after each ADF pipeline completion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

# ── Unity Catalog target ──────────────────────────────────────────────
CATALOG         = "eng_data_platform"        # change to your catalog name
BRONZE_SCHEMA   = "bronze_plm"               # all bronze tables land here

# ── ADLS Gen2 paths (abfss protocol) ─────────────────────────────────
STORAGE_ACCOUNT = "stengdataplatformdev"
CONTAINER       = "bronze"

BASE_PATH       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

CAD_PATH        = f"{BASE_PATH}/cad/"
PLM_REV_PATH    = f"{BASE_PATH}/plm/revisions/"
PLM_BOM_PATH    = f"{BASE_PATH}/plm/bom/"
ERP_COST_PATH   = f"{BASE_PATH}/erp/"

# ── Runtime metadata ──────────────────────────────────────────────────
from datetime import datetime, timezone
PIPELINE_RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {BRONZE_SCHEMA}")
print(f"Base path: {BASE_PATH}")
print(f"Run time : {PIPELINE_RUN_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bootstrap — create catalog schema if not exists

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} managed location 'abfss://adb@stengdataplatformdev.dfs.core.windows.net/adb/'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"Schema ready: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Helper — audit columns
# MAGIC
# MAGIC Every bronze table gets the same 4 audit columns appended:
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `_source_path` | ADLS path the data was read from |
# MAGIC | `_source_domain` | cad / plm / erp |
# MAGIC | `_bronze_ingested_at` | UTC timestamp this notebook ran |
# MAGIC | `_bronze_pipeline_run` | ADF / notebook run identifier |

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_audit_columns(df: DataFrame, source_path: str, domain: str) -> DataFrame:
    """Append standard bronze audit columns to any DataFrame."""
    return (
        df
        .withColumn("_source_path",         F.lit(source_path))
        .withColumn("_source_domain",        F.lit(domain))
        .withColumn("_bronze_ingested_at",   F.lit(PIPELINE_RUN_TS).cast("timestamp"))
        .withColumn("_bronze_pipeline_run",  F.lit(PIPELINE_RUN_TS))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Helper — write to Unity Catalog (Delta MERGE)
# MAGIC
# MAGIC Uses Delta `MERGE` so re-runs are idempotent — no duplicate rows.

# COMMAND ----------

from delta.tables import DeltaTable

def write_bronze_table(
    df: DataFrame,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None
):
    """
    Write DataFrame to Unity Catalog bronze schema as a Delta table.
    - First run  : CREATE TABLE + full insert
    - Subsequent : MERGE on merge_keys (upsert)
    """
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    # ── Check if table already exists ────────────────────────────────
    table_exists = spark.catalog.tableExists(full_table)

    if not table_exists:
        print(f"  [CREATE] {full_table}")
        writer = df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(full_table)

    else:
        print(f"  [MERGE]  {full_table}  keys={merge_keys}")
        delta_tbl = DeltaTable.forName(spark, full_table)

        # Build the ON condition from merge keys
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_keys]
        )

        # All non-key columns go into the update map
        update_cols = {c: f"source.{c}" for c in df.columns if c not in merge_keys}

        (
            delta_tbl.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_cols)
            .whenNotMatchedInsertAll()
            .execute()
        )

    row_count = spark.table(full_table).count()
    print(f"  → {full_table}  rows={row_count:,}")
    return full_table


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · CAD Metadata
# MAGIC
# MAGIC **Source**: CSV files landed by `PL_CAD_Metadata_Ingesion` (ADF)
# MAGIC **Schema**: file-name-dated CSVs, e.g. `cad_metadata_2026-03-10.csv`
# MAGIC **Merge key**: `part_id` (natural key from CAD system)

# COMMAND ----------

print("── CAD Metadata ─────────────────────────────────────")

cad_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")   # picks up all sub-folders
    .load(CAD_PATH)
)

# Add source file name as a traceability column
cad_raw = cad_raw.withColumn("_source_file", F.col("_metadata.file_path"))
#cad_raw = cad_raw.withColumn("_source_file", F.input_file_name()) # input_file_name are not supported in Unity Catalog.

cad_df = add_audit_columns(cad_raw, CAD_PATH, "cad")

print(f"  Rows read : {cad_df.count():,}")
print(f"  Columns   : {cad_df.columns}")

cad_df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# Deduplicate: keep the latest row per part_id (by source file, descending)
w = Window.partitionBy("part_id").orderBy(F.desc("_source_file"))
cad_df_deduped = (
    cad_df
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

write_bronze_table(
    df          = cad_df_deduped,
    table_name  = "cad_metadata",
    merge_keys  = ["part_id"],          # ← update to your actual CAD primary key
    partition_cols = None
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · PLM — Revisions
# MAGIC
# MAGIC **Source**: Delta table written by `df_upsert_plm_revisions` (ADF Data Flow)
# MAGIC **Merge key**: `part_id` + `revision`

# COMMAND ----------

print("── PLM Revisions ────────────────────────────────────")

plm_rev_raw = (
    spark.read
    .format("parquet")
    .load(PLM_REV_PATH)
)

plm_rev_df = add_audit_columns(plm_rev_raw, PLM_REV_PATH, "plm")

print(f"  Rows read : {plm_rev_df.count():,}")
plm_rev_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

w = Window.partitionBy("part_id", "revision").orderBy(F.col("last_modified").desc())
plm_rev_df = plm_rev_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

write_bronze_table(
    df             = plm_rev_df,
    table_name     = "plm_revisions",
    merge_keys     = ["part_id", "revision"],
    partition_cols = ["status"]             # partition by Released / Draft etc.
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · PLM — Bill of Materials
# MAGIC
# MAGIC **Source**: Delta table written by `df_upsert_plm_bom` (ADF Data Flow)
# MAGIC **Merge key**: `assembly_id` + `part_id`

# COMMAND ----------

print("── PLM BOM ──────────────────────────────────────────")

plm_bom_raw = (
    spark.read
    .format("parquet")
    .load(PLM_BOM_PATH)
)

plm_bom_df = add_audit_columns(plm_bom_raw, PLM_BOM_PATH, "plm")

print(f"  Rows read : {plm_bom_df.count():,}")
plm_bom_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

w = Window.partitionBy("assembly_id", "part_id").orderBy(F.col("last_modified").desc())
plm_bom_df = plm_bom_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

write_bronze_table(
    df             = plm_bom_df,
    table_name     = "plm_bom",
    merge_keys     = ["assembly_id", "part_id"],
    partition_cols = None
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · ERP — Cost
# MAGIC
# MAGIC **Source**: Delta table written by `df_upsert_erp_cost` (ADF Data Flow)
# MAGIC **Merge key**: `part_id` + `supplier_name`

# COMMAND ----------

print("── ERP Cost ─────────────────────────────────────────")

erp_cost_raw = (
    spark.read
    .format("parquet")
    .load(ERP_COST_PATH)
)

erp_cost_df = add_audit_columns(erp_cost_raw, ERP_COST_PATH, "erp")

print(f"  Rows read : {erp_cost_df.count():,}")
erp_cost_df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# Deduplicate: keep the latest row per (part_id, supplier_name)
w = Window.partitionBy("part_id", "supplier_name").orderBy(F.desc("last_updated"))
erp_cost_deduped = (
    erp_cost_df
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

write_bronze_table(
    df             = erp_cost_deduped,
    table_name     = "erp_cost",
    merge_keys     = ["part_id", "supplier_name"],
    partition_cols = ["currency"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Validation — row counts across all bronze tables

# COMMAND ----------

print("══ Bronze Layer Summary ══════════════════════════════")

tables = [
    ("cad_metadata",    "cad"),
    ("plm_revisions",   "plm"),
    ("plm_bom",         "plm"),
    ("erp_cost",        "erp"),
]

summary_rows = []
for tbl, domain in tables:
    full = f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}"
    try:
        cnt  = spark.table(full).count()
        latest = (
            spark.table(full)
            .agg(F.max("_bronze_ingested_at").alias("latest_ingest"))
            .collect()[0]["latest_ingest"]
        )
        summary_rows.append((domain, tbl, cnt, str(latest)))
        print(f"  {domain:6s}  {tbl:25s}  rows={cnt:>8,}  latest={latest}")
    except Exception as e:
        print(f"  !! {full} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Optimise Delta tables (Z-Order on key columns)
# MAGIC
# MAGIC Run this cell periodically (e.g. weekly) — not every pipeline run.
# MAGIC It compacts small files and co-locates data for faster queries.

# COMMAND ----------

def optimize_table(table_name: str, zorder_cols: list):
    full = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    cols = ", ".join(zorder_cols)
    print(f"  OPTIMIZE {full} ZORDER({cols})")
    spark.sql(f"OPTIMIZE {full} ZORDER BY ({cols})")

# Uncomment to run:
# optimize_table("cad_metadata",  ["file_id"])
# optimize_table("plm_revisions", ["part_id", "revision"])
# optimize_table("plm_bom",       ["assembly_id", "part_id"])
# optimize_table("erp_cost",      ["part_id", "supplier_name"])

print("OPTIMIZE cells are commented out — run manually or on a weekly schedule.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Table properties & tagging (Unity Catalog)

# COMMAND ----------

tag_map = {
    "cad_metadata"  : {"domain": "cad",  "layer": "bronze", "pii": "false", "owner": "eng-data-platform"},
    "plm_revisions" : {"domain": "plm",  "layer": "bronze", "pii": "false", "owner": "eng-data-platform"},
    "plm_bom"       : {"domain": "plm",  "layer": "bronze", "pii": "false", "owner": "eng-data-platform"},
    "erp_cost"      : {"domain": "erp",  "layer": "bronze", "pii": "false", "owner": "eng-data-platform"},
}

for tbl, tags in tag_map.items():
    full = f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}"
    for k, v in tags.items():
        spark.sql(f"ALTER TABLE {full} SET TAGS ('{k}' = '{v}')")
    print(f"  Tagged: {full}  {tags}")

print("\nBronze layer notebook complete.")

# COMMAND ----------


