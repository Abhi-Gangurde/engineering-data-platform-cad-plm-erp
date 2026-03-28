# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Silver Layer — CAD · PLM · ERP
# MAGIC **Layer**: Silver (cleansed · typed · validated · conformed)
# MAGIC
# MAGIC **Source** : `<catalog>.bronze_plm.*`
# MAGIC **Target**  : `<catalog>.silver_plm.*`
# MAGIC
# MAGIC | Bronze table | Silver table(s) | Key transforms |
# MAGIC |---|---|---|
# MAGIC | `cad_metadata` | `cad_metadata` | Parse dates, trim strings, drop malformed rows |
# MAGIC | `plm_revisions` | `plm_revisions` | Cast types, validate status, flag open revisions |
# MAGIC | `plm_bom` | `plm_bom` | Validate quantities, enrich with active revision |
# MAGIC | `erp_cost` | `erp_cost` | Validate cost > 0, standardise currency |
# MAGIC | plm_revisions + plm_bom + erp_cost | `part_cost_view` | Unified conformed view per part |
# MAGIC
# MAGIC > **Silver rules**: no raw audit columns exposed, all types explicit,
# MAGIC > all nulls intentional, all string values trimmed and upper/lower-cased consistently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

CATALOG         = "eng_data_platform"   # same catalog as bronze
BRONZE_SCHEMA   = "bronze_plm"
SILVER_SCHEMA   = "silver_plm"

from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql  import types as T
from delta.tables import DeltaTable

PIPELINE_RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# Valid lookup values — drives validation rules
VALID_STATUSES   = ["Released", "Draft", "Obsolete", "In Review", "Approved"]
VALID_CURRENCIES = ["INR", "USD", "EUR", "GBP", "JPY"]

print(f"Source : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Target : {CATALOG}.{SILVER_SCHEMA}")
print(f"Run at : {PIPELINE_RUN_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bootstrap — create silver schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
print(f"Schema ready: {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Helpers

# COMMAND ----------

# ── Audit columns for silver ──────────────────────────────────────────
def add_silver_audit(df: DataFrame, source_table: str) -> DataFrame:
    return (
        df
        .withColumn("_silver_source_table",   F.lit(f"{CATALOG}.{BRONZE_SCHEMA}.{source_table}"))
        .withColumn("_silver_processed_at",   F.lit(PIPELINE_RUN_TS).cast("timestamp"))
        .withColumn("_silver_pipeline_run",   F.lit(PIPELINE_RUN_TS))
    )

# ── Quarantine helper — split valid vs rejected rows ─────────────────
def quarantine_split(df: DataFrame, condition, label: str):
    """
    Returns (clean_df, rejected_df).
    rejected_df has an extra _rejection_reason column.
    """
    clean    = df.filter(condition)
    rejected = (
        df.filter(~condition)
          .withColumn("_rejection_reason", F.lit(label))
    )
    rej_cnt = rejected.count()
    if rej_cnt > 0:
        print(f"  ⚠  Quarantined {rej_cnt:,} rows — reason: {label}")
    return clean, rejected

# ── Write silver table (MERGE) ────────────────────────────────────────
def write_silver_table(
    df: DataFrame,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None
):
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    exists = spark.catalog.tableExists(full)

    if not exists:
        print(f"  [CREATE] {full}")
        writer = (
            df.write.format("delta")
              .mode("overwrite")
              .option("mergeSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(full)
    else:
        print(f"  [MERGE]  {full}  keys={merge_keys}")
        delta_tbl   = DeltaTable.forName(spark, full)
        merge_cond  = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        update_map  = {c: f"s.{c}" for c in df.columns if c not in merge_keys}
        (
            delta_tbl.alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsertAll()
            .execute()
        )

    cnt = spark.table(full).count()
    print(f"  → {full}  rows={cnt:,}")
    return full

# ── Write quarantine table ────────────────────────────────────────────
def write_quarantine(df: DataFrame, table_name: str):
    if df.isEmpty():
        return
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}_quarantine"
    (
        df.withColumn("_quarantine_ts", F.lit(PIPELINE_RUN_TS).cast("timestamp"))
          .write.format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(full)
    )
    print(f"  → Quarantine: {full}  rows={df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · CAD Metadata — Silver
# MAGIC
# MAGIC **Transforms applied**
# MAGIC - Trim all string columns
# MAGIC - Parse `file_date` from filename pattern `cad_metadata_YYYY-MM-DD`
# MAGIC - Cast columns to correct types
# MAGIC - Drop rows with null `file_id`
# MAGIC - Drop bronze audit columns

# COMMAND ----------

print("══ CAD Metadata ══════════════════════════════════════")

cad_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.cad_metadata")

# ── Step 1: Trim all string columns ──────────────────────────────────
str_cols = [f.name for f in cad_bronze.schema.fields if isinstance(f.dataType, T.StringType)
            and not f.name.startswith("_")]

cad_trimmed = cad_bronze
for col in str_cols:
    cad_trimmed = cad_trimmed.withColumn(col, F.trim(F.col(col)))

# ── Step 2: Parse file_date from _source_file path ───────────────────
cad_typed = (
    cad_trimmed
    .withColumn(
        "file_date",
        F.when(
            F.regexp_extract(F.col("_source_file"), r"cad_metadata_(\d{4}-\d{2}-\d{2})", 1) != "",
            F.to_date(
                F.regexp_extract(F.col("_source_file"), r"cad_metadata_(\d{4}-\d{2}-\d{2})", 1),
                "yyyy-MM-dd"
            )
        )
    )
)

# ── Step 3: Drop bronze audit columns (prefix _bronze / _source) ─────
drop_cols = [c for c in cad_typed.columns if c.startswith("_bronze") or c.startswith("_source")]
cad_clean = cad_typed.drop(*drop_cols)

# ── Step 4: Quarantine rows missing file_id ───────────────────────────
cad_valid, cad_rejected = quarantine_split(
    cad_clean,
    F.col("part_id").isNotNull(),
    "null part_id"
)

# ── Step 5: Add silver audit cols ─────────────────────────────────────
cad_silver = add_silver_audit(cad_valid, "cad_metadata")

print(f"  Valid rows     : {cad_valid.count():,}")
cad_silver.display()

# COMMAND ----------

# Dedup on merge key to prevent MERGE ambiguity from overlapping source files
from pyspark.sql.window import Window

_w = Window.partitionBy("part_id").orderBy(F.col("_silver_processed_at").desc())
cad_silver_deduped = (
    cad_silver
    .withColumn("_rn", F.row_number().over(_w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

write_silver_table(
    df             = cad_silver_deduped,
    table_name     = "cad_metadata",
    merge_keys     = ["part_id"],
    partition_cols = None
)
write_quarantine(cad_rejected, "cad_metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · PLM Revisions — Silver
# MAGIC
# MAGIC **Transforms applied**
# MAGIC - Cast `effective_from`, `effective_to`, `last_modified` → `TimestampType`
# MAGIC - Trim + title-case `status`
# MAGIC - Validate `status` in allowed list
# MAGIC - Flag open revisions (`effective_to IS NULL`) as `is_current_revision`
# MAGIC - Validate `effective_from IS NOT NULL`
# MAGIC - Deduplicate on `(part_id, revision)` keeping latest `last_modified`

# COMMAND ----------

print("══ PLM Revisions ═════════════════════════════════════")

rev_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.plm_revisions")

# ── Step 1: Cast and clean ────────────────────────────────────────────
rev_typed = (
    rev_bronze
    .withColumn("part_id",        F.trim(F.col("part_id")))
    .withColumn("revision",       F.trim(F.upper(F.col("revision"))))
    .withColumn("status",         F.trim(F.initcap(F.col("status"))))
    .withColumn("effective_from", F.col("effective_from").cast("timestamp"))
    .withColumn("effective_to",   F.col("effective_to").cast("timestamp"))
    .withColumn("last_modified",  F.col("last_modified").cast("timestamp"))
)

# ── Step 2: Quarantine — null effective_from ──────────────────────────
rev_typed, rev_rej1 = quarantine_split(
    rev_typed,
    F.col("effective_from").isNotNull(),
    "null effective_from"
)

# ── Step 3: Quarantine — invalid status values ────────────────────────
rev_typed, rev_rej2 = quarantine_split(
    rev_typed,
    F.col("status").isin(VALID_STATUSES),
    f"status not in {VALID_STATUSES}"
)

all_rev_rejected = rev_rej1.unionByName(rev_rej2, allowMissingColumns=True)

# ── Step 4: Dedup — keep latest last_modified per (part_id, revision) ─
from pyspark.sql.window import Window

rev_dedup_window = (
    Window.partitionBy("part_id", "revision")
          .orderBy(F.col("last_modified").desc())
)
rev_deduped = (
    rev_typed
    .withColumn("_row_num", F.row_number().over(rev_dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# ── Step 5: Derived columns ───────────────────────────────────────────
rev_enriched = (
    rev_deduped
    .withColumn("is_current_revision", F.col("effective_to").isNull())
    .withColumn("revision_duration_days",
        F.when(
            F.col("effective_to").isNotNull(),
            F.datediff(F.col("effective_to"), F.col("effective_from"))
        ).otherwise(
            F.datediff(F.current_date(), F.col("effective_from"))
        )
    )
)

# ── Step 6: Drop bronze audit cols, add silver audit ──────────────────
drop_cols = [c for c in rev_enriched.columns if c.startswith("_bronze") or c.startswith("_source")]
rev_silver = add_silver_audit(rev_enriched.drop(*drop_cols), "plm_revisions")

print(f"  Valid rows     : {rev_silver.count():,}")
rev_silver.display()

# COMMAND ----------

write_silver_table(
    df             = rev_silver,
    table_name     = "plm_revisions",
    merge_keys     = ["part_id", "revision"],
    partition_cols = ["status"]
)
write_quarantine(all_rev_rejected, "plm_revisions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · PLM BOM — Silver
# MAGIC
# MAGIC **Transforms applied**
# MAGIC - Trim string columns
# MAGIC - Cast `quantity` → `IntegerType`, `last_modified` → `TimestampType`
# MAGIC - Quarantine rows where `quantity <= 0` or null
# MAGIC - Enrich with `is_current_revision` flag from silver revisions
# MAGIC - Deduplicate on `(assembly_id, part_id)`

# COMMAND ----------

print("══ PLM BOM ═══════════════════════════════════════════")

bom_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.plm_bom")

# ── Step 1: Cast and clean ────────────────────────────────────────────
bom_typed = (
    bom_bronze
    .withColumn("assembly_id",   F.trim(F.col("assembly_id")))
    .withColumn("part_id",       F.trim(F.col("part_id")))
    .withColumn("quantity",      F.col("quantity").cast("integer"))
    .withColumn("last_modified", F.col("last_modified").cast("timestamp"))
)

# ── Step 2: Quarantine — invalid quantity ─────────────────────────────
bom_typed, bom_rejected = quarantine_split(
    bom_typed,
    (F.col("quantity").isNotNull()) & (F.col("quantity") > 0),
    "quantity null or <= 0"
)

# ── Step 3: Dedup on (assembly_id, part_id) ───────────────────────────
bom_dedup_window = (
    Window.partitionBy("assembly_id", "part_id")
          .orderBy(F.col("last_modified").desc())
)
bom_deduped = (
    bom_typed
    .withColumn("_row_num", F.row_number().over(bom_dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# ── Step 4: Enrich — join active revision flag ────────────────────────
rev_silver_ref = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.plm_revisions")
    .filter(F.col("is_current_revision") == True)
    .select("part_id", "revision", "status")
    .withColumnRenamed("revision", "current_revision")
    .withColumnRenamed("status",   "current_status")
)

bom_enriched = (
    bom_deduped
    .join(rev_silver_ref, on="part_id", how="left")
    .withColumn("has_active_revision", F.col("current_revision").isNotNull())
)

# ── Step 5: Drop bronze audit cols, add silver audit ──────────────────
drop_cols = [c for c in bom_enriched.columns if c.startswith("_bronze") or c.startswith("_source")]
bom_silver = add_silver_audit(bom_enriched.drop(*drop_cols), "plm_bom")

print(f"  Valid rows     : {bom_silver.count():,}")
bom_silver.display()

# COMMAND ----------

write_silver_table(
    df             = bom_silver,
    table_name     = "plm_bom",
    merge_keys     = ["assembly_id", "part_id"],
    partition_cols = None
)
write_quarantine(bom_rejected, "plm_bom")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · ERP Cost — Silver
# MAGIC
# MAGIC **Transforms applied**
# MAGIC - Trim string columns
# MAGIC - Cast `cost` → `DecimalType(18,4)`, `last_updated` → `TimestampType`
# MAGIC - Validate `cost > 0`
# MAGIC - Validate `currency` in allowed list
# MAGIC - Standardise `supplier_name` to UPPER case
# MAGIC - Deduplicate on `(part_id, supplier_name)` keeping latest `last_updated`

# COMMAND ----------

print("══ ERP Cost ══════════════════════════════════════════")

erp_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.erp_cost")

# ── Step 1: Cast and clean ────────────────────────────────────────────
erp_typed = (
    erp_bronze
    .withColumn("part_id",       F.trim(F.col("part_id")))
    .withColumn("supplier_name", F.trim(F.upper(F.col("supplier_name"))))
    .withColumn("currency",      F.trim(F.upper(F.col("currency"))))
    .withColumn("cost",          F.col("cost").cast(T.DecimalType(18, 4)))
    .withColumn("last_updated",  F.col("last_updated").cast("timestamp"))
)

# ── Step 2: Quarantine — cost <= 0 or null ────────────────────────────
erp_typed, erp_rej1 = quarantine_split(
    erp_typed,
    (F.col("cost").isNotNull()) & (F.col("cost") > 0),
    "cost null or <= 0"
)

# ── Step 3: Quarantine — invalid currency ────────────────────────────
erp_typed, erp_rej2 = quarantine_split(
    erp_typed,
    F.col("currency").isin(VALID_CURRENCIES),
    f"currency not in {VALID_CURRENCIES}"
)

all_erp_rejected = erp_rej1.unionByName(erp_rej2, allowMissingColumns=True)

# ── Step 4: Dedup on (part_id, supplier_name) ─────────────────────────
erp_dedup_window = (
    Window.partitionBy("part_id", "supplier_name")
          .orderBy(F.col("last_updated").desc())
)
erp_deduped = (
    erp_typed
    .withColumn("_row_num", F.row_number().over(erp_dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# ── Step 5: Drop bronze audit cols, add silver audit ──────────────────
drop_cols = [c for c in erp_deduped.columns if c.startswith("_bronze") or c.startswith("_source")]
erp_silver = add_silver_audit(erp_deduped.drop(*drop_cols), "erp_cost")

print(f"  Valid rows     : {erp_silver.count():,}")
erp_silver.display()

# COMMAND ----------

write_silver_table(
    df             = erp_silver,
    table_name     = "erp_cost",
    merge_keys     = ["part_id", "supplier_name"],
    partition_cols = ["currency"]
)
write_quarantine(all_erp_rejected, "erp_cost")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Conformed view — `part_cost_view`
# MAGIC
# MAGIC Joins the three silver tables to produce a unified per-part cost view.
# MAGIC This is the primary table consumed by downstream gold / reporting layers.
# MAGIC
# MAGIC **Grain**: one row per `(part_id, supplier_name, assembly_id)`
# MAGIC
# MAGIC | Column | Source |
# MAGIC |---|---|
# MAGIC | part_id, revision, status, effective_from, effective_to | plm_revisions (current only) |
# MAGIC | assembly_id, quantity | plm_bom |
# MAGIC | supplier_name, cost, currency | erp_cost (lowest cost per part) |
# MAGIC | total_assembly_cost | cost × quantity |

# COMMAND ----------

print("══ Conformed view: part_cost_view ════════════════════")

# Read silver tables
rev_s = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.plm_revisions")
    .filter(F.col("is_current_revision") == True)
    .select("part_id", "revision", "status",
            "effective_from", "effective_to", "revision_duration_days")
)

bom_s = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.plm_bom")
    .select("assembly_id", "part_id", "quantity",
            "current_revision", "has_active_revision")
)

erp_s = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.erp_cost")
    .select("part_id", "supplier_name", "cost", "currency", "last_updated")
)

# ── Join: BOM ←→ active revision ──────────────────────────────────────
bom_rev = bom_s.join(rev_s, on="part_id", how="left")

# ── Join: (BOM + revision) ←→ ERP cost ───────────────────────────────
part_cost = bom_rev.join(erp_s, on="part_id", how="left")

# ── Derived: total_assembly_cost ──────────────────────────────────────
part_cost_enriched = (
    part_cost
    .withColumn(
        "total_assembly_cost",
        F.round(F.col("cost") * F.col("quantity"), 4)
    )
    .withColumn(
        "cost_data_available",
        F.col("cost").isNotNull()
    )
    .withColumn(
        "revision_data_available",
        F.col("revision").isNotNull()
    )
)

# ── Silver audit ──────────────────────────────────────────────────────
part_cost_silver = add_silver_audit(part_cost_enriched, "part_cost_view")

print(f"  Rows in conformed view : {part_cost_silver.count():,}")
part_cost_silver.display()

# COMMAND ----------

write_silver_table(
    df             = part_cost_silver,
    table_name     = "part_cost_view",
    merge_keys     = ["part_id", "supplier_name", "assembly_id"],
    partition_cols = ["currency"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Quarantine summary

# COMMAND ----------

print("══ Quarantine Summary ════════════════════════════════")

quarantine_tables = [
    "cad_metadata_quarantine",
    "plm_revisions_quarantine",
    "plm_bom_quarantine",
    "erp_cost_quarantine",
]

for qt in quarantine_tables:
    full = f"{CATALOG}.{SILVER_SCHEMA}.{qt}"
    try:
        cnt = spark.table(full).count()
        if cnt > 0:
            print(f"  ⚠  {qt:35s}  rows={cnt:>6,}  ← review required")
            spark.table(full).groupBy("_rejection_reason").count().display()
        else:
            print(f"  ✓  {qt:35s}  rows=     0")
    except Exception:
        print(f"  –  {qt:35s}  (not yet created)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Data quality summary

# COMMAND ----------

print("══ Silver Layer Row Count Summary ════════════════════")

silver_tables = [
    ("cad_metadata",    ["file_id"]),
    ("plm_revisions",   ["part_id", "revision"]),
    ("plm_bom",         ["assembly_id", "part_id"]),
    ("erp_cost",        ["part_id", "supplier_name"]),
    ("part_cost_view",  ["part_id", "supplier_name", "assembly_id"]),
]

for tbl, keys in silver_tables:
    full = f"{CATALOG}.{SILVER_SCHEMA}.{tbl}"
    try:
        df   = spark.table(full)
        cnt  = df.count()
        null_key_cnt = df.filter(
            F.greatest(*[F.col(k).isNull().cast("int") for k in keys]) == 1
        ).count()
        latest = df.agg(F.max("_silver_processed_at")).collect()[0][0]
        status = "✓" if null_key_cnt == 0 else "⚠ null keys"
        print(f"  {status}  {tbl:30s}  rows={cnt:>8,}  null_keys={null_key_cnt}  latest={latest}")
    except Exception as e:
        print(f"  !!  {tbl} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Table tagging (Unity Catalog)

# COMMAND ----------

tag_map = {
    "cad_metadata"   : {"domain": "cad", "layer": "silver", "pii": "false", "owner": "eng-data-platform"},
    "plm_revisions"  : {"domain": "plm", "layer": "silver", "pii": "false", "owner": "eng-data-platform"},
    "plm_bom"        : {"domain": "plm", "layer": "silver", "pii": "false", "owner": "eng-data-platform"},
    "erp_cost"       : {"domain": "erp", "layer": "silver", "pii": "false", "owner": "eng-data-platform"},
    "part_cost_view" : {"domain": "conformed", "layer": "silver", "pii": "false", "owner": "eng-data-platform"},
}

for tbl, tags in tag_map.items():
    full = f"{CATALOG}.{SILVER_SCHEMA}.{tbl}"
    for k, v in tags.items():
        spark.sql(f"ALTER TABLE {full} SET TAGS ('{k}' = '{v}')")
    print(f"  Tagged: {full}")

print("\nSilver layer notebook complete.")
