# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — CAD · PLM · ERP
# MAGIC **Layer**: Gold (aggregated · business-ready · reporting-optimised)
# MAGIC
# MAGIC **Source** : `<catalog>.silver_plm.*`
# MAGIC **Target**  : `<catalog>.gold_plm.*`
# MAGIC
# MAGIC | Gold Table | Grain | Business Question |
# MAGIC |---|---|---|
# MAGIC | `dim_part` | one row per part | What is this part and its current revision? |
# MAGIC | `dim_supplier` | one row per supplier | Who are our active suppliers? |
# MAGIC | `fact_bom_cost` | assembly × part × supplier | What does each assembly cost to build? |
# MAGIC | `agg_assembly_cost` | one row per assembly | Total cost per assembly |
# MAGIC | `agg_supplier_cost` | one row per supplier | Spend and part coverage per supplier |
# MAGIC | `agg_part_revision_history` | part × revision | Full revision lifecycle per part |
# MAGIC | `agg_cad_activity` | date | How many CAD files processed per day? |
# MAGIC
# MAGIC > **Gold rules**: no audit columns exposed to consumers, all metrics
# MAGIC > pre-computed, all joins already done, partition/Z-Order for query performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

CATALOG       = "eng_data_platform"
SILVER_SCHEMA = "silver_plm"
GOLD_SCHEMA   = "gold_plm"

from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

PIPELINE_RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
RUN_DATE        = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"Source : {CATALOG}.{SILVER_SCHEMA}")
print(f"Target : {CATALOG}.{GOLD_SCHEMA}")
print(f"Run at : {PIPELINE_RUN_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bootstrap — create gold schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
print(f"Schema ready: {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Helpers

# COMMAND ----------

def add_gold_audit(df: DataFrame) -> DataFrame:
    """Stamp gold-layer lineage columns."""
    return (
        df
        .withColumn("_gold_processed_at",  F.lit(PIPELINE_RUN_TS).cast("timestamp"))
        .withColumn("_gold_pipeline_run",  F.lit(PIPELINE_RUN_TS))
    )

def write_gold_table(
    df: DataFrame,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None,
    zorder_cols: list   = None
):
    """Write DataFrame to gold Unity Catalog table via MERGE."""
    full   = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
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
        delta_tbl  = DeltaTable.forName(spark, full)
        merge_cond = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        update_map = {c: f"s.{c}" for c in df.columns if c not in merge_keys}
        (
            delta_tbl.alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsertAll()
            .execute()
        )

    cnt = spark.table(full).count()
    print(f"  → {full}  rows={cnt:,}")

    if zorder_cols:
        cols = ", ".join(zorder_cols)
        print(f"  OPTIMIZE ZORDER({cols})")
        spark.sql(f"OPTIMIZE {full} ZORDER BY ({cols})")

    return full


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Load silver tables (read once, reuse)

# COMMAND ----------

print("── Loading silver tables ─────────────────────────────")

sv_revisions  = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.plm_revisions")
sv_bom        = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.plm_bom")
sv_erp        = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.erp_cost")
sv_part_cost  = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.part_cost_view")
sv_cad        = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.cad_metadata")

# Cache tables used in multiple gold builds
sv_revisions.cache()
sv_erp.cache()
sv_part_cost.cache()

print("  plm_revisions  :", sv_revisions.count())
print("  plm_bom        :", sv_bom.count())
print("  erp_cost       :", sv_erp.count())
print("  part_cost_view :", sv_part_cost.count())
print("  cad_metadata   :", sv_cad.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · `dim_part` — Part Dimension
# MAGIC
# MAGIC One row per `part_id`.
# MAGIC Carries the **current active revision** details and a summary of total revision count.
# MAGIC This is the central dimension all fact tables join to.

# COMMAND ----------

print("══ dim_part ══════════════════════════════════════════")

# Current (active) revision per part
current_rev = (
    sv_revisions
    .filter(F.col("is_current_revision") == True)
    .select(
        "part_id",
        F.col("revision").alias("current_revision"),
        F.col("status").alias("current_status"),
        F.col("effective_from").alias("current_effective_from"),
        F.col("revision_duration_days").alias("current_revision_age_days")
    )
)

# Total revision count per part
rev_counts = (
    sv_revisions
    .groupBy("part_id")
    .agg(
        F.count("revision").alias("total_revision_count"),
        F.min("effective_from").alias("first_released_at"),
        F.max("last_modified").alias("last_revision_change_at")
    )
)

# Supplier count per part from ERP
supplier_counts = (
    sv_erp
    .groupBy("part_id")
    .agg(
        F.count("supplier_name").alias("supplier_count"),
        F.min("cost").alias("min_cost"),
        F.max("cost").alias("max_cost"),
        F.first("currency").alias("currency")
    )
)

dim_part_df = (
    current_rev
    .join(rev_counts,      on="part_id", how="left")
    .join(supplier_counts, on="part_id", how="left")
    .withColumn("has_multi_supplier",  F.col("supplier_count") > 1)
    .withColumn("cost_spread",
        F.when(F.col("min_cost").isNotNull(),
               F.round(F.col("max_cost") - F.col("min_cost"), 4))
        .otherwise(F.lit(None))
    )
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

dim_part_gold = add_gold_audit(dim_part_df)

print(f"  Parts in dimension : {dim_part_gold.count():,}")
dim_part_gold.display()

# COMMAND ----------

write_gold_table(
    df             = dim_part_gold,
    table_name     = "dim_part",
    merge_keys     = ["part_id"],
    zorder_cols    = ["part_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `dim_supplier` — Supplier Dimension
# MAGIC
# MAGIC One row per `supplier_name`.
# MAGIC Carries part coverage count, total spend, and currency.

# COMMAND ----------

print("══ dim_supplier ══════════════════════════════════════")

dim_supplier_df = (
    sv_erp
    .groupBy("supplier_name", "currency")
    .agg(
        F.count("part_id").alias("parts_supplied"),
        F.sum("cost").alias("total_listed_cost"),
        F.avg("cost").alias("avg_cost_per_part"),
        F.min("cost").alias("min_part_cost"),
        F.max("cost").alias("max_part_cost"),
        F.max("last_updated").alias("last_price_update_at")
    )
    .withColumn("avg_cost_per_part", F.round("avg_cost_per_part", 4))
    .withColumn("total_listed_cost", F.round("total_listed_cost", 4))
    .withColumn("is_multi_part_supplier", F.col("parts_supplied") > 1)
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

dim_supplier_gold = add_gold_audit(dim_supplier_df)

print(f"  Suppliers in dimension : {dim_supplier_gold.count():,}")
dim_supplier_gold.display()

# COMMAND ----------

write_gold_table(
    df          = dim_supplier_gold,
    table_name  = "dim_supplier",
    merge_keys  = ["supplier_name"],
    zorder_cols = ["supplier_name"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · `fact_bom_cost` — BOM Cost Fact Table
# MAGIC
# MAGIC **Grain**: one row per `(assembly_id, part_id, supplier_name)`
# MAGIC
# MAGIC This is the core fact table — it answers:
# MAGIC - What does each line item of an assembly cost?
# MAGIC - Which supplier is providing each part?
# MAGIC - What is the extended cost (cost × quantity)?

# COMMAND ----------

print("══ fact_bom_cost ═════════════════════════════════════")

fact_bom_cost_df = (
    sv_part_cost
    .select(
        "assembly_id",
        "part_id",
        "supplier_name",
        "revision",
        "status",
        "quantity",
        "cost",
        "currency",
        "total_assembly_cost",
        "effective_from",
        "effective_to",
        F.col("has_active_revision").alias("is_current_revision"),
        "has_active_revision",
        "cost_data_available",
        "revision_data_available"
    )
    # Flag cheapest supplier per part
    .withColumn(
        "rank_by_cost",
        F.rank().over(
            Window.partitionBy("part_id")
                  .orderBy(F.col("cost").asc_nulls_last())
        )
    )
    .withColumn("is_lowest_cost_supplier", F.col("rank_by_cost") == 1)
    .drop("rank_by_cost")
    # Cost variance from cheapest option
    .withColumn(
        "min_cost_for_part",
        F.min("cost").over(Window.partitionBy("part_id"))
    )
    .withColumn(
        "cost_vs_cheapest",
        F.when(
            F.col("cost").isNotNull() & F.col("min_cost_for_part").isNotNull(),
            F.round(F.col("cost") - F.col("min_cost_for_part"), 4)
        ).otherwise(F.lit(None))
    )
    .drop("min_cost_for_part")
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

fact_bom_cost_gold = add_gold_audit(fact_bom_cost_df)

print(f"  Fact rows : {fact_bom_cost_gold.count():,}")
fact_bom_cost_gold.display()

# COMMAND ----------

write_gold_table(
    df             = fact_bom_cost_gold,
    table_name     = "fact_bom_cost",
    merge_keys     = ["assembly_id", "part_id", "supplier_name"],
    partition_cols = ["currency"],
    zorder_cols    = ["assembly_id", "part_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · `agg_assembly_cost` — Total Cost per Assembly
# MAGIC
# MAGIC **Grain**: one row per `assembly_id`
# MAGIC
# MAGIC Answers: *What is the total BOM cost to build assembly A2001?*
# MAGIC Uses the **lowest-cost supplier per part** for the optimistic cost
# MAGIC and **all suppliers** for the realistic (average) cost.

# COMMAND ----------

print("══ agg_assembly_cost ═════════════════════════════════")

# Cheapest option per (assembly, part)
cheapest_per_part = (
    fact_bom_cost_gold
    .filter(F.col("is_lowest_cost_supplier") == True)
    .groupBy("assembly_id")
    .agg(
        F.sum("total_assembly_cost").alias("min_total_assembly_cost"),
        F.count("part_id").alias("total_parts_count"),
        F.sum(F.when(F.col("cost_data_available"), 1).otherwise(0))
         .alias("parts_with_cost"),
        F.sum(F.when(F.col("is_current_revision"), 1).otherwise(0))
         .alias("parts_with_current_revision"),
        F.first("currency").alias("currency")
    )
)

# Average cost across all suppliers per (assembly, part)
avg_cost_per_part = (
    fact_bom_cost_gold
    .groupBy("assembly_id", "part_id")
    .agg(F.avg("cost").alias("avg_cost"), F.first("quantity").alias("qty"))
    .withColumn("avg_line_cost", F.col("avg_cost") * F.col("qty"))
    .groupBy("assembly_id")
    .agg(F.sum("avg_line_cost").alias("avg_total_assembly_cost"))
    .withColumn("avg_total_assembly_cost", F.round("avg_total_assembly_cost", 4))
)

agg_assembly_cost_df = (
    cheapest_per_part
    .join(avg_cost_per_part, on="assembly_id", how="left")
    .withColumn("min_total_assembly_cost",  F.round("min_total_assembly_cost", 4))
    .withColumn("parts_coverage_pct",
        F.round(F.col("parts_with_cost") / F.col("total_parts_count") * 100, 2)
    )
    .withColumn("revision_coverage_pct",
        F.round(F.col("parts_with_current_revision") / F.col("total_parts_count") * 100, 2)
    )
    .withColumn("cost_data_completeness",
        F.when(F.col("parts_coverage_pct") == 100, "Complete")
         .when(F.col("parts_coverage_pct") >= 80,  "Mostly Complete")
         .otherwise("Incomplete")
    )
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

agg_assembly_cost_gold = add_gold_audit(agg_assembly_cost_df)

print(f"  Assembly rows : {agg_assembly_cost_gold.count():,}")
agg_assembly_cost_gold.display()

# COMMAND ----------

write_gold_table(
    df          = agg_assembly_cost_gold,
    table_name  = "agg_assembly_cost",
    merge_keys  = ["assembly_id"],
    zorder_cols = ["assembly_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · `agg_supplier_cost` — Supplier Spend Analysis
# MAGIC
# MAGIC **Grain**: one row per `supplier_name`
# MAGIC
# MAGIC Answers: *Which suppliers cover the most parts?
# MAGIC Who offers the cheapest average pricing?
# MAGIC What share of total BOM spend goes to each supplier?*

# COMMAND ----------

print("══ agg_supplier_cost ═════════════════════════════════")

total_spend = (
    fact_bom_cost_gold
    .filter(F.col("is_lowest_cost_supplier") == True)
    .agg(F.sum("total_assembly_cost").alias("grand_total"))
    .collect()[0]["grand_total"] or 1
)

agg_supplier_cost_df = (
    fact_bom_cost_gold
    .groupBy("supplier_name", "currency")
    .agg(
        F.count("part_id").alias("parts_supplied"),
        F.countDistinct("assembly_id").alias("assemblies_covered"),
        F.sum("total_assembly_cost").alias("total_extended_cost"),
        F.avg("cost").alias("avg_unit_cost"),
        F.min("cost").alias("min_unit_cost"),
        F.max("cost").alias("max_unit_cost"),
        F.sum(
            F.when(F.col("is_lowest_cost_supplier"), F.col("total_assembly_cost"))
             .otherwise(0)
        ).alias("lowest_cost_spend")
    )
    .withColumn("avg_unit_cost",      F.round("avg_unit_cost",      4))
    .withColumn("total_extended_cost",F.round("total_extended_cost",4))
    .withColumn("lowest_cost_spend",  F.round("lowest_cost_spend",  4))
    .withColumn("spend_share_pct",
        F.round(F.col("lowest_cost_spend") / F.lit(total_spend) * 100, 2)
    )
    .withColumn("supplier_tier",
        F.when(F.col("spend_share_pct") >= 30, "Strategic")
         .when(F.col("spend_share_pct") >= 10, "Preferred")
         .otherwise("Tactical")
    )
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

agg_supplier_cost_gold = add_gold_audit(agg_supplier_cost_df)

print(f"  Supplier rows : {agg_supplier_cost_gold.count():,}")
agg_supplier_cost_gold.display()

# COMMAND ----------

write_gold_table(
    df          = agg_supplier_cost_gold,
    table_name  = "agg_supplier_cost",
    merge_keys  = ["supplier_name"],
    zorder_cols = ["supplier_name"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · `agg_part_revision_history` — Revision Lifecycle
# MAGIC
# MAGIC **Grain**: one row per `(part_id, revision)`
# MAGIC
# MAGIC Answers: *How many times has P1001 been revised?
# MAGIC How long did each revision stay active?
# MAGIC Which revision was active on a given date?*

# COMMAND ----------

print("══ agg_part_revision_history ═════════════════════════")

# Lead window to compute time-to-next-revision
lead_window = Window.partitionBy("part_id").orderBy("effective_from")

agg_rev_history_df = (
    sv_revisions
    .select(
        "part_id", "revision", "status",
        "effective_from", "effective_to",
        "is_current_revision", "revision_duration_days",
        "last_modified"
    )
    .withColumn(
        "next_revision",
        F.lead("revision").over(lead_window)
    )
    .withColumn(
        "revision_sequence",
        F.rank().over(lead_window)
    )
    .withColumn(
        "days_to_next_revision",
        F.when(
            F.col("effective_to").isNotNull(),
            F.datediff(F.col("effective_to"), F.col("effective_from"))
        ).otherwise(F.lit(None))
    )
    .withColumn(
        "revision_stage",
        F.when(F.col("revision_sequence") == 1, "Initial Release")
         .when(F.col("is_current_revision") == True, "Current")
         .otherwise("Superseded")
    )
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

agg_rev_history_gold = add_gold_audit(agg_rev_history_df)

print(f"  Revision history rows : {agg_rev_history_gold.count():,}")
agg_rev_history_gold.display()

# COMMAND ----------

write_gold_table(
    df             = agg_rev_history_gold,
    table_name     = "agg_part_revision_history",
    merge_keys     = ["part_id", "revision"],
    partition_cols = ["status"],
    zorder_cols    = ["part_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · `agg_cad_activity` — CAD File Processing Activity
# MAGIC
# MAGIC **Grain**: one row per `file_date`
# MAGIC
# MAGIC Answers: *How many CAD files were processed each day?
# MAGIC Is the CAD data pipeline keeping up with engineering output?*

# COMMAND ----------

print("══ agg_cad_activity ══════════════════════════════════")

agg_cad_df = (
    sv_cad
    .groupBy("file_date")
    .agg(
        F.count("part_id").alias("files_processed"),
        F.countDistinct("part_id").alias("unique_files"),
        F.min("_silver_processed_at").alias("first_processed_at"),
        F.max("_silver_processed_at").alias("last_processed_at")
    )
    .withColumn(
        "processing_lag_days",
        F.datediff(F.col("first_processed_at").cast("date"), F.col("file_date"))
    )
    .withColumn(
        "lag_status",
        F.when(F.col("processing_lag_days") == 0, "Same Day")
         .when(F.col("processing_lag_days") <= 1, "Next Day")
         .when(F.col("processing_lag_days") <= 3, "Within 3 Days")
         .otherwise("Delayed")
    )
    .withColumn("_gold_run_date", F.lit(RUN_DATE).cast("date"))
)

agg_cad_gold = add_gold_audit(agg_cad_df)

print(f"  CAD activity rows : {agg_cad_gold.count():,}")
agg_cad_gold.display()

# COMMAND ----------

write_gold_table(
    df             = agg_cad_gold,
    table_name     = "agg_cad_activity",
    merge_keys     = ["file_date"],
    partition_cols = None,
    zorder_cols    = ["file_date"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Gold Layer Summary

# COMMAND ----------

print("══ Gold Layer Summary ════════════════════════════════")

gold_tables = [
    ("dim_part",                 ["part_id"]),
    ("dim_supplier",             ["supplier_name"]),
    ("fact_bom_cost",            ["assembly_id", "part_id", "supplier_name"]),
    ("agg_assembly_cost",        ["assembly_id"]),
    ("agg_supplier_cost",        ["supplier_name"]),
    ("agg_part_revision_history",["part_id", "revision"]),
    ("agg_cad_activity",         ["file_date"]),
]

print(f"\n{'Table':<35} {'Rows':>10}  {'Latest run'}")
print("─" * 70)
for tbl, _ in gold_tables:
    full = f"{CATALOG}.{GOLD_SCHEMA}.{tbl}"
    try:
        df      = spark.table(full)
        cnt     = df.count()
        latest  = df.agg(F.max("_gold_processed_at")).collect()[0][0]
        print(f"  {tbl:<33} {cnt:>10,}  {latest}")
    except Exception as e:
        print(f"  !! {tbl} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Unity Catalog tags

# COMMAND ----------

tag_map = {
    "dim_part":                  {"domain": "plm",       "layer": "gold", "type": "dimension", "pii": "false"},
    "dim_supplier":              {"domain": "erp",        "layer": "gold", "type": "dimension", "pii": "false"},
    "fact_bom_cost":             {"domain": "conformed",  "layer": "gold", "type": "fact",      "pii": "false"},
    "agg_assembly_cost":         {"domain": "conformed",  "layer": "gold", "type": "aggregate", "pii": "false"},
    "agg_supplier_cost":         {"domain": "erp",        "layer": "gold", "type": "aggregate", "pii": "false"},
    "agg_part_revision_history": {"domain": "plm",        "layer": "gold", "type": "aggregate", "pii": "false"},
    "agg_cad_activity":          {"domain": "cad",        "layer": "gold", "type": "aggregate", "pii": "false"},
}

for tbl, tags in tag_map.items():
    full = f"{CATALOG}.{GOLD_SCHEMA}.{tbl}"
    for k, v in tags.items():
        spark.sql(f"ALTER TABLE {full} SET TAGS ('{k}' = '{v}')")
    print(f"  Tagged: {full}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · Grant read access (Unity Catalog)
# MAGIC
# MAGIC Uncomment and update group names before first run.

# COMMAND ----------

# GRANTS = {
#     "reporting_team"    : ["fact_bom_cost", "agg_assembly_cost", "agg_supplier_cost"],
#     "plm_engineers"     : ["dim_part", "agg_part_revision_history", "agg_cad_activity"],
#     "procurement_team"  : ["dim_supplier", "agg_supplier_cost", "fact_bom_cost"],
#     "data_analysts"     : [t for t, _ in gold_tables],   # all gold tables
# }

# for group, tables in GRANTS.items():
#     for tbl in tables:
#         full = f"{CATALOG}.{GOLD_SCHEMA}.{tbl}"
#         spark.sql(f"GRANT SELECT ON TABLE {full} TO `{group}`")
#         print(f"  GRANT SELECT ON {full} TO `{group}`")

print("Grant statements are commented out.")
print("Update group names and uncomment before first run in production.")

print("\nGold layer notebook complete.")
