# Databricks notebook source
# MAGIC %md
# MAGIC # Serving Layer — CAD · PLM · ERP
# MAGIC **Layer**: Serving (BI-ready · access-controlled · query-optimised)
# MAGIC
# MAGIC **Source** : `<catalog>.gold_plm.*`
# MAGIC **Target**  : `<catalog>.serving_plm.*`
# MAGIC
# MAGIC | Serving Object | Type | Primary Consumer |
# MAGIC |---|---|---|
# MAGIC | `vw_assembly_cost_dashboard` | View | Power BI — cost dashboard |
# MAGIC | `vw_part_catalogue` | View | Engineering — part lookup |
# MAGIC | `vw_supplier_scorecard` | View | Procurement — supplier review |
# MAGIC | `vw_revision_tracker` | View | PLM team — revision status |
# MAGIC | `vw_cad_pipeline_health` | View | Ops — pipeline monitoring |
# MAGIC | `mart_assembly_cost` | Delta table | Power BI DirectQuery / export |
# MAGIC | `mart_supplier_scorecard` | Delta table | Procurement reports |
# MAGIC | `mart_open_revisions` | Delta table | Engineering alerts |
# MAGIC | `api_part_cost_lookup` | Delta table | Application API |
# MAGIC
# MAGIC > **Serving rules**: consumer-friendly column names, no internal audit columns,
# MAGIC > row-level security enforced, all metrics labelled with units,
# MAGIC > views are live (always current), marts are pre-materialised for performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

CATALOG        = "eng_data_platform"
GOLD_SCHEMA    = "gold_plm"
SERVING_SCHEMA = "serving_plm"

from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

PIPELINE_RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
RUN_DATE        = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ── Consumer groups mapped to domain access ───────────────────────────
# Used in Row-Level Security and GRANT statements
DOMAIN_GROUPS = {
    "engineering"   : ["plm", "cad"],
    "procurement"   : ["erp", "conformed"],
    "management"    : ["plm", "cad", "erp", "conformed"],   # full access
    "data_analysts" : ["plm", "cad", "erp", "conformed"],
}

print(f"Source : {CATALOG}.{GOLD_SCHEMA}")
print(f"Target : {CATALOG}.{SERVING_SCHEMA}")
print(f"Run at : {PIPELINE_RUN_TS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bootstrap — serving schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SERVING_SCHEMA}")
print(f"Schema ready: {CATALOG}.{SERVING_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Helpers

# COMMAND ----------

def write_mart(
    df: DataFrame,
    table_name: str,
    merge_keys: list,
    partition_cols: list = None,
    zorder_cols:    list = None,
    description:    str  = ""
):
    """Write a materialised mart table to serving schema via MERGE."""
    full   = f"{CATALOG}.{SERVING_SCHEMA}.{table_name}"
    exists = spark.catalog.tableExists(full)

    # Stamp serving audit (lightweight — just run timestamp)
    df = df.withColumn("_refreshed_at", F.lit(PIPELINE_RUN_TS).cast("timestamp"))

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

        if description:
            spark.sql(f"COMMENT ON TABLE {full} IS '{description}'")
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

    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {full} ZORDER BY ({cols})")
        print(f"  OPTIMIZE ZORDER({cols})")

    cnt = spark.table(full).count()
    print(f"  → {full}  rows={cnt:,}")
    return full


def create_view(view_name: str, select_sql: str, description: str = ""):
    """Create or replace a view in the serving schema."""
    full = f"{CATALOG}.{SERVING_SCHEMA}.{view_name}"
    spark.sql(f"CREATE OR REPLACE VIEW {full} AS {select_sql}")
    if description:
        spark.sql(f"COMMENT ON VIEW {full} IS '{description}'")
    print(f"  [VIEW]   {full}")
    return full


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Load gold tables (read once, cache for reuse)

# COMMAND ----------

print("── Loading gold tables ───────────────────────────────")

g_fact       = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_bom_cost")
g_dim_part   = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_part")
g_dim_sup    = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_supplier")
g_agg_asm    = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_assembly_cost")
g_agg_sup    = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_supplier_cost")
g_rev_hist   = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_part_revision_history")
g_cad        = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_cad_activity")

g_fact.cache()
g_dim_part.cache()
g_agg_sup.cache()

print(f"  fact_bom_cost            : {g_fact.count():,}")
print(f"  dim_part                 : {g_dim_part.count():,}")
print(f"  dim_supplier             : {g_dim_sup.count():,}")
print(f"  agg_assembly_cost        : {g_agg_asm.count():,}")
print(f"  agg_supplier_cost        : {g_agg_sup.count():,}")
print(f"  agg_part_revision_history: {g_rev_hist.count():,}")
print(f"  agg_cad_activity         : {g_cad.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Views — live, always-current SQL views
# MAGIC
# MAGIC Views sit directly on gold tables.
# MAGIC They are always up to date and cost nothing to refresh.
# MAGIC Use views for interactive dashboards and ad-hoc queries.

# COMMAND ----------

print("══ Creating serving views ════════════════════════════")

# ── 4.1 Assembly Cost Dashboard ───────────────────────────────────────
create_view(
    view_name = "vw_assembly_cost_dashboard",
    select_sql = f"""
        SELECT
            f.assembly_id                           AS `Assembly ID`,
            f.part_id                               AS `Part ID`,
            f.revision                              AS `Revision`,
            f.status                                AS `Part Status`,
            f.supplier_name                         AS `Supplier`,
            f.quantity                              AS `Qty`,
            f.cost                                  AS `Unit Cost`,
            f.currency                              AS `Currency`,
            f.total_assembly_cost                   AS `Line Cost`,
            f.is_lowest_cost_supplier               AS `Is Cheapest Supplier`,
            f.cost_vs_cheapest                      AS `Cost Premium vs Cheapest`,
            a.min_total_assembly_cost               AS `Assembly Min Cost`,
            a.avg_total_assembly_cost               AS `Assembly Avg Cost`,
            a.total_parts_count                     AS `Total Parts`,
            a.parts_coverage_pct                    AS `Cost Coverage %`,
            a.cost_data_completeness                AS `Data Completeness`,
            f.is_current_revision                   AS `Is Current Revision`,
            f.has_active_revision                   AS `Has Active Revision`
        FROM {CATALOG}.{GOLD_SCHEMA}.fact_bom_cost           f
        LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.agg_assembly_cost  a
               ON f.assembly_id = a.assembly_id
    """,
    description = "Power BI cost dashboard — assembly cost by part and supplier"
)

# ── 4.2 Part Catalogue ────────────────────────────────────────────────
create_view(
    view_name = "vw_part_catalogue",
    select_sql = f"""
        SELECT
            p.part_id                               AS `Part ID`,
            p.current_revision                      AS `Current Revision`,
            p.current_status                        AS `Status`,
            p.current_effective_from                AS `Active Since`,
            p.current_revision_age_days             AS `Days Active`,
            p.total_revision_count                  AS `Total Revisions`,
            p.first_released_at                     AS `First Released`,
            p.last_revision_change_at               AS `Last Changed`,
            p.supplier_count                        AS `Supplier Count`,
            p.min_cost                              AS `Lowest Cost`,
            p.max_cost                              AS `Highest Cost`,
            p.cost_spread                           AS `Cost Spread`,
            p.currency                              AS `Currency`,
            p.has_multi_supplier                    AS `Multi-Supplier`
        FROM {CATALOG}.{GOLD_SCHEMA}.dim_part p
        ORDER BY p.part_id
    """,
    description = "Engineering part catalogue with current revision and cost summary"
)

# ── 4.3 Supplier Scorecard ────────────────────────────────────────────
create_view(
    view_name = "vw_supplier_scorecard",
    select_sql = f"""
        SELECT
            s.supplier_name                         AS `Supplier`,
            s.currency                              AS `Currency`,
            s.parts_supplied                        AS `Parts Supplied`,
            s.assemblies_covered                    AS `Assemblies Covered`,
            s.avg_unit_cost                         AS `Avg Unit Cost`,
            s.min_unit_cost                         AS `Min Unit Cost`,
            s.max_unit_cost                         AS `Max Unit Cost`,
            s.total_extended_cost                   AS `Total Extended Cost`,
            s.lowest_cost_spend                     AS `Spend (Cheapest Option)`,
            s.spend_share_pct                       AS `Spend Share %`,
            s.supplier_tier                         AS `Tier`,
            d.last_price_update_at                  AS `Last Price Update`,
            d.is_multi_part_supplier                AS `Supplies Multiple Parts`
        FROM {CATALOG}.{GOLD_SCHEMA}.agg_supplier_cost s
        LEFT JOIN {CATALOG}.{GOLD_SCHEMA}.dim_supplier d
               ON s.supplier_name = d.supplier_name
        ORDER BY s.spend_share_pct DESC
    """,
    description = "Procurement supplier scorecard with tier classification and spend share"
)

# ── 4.4 Revision Tracker ──────────────────────────────────────────────
create_view(
    view_name = "vw_revision_tracker",
    select_sql = f"""
        SELECT
            r.part_id                               AS `Part ID`,
            r.revision                              AS `Revision`,
            r.revision_sequence                     AS `Rev #`,
            r.revision_stage                        AS `Stage`,
            r.status                                AS `Status`,
            r.effective_from                        AS `Effective From`,
            r.effective_to                          AS `Effective To`,
            r.is_current_revision                   AS `Is Current`,
            r.revision_duration_days                AS `Duration (Days)`,
            r.days_to_next_revision                 AS `Days Until Superseded`,
            r.next_revision                         AS `Superseded By`
        FROM {CATALOG}.{GOLD_SCHEMA}.agg_part_revision_history r
        ORDER BY r.part_id, r.revision_sequence
    """,
    description = "PLM revision tracker — full lifecycle per part including supersession chain"
)

# ── 4.5 CAD Pipeline Health ───────────────────────────────────────────
create_view(
    view_name = "vw_cad_pipeline_health",
    select_sql = f"""
        SELECT
            c.file_date                             AS `File Date`,
            c.files_processed                       AS `Files Processed`,
            c.unique_files                          AS `Unique Files`,
            c.first_processed_at                    AS `First Processed At`,
            c.last_processed_at                     AS `Last Processed At`,
            c.processing_lag_days                   AS `Lag (Days)`,
            c.lag_status                            AS `Lag Status`,
            CASE
                WHEN c.lag_status = 'Same Day'    THEN 'On Time'
                WHEN c.lag_status = 'Next Day'    THEN 'Acceptable'
                ELSE                                   'At Risk'
            END                                     AS `Health Status`
        FROM {CATALOG}.{GOLD_SCHEMA}.agg_cad_activity c
        ORDER BY c.file_date DESC
    """,
    description = "CAD pipeline health monitor — processing lag and file throughput per day"
)

print("\nAll views created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `mart_assembly_cost` — Materialised BOM Cost Mart
# MAGIC
# MAGIC **Consumer**: Power BI DirectQuery, scheduled Excel exports, cost reports
# MAGIC
# MAGIC Flat, denormalised table — one row per `(assembly_id, part_id, supplier_name)`.
# MAGIC Includes every column a cost analyst needs without any further joins.
# MAGIC Columns are business-labelled (no internal prefixes).

# COMMAND ----------

print("══ mart_assembly_cost ════════════════════════════════")

mart_asm_df = (
    g_fact
    .join(
        g_agg_asm.select(
            "assembly_id",
            "min_total_assembly_cost",
            "avg_total_assembly_cost",
            "total_parts_count",
            "parts_with_cost",
            "parts_coverage_pct",
            "cost_data_completeness"
        ),
        on="assembly_id", how="left"
    )
    .join(
        g_dim_part.select(
            "part_id",
            F.col("total_revision_count").alias("part_total_revisions"),
            F.col("first_released_at").alias("part_first_released")
        ),
        on="part_id", how="left"
    )
    # Business-friendly column renaming
    .select(
        F.col("assembly_id")                    .alias("assembly_id"),
        F.col("part_id")                        .alias("part_id"),
        F.col("revision")                       .alias("part_revision"),
        F.col("status")                         .alias("part_status"),
        F.col("supplier_name")                  .alias("supplier_name"),
        F.col("quantity")                       .alias("quantity"),
        F.col("cost")                           .alias("unit_cost"),
        F.col("currency")                       .alias("currency"),
        F.col("total_assembly_cost")            .alias("line_cost"),
        F.col("is_lowest_cost_supplier")        .alias("is_cheapest_supplier"),
        F.col("cost_vs_cheapest")               .alias("cost_premium_inr"),
        F.col("is_current_revision")            .alias("is_current_revision"),
        F.col("has_active_revision")            .alias("has_active_revision"),
        F.col("cost_data_available")            .alias("has_erp_cost"),
        F.col("min_total_assembly_cost")        .alias("assembly_min_cost"),
        F.col("avg_total_assembly_cost")        .alias("assembly_avg_cost"),
        F.col("total_parts_count")              .alias("assembly_total_parts"),
        F.col("parts_coverage_pct")             .alias("assembly_cost_coverage_pct"),
        F.col("cost_data_completeness")         .alias("assembly_data_completeness"),
        F.col("part_total_revisions")           .alias("part_revision_count"),
        F.col("part_first_released")            .alias("part_first_released_at"),
        F.lit(RUN_DATE).cast("date")            .alias("report_date")
    )
)

write_mart(
    df             = mart_asm_df,
    table_name     = "mart_assembly_cost",
    merge_keys     = ["assembly_id", "part_id", "supplier_name"],
    partition_cols = ["currency"],
    zorder_cols    = ["assembly_id", "part_id"],
    description    = "Flat BOM cost mart — fully denormalised for Power BI and Excel export"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · `mart_supplier_scorecard` — Procurement Mart
# MAGIC
# MAGIC **Consumer**: Procurement team reports, supplier review meetings
# MAGIC
# MAGIC One row per supplier with all KPIs pre-computed and labelled.
# MAGIC Includes recommended action based on tier and coverage.

# COMMAND ----------

print("══ mart_supplier_scorecard ═══════════════════════════")

mart_sup_df = (
    g_agg_sup
    .join(
        g_dim_sup.select("supplier_name", "last_price_update_at", "is_multi_part_supplier"),
        on="supplier_name", how="left"
    )
    .withColumn(
        "days_since_price_update",
        F.datediff(F.current_date(), F.col("last_price_update_at").cast("date"))
    )
    .withColumn(
        "price_freshness",
        F.when(F.col("days_since_price_update") <= 30,  "Current")
         .when(F.col("days_since_price_update") <= 90,  "Stale")
         .otherwise("Outdated")
    )
    .withColumn(
        "recommended_action",
        F.when(
            (F.col("supplier_tier") == "Strategic") & (F.col("price_freshness") == "Current"),
            "Maintain — key supplier, pricing current"
        ).when(
            (F.col("supplier_tier") == "Strategic") & (F.col("price_freshness") != "Current"),
            "Urgent — renegotiate pricing with strategic supplier"
        ).when(
            (F.col("supplier_tier") == "Preferred") & (F.col("parts_supplied") == 1),
            "Review — single-part preferred supplier, consider consolidation"
        ).when(
            F.col("supplier_tier") == "Tactical",
            "Monitor — low spend share, evaluate if needed"
        ).otherwise("Review")
    )
    .select(
        "supplier_name",
        "currency",
        "parts_supplied",
        "assemblies_covered",
        "avg_unit_cost",
        "min_unit_cost",
        "max_unit_cost",
        "total_extended_cost",
        "lowest_cost_spend",
        "spend_share_pct",
        "supplier_tier",
        "last_price_update_at",
        "days_since_price_update",
        "price_freshness",
        "recommended_action",
        "is_multi_part_supplier",
        F.lit(RUN_DATE).cast("date").alias("report_date")
    )
)

write_mart(
    df          = mart_sup_df,
    table_name  = "mart_supplier_scorecard",
    merge_keys  = ["supplier_name"],
    zorder_cols = ["supplier_name"],
    description = "Procurement supplier scorecard with tier, spend share, and recommended action"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · `mart_open_revisions` — Engineering Alerts Mart
# MAGIC
# MAGIC **Consumer**: Engineering team, PLM governance alerts
# MAGIC
# MAGIC Contains only parts with open (current) revisions and flags those
# MAGIC that are overdue for review based on age thresholds.

# COMMAND ----------

print("══ mart_open_revisions ═══════════════════════════════")

# Age thresholds in days — adjust to your PLM governance policy
REVIEW_THRESHOLD_DAYS   = 180   # flag for review after 6 months
ESCALATION_THRESHOLD_DAYS = 365 # escalate after 12 months

mart_rev_df = (
    g_rev_hist
    .filter(F.col("is_current_revision") == True)
    .join(
        g_dim_part.select(
            "part_id",
            "supplier_count",
            "min_cost",
            "currency"
        ),
        on="part_id", how="left"
    )
    .withColumn(
        "review_status",
        F.when(
            F.col("revision_duration_days") >= ESCALATION_THRESHOLD_DAYS,
            "Escalate — overdue for revision"
        ).when(
            F.col("revision_duration_days") >= REVIEW_THRESHOLD_DAYS,
            "Review — approaching revision threshold"
        ).otherwise("On Track")
    )
    .withColumn(
        "days_until_review_due",
        F.greatest(
            F.lit(REVIEW_THRESHOLD_DAYS) - F.col("revision_duration_days"),
            F.lit(0)
        )
    )
    .withColumn(
        "has_cost_data",
        F.col("min_cost").isNotNull()
    )
    .select(
        "part_id",
        "revision",
        "status",
        "effective_from",
        F.col("revision_duration_days")     .alias("days_active"),
        "days_until_review_due",
        "review_status",
        "revision_sequence",
        F.col("supplier_count")             .alias("supplier_count"),
        F.col("min_cost")                   .alias("current_min_cost"),
        "currency",
        "has_cost_data",
        F.lit(RUN_DATE).cast("date")        .alias("report_date")
    )
    .orderBy(F.col("days_active").desc())
)

write_mart(
    df          = mart_rev_df,
    table_name  = "mart_open_revisions",
    merge_keys  = ["part_id", "revision"],
    zorder_cols = ["part_id", "review_status"],
    description = "Engineering alert mart — open revisions flagged by age against governance thresholds"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · `api_part_cost_lookup` — Application API Table
# MAGIC
# MAGIC **Consumer**: REST API / application layer that needs point lookups
# MAGIC by `part_id`
# MAGIC
# MAGIC Optimised for single-row lookups — one row per `part_id`,
# MAGIC all relevant cost and revision info collapsed into a single record.
# MAGIC Z-Ordered on `part_id` for sub-second point query performance.

# COMMAND ----------

print("══ api_part_cost_lookup ══════════════════════════════")

# Cheapest supplier per part as a struct
cheapest_supplier = (
    g_fact
    .filter(F.col("is_lowest_cost_supplier") == True)
    .select(
        "part_id",
        F.col("supplier_name")          .alias("cheapest_supplier"),
        F.col("cost")                   .alias("cheapest_unit_cost"),
        F.col("currency")               .alias("cost_currency")
    )
)

# All suppliers as an array (for API response)
all_suppliers = (
    g_fact
    .groupBy("part_id")
    .agg(
        F.collect_list(
            F.struct(
                F.col("supplier_name").alias("supplier"),
                F.col("cost").alias("unit_cost"),
                F.col("currency").alias("currency")
            )
        ).alias("all_suppliers")
    )
)

api_lookup_df = (
    g_dim_part
    .select(
        "part_id",
        "current_revision",
        "current_status",
        "current_effective_from",
        "total_revision_count"
    )
    .join(cheapest_supplier, on="part_id", how="left")
    .join(all_suppliers,     on="part_id", how="left")
    .withColumn(
        "is_available",
        F.col("current_status").isin(["Released"])
    )
    .withColumn(
        "api_response_json",
        F.to_json(
            F.struct(
                F.col("part_id"),
                F.col("current_revision"),
                F.col("current_status"),
                F.col("is_available"),
                F.col("cheapest_supplier"),
                F.col("cheapest_unit_cost"),
                F.col("cost_currency"),
                F.col("all_suppliers")
            )
        )
    )
    .select(
        "part_id",
        "current_revision",
        "current_status",
        "is_available",
        "cheapest_supplier",
        "cheapest_unit_cost",
        "cost_currency",
        "all_suppliers",
        "api_response_json",
        F.lit(PIPELINE_RUN_TS).cast("timestamp").alias("cache_refreshed_at")
    )
)

write_mart(
    df          = api_lookup_df,
    table_name  = "api_part_cost_lookup",
    merge_keys  = ["part_id"],
    zorder_cols = ["part_id"],
    description = "API cache table — point lookups by part_id returning cost and revision in one row"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Row-Level Security (Unity Catalog)
# MAGIC
# MAGIC Restrict `mart_assembly_cost` so that:
# MAGIC - `procurement_team`  sees only cost/supplier columns (no revision internals)
# MAGIC - `plm_engineers`     sees only revision/BOM columns (no supplier pricing)
# MAGIC - `management`        sees everything
# MAGIC
# MAGIC Implemented via **column masks** and **row filters** on the mart table.

# COMMAND ----------

# ── Row filter — restrict assembly rows by group membership ──────────
# Only relevant if you have assembly-level access segmentation.
# Uncomment and replace 'procurement_team' with your actual UC group.

# spark.sql(f"""
#     CREATE OR REPLACE ROW FILTER filter_assembly_access
#     ON TABLE {CATALOG}.{SERVING_SCHEMA}.mart_assembly_cost
#     (assembly_id STRING)
#     RETURN
#         IS_ACCOUNT_GROUP_MEMBER('management')
#         OR IS_ACCOUNT_GROUP_MEMBER('data_analysts')
#         OR (IS_ACCOUNT_GROUP_MEMBER('procurement_team') AND assembly_id IS NOT NULL)
#         OR (IS_ACCOUNT_GROUP_MEMBER('plm_engineers')    AND assembly_id IS NOT NULL)
# """)

# ── Column mask — hide unit_cost from non-procurement users ──────────
# spark.sql(f"""
#     CREATE OR REPLACE COLUMN MASK mask_cost_for_non_procurement
#     ON COLUMN {CATALOG}.{SERVING_SCHEMA}.mart_assembly_cost.unit_cost
#     USING COLUMNS (unit_cost)
#     RETURN
#         CASE
#             WHEN IS_ACCOUNT_GROUP_MEMBER('procurement_team') THEN unit_cost
#             WHEN IS_ACCOUNT_GROUP_MEMBER('management')       THEN unit_cost
#             WHEN IS_ACCOUNT_GROUP_MEMBER('data_analysts')    THEN unit_cost
#             ELSE NULL
#         END
# """)

print("RLS statements are commented out.")
print("Update group names and uncomment to enforce column/row-level access.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · GRANT access to serving objects

# COMMAND ----------

# Serving views — grant to consumer groups
# Uncomment and update group names before running in production.

# GRANTS = {
#     "plm_engineers": [
#         "vw_part_catalogue",
#         "vw_revision_tracker",
#         "vw_cad_pipeline_health",
#         "mart_open_revisions",
#     ],
#     "procurement_team": [
#         "vw_supplier_scorecard",
#         "vw_assembly_cost_dashboard",
#         "mart_supplier_scorecard",
#         "mart_assembly_cost",
#     ],
#     "management": [
#         "vw_assembly_cost_dashboard",
#         "vw_part_catalogue",
#         "vw_supplier_scorecard",
#         "vw_revision_tracker",
#         "vw_cad_pipeline_health",
#         "mart_assembly_cost",
#         "mart_supplier_scorecard",
#         "mart_open_revisions",
#     ],
#     "data_analysts": [
#         "vw_assembly_cost_dashboard",
#         "vw_part_catalogue",
#         "vw_supplier_scorecard",
#         "vw_revision_tracker",
#         "vw_cad_pipeline_health",
#         "mart_assembly_cost",
#         "mart_supplier_scorecard",
#         "mart_open_revisions",
#         "api_part_cost_lookup",
#     ],
# }

# for group, objects in GRANTS.items():
#     for obj in objects:
#         full = f"{CATALOG}.{SERVING_SCHEMA}.{obj}"
#         spark.sql(f"GRANT SELECT ON TABLE {full} TO `{group}`")
#         print(f"  GRANT SELECT ON {full} → `{group}`")

print("GRANT statements are commented out.")
print("Update group names and uncomment before running in production.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · UC Tags on serving objects

# COMMAND ----------

tag_map = {
    "vw_assembly_cost_dashboard" : {"layer": "serving", "type": "view",  "consumer": "power-bi",     "domain": "conformed"},
    "vw_part_catalogue"          : {"layer": "serving", "type": "view",  "consumer": "engineering",  "domain": "plm"},
    "vw_supplier_scorecard"      : {"layer": "serving", "type": "view",  "consumer": "procurement",  "domain": "erp"},
    "vw_revision_tracker"        : {"layer": "serving", "type": "view",  "consumer": "engineering",  "domain": "plm"},
    "vw_cad_pipeline_health"     : {"layer": "serving", "type": "view",  "consumer": "ops",          "domain": "cad"},
    "mart_assembly_cost"         : {"layer": "serving", "type": "mart",  "consumer": "power-bi",     "domain": "conformed"},
    "mart_supplier_scorecard"    : {"layer": "serving", "type": "mart",  "consumer": "procurement",  "domain": "erp"},
    "mart_open_revisions"        : {"layer": "serving", "type": "mart",  "consumer": "engineering",  "domain": "plm"},
    "api_part_cost_lookup"       : {"layer": "serving", "type": "mart",  "consumer": "application",  "domain": "conformed"},
}

for obj, tags in tag_map.items():
    full = f"{CATALOG}.{SERVING_SCHEMA}.{obj}"
    try:
        for k, v in tags.items():
            spark.sql(f"ALTER TABLE {full} SET TAGS ('{k}' = '{v}')")
        print(f"  Tagged: {obj}")
    except Exception as e:
        print(f"  !! Could not tag {obj}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Serving Layer Summary

# COMMAND ----------

print("══ Serving Layer Summary ═════════════════════════════")
print(f"\n{'Object':<35} {'Type':<8} {'Consumer'}")
print("─" * 70)

serving_objects = [
    ("vw_assembly_cost_dashboard",  "VIEW",  "Power BI — cost dashboard"),
    ("vw_part_catalogue",           "VIEW",  "Engineering — part lookup"),
    ("vw_supplier_scorecard",       "VIEW",  "Procurement — supplier review"),
    ("vw_revision_tracker",         "VIEW",  "PLM team — revision status"),
    ("vw_cad_pipeline_health",      "VIEW",  "Ops — pipeline monitoring"),
    ("mart_assembly_cost",          "MART",  "Power BI DirectQuery / Excel"),
    ("mart_supplier_scorecard",     "MART",  "Procurement reports"),
    ("mart_open_revisions",         "MART",  "Engineering alerts"),
    ("api_part_cost_lookup",        "MART",  "Application REST API"),
]

for obj, obj_type, consumer in serving_objects:
    full = f"{CATALOG}.{SERVING_SCHEMA}.{obj}"
    try:
        if obj_type == "MART":
            cnt = spark.table(full).count()
            print(f"  {obj:<35} {obj_type:<8} rows={cnt:>6,}  → {consumer}")
        else:
            spark.table(full).limit(1).count()   # validate view resolves
            print(f"  {obj:<35} {obj_type:<8} live         → {consumer}")
    except Exception as e:
        print(f"  !! {obj} — {e}")

print(f"\nServing layer notebook complete.  Run at: {PIPELINE_RUN_TS}")
