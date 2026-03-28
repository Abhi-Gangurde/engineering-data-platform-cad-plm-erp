# Databricks notebook source
# MAGIC %md
# MAGIC # Governance and Monitoring — CAD · PLM · ERP
# MAGIC **Layer**: Governance (cross-cutting — runs after Serving)
# MAGIC
# MAGIC **Target schema**: `<catalog>.govern_plm`
# MAGIC
# MAGIC | Section | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Pipeline run log — record every layer's execution status and duration |
# MAGIC | 2 | Data quality metrics — row counts, null rates, quarantine rates per layer |
# MAGIC | 3 | Schema drift detection — flag new or dropped columns vs baseline |
# MAGIC | 4 | Watermark health — detect stale or drifting watermarks |
# MAGIC | 5 | Delta table health — file count, size, vacuum and optimize status |
# MAGIC | 6 | Unity Catalog audit log — who accessed what and when |
# MAGIC | 7 | SLA and alerting — breach detection with Teams / email webhook |
# MAGIC | 8 | Delta maintenance — VACUUM + OPTIMIZE on all managed tables |
# MAGIC | 9 | Governance summary dashboard table |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration

# COMMAND ----------

CATALOG         = "eng_data_platform"
BRONZE_SCHEMA   = "bronze_plm"
SILVER_SCHEMA   = "silver_plm"
GOLD_SCHEMA     = "gold_plm"
SERVING_SCHEMA  = "serving_plm"
GOVERN_SCHEMA   = "govern_plm"

# ── SLA thresholds ────────────────────────────────────────────────────
SLA_MAX_DURATION_MINUTES  = 60     # alert if any layer exceeds 60 min
SLA_MAX_QUARANTINE_PCT    = 5.0    # alert if > 5% rows quarantined
SLA_MAX_WATERMARK_AGE_HRS = 25     # alert if watermark not updated in 25h
SLA_MAX_NULL_RATE_PCT     = 2.0    # alert if key column null rate > 2%

# ── Alert webhook (Teams / Slack) — replace with your endpoint ────────
ALERT_WEBHOOK_URL = ""             # e.g. https://your-org.webhook.office.com/...
ALERT_EMAIL       = ""             # e.g. data-platform-alerts@yourorg.com

# ── Watermark file path ───────────────────────────────────────────────
STORAGE_ACCOUNT   = "stengdataplatformdev"
WATERMARK_URL     = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/bronze/plm/watermark/plm_watermark.json"

from datetime import datetime, timezone, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable
import json, time, requests

PIPELINE_RUN_TS  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
RUN_DATE         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
NOTEBOOK_START   = time.time()

print(f"Govern schema : {CATALOG}.{GOVERN_SCHEMA}")
print(f"Run timestamp : {PIPELINE_RUN_TS}")
print(f"SLA (duration): {SLA_MAX_DURATION_MINUTES} min")
print(f"SLA (quarant) : {SLA_MAX_QUARANTINE_PCT}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Bootstrap — governance schema and tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}")

# Pipeline run log table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}.pipeline_run_log (
        run_id              STRING,
        run_date            DATE,
        layer               STRING,
        table_name          STRING,
        status              STRING,
        rows_read           BIGINT,
        rows_written        BIGINT,
        rows_quarantined    BIGINT,
        duration_seconds    DOUBLE,
        error_message       STRING,
        logged_at           TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (run_date)
""")

# DQ metrics table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}.dq_metrics (
        run_id              STRING,
        run_date            DATE,
        layer               STRING,
        table_name          STRING,
        metric_name         STRING,
        metric_value        DOUBLE,
        threshold           DOUBLE,
        passed              BOOLEAN,
        logged_at           TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (run_date)
""")

# Table health table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}.table_health (
        run_date            DATE,
        catalog_name        STRING,
        schema_name         STRING,
        table_name          STRING,
        row_count           BIGINT,
        size_bytes          BIGINT,
        num_files           BIGINT,
        last_modified       TIMESTAMP,
        delta_version       BIGINT,
        logged_at           TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (run_date)
""")

# Schema baseline table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}.schema_baseline (
        table_name          STRING,
        column_name         STRING,
        data_type           STRING,
        is_nullable         BOOLEAN,
        baseline_date       DATE,
        logged_at           TIMESTAMP
    )
    USING DELTA
""")

# Alert log table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOVERN_SCHEMA}.alert_log (
        run_id              STRING,
        run_date            DATE,
        alert_type          STRING,
        severity            STRING,
        table_name          STRING,
        metric_name         STRING,
        metric_value        DOUBLE,
        threshold           DOUBLE,
        message             STRING,
        webhook_sent        BOOLEAN,
        logged_at           TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (run_date)
""")

import uuid
RUN_ID = str(uuid.uuid4())[:8]
print(f"Schema and tables ready: {CATALOG}.{GOVERN_SCHEMA}")
print(f"Run ID: {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Helpers

# COMMAND ----------

alerts_raised = []

def log_pipeline_run(layer, table, status, rows_read=0, rows_written=0,
                     rows_quarantined=0, duration_sec=0.0, error_msg=None):
    """Append one row to govern_plm.pipeline_run_log."""
    row = [(RUN_ID, RUN_DATE, layer, table, status,
            rows_read, rows_written, rows_quarantined,
            round(duration_sec, 2), error_msg, PIPELINE_RUN_TS)]
    schema = T.StructType([
        T.StructField("run_id",           T.StringType()),
        T.StructField("run_date",         T.StringType()),
        T.StructField("layer",            T.StringType()),
        T.StructField("table_name",       T.StringType()),
        T.StructField("status",           T.StringType()),
        T.StructField("rows_read",        T.LongType()),
        T.StructField("rows_written",     T.LongType()),
        T.StructField("rows_quarantined", T.LongType()),
        T.StructField("duration_seconds", T.DoubleType()),
        T.StructField("error_message",    T.StringType()),
        T.StructField("logged_at",        T.StringType()),
    ])
    (spark.createDataFrame(row, schema)
          .withColumn("run_date",   F.col("run_date").cast("date"))
          .withColumn("logged_at",  F.col("logged_at").cast("timestamp"))
          .write.format("delta").mode("append")
          .saveAsTable(f"{CATALOG}.{GOVERN_SCHEMA}.pipeline_run_log"))


def log_dq_metric(layer, table, metric, value, threshold, passed):
    """Append one DQ metric row."""
    row = [(RUN_ID, RUN_DATE, layer, table, metric,
            float(value), float(threshold), passed, PIPELINE_RUN_TS)]
    schema = T.StructType([
        T.StructField("run_id",       T.StringType()),
        T.StructField("run_date",     T.StringType()),
        T.StructField("layer",        T.StringType()),
        T.StructField("table_name",   T.StringType()),
        T.StructField("metric_name",  T.StringType()),
        T.StructField("metric_value", T.DoubleType()),
        T.StructField("threshold",    T.DoubleType()),
        T.StructField("passed",       T.BooleanType()),
        T.StructField("logged_at",    T.StringType()),
    ])
    (spark.createDataFrame(row, schema)
          .withColumn("run_date",  F.col("run_date").cast("date"))
          .withColumn("logged_at", F.col("logged_at").cast("timestamp"))
          .write.format("delta").mode("append")
          .saveAsTable(f"{CATALOG}.{GOVERN_SCHEMA}.dq_metrics"))


def raise_alert(alert_type, severity, table, metric, value, threshold, message):
    """Record alert and optionally fire webhook."""
    alerts_raised.append({
        "run_id": RUN_ID, "run_date": RUN_DATE,
        "alert_type": alert_type, "severity": severity,
        "table_name": table, "metric_name": metric,
        "metric_value": float(value), "threshold": float(threshold),
        "message": message, "webhook_sent": False,
        "logged_at": PIPELINE_RUN_TS
    })
    icon = "🔴" if severity == "Critical" else "🟡"
    print(f"  {icon} ALERT [{severity}] {alert_type} | {table} | {metric}={value} (threshold={threshold}) | {message}")


def flush_alerts():
    """Write all buffered alerts to alert_log and fire webhook if configured."""
    if not alerts_raised:
        print("  No alerts raised.")
        return

    webhook_sent = False
    if ALERT_WEBHOOK_URL:
        try:
            summary_lines = [f"- {a['severity']}: {a['message']}" for a in alerts_raised]
            payload = {
                "text": f"**Data Platform Alert — {RUN_DATE}**\n\n" + "\n".join(summary_lines)
            }
            resp = requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=10)
            webhook_sent = resp.status_code == 200
            print(f"  Webhook fired: {resp.status_code}")
        except Exception as e:
            print(f"  Webhook failed: {e}")

    rows = [(
        a["run_id"], a["run_date"], a["alert_type"], a["severity"],
        a["table_name"], a["metric_name"], a["metric_value"],
        a["threshold"], a["message"], webhook_sent, a["logged_at"]
    ) for a in alerts_raised]

    schema = T.StructType([
        T.StructField("run_id",       T.StringType()),
        T.StructField("run_date",     T.StringType()),
        T.StructField("alert_type",   T.StringType()),
        T.StructField("severity",     T.StringType()),
        T.StructField("table_name",   T.StringType()),
        T.StructField("metric_name",  T.StringType()),
        T.StructField("metric_value", T.DoubleType()),
        T.StructField("threshold",    T.DoubleType()),
        T.StructField("message",      T.StringType()),
        T.StructField("webhook_sent", T.BooleanType()),
        T.StructField("logged_at",    T.StringType()),
    ])
    (spark.createDataFrame(rows, schema)
          .withColumn("run_date",  F.col("run_date").cast("date"))
          .withColumn("logged_at", F.col("logged_at").cast("timestamp"))
          .write.format("delta").mode("append")
          .saveAsTable(f"{CATALOG}.{GOVERN_SCHEMA}.alert_log"))

    print(f"  {len(alerts_raised)} alert(s) written to govern_plm.alert_log")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Data Quality Metrics — all layers

# COMMAND ----------

print("══ Data Quality Metrics ══════════════════════════════")

ALL_TABLES = [
    # (schema,         table_name,             key_columns,                     layer)
    (BRONZE_SCHEMA,  "cad_metadata",           ["file_id"],                     "bronze"),
    (BRONZE_SCHEMA,  "plm_revisions",          ["part_id", "revision"],         "bronze"),
    (BRONZE_SCHEMA,  "plm_bom",                ["assembly_id", "part_id"],      "bronze"),
    (BRONZE_SCHEMA,  "erp_cost",               ["part_id", "supplier_name"],    "bronze"),
    (SILVER_SCHEMA,  "cad_metadata",           ["file_id"],                     "silver"),
    (SILVER_SCHEMA,  "plm_revisions",          ["part_id", "revision"],         "silver"),
    (SILVER_SCHEMA,  "plm_bom",                ["assembly_id", "part_id"],      "silver"),
    (SILVER_SCHEMA,  "erp_cost",               ["part_id", "supplier_name"],    "silver"),
    (SILVER_SCHEMA,  "part_cost_view",         ["part_id", "supplier_name"],    "silver"),
    (GOLD_SCHEMA,    "dim_part",               ["part_id"],                     "gold"),
    (GOLD_SCHEMA,    "dim_supplier",           ["supplier_name"],               "gold"),
    (GOLD_SCHEMA,    "fact_bom_cost",          ["assembly_id", "part_id"],      "gold"),
    (GOLD_SCHEMA,    "agg_assembly_cost",      ["assembly_id"],                 "gold"),
    (GOLD_SCHEMA,    "agg_supplier_cost",      ["supplier_name"],               "gold"),
    (SERVING_SCHEMA, "mart_assembly_cost",     ["assembly_id", "part_id"],      "serving"),
    (SERVING_SCHEMA, "mart_supplier_scorecard",["supplier_name"],               "serving"),
    (SERVING_SCHEMA, "mart_open_revisions",    ["part_id", "revision"],         "serving"),
    (SERVING_SCHEMA, "api_part_cost_lookup",   ["part_id"],                     "serving"),
]

QUARANTINE_TABLES = [
    (SILVER_SCHEMA, "plm_revisions_quarantine",  "silver"),
    (SILVER_SCHEMA, "plm_bom_quarantine",         "silver"),
    (SILVER_SCHEMA, "erp_cost_quarantine",        "silver"),
    (SILVER_SCHEMA, "cad_metadata_quarantine",    "silver"),
]

print(f"\n{'Layer':<8} {'Table':<35} {'Rows':>8}  {'Null key':>9}  {'Status'}")
print("─" * 75)

for schema, tbl, keys, layer in ALL_TABLES:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        df  = spark.table(full)
        cnt = df.count()

        # Null rate on key columns
        null_cnt = df.filter(
            F.greatest(*[F.col(k).isNull().cast("int") for k in keys]) == 1
        ).count()
        null_pct = round((null_cnt / cnt * 100) if cnt > 0 else 0, 2)

        passed = null_pct <= SLA_MAX_NULL_RATE_PCT
        status = "✓" if passed else "⚠"

        log_dq_metric(layer, tbl, "row_count",    cnt,      0,                       True)
        log_dq_metric(layer, tbl, "null_key_pct", null_pct, SLA_MAX_NULL_RATE_PCT,   passed)

        if not passed:
            raise_alert("DataQuality", "Warning", tbl, "null_key_pct",
                        null_pct, SLA_MAX_NULL_RATE_PCT,
                        f"{tbl}: null key rate {null_pct}% exceeds threshold {SLA_MAX_NULL_RATE_PCT}%")

        print(f"  {layer:<6} {tbl:<35} {cnt:>8,}  {null_pct:>8.2f}%  {status}")
        log_pipeline_run(layer, tbl, "success", rows_read=cnt, rows_written=cnt)

    except Exception as e:
        print(f"  !! {full} — {e}")
        log_pipeline_run(layer, tbl, "error", error_msg=str(e))

# ── Quarantine rates ──────────────────────────────────────────────────
print(f"\n{'Layer':<8} {'Table':<35} {'Q-rows':>8}  {'Source rows':>12}  {'Q rate':>8}")
print("─" * 80)

for schema, qtbl, layer in QUARANTINE_TABLES:
    source_tbl = qtbl.replace("_quarantine", "")
    full_q     = f"{CATALOG}.{schema}.{qtbl}"
    full_s     = f"{CATALOG}.{schema}.{source_tbl}"
    try:
        q_cnt = spark.table(full_q).count()
        s_cnt = spark.table(full_s).count()
        total = q_cnt + s_cnt
        q_pct = round((q_cnt / total * 100) if total > 0 else 0, 2)
        passed = q_pct <= SLA_MAX_QUARANTINE_PCT
        status = "✓" if passed else "⚠"

        log_dq_metric(layer, source_tbl, "quarantine_pct", q_pct, SLA_MAX_QUARANTINE_PCT, passed)

        if not passed:
            raise_alert("Quarantine", "Critical", source_tbl, "quarantine_pct",
                        q_pct, SLA_MAX_QUARANTINE_PCT,
                        f"{source_tbl}: {q_pct}% rows quarantined — exceeds {SLA_MAX_QUARANTINE_PCT}% threshold")

        print(f"  {layer:<6} {source_tbl:<35} {q_cnt:>8,}  {total:>12,}  {q_pct:>7.2f}% {status}")
    except Exception:
        print(f"  –  {qtbl} not yet created (no bad rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Schema Drift Detection

# COMMAND ----------

print("══ Schema Drift Detection ════════════════════════════")

TRACKED_TABLES = [
    (SILVER_SCHEMA, "plm_revisions"),
    (SILVER_SCHEMA, "plm_bom"),
    (SILVER_SCHEMA, "erp_cost"),
    (GOLD_SCHEMA,   "fact_bom_cost"),
    (GOLD_SCHEMA,   "dim_part"),
]

baseline_full = f"{CATALOG}.{GOVERN_SCHEMA}.schema_baseline"

for schema, tbl in TRACKED_TABLES:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        current_schema = spark.table(full).schema

        # Check if baseline exists for this table
        try:
            baseline = (
                spark.table(baseline_full)
                .filter(F.col("table_name") == tbl)
                .select("column_name", "data_type")
                .collect()
            )
            baseline_cols = {r["column_name"]: r["data_type"] for r in baseline}
        except Exception:
            baseline_cols = {}

        current_cols = {
            f.name: str(f.dataType)
            for f in current_schema.fields
            if not f.name.startswith("_")
        }

        if not baseline_cols:
            # First run — save baseline
            rows = [(tbl, col, dtype, True, RUN_DATE, PIPELINE_RUN_TS)
                    for col, dtype in current_cols.items()]
            bl_schema = T.StructType([
                T.StructField("table_name",   T.StringType()),
                T.StructField("column_name",  T.StringType()),
                T.StructField("data_type",    T.StringType()),
                T.StructField("is_nullable",  T.BooleanType()),
                T.StructField("baseline_date",T.StringType()),
                T.StructField("logged_at",    T.StringType()),
            ])
            (spark.createDataFrame(rows, bl_schema)
                  .withColumn("baseline_date", F.col("baseline_date").cast("date"))
                  .withColumn("logged_at",     F.col("logged_at").cast("timestamp"))
                  .write.format("delta").mode("append")
                  .saveAsTable(baseline_full))
            print(f"  [BASELINE SAVED] {tbl}  cols={len(current_cols)}")

        else:
            # Compare current vs baseline
            added   = set(current_cols) - set(baseline_cols)
            dropped = set(baseline_cols) - set(current_cols)
            changed = {c for c in current_cols if c in baseline_cols
                       and current_cols[c] != baseline_cols[c]}

            if added or dropped or changed:
                msg = f"{tbl}: schema drift — added={list(added)} dropped={list(dropped)} changed={list(changed)}"
                raise_alert("SchemaDrift", "Critical", tbl, "schema_drift", 1, 0, msg)
                print(f"  ⚠  {tbl}: drift detected")
                if added:   print(f"       Added  : {list(added)}")
                if dropped: print(f"       Dropped: {list(dropped)}")
                if changed: print(f"       Changed: {list(changed)}")
            else:
                print(f"  ✓  {tbl}: schema matches baseline  cols={len(current_cols)}")

    except Exception as e:
        print(f"  !! {full} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Watermark Health

# COMMAND ----------

print("══ Watermark Health ══════════════════════════════════")

try:
    import urllib.request
    with urllib.request.urlopen(WATERMARK_URL, timeout=10) as resp:
        watermarks = json.loads(resp.read().decode())

    now_utc = datetime.now(timezone.utc)
    print(f"\n{'Table':<25} {'Last watermark':<25} {'Age (hrs)':>10}  {'Status'}")
    print("─" * 70)

    for entry in watermarks:
        tbl  = entry.get("table_name", "unknown")
        wm   = entry.get("last_watermark", "1900-01-01 00:00:00")
        try:
            wm_dt   = datetime.strptime(wm, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_hrs = round((now_utc - wm_dt).total_seconds() / 3600, 1)
            passed  = age_hrs <= SLA_MAX_WATERMARK_AGE_HRS
            status  = "✓" if passed else "⚠"

            log_dq_metric("pipeline", tbl, "watermark_age_hrs",
                          age_hrs, SLA_MAX_WATERMARK_AGE_HRS, passed)

            if not passed:
                raise_alert("WatermarkStale", "Critical", tbl, "watermark_age_hrs",
                            age_hrs, SLA_MAX_WATERMARK_AGE_HRS,
                            f"{tbl}: watermark not updated for {age_hrs}h — pipeline may have stalled")

            print(f"  {tbl:<25} {wm:<25} {age_hrs:>9.1f}h  {status}")

        except ValueError:
            print(f"  !! {tbl}: cannot parse watermark '{wm}'")

except Exception as e:
    print(f"  !! Could not read watermark file: {e}")
    print(f"     URL: {WATERMARK_URL}")
    print(f"     Ensure Databricks has network access and MSI permission to read blob.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Delta Table Health and Size Reporting

# COMMAND ----------

print("══ Delta Table Health ════════════════════════════════")

HEALTH_TABLES = [
    (BRONZE_SCHEMA,  "plm_revisions"),
    (BRONZE_SCHEMA,  "plm_bom"),
    (BRONZE_SCHEMA,  "erp_cost"),
    (SILVER_SCHEMA,  "plm_revisions"),
    (SILVER_SCHEMA,  "plm_bom"),
    (SILVER_SCHEMA,  "erp_cost"),
    (SILVER_SCHEMA,  "part_cost_view"),
    (GOLD_SCHEMA,    "fact_bom_cost"),
    (GOLD_SCHEMA,    "dim_part"),
    (GOLD_SCHEMA,    "agg_assembly_cost"),
    (SERVING_SCHEMA, "mart_assembly_cost"),
    (SERVING_SCHEMA, "mart_supplier_scorecard"),
    (SERVING_SCHEMA, "api_part_cost_lookup"),
]

health_rows = []

print(f"\n{'Schema':<15} {'Table':<30} {'Rows':>8}  {'Files':>6}  {'Size MB':>8}  {'Version':>8}")
print("─" * 80)

for schema, tbl in HEALTH_TABLES:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        detail      = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
        history     = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").collect()[0]
        row_count   = spark.table(full).count()
        num_files   = detail["numFiles"]
        size_bytes  = detail["sizeInBytes"]
        size_mb     = round(size_bytes / 1024 / 1024, 2)
        version     = history["version"]
        last_mod    = str(detail["lastModified"])

        health_rows.append((
            RUN_DATE, CATALOG, schema, tbl,
            row_count, size_bytes, num_files, last_mod, version, PIPELINE_RUN_TS
        ))

        print(f"  {schema:<15} {tbl:<30} {row_count:>8,}  {num_files:>6,}  {size_mb:>7.2f}  v{version:>6}")

    except Exception as e:
        print(f"  !! {full} — {e}")

# Write health rows
if health_rows:
    h_schema = T.StructType([
        T.StructField("run_date",     T.StringType()),
        T.StructField("catalog_name", T.StringType()),
        T.StructField("schema_name",  T.StringType()),
        T.StructField("table_name",   T.StringType()),
        T.StructField("row_count",    T.LongType()),
        T.StructField("size_bytes",   T.LongType()),
        T.StructField("num_files",    T.LongType()),
        T.StructField("last_modified",T.StringType()),
        T.StructField("delta_version",T.LongType()),
        T.StructField("logged_at",    T.StringType()),
    ])
    (spark.createDataFrame(health_rows, h_schema)
          .withColumn("run_date",     F.col("run_date").cast("date"))
          .withColumn("last_modified",F.col("last_modified").cast("timestamp"))
          .withColumn("logged_at",    F.col("logged_at").cast("timestamp"))
          .write.format("delta").mode("append")
          .saveAsTable(f"{CATALOG}.{GOVERN_SCHEMA}.table_health"))
    print(f"\n  Health data written for {len(health_rows)} tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Unity Catalog Audit Log

# COMMAND ----------

print("══ Unity Catalog Audit Log ═══════════════════════════")

try:
    # UC system table — available when audit log is enabled in UC settings
    audit_df = (
        spark.table("system.access.audit")
        .filter(
            (F.col("event_date") == F.current_date()) &
            (F.col("service_name") == "unityCatalog") &
            (F.col("action_name").isin(
                "getTable", "selectFromTable", "describeTable",
                "createTable", "updateTable", "deleteFromTable"
            ))
        )
        .select(
            "event_time",
            "user_identity",
            "action_name",
            "request_params.full_name_arg",
            "response.status_code"
        )
        .filter(F.col("full_name_arg").like(f"{CATALOG}.%"))
    )

    access_count = audit_df.count()
    print(f"  UC audit events today for {CATALOG}.*: {access_count:,}")

    if access_count > 0:
        print("\n  Top 10 accessed tables today:")
        (
            audit_df
            .groupBy("full_name_arg", "action_name")
            .count()
            .orderBy(F.col("count").desc())
            .limit(10)
            .display()
        )

        print("\n  Access by user:")
        (
            audit_df
            .groupBy("user_identity")
            .count()
            .orderBy(F.col("count").desc())
            .display()
        )

except Exception as e:
    print(f"  UC audit system table not available: {e}")
    print(f"  Enable it via: Account Console → Settings → Audit log → Enable system tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · SLA Breach Summary and Alert Flush

# COMMAND ----------

print("══ SLA Summary ═══════════════════════════════════════")

total_duration_min = round((time.time() - NOTEBOOK_START) / 60, 1)

if total_duration_min > SLA_MAX_DURATION_MINUTES:
    raise_alert("SLABreach", "Critical", "govern_notebook",
                "duration_minutes", total_duration_min, SLA_MAX_DURATION_MINUTES,
                f"Governance notebook ran for {total_duration_min} min — SLA is {SLA_MAX_DURATION_MINUTES} min")

print(f"  Total governance notebook duration : {total_duration_min} min")
print(f"  Total alerts raised                : {len(alerts_raised)}")
print(f"  SLA threshold (duration)           : {SLA_MAX_DURATION_MINUTES} min")
print(f"  SLA threshold (quarantine)         : {SLA_MAX_QUARANTINE_PCT}%")
print(f"  SLA threshold (watermark age)      : {SLA_MAX_WATERMARK_AGE_HRS}h")

flush_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Delta Maintenance — VACUUM + OPTIMIZE

# COMMAND ----------

print("══ Delta Maintenance ══════════════════════════════════")

VACUUM_HOURS     = 168   # 7 days — adjust down only if storage is very constrained

MAINTENANCE_TABLES = [
    (BRONZE_SCHEMA,  "plm_revisions",   ["part_id", "revision"]),
    (BRONZE_SCHEMA,  "plm_bom",         ["assembly_id", "part_id"]),
    (BRONZE_SCHEMA,  "erp_cost",        ["part_id", "supplier_name"]),
    (SILVER_SCHEMA,  "plm_revisions",   ["part_id", "revision"]),
    (SILVER_SCHEMA,  "plm_bom",         ["assembly_id", "part_id"]),
    (SILVER_SCHEMA,  "erp_cost",        ["part_id", "supplier_name"]),
    (SILVER_SCHEMA,  "part_cost_view",  ["part_id"]),
    (GOLD_SCHEMA,    "fact_bom_cost",   ["assembly_id", "part_id"]),
    (GOLD_SCHEMA,    "dim_part",        ["part_id"]),
    (GOLD_SCHEMA,    "agg_assembly_cost",["assembly_id"]),
    (SERVING_SCHEMA, "mart_assembly_cost",["assembly_id", "part_id"]),
    (SERVING_SCHEMA, "mart_supplier_scorecard",["supplier_name"]),
    (SERVING_SCHEMA, "api_part_cost_lookup",["part_id"]),
]

for schema, tbl, zorder_keys in MAINTENANCE_TABLES:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        # VACUUM — remove files older than retention window
        spark.sql(f"VACUUM {full} RETAIN {VACUUM_HOURS} HOURS")

        # OPTIMIZE + Z-ORDER — compact small files, co-locate by key columns
        cols = ", ".join(zorder_keys)
        spark.sql(f"OPTIMIZE {full} ZORDER BY ({cols})")

        print(f"  ✓  {schema}.{tbl}  VACUUM({VACUUM_HOURS}h) + OPTIMIZE ZORDER({cols})")

    except Exception as e:
        print(f"  !! {full} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Governance Summary Table (for Power BI / Databricks SQL)

# COMMAND ----------

print("══ Governance Summary ════════════════════════════════")

summary_sql = f"""
    SELECT
        m.run_date,
        m.layer,
        m.table_name,
        MAX(CASE WHEN m.metric_name = 'row_count'       THEN m.metric_value END) AS row_count,
        MAX(CASE WHEN m.metric_name = 'null_key_pct'    THEN m.metric_value END) AS null_key_pct,
        MAX(CASE WHEN m.metric_name = 'quarantine_pct'  THEN m.metric_value END) AS quarantine_pct,
        MAX(CASE WHEN m.metric_name = 'watermark_age_hrs' THEN m.metric_value END) AS watermark_age_hrs,
        MIN(m.passed)                                                               AS all_checks_passed,
        COUNT(CASE WHEN m.passed = false THEN 1 END)                               AS failed_checks,
        h.num_files,
        h.size_bytes,
        h.delta_version
    FROM {CATALOG}.{GOVERN_SCHEMA}.dq_metrics     m
    LEFT JOIN {CATALOG}.{GOVERN_SCHEMA}.table_health h
           ON m.run_date = h.run_date
          AND m.table_name = h.table_name
    WHERE m.run_date = current_date()
    GROUP BY m.run_date, m.layer, m.table_name, h.num_files, h.size_bytes, h.delta_version
    ORDER BY m.layer, m.table_name
"""

summary_df = spark.sql(summary_sql)
(
    summary_df
    .write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOVERN_SCHEMA}.daily_summary")
)

print(f"  Summary written to: {CATALOG}.{GOVERN_SCHEMA}.daily_summary")
summary_df.display()

print(f"\nGovernance notebook complete.  Run ID: {RUN_ID}  Duration: {total_duration_min} min")
