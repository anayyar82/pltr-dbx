# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Generate live SDP data (real-time E2E demo)
# MAGIC
# MAGIC Simulates a **Foundry / NOC export landing in bronze volumes**, then propagates through:
# MAGIC
# MAGIC ```text
# MAGIC This notebook  →  bronze volumes  →  DLT pipeline  →  gold tables  →  MV refresh  →  App + Genie
# MAGIC ```
# MAGIC
# MAGIC **Run order for a live demo**
# MAGIC
# MAGIC | Step | What | Where |
# MAGIC |------|------|-------|
# MAGIC | **1** | Run **all cells** in this notebook | Here |
# MAGIC | **2** | Open **Ops Console App** → Refresh board | [att-sdp-ops-ankur](https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/) |
# MAGIC | **3** | Ask Genie a question about the new incident | App → **Genie Analytics** tab |
# MAGIC | **4** | (Optional) Update incident status in App | Lakebase writeback path |
# MAGIC
# MAGIC **Prerequisite:** baseline data deployed once via `99_deploy_all_att_sdp` or `sdp_service_delivery_refresh` job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Configure

# COMMAND ----------

dbutils.widgets.dropdown(
    "scenario",
    "new_p1_dallas",
    [
        "new_p1_dallas",
        "new_p2_chicago",
        "escalate_chicago_uverse",
        "new_provisioning_order",
        "demo_burst",
    ],
    "Scenario",
)
dbutils.widgets.dropdown("write_mode", "append", ["append", "snapshot"], "Bronze write mode (append for streaming pipeline)")
dbutils.widgets.dropdown("auto_run_dlt", "false", ["true", "false"], "Auto-run DLT pipeline (not needed when continuous=true)")
dbutils.widgets.dropdown("skip_mv_refresh", "false", ["true", "false"], "Skip MV refresh (set true when job runs DLT+MV separately)")
dbutils.widgets.dropdown("skip_after_summary", "false", ["true", "false"], "Skip AFTER KPI summary")
dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SCENARIO = dbutils.widgets.get("scenario")
WRITE_MODE = dbutils.widgets.get("write_mode")
AUTO_RUN_DLT = dbutils.widgets.get("auto_run_dlt") == "true"
SKIP_MV_REFRESH = dbutils.widgets.get("skip_mv_refresh") == "true"
SKIP_AFTER_SUMMARY = dbutils.widgets.get("skip_after_summary") == "true"
FQ = f"{CATALOG}.{SCHEMA}"

print(f"Target   : {FQ}")
print(f"Scenario  : {SCENARIO}")
print(f"Write mode: {WRITE_MODE}")
print(f"Auto DLT  : {AUTO_RUN_DLT}")
print(f"Skip MV  : {SKIP_MV_REFRESH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — BEFORE snapshot (baseline KPIs)

# COMMAND ----------

from pyspark.sql import functions as F

before_kpis = spark.sql(f"SELECT * FROM {FQ}.metric_sdp_executive_summary").collect()[0]
before_incidents = spark.sql(
    f"""
    SELECT incident_id, title, severity, status, market, updated_at
    FROM {FQ}.service_incident
    WHERE status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
    ORDER BY opened_at DESC
    """
)

print("=== BEFORE KPIs ===")
print(f"  Open incidents       : {before_kpis['total_open_incidents']}")
print(f"  P1 critical          : {before_kpis['open_p1_incidents']}")
print(f"  Orders provisioning  : {before_kpis['orders_in_provisioning']}")
print(f"  Technicians available: {before_kpis['available_technicians']}")
print(f"  Open incident rows   : {before_incidents.count()}")

display(before_incidents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Generate live events
# MAGIC
# MAGIC Creates new rows (or updates) with **current timestamps** so the demo feels live.

# COMMAND ----------

from datetime import datetime, timezone, date

now = datetime.now(timezone.utc)
now_dt = now
now_ts = now.strftime("%Y-%m-%d %H:%M:%S")
batch_id = now.strftime("%H%M%S")
new_incident_id = f"INC-L{batch_id}"
new_order_id = f"ORD-L{batch_id}"

generated = {"incidents": [], "orders": [], "accounts": [], "technicians": []}


def _parse_ts(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _parse_date(value):
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _incident(**kwargs):
    row = {
        "incident_id": kwargs.get("incident_id", new_incident_id),
        "customer_account_id": kwargs["customer_account_id"],
        "market": kwargs["market"],
        "severity": kwargs["severity"],
        "status": kwargs["status"],
        "title": kwargs["title"],
        "description": kwargs.get("description", ""),
        "service_type": kwargs.get("service_type", "FIBER"),
        "opened_at": _parse_ts(kwargs.get("opened_at", now_dt)),
        "resolved_at": None,
        "assigned_technician_id": kwargs.get("assigned_technician_id"),
        "ops_notes": kwargs.get("ops_notes"),
        "updated_at": _parse_ts(kwargs.get("updated_at", now_dt)),
    }
    generated["incidents"].append(row)
    return row


def _order(**kwargs):
    row = {
        "order_id": kwargs.get("order_id", new_order_id),
        "customer_account_id": kwargs["customer_account_id"],
        "service_type": kwargs.get("service_type", "FIBER"),
        "status": kwargs.get("status", "PROVISIONING"),
        "market": kwargs["market"],
        "promised_date": _parse_date(kwargs.get("promised_date", "2026-06-20")),
        "updated_at": _parse_ts(kwargs.get("updated_at", now_dt)),
    }
    generated["orders"].append(row)
    return row


if SCENARIO == "new_p1_dallas":
    _incident(
        customer_account_id="ACC-1001",
        market="DALLAS",
        severity="P1",
        status="OPEN",
        title=f"Live demo — Fiber cut on I-35 ({batch_id})",
        description="NOC alarm: new P1 fiber cut reported during demo. Multiple enterprise circuits down.",
        service_type="FIBER",
        ops_notes="Auto-generated live feed",
    )
    print(f"Generated new P1 in Dallas: {new_incident_id}")

elif SCENARIO == "new_p2_chicago":
    _incident(
        customer_account_id="ACC-1004",
        market="CHICAGO",
        severity="P2",
        status="OPEN",
        title=f"Live demo — MPLS core degradation ({batch_id})",
        description="Latency spike on Chicago MPLS core — residential + business impact.",
        service_type="ENTERPRISE",
    )
    print(f"Generated new P2 in Chicago: {new_incident_id}")

elif SCENARIO == "escalate_chicago_uverse":
    _incident(
        incident_id="INC-9004",
        customer_account_id="ACC-1004",
        market="CHICAGO",
        severity="P2",
        status="IN_PROGRESS",
        title="Slow uverse speeds reported — ESCALATED",
        description="Customer speed test shows 50% of subscribed bandwidth. Escalated during live demo.",
        service_type="UVERSE",
        opened_at="2026-06-05 08:00:00",
        assigned_technician_id="TECH-402",
        ops_notes=f"Escalated live at {now_ts}",
        updated_at=now_ts,
    )
    print("Generated status update for INC-9004 (P4 OPEN → P2 IN_PROGRESS)")

elif SCENARIO == "new_provisioning_order":
    _order(
        customer_account_id="ACC-1003",
        market="ATLANTA",
        service_type="ENTERPRISE",
        status="PROVISIONING",
    )
    print(f"Generated new provisioning order: {new_order_id}")

elif SCENARIO == "demo_burst":
    _incident(
        customer_account_id="ACC-1001",
        market="DALLAS",
        severity="P1",
        status="OPEN",
        title=f"Live burst — Dallas fiber P1 ({batch_id})",
        description="Demo burst scenario: new critical incident.",
        service_type="FIBER",
    )
    _incident(
        incident_id="INC-9002",
        customer_account_id="ACC-1002",
        market="DALLAS",
        severity="P3",
        status="DISPATCHED",
        title="Intermittent wireless signal degradation",
        description="Residential customer reports dropped calls.",
        service_type="WIRELESS",
        opened_at="2026-06-04 14:30:00",
        assigned_technician_id="TECH-201",
        ops_notes=f"Dispatched live at {now_ts}",
        updated_at=now_ts,
    )
    _order(
        customer_account_id="ACC-1005",
        market="ATLANTA",
        service_type="FIBER",
        status="PROVISIONING",
    )
    print(f"Demo burst: {new_incident_id}, INC-9002 update, {new_order_id}")

print("\nGenerated events:")
for kind, rows in generated.items():
    if rows:
        print(f"  {kind}: {len(rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Merge into bronze volumes (Foundry export landing)
# MAGIC
# MAGIC Reads current gold state, applies mutations, writes to `/Volumes/.../sdp_exports/`.
# MAGIC **append** mode (default): append-only rows for the continuous streaming DLT pipeline.
# MAGIC **snapshot** mode: full overwrite (legacy batch — requires DLT full refresh).

# COMMAND ----------

VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/sdp_exports"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.sdp_exports COMMENT 'SDP bronze export landing'")


def append_to_volume(table: str, volume_entity: str, new_rows: list[dict]):
    if not new_rows:
        print(f"  {volume_entity}: nothing to append")
        return
    schema = spark.table(f"{FQ}.{table}").schema
    updates_df = spark.createDataFrame(new_rows, schema=schema)
    path = f"{VOLUME_BASE}/{volume_entity}"
    updates_df.write.format("delta").mode("append").save(path)
    print(f"  {volume_entity}: appended {len(new_rows)} row(s) → {path}")


def merge_and_write_volume(table: str, volume_entity: str, key: str, new_rows: list[dict]):
    base_df = spark.table(f"{FQ}.{table}")
    if not new_rows:
        base_df.write.format("delta").mode("overwrite").save(f"{VOLUME_BASE}/{volume_entity}")
        print(f"  {volume_entity}: {base_df.count()} rows (no changes)")
        return

    updates_df = spark.createDataFrame(new_rows, schema=base_df.schema)
    for col_name in base_df.columns:
        if col_name not in updates_df.columns:
            updates_df = updates_df.withColumn(col_name, F.lit(None).cast(base_df.schema[col_name].dataType))

    updates_df = updates_df.select(base_df.columns)
    combined = base_df.unionByName(updates_df)
    merged = combined.orderBy(F.col("updated_at").desc()).dropDuplicates([key])
    path = f"{VOLUME_BASE}/{volume_entity}"
    merged.write.format("delta").mode("overwrite").save(path)
    print(f"  {volume_entity}: {merged.count()} rows written → {path}")


if WRITE_MODE == "append":
    append_to_volume("customer_account", "customer_accounts", generated["accounts"])
    append_to_volume("field_technician", "field_technicians", generated["technicians"])
    append_to_volume("service_incident", "service_incidents", generated["incidents"])
    append_to_volume("service_order", "service_orders", generated["orders"])
else:
    merge_and_write_volume("customer_account", "customer_accounts", "account_id", generated["accounts"])
    merge_and_write_volume("field_technician", "field_technicians", "technician_id", generated["technicians"])
    merge_and_write_volume("service_incident", "service_incidents", "incident_id", generated["incidents"])
    merge_and_write_volume("service_order", "service_orders", "order_id", generated["orders"])

print(f"\nBronze volumes updated ({WRITE_MODE}). Continuous DLT merges into gold automatically.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Run DLT pipeline
# MAGIC
# MAGIC Propagates bronze → silver → gold (`apply_changes` SCD Type 1).
# MAGIC
# MAGIC Set widget **Auto-run DLT pipeline** = `false` if you prefer to trigger manually:
# MAGIC ```bash
# MAGIC databricks bundle run sdp_service_delivery_refresh -t e2_demo --profile e2-demo-field-eng
# MAGIC ```

# COMMAND ----------

pipeline_id = None
update_id = None

if AUTO_RUN_DLT:
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        for p in w.pipelines.list_pipelines():
            if "sdp_service_delivery_dlt" in p.name:
                pipeline_id = p.pipeline_id
                break

        if not pipeline_id:
            raise RuntimeError("Pipeline sdp_service_delivery_dlt not found. Run: databricks bundle deploy -t e2_demo")

        print(f"Starting DLT pipeline: {pipeline_id}")
        update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
        update_id = update.update_id
        print(f"Update ID: {update_id} — waiting for completion…")

        import time

        for _ in range(120):
            status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
            state = status.update.state.value if status.update and status.update.state else "UNKNOWN"
            if state in ("COMPLETED", "FAILED", "CANCELED"):
                print(f"DLT update finished: {state}")
                if state != "COMPLETED":
                    raise RuntimeError(f"DLT pipeline failed: {status.update}")
                break
            time.sleep(5)
        else:
            print("DLT still running — check pipeline UI. Continuing to MV refresh…")
    except Exception as exc:
        print(f"DLT auto-run skipped/failed: {exc}")
        print("Run manually: databricks bundle run sdp_service_delivery_refresh -t e2_demo --profile e2-demo-field-eng")
else:
    print("Auto-run DLT disabled. Trigger the pipeline manually, then run Step 5.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Refresh dispatch board (materialized view)
# MAGIC
# MAGIC Skipped when `skip_mv_refresh=true` (e.g. `sdp_live_demo` job runs notebook `05` after DLT).

# COMMAND ----------

if not SKIP_MV_REFRESH:
    spark.sql(f"REFRESH MATERIALIZED VIEW {FQ}.mv_incident_dispatch_board")
    print(f"Refreshed {FQ}.mv_incident_dispatch_board")
else:
    print("Skipping MV refresh — downstream job task will refresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — AFTER snapshot (verify the live update)
# MAGIC
# MAGIC Skipped when `skip_after_summary=true` (e.g. final task in `sdp_live_demo` job).

# COMMAND ----------

if not SKIP_AFTER_SUMMARY:
    after_kpis = spark.sql(f"SELECT * FROM {FQ}.metric_sdp_executive_summary").collect()[0]
    after_incidents = spark.sql(
        f"""
        SELECT incident_id, title, severity, incident_status, market, opened_at, ops_notes
        FROM {FQ}.mv_incident_dispatch_board
        WHERE incident_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
        ORDER BY opened_at DESC
        LIMIT 20
        """
    )

    print("=== AFTER KPIs ===")
    print(f"  Open incidents       : {after_kpis['total_open_incidents']}  (was {before_kpis['total_open_incidents']})")
    print(f"  P1 critical          : {after_kpis['open_p1_incidents']}  (was {before_kpis['open_p1_incidents']})")
    print(f"  Orders provisioning  : {after_kpis['orders_in_provisioning']}  (was {before_kpis['orders_in_provisioning']})")
    print(f"  Technicians available: {after_kpis['available_technicians']}  (was {before_kpis['available_technicians']})")

    display(after_incidents)
else:
    print("Skipping AFTER summary — downstream job task will print KPIs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Demo script (what to show next)
# MAGIC
# MAGIC ### Ops Console App
# MAGIC 1. Open https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/
# MAGIC 2. Click **Refresh board** — new incidents appear on the dispatch table
# MAGIC 3. Switch to **Genie Analytics** and ask:
# MAGIC    - *"How many P1 incidents are open in Dallas?"*
# MAGIC    - *"Show incidents opened in the last hour"*
# MAGIC    - *"Give me the current SDP executive summary"*
# MAGIC
# MAGIC ### Optional writeback (Foundry Actions)
# MAGIC 1. In the App, set a new incident to **IN_PROGRESS** → **Update**
# MAGIC 2. Verify in SQL:
# MAGIC    ```sql
# MAGIC    SELECT * FROM users.ankur_nayyar.service_incident_ops
# MAGIC    WHERE incident_id LIKE 'INC-L%'
# MAGIC    ORDER BY updated_at DESC;
# MAGIC    ```

# COMMAND ----------

genie_questions = {
    "new_p1_dallas": [
        f'How many P1 incidents are open in Dallas?',
        f'Describe incident {new_incident_id}',
    ],
    "new_p2_chicago": [
        f'How many open incidents are in Chicago?',
        f'What P2 incidents are open right now?',
    ],
    "escalate_chicago_uverse": [
        'What is the status of INC-9004?',
        'How many P2 incidents are open in Chicago?',
    ],
    "new_provisioning_order": [
        'How many orders are stuck in provisioning?',
        'Which markets have the most pending fiber orders?',
    ],
    "demo_burst": [
        'Give me the current SDP executive summary',
        'How many P1 incidents are open in Dallas?',
        'How many orders are in provisioning?',
    ],
}

print("Suggested Genie questions for this scenario:\n")
for q in genie_questions.get(SCENARIO, []):
    print(f"  • {q}")

print(f"""
---
E2E COMPLETE — scenario: {SCENARIO}
New incident ID (if created): {new_incident_id if generated['incidents'] and SCENARIO != 'escalate_chicago_uverse' else 'n/a'}
Pipeline update ID: {update_id or 'manual'}
""")
