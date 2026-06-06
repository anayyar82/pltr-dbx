# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Sync gold tables to Lakebase Postgres
# MAGIC
# MAGIC Triggers **TRIGGERED** synced-table pipelines after DLT updates UC gold:
# MAGIC - `service_incident` → `service_incident_pg` (Postgres read replica for low-latency App reads)
# MAGIC - `service_incident_ops` → `service_incident_ops_pg`
# MAGIC
# MAGIC App writeback uses writable Postgres `service_incident_ops` (Lakehouse Sync → UC).

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

SYNCED_TABLES = [
    f"{CATALOG}.{SCHEMA}.service_incident_pg",
    f"{CATALOG}.{SCHEMA}.service_incident_ops_pg",
]

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _trigger_sync(synced_table_id: str) -> str | None:
    path = f"/api/2.0/postgres/synced_tables/{synced_table_id}"
    meta = w.api_client.do("GET", path)
    pipeline_id = (meta.get("status") or {}).get("pipeline_id")
    if not pipeline_id:
        print(f"  SKIP {synced_table_id}: no sync pipeline (create via scripts/setup_lakebase_sync.py)")
        return None
    update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
    print(f"  START {synced_table_id} → pipeline {pipeline_id} update {update.update_id}")
    return pipeline_id, update.update_id


def _wait_pipeline(pipeline_id: str, update_id: str, label: str, timeout_sec: int = 600) -> None:
    for _ in range(timeout_sec // 10):
        status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        state = status.update.state.value if status.update and status.update.state else "UNKNOWN"
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            print(f"  DONE  {label}: {state}")
            if state != "COMPLETED":
                raise RuntimeError(f"Lakebase sync failed for {label}: {state}")
            return
        time.sleep(10)
    print(f"  WARN  {label}: still running after {timeout_sec}s — check DLT pipeline UI")


pending = []
for synced_id in SYNCED_TABLES:
    result = _trigger_sync(synced_id)
    if result:
        pending.append((synced_id, *result))

for synced_id, pipeline_id, update_id in pending:
    _wait_pipeline(pipeline_id, update_id, synced_id)

# COMMAND ----------

incident_count = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.service_incident").collect()[0]["n"]
print(f"\nUC gold service_incident rows: {incident_count}")
print("Lakebase sync complete — Ops Console App can read fresh data from Postgres replicas.")
print("https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/")
