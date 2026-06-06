# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Continuous SDP event stream (append-only)
# MAGIC
# MAGIC Appends simulated NOC / Foundry export events to bronze landing volumes.
# MAGIC The **continuous DLT pipeline** (`sdp_service_delivery_dlt`) picks these up automatically.
# MAGIC
# MAGIC **Typical setup**
# MAGIC 1. Deploy bundle (`continuous: true` on DLT)
# MAGIC 2. Seed baseline once (`99_deploy_all_att_sdp` or `03_seed_sdp_sample_data`)
# MAGIC 3. Enable job `sdp_stream_feed` (every 3 min) + `sdp_stream_mv_refresh` (every 5 min)
# MAGIC 4. Open Ops Console → **Refresh board** to see new `INC-S*` rows

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")
dbutils.widgets.text("events_per_run", "1")
dbutils.widgets.dropdown("include_orders", "true", ["true", "false"])

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EVENTS_PER_RUN = max(1, int(dbutils.widgets.get("events_per_run")))
INCLUDE_ORDERS = dbutils.widgets.get("include_orders") == "true"
FQ = f"{CATALOG}.{SCHEMA}"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/sdp_exports"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.sdp_exports COMMENT 'SDP bronze export landing'")

# COMMAND ----------

import random
from datetime import datetime, timezone, date

from pyspark.sql import functions as F

now = datetime.now(timezone.utc)
batch_id = now.strftime("%H%M%S")

MARKETS = ["DALLAS", "CHICAGO", "ATLANTA"]
SEVERITIES = ["P1", "P2", "P3"]
STATUSES = ["OPEN", "IN_PROGRESS", "DISPATCHED"]
SERVICE_TYPES = ["FIBER", "WIRELESS", "UVERSE", "ENTERPRISE"]
ACCOUNTS = ["ACC-1001", "ACC-1002", "ACC-1003", "ACC-1004", "ACC-1005"]
TECHS = ["TECH-201", "TECH-301", "TECH-402"]

TITLES = [
    "Fiber cut reported on backbone span",
    "MPLS latency spike — customer impact",
    "Wireless sector degradation",
    "Enterprise circuit flapping",
    "Uverse speed complaint — escalated",
    "Provisioning delay — fiber install",
]

generated = {"incidents": [], "orders": []}


def _parse_ts(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def append_rows(table: str, volume_entity: str, rows: list[dict]):
    if not rows:
        return 0
    schema = spark.table(f"{FQ}.{table}").schema
    df = spark.createDataFrame(rows, schema=schema)
    path = f"{VOLUME_BASE}/{volume_entity}"
    df.write.format("delta").mode("append").save(path)
    return len(rows)


# Occasionally append a status update for an existing open incident (SCD upsert)
open_incidents = (
    spark.table(f"{FQ}.service_incident")
    .filter(F.col("status").isin("OPEN", "IN_PROGRESS"))
    .select("incident_id", "customer_account_id", "market", "severity", "title", "description",
            "service_type", "opened_at", "assigned_technician_id", "ops_notes")
    .limit(20)
    .collect()
)

for i in range(EVENTS_PER_RUN):
    if open_incidents and random.random() < 0.35:
        src = random.choice(open_incidents)
        new_status = random.choice(["IN_PROGRESS", "DISPATCHED"])
        generated["incidents"].append({
            "incident_id": src["incident_id"],
            "customer_account_id": src["customer_account_id"],
            "market": src["market"],
            "severity": src["severity"],
            "status": new_status,
            "title": src["title"],
            "description": src["description"],
            "service_type": src["service_type"],
            "opened_at": src["opened_at"],
            "resolved_at": None,
            "assigned_technician_id": src["assigned_technician_id"] or random.choice(TECHS),
            "ops_notes": f"Stream update {batch_id}-{i}",
            "updated_at": _parse_ts(now),
        })
    else:
        market = random.choice(MARKETS)
        generated["incidents"].append({
            "incident_id": f"INC-S{batch_id}{i:02d}",
            "customer_account_id": random.choice(ACCOUNTS),
            "market": market,
            "severity": random.choice(SEVERITIES),
            "status": "OPEN",
            "title": f"{random.choice(TITLES)} ({batch_id})",
            "description": "Continuous stream — simulated NOC alarm.",
            "service_type": random.choice(SERVICE_TYPES),
            "opened_at": _parse_ts(now),
            "resolved_at": None,
            "assigned_technician_id": None,
            "ops_notes": f"Stream feed {batch_id}",
            "updated_at": _parse_ts(now),
        })

if INCLUDE_ORDERS and random.random() < 0.5:
    generated["orders"].append({
        "order_id": f"ORD-S{batch_id}",
        "customer_account_id": random.choice(ACCOUNTS),
        "service_type": random.choice(SERVICE_TYPES),
        "status": "PROVISIONING",
        "market": random.choice(MARKETS),
        "promised_date": date(2026, 6, 20),
        "updated_at": _parse_ts(now),
    })

# COMMAND ----------

n_inc = append_rows("service_incident", "service_incidents", generated["incidents"])
n_ord = append_rows("service_order", "service_orders", generated["orders"])

print(f"Appended {n_inc} incident row(s), {n_ord} order row(s) at {now.isoformat()}")
print("Continuous DLT pipeline will merge into gold within ~1–2 minutes.")
print("Refresh dispatch MV (job sdp_stream_mv_refresh) then Ops Console board.")

if generated["incidents"]:
    display(spark.createDataFrame(generated["incidents"]))
