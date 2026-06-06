# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bootstrap bronze seed (DLT-safe)
# MAGIC Creates schema + volume, seeds **bronze volumes only**, and non-DLT gold tables.
# MAGIC DLT owns: `customer_account`, `service_incident`, `service_order`, `field_technician`, `service_incident_ops`.

# COMMAND ----------

from datetime import datetime, timezone, date

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'ATT SDP demo — Palantir migration showcase'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.sdp_exports COMMENT 'SDP bronze export landing'")
print(f"Bootstrap target: {FQ}")

# COMMAND ----------

def _ts(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _write_volume(entity: str, df) -> None:
    path = f"/Volumes/{CATALOG}/{SCHEMA}/sdp_exports/{entity}"
    df.write.format("delta").mode("overwrite").save(path)
    print(f"  volume: {path} ({df.count()} rows)")

# COMMAND ----------

accounts_df = spark.createDataFrame(
    [
        ("ACC-1001", "Metro Fiber LLC", "DALLAS", "ENTERPRISE", _ts("2026-06-01 08:00:00")),
        ("ACC-1002", "Johnson Residence", "DALLAS", "RESIDENTIAL", _ts("2026-06-01 08:00:00")),
        ("ACC-1003", "Peachtree Business Park", "ATLANTA", "BUSINESS", _ts("2026-06-01 08:00:00")),
        ("ACC-1004", "Lakeview Apartments", "CHICAGO", "RESIDENTIAL", _ts("2026-06-01 08:00:00")),
        ("ACC-1005", "Global Logistics Inc", "ATLANTA", "ENTERPRISE", _ts("2026-06-01 08:00:00")),
    ],
    schema=StructType([
        StructField("account_id", StringType(), False),
        StructField("account_name", StringType(), False),
        StructField("market", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]),
)

technicians_df = spark.createDataFrame(
    [
        ("TECH-201", "Maria Santos", "DALLAS", "DISPATCHED", "L2", _ts("2026-06-05 07:00:00")),
        ("TECH-202", "James Chen", "DALLAS", "AVAILABLE", "L3", _ts("2026-06-05 06:00:00")),
        ("TECH-203", "Priya Patel", "DALLAS", "AVAILABLE", "L2", _ts("2026-06-05 06:00:00")),
        ("TECH-301", "Robert Williams", "ATLANTA", "DISPATCHED", "L3", _ts("2026-06-05 05:30:00")),
        ("TECH-302", "Angela Brooks", "ATLANTA", "AVAILABLE", "L2", _ts("2026-06-05 06:00:00")),
        ("TECH-401", "Kevin O'Brien", "CHICAGO", "AVAILABLE", "L1", _ts("2026-06-05 06:00:00")),
        ("TECH-402", "Sofia Martinez", "CHICAGO", "AVAILABLE", "L2", _ts("2026-06-05 06:00:00")),
    ],
    schema=StructType([
        StructField("technician_id", StringType(), False),
        StructField("technician_name", StringType(), False),
        StructField("market", StringType(), False),
        StructField("status", StringType(), False),
        StructField("skill_level", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]),
)

incidents_df = spark.createDataFrame(
    [
        ("INC-9001", "ACC-1001", "DALLAS", "P1", "OPEN", "Fiber backbone outage — Downtown Dallas",
         "Multiple enterprise customers reporting total loss of fiber connectivity.", "FIBER",
         _ts("2026-06-05 06:12:00"), None, None, None, _ts("2026-06-05 06:12:00")),
        ("INC-9002", "ACC-1002", "DALLAS", "P3", "IN_PROGRESS", "Intermittent wireless signal degradation",
         "Residential customer reports dropped calls.", "WIRELESS", _ts("2026-06-04 14:30:00"), None,
         "TECH-201", "Tower inspection scheduled", _ts("2026-06-05 07:00:00")),
        ("INC-9003", "ACC-1003", "ATLANTA", "P2", "DISPATCHED", "Business park circuit flapping",
         "BGP sessions flapping on primary circuit.", "ENTERPRISE", _ts("2026-06-05 04:45:00"), None,
         "TECH-301", "Technician en route", _ts("2026-06-05 05:30:00")),
        ("INC-9004", "ACC-1004", "CHICAGO", "P4", "OPEN", "Slow uverse speeds reported",
         "Customer speed test shows 50% of subscribed bandwidth.", "UVERSE", _ts("2026-06-05 08:00:00"),
         None, None, None, _ts("2026-06-05 08:00:00")),
        ("INC-9005", "ACC-1005", "ATLANTA", "P1", "OPEN", "Enterprise VPN gateway failure",
         "VPN concentrator unreachable — 200 users affected.", "ENTERPRISE", _ts("2026-06-05 07:15:00"),
         None, None, "Escalated to NOC L3", _ts("2026-06-05 07:20:00")),
    ],
    schema=StructType([
        StructField("incident_id", StringType(), False),
        StructField("customer_account_id", StringType(), False),
        StructField("market", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("opened_at", TimestampType(), False),
        StructField("resolved_at", TimestampType(), True),
        StructField("assigned_technician_id", StringType(), True),
        StructField("ops_notes", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ]),
)

orders_df = spark.createDataFrame(
    [
        ("ORD-5001", "ACC-1002", "FIBER", "PROVISIONING", "DALLAS", date(2026, 6, 10), _ts("2026-06-03 10:00:00")),
        ("ORD-5002", "ACC-1003", "ENTERPRISE", "ACTIVE", "ATLANTA", date(2026, 5, 20), _ts("2026-05-20 16:00:00")),
        ("ORD-5003", "ACC-1004", "UVERSE", "PENDING", "CHICAGO", date(2026, 6, 15), _ts("2026-06-04 09:00:00")),
        ("ORD-5004", "ACC-1001", "FIBER", "PROVISIONING", "DALLAS", date(2026, 6, 8), _ts("2026-06-05 06:00:00")),
        ("ORD-5005", "ACC-1005", "ENTERPRISE", "PROVISIONING", "ATLANTA", date(2026, 6, 12), _ts("2026-06-04 11:00:00")),
    ],
    schema=StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_account_id", StringType(), False),
        StructField("service_type", StringType(), False),
        StructField("status", StringType(), False),
        StructField("market", StringType(), False),
        StructField("promised_date", DateType(), True),
        StructField("updated_at", TimestampType(), False),
    ]),
)

print("Writing bronze volume exports …")
_write_volume("customer_accounts", accounts_df)
_write_volume("field_technicians", technicians_df)
_write_volume("service_incidents", incidents_df)
_write_volume("service_orders", orders_df)

# COMMAND ----------

# Non-DLT gold tables (links + audit)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.bridge_incident_technician (
  incident_id STRING NOT NULL,
  technician_id STRING NOT NULL,
  assignment_status STRING NOT NULL,
  assigned_at TIMESTAMP NOT NULL,
  assigned_by STRING,
  CONSTRAINT pk_bridge_incident_technician PRIMARY KEY (incident_id, technician_id)
) USING DELTA
COMMENT 'Foundry link: TechnicianAssignedToIncident'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.agent_audit_log (
  audit_id STRING NOT NULL,
  agent_name STRING NOT NULL,
  tool_name STRING NOT NULL,
  user_id STRING,
  query_text STRING,
  result_summary STRING,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_agent_audit_log PRIMARY KEY (audit_id)
) USING DELTA
COMMENT 'AgentBricks audit trail'
""")

spark.sql(f"""
INSERT OVERWRITE {SCHEMA}.bridge_incident_technician VALUES
  ('INC-9003','TECH-301','CONFIRMED',timestamp('2026-06-05 05:30:00'),'seed_script')
""")

spark.sql(f"ALTER TABLE {SCHEMA}.bridge_incident_technician SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
print("Non-DLT gold tables ready.")

# COMMAND ----------

print(f"""
BRONZE BOOTSTRAP COMPLETE — {FQ}
Bronze volumes seeded for DLT autoloader path.
Next: run sdp DLT full refresh, then deploy semantic layer (MV + KPI views).
""")
