# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Deploy semantic layer (MV + KPI views)
# MAGIC Run **after** DLT has materialized gold tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"USE CATALOG {CATALOG}")

# Writable ops overlay — DLT owns service_incident_ops as a view; App MERGEs here.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.service_incident_ops_writeback (
  incident_id STRING NOT NULL,
  status STRING NOT NULL,
  ops_notes STRING,
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING,
  CONSTRAINT pk_ops_writeback PRIMARY KEY (incident_id)
)
USING DELTA
COMMENT 'App writeback overlay — status overrides visible immediately on dispatch board'
""")
print(f"Writable overlay ready: {FQ}.service_incident_ops_writeback")

# COMMAND ----------

# Lakebase tblproperties are set on the DLT gold table definition (sdp_service_delivery.py).

# COMMAND ----------

spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.mv_incident_dispatch_board")
spark.sql(f"""
CREATE MATERIALIZED VIEW {SCHEMA}.mv_incident_dispatch_board
REFRESH POLICY INCREMENTAL
COMMENT 'Operational dispatch board — serverless incremental refresh'
AS
SELECT
  i.incident_id, i.title, i.severity,
  COALESCE(o.status, i.status) AS incident_status,
  i.market, i.service_type, i.opened_at,
  COALESCE(o.ops_notes, i.ops_notes) AS ops_notes,
  a.account_name, a.tier AS account_tier,
  t.technician_id, t.technician_name, t.status AS technician_status,
  b.assignment_status, b.assigned_at
FROM {SCHEMA}.service_incident i
JOIN {SCHEMA}.customer_account a ON i.customer_account_id = a.account_id
LEFT JOIN {SCHEMA}.service_incident_ops o ON i.incident_id = o.incident_id
LEFT JOIN {SCHEMA}.bridge_incident_technician b
  ON i.incident_id = b.incident_id AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
LEFT JOIN {SCHEMA}.field_technician t ON b.technician_id = t.technician_id
""")
spark.sql(f"REFRESH MATERIALIZED VIEW {SCHEMA}.mv_incident_dispatch_board")
print("Dispatch board MV created and refreshed.")

# COMMAND ----------

kpi_views = [
f"""
CREATE OR REPLACE VIEW {SCHEMA}.metric_open_incidents_by_market AS
SELECT i.market, i.severity, COUNT(*) AS open_incidents, MIN(i.opened_at) AS oldest_opened_at
FROM {SCHEMA}.service_incident i
LEFT JOIN {SCHEMA}.service_incident_ops o ON i.incident_id = o.incident_id
WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
GROUP BY i.market, i.severity
""",
f"""
CREATE OR REPLACE VIEW {SCHEMA}.metric_incident_mttr AS
SELECT market, service_type,
  AVG(TIMESTAMPDIFF(HOUR, opened_at, COALESCE(resolved_at, current_timestamp()))) AS avg_hours_to_resolve,
  COUNT(*) AS incident_count
FROM {SCHEMA}.service_incident
WHERE opened_at >= date_sub(current_date(), 30)
GROUP BY market, service_type
""",
f"""
CREATE OR REPLACE VIEW {SCHEMA}.metric_order_fulfillment_sla AS
SELECT market, service_type, status, COUNT(*) AS order_count,
  SUM(CASE WHEN promised_date >= current_date() THEN 1 ELSE 0 END) AS on_track_count
FROM {SCHEMA}.service_order
GROUP BY market, service_type, status
""",
f"""
CREATE OR REPLACE VIEW {SCHEMA}.metric_technician_utilization AS
SELECT t.market, t.status, t.skill_level, COUNT(*) AS technician_count,
  COUNT(DISTINCT b.incident_id) AS active_assignments
FROM {SCHEMA}.field_technician t
LEFT JOIN {SCHEMA}.bridge_incident_technician b
  ON t.technician_id = b.technician_id AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
GROUP BY t.market, t.status, t.skill_level
""",
f"""
CREATE OR REPLACE VIEW {SCHEMA}.metric_sdp_executive_summary AS
SELECT
  (SELECT COUNT(*) FROM {SCHEMA}.service_incident i
   LEFT JOIN {SCHEMA}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE COALESCE(o.status, i.status) IN ('OPEN','IN_PROGRESS','DISPATCHED')) AS total_open_incidents,
  (SELECT COUNT(*) FROM {SCHEMA}.service_incident i
   LEFT JOIN {SCHEMA}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE i.severity = 'P1'
     AND COALESCE(o.status, i.status) IN ('OPEN','IN_PROGRESS','DISPATCHED')) AS open_p1_incidents,
  (SELECT COUNT(*) FROM {SCHEMA}.service_order WHERE status = 'PROVISIONING') AS orders_in_provisioning,
  (SELECT COUNT(*) FROM {SCHEMA}.field_technician WHERE status = 'AVAILABLE') AS available_technicians
""",
]

for view_sql in kpi_views:
    spark.sql(view_sql)
print("KPI views created.")

# COMMAND ----------

summary = spark.sql(f"SELECT * FROM {SCHEMA}.metric_sdp_executive_summary").collect()[0]
print(f"""
SEMANTIC LAYER COMPLETE — {FQ}
Open incidents : {summary['total_open_incidents']}
Open P1        : {summary['open_p1_incidents']}
Provisioning   : {summary['orders_in_provisioning']}
Techs available: {summary['available_technicians']}
""")
