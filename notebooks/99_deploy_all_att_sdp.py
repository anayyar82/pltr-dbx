# Databricks notebook source
# MAGIC %md
# MAGIC # ATT SDP — Full Deploy to `users.ankur_nayyar`
# MAGIC Standalone deploy (creates gold tables directly). **For DLT workflows** use job `sdp_semantic_setup` instead (`01_bootstrap_bronze_seed` → DLT → `10_deploy_semantic_layer`).
# MAGIC
# MAGIC **Workspace:** e2-demo-field-eng  
# MAGIC **Target:** `users.ankur_nayyar`

# COMMAND ----------

CATALOG = "users"
SCHEMA = "ankur_nayyar"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'ATT SDP demo — Palantir migration showcase'")
print(f"Using {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gold ontology tables

# COMMAND ----------

ddl_statements = [
f"""
CREATE TABLE IF NOT EXISTS {FQ}.customer_account (
  account_id STRING NOT NULL,
  account_name STRING NOT NULL,
  market STRING NOT NULL,
  tier STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_customer_account PRIMARY KEY (account_id)
) USING DELTA
COMMENT 'Foundry object: CustomerAccount'
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.field_technician (
  technician_id STRING NOT NULL,
  technician_name STRING NOT NULL,
  market STRING NOT NULL,
  status STRING NOT NULL,
  skill_level STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_field_technician PRIMARY KEY (technician_id)
) USING DELTA
COMMENT 'Foundry object: FieldTechnician'
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.service_incident (
  incident_id STRING NOT NULL,
  customer_account_id STRING NOT NULL,
  market STRING NOT NULL,
  severity STRING NOT NULL,
  status STRING NOT NULL,
  title STRING NOT NULL,
  description STRING,
  service_type STRING,
  opened_at TIMESTAMP NOT NULL,
  resolved_at TIMESTAMP,
  assigned_technician_id STRING,
  ops_notes STRING,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_service_incident PRIMARY KEY (incident_id)
) USING DELTA
COMMENT 'Foundry object: ServiceIncident'
TBLPROPERTIES ('lakebase.sync_enabled' = 'true')
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.service_order (
  order_id STRING NOT NULL,
  customer_account_id STRING NOT NULL,
  service_type STRING NOT NULL,
  status STRING NOT NULL,
  market STRING NOT NULL,
  promised_date DATE,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_service_order PRIMARY KEY (order_id)
) USING DELTA
COMMENT 'Foundry object: ServiceOrder'
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.service_incident_ops (
  incident_id STRING NOT NULL,
  status STRING NOT NULL,
  assigned_technician_id STRING,
  ops_notes STRING,
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING,
  CONSTRAINT pk_service_incident_ops PRIMARY KEY (incident_id)
) USING DELTA
COMMENT 'Lakebase writeback target'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'lakebase.sync_direction' = 'bidirectional')
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.bridge_incident_technician (
  incident_id STRING NOT NULL,
  technician_id STRING NOT NULL,
  assignment_status STRING NOT NULL,
  assigned_at TIMESTAMP NOT NULL,
  assigned_by STRING,
  CONSTRAINT pk_bridge_incident_technician PRIMARY KEY (incident_id, technician_id)
) USING DELTA
COMMENT 'Foundry link: TechnicianAssignedToIncident'
""",
f"""
CREATE TABLE IF NOT EXISTS {FQ}.agent_audit_log (
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
""",
]

for stmt in ddl_statements:
    spark.sql(stmt)
print("Gold tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed demo data

# COMMAND ----------

# MAGIC ## 2. Seed demo data (SQL — Spark Connect safe)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.sdp_exports COMMENT 'SDP bronze export landing'")

seed_sql = [
f"""INSERT OVERWRITE {FQ}.customer_account VALUES
  ('ACC-1001','Metro Fiber LLC','DALLAS','ENTERPRISE',timestamp('2026-06-01 08:00:00')),
  ('ACC-1002','Johnson Residence','DALLAS','RESIDENTIAL',timestamp('2026-06-01 08:00:00')),
  ('ACC-1003','Peachtree Business Park','ATLANTA','BUSINESS',timestamp('2026-06-01 08:00:00')),
  ('ACC-1004','Lakeview Apartments','CHICAGO','RESIDENTIAL',timestamp('2026-06-01 08:00:00')),
  ('ACC-1005','Global Logistics Inc','ATLANTA','ENTERPRISE',timestamp('2026-06-01 08:00:00'))""",
f"""INSERT OVERWRITE {FQ}.field_technician VALUES
  ('TECH-201','Maria Santos','DALLAS','DISPATCHED','L2',timestamp('2026-06-05 07:00:00')),
  ('TECH-202','James Chen','DALLAS','AVAILABLE','L3',timestamp('2026-06-05 06:00:00')),
  ('TECH-203','Priya Patel','DALLAS','AVAILABLE','L2',timestamp('2026-06-05 06:00:00')),
  ('TECH-301','Robert Williams','ATLANTA','DISPATCHED','L3',timestamp('2026-06-05 05:30:00')),
  ('TECH-302','Angela Brooks','ATLANTA','AVAILABLE','L2',timestamp('2026-06-05 06:00:00')),
  ('TECH-401','Kevin O''Brien','CHICAGO','AVAILABLE','L1',timestamp('2026-06-05 06:00:00')),
  ('TECH-402','Sofia Martinez','CHICAGO','AVAILABLE','L2',timestamp('2026-06-05 06:00:00'))""",
f"""INSERT OVERWRITE {FQ}.service_incident VALUES
  ('INC-9001','ACC-1001','DALLAS','P1','OPEN','Fiber backbone outage — Downtown Dallas','Multiple enterprise customers reporting total loss of fiber connectivity.','FIBER',timestamp('2026-06-05 06:12:00'),cast(null as timestamp),cast(null as string),cast(null as string),timestamp('2026-06-05 06:12:00')),
  ('INC-9002','ACC-1002','DALLAS','P3','IN_PROGRESS','Intermittent wireless signal degradation','Residential customer reports dropped calls.','WIRELESS',timestamp('2026-06-04 14:30:00'),cast(null as timestamp),'TECH-201','Tower inspection scheduled',timestamp('2026-06-05 07:00:00')),
  ('INC-9003','ACC-1003','ATLANTA','P2','DISPATCHED','Business park circuit flapping','BGP sessions flapping on primary circuit.','ENTERPRISE',timestamp('2026-06-05 04:45:00'),cast(null as timestamp),'TECH-301','Technician en route',timestamp('2026-06-05 05:30:00')),
  ('INC-9004','ACC-1004','CHICAGO','P4','OPEN','Slow uverse speeds reported','Customer speed test shows 50% of subscribed bandwidth.','UVERSE',timestamp('2026-06-05 08:00:00'),cast(null as timestamp),cast(null as string),cast(null as string),timestamp('2026-06-05 08:00:00')),
  ('INC-9005','ACC-1005','ATLANTA','P1','OPEN','Enterprise VPN gateway failure','VPN concentrator unreachable — 200 users affected.','ENTERPRISE',timestamp('2026-06-05 07:15:00'),cast(null as timestamp),cast(null as string),'Escalated to NOC L3',timestamp('2026-06-05 07:20:00'))""",
f"""INSERT OVERWRITE {FQ}.service_order VALUES
  ('ORD-5001','ACC-1002','FIBER','PROVISIONING','DALLAS',date('2026-06-10'),timestamp('2026-06-03 10:00:00')),
  ('ORD-5002','ACC-1003','ENTERPRISE','ACTIVE','ATLANTA',date('2026-05-20'),timestamp('2026-05-20 16:00:00')),
  ('ORD-5003','ACC-1004','UVERSE','PENDING','CHICAGO',date('2026-06-15'),timestamp('2026-06-04 09:00:00')),
  ('ORD-5004','ACC-1001','FIBER','PROVISIONING','DALLAS',date('2026-06-08'),timestamp('2026-06-05 06:00:00')),
  ('ORD-5005','ACC-1005','ENTERPRISE','PROVISIONING','ATLANTA',date('2026-06-12'),timestamp('2026-06-04 11:00:00'))""",
f"""INSERT OVERWRITE {FQ}.bridge_incident_technician VALUES
  ('INC-9003','TECH-301','CONFIRMED',timestamp('2026-06-05 05:30:00'),'seed_script')""",
f"""INSERT OVERWRITE {FQ}.service_incident_ops
  SELECT incident_id, status, assigned_technician_id, ops_notes, updated_at, 'seed_script'
  FROM {FQ}.service_incident""",
]

for sql in seed_sql:
    spark.sql(sql)
print("Seed data loaded via SQL.")

# Export to volumes for DLT pipeline bronze landing
for entity, table in [
    ("customer_accounts", "customer_account"),
    ("field_technicians", "field_technician"),
    ("service_incidents", "service_incident"),
    ("service_orders", "service_order"),
]:
    path = f"/Volumes/{CATALOG}/{SCHEMA}/sdp_exports/{entity}"
    spark.table(f"{FQ}.{table}").write.format("delta").mode("overwrite").save(path)
    print(f"  volume: {path}")

print("Volume exports ready for DLT.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row tracking + serverless materialized view (incremental refresh)

# COMMAND ----------

# DLT gold tables: row tracking set in sdp_service_delivery.py table_properties
spark.sql(f"ALTER TABLE {FQ}.bridge_incident_technician SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
print("Row tracking enabled on bridge_incident_technician (DLT gold tables via pipeline definition)")

# COMMAND ----------

spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {FQ}.mv_incident_dispatch_board")
spark.sql(f"""
CREATE MATERIALIZED VIEW {FQ}.mv_incident_dispatch_board
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
FROM {FQ}.service_incident i
JOIN {FQ}.customer_account a ON i.customer_account_id = a.account_id
LEFT JOIN {FQ}.service_incident_ops o ON i.incident_id = o.incident_id
LEFT JOIN {FQ}.bridge_incident_technician b
  ON i.incident_id = b.incident_id AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
LEFT JOIN {FQ}.field_technician t ON b.technician_id = t.technician_id
""")
spark.sql(f"REFRESH MATERIALIZED VIEW {FQ}.mv_incident_dispatch_board")
print("Materialized view created with INCREMENTAL refresh policy.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. KPI views (Genie-ready)

# COMMAND ----------

kpi_views = [
f"""
CREATE OR REPLACE VIEW {FQ}.metric_open_incidents_by_market AS
SELECT i.market, i.severity, COUNT(*) AS open_incidents, MIN(i.opened_at) AS oldest_opened_at
FROM {FQ}.service_incident i
LEFT JOIN {FQ}.service_incident_ops o ON i.incident_id = o.incident_id
WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
GROUP BY i.market, i.severity
""",
f"""
CREATE OR REPLACE VIEW {FQ}.metric_incident_mttr AS
SELECT market, service_type,
  AVG(TIMESTAMPDIFF(HOUR, opened_at, COALESCE(resolved_at, current_timestamp()))) AS avg_hours_to_resolve,
  COUNT(*) AS incident_count
FROM {FQ}.service_incident
WHERE opened_at >= date_sub(current_date(), 30)
GROUP BY market, service_type
""",
f"""
CREATE OR REPLACE VIEW {FQ}.metric_order_fulfillment_sla AS
SELECT market, service_type, status, COUNT(*) AS order_count,
  SUM(CASE WHEN promised_date >= current_date() THEN 1 ELSE 0 END) AS on_track_count
FROM {FQ}.service_order
GROUP BY market, service_type, status
""",
f"""
CREATE OR REPLACE VIEW {FQ}.metric_technician_utilization AS
SELECT t.market, t.status, t.skill_level, COUNT(*) AS technician_count,
  COUNT(DISTINCT b.incident_id) AS active_assignments
FROM {FQ}.field_technician t
LEFT JOIN {FQ}.bridge_incident_technician b
  ON t.technician_id = b.technician_id AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
GROUP BY t.market, t.status, t.skill_level
""",
f"""
CREATE OR REPLACE VIEW {FQ}.metric_sdp_executive_summary AS
SELECT
  (SELECT COUNT(*) FROM {FQ}.service_incident i
   LEFT JOIN {FQ}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE COALESCE(o.status, i.status) IN ('OPEN','IN_PROGRESS','DISPATCHED')) AS total_open_incidents,
  (SELECT COUNT(*) FROM {FQ}.service_incident i
   LEFT JOIN {FQ}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE i.severity = 'P1'
     AND COALESCE(o.status, i.status) IN ('OPEN','IN_PROGRESS','DISPATCHED')) AS open_p1_incidents,
  (SELECT COUNT(*) FROM {FQ}.service_order WHERE status = 'PROVISIONING') AS orders_in_provisioning,
  (SELECT COUNT(*) FROM {FQ}.field_technician WHERE status = 'AVAILABLE') AS available_technicians
""",
]

for v in kpi_views:
    spark.sql(v)
print("Views created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify deployment

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {FQ}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {FQ}.metric_sdp_executive_summary"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT incident_id, title, severity, incident_status, market, account_name, technician_name
  FROM {FQ}.mv_incident_dispatch_board
  WHERE incident_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
  ORDER BY CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Next steps (manual in workspace UI)
# MAGIC
# MAGIC | Feature | Action |
# MAGIC |---------|--------|
# MAGIC | **Genie** | AI/BI → Genie → New Space → add tables from `users.ankur_nayyar` |
# MAGIC | **AgentBricks** | AI → Agents → create agent with tools from `config/sdp_agent_tools.yaml` |
# MAGIC | **App** | Deploy `apps/sdp_ops_console/` with `DBX_CATALOG=users`, `DBX_GOLD_SCHEMA=ankur_nayyar` |
# MAGIC | **Lakebase** | Database → Lakebase → sync `service_incident_ops` bidirectionally |
# MAGIC | **DLT pipeline** | `databricks bundle deploy -t e2_demo` after CLI auth |

print(f"""
DEPLOY COMPLETE
===============
Catalog.Schema : {FQ}
Tables         : customer_account, service_incident, service_order, field_technician,
                 bridge_incident_technician, service_incident_ops, agent_audit_log
Views          : mv_incident_dispatch_board, metric_sdp_executive_summary, + 4 KPI views
Open P1s       : INC-9001 (Dallas fiber), INC-9005 (Atlanta VPN)
Genie question : "How many P1 incidents are open in Dallas?"
""")
