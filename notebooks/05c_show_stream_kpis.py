# Databricks notebook source
# MAGIC %md
# MAGIC # Show SDP KPIs after stream ingest (read-only, serverless)

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

kpis = spark.sql(f"SELECT * FROM {FQ}.metric_sdp_executive_summary").collect()[0]
incidents = spark.sql(
    f"""
    SELECT incident_id, title, severity, incident_status, market, opened_at, ops_notes
    FROM {FQ}.mv_incident_dispatch_board
    WHERE incident_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
    ORDER BY opened_at DESC
    LIMIT 20
    """
)

print("=== LIVE DEMO KPIs (ready for Ops Console) ===")
print(f"  Open incidents       : {kpis['total_open_incidents']}")
print(f"  P1 critical          : {kpis['open_p1_incidents']}")
print(f"  Orders provisioning  : {kpis['orders_in_provisioning']}")
print(f"  Technicians available: {kpis['available_technicians']}")
print("\nOpen the Ops Console App and click Refresh board to see new data.")
print("https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/")

display(incidents)
