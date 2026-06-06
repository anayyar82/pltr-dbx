# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh SDP materialized views
# MAGIC Run on **serverless compute** (MV is created/refreshed on serverless generic compute).

# COMMAND ----------

dbutils.widgets.dropdown("show_kpi_summary", "false", ["true", "false"], "Print KPI + dispatch board after refresh")
dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SHOW_KPI_SUMMARY = dbutils.widgets.get("show_kpi_summary") == "true"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

spark.sql(f"REFRESH MATERIALIZED VIEW {SCHEMA}.mv_incident_dispatch_board")
print(f"Refreshed {CATALOG}.{SCHEMA}.mv_incident_dispatch_board")

# COMMAND ----------

if SHOW_KPI_SUMMARY:
    kpis = spark.sql(f"SELECT * FROM {SCHEMA}.metric_sdp_executive_summary").collect()[0]
    incidents = spark.sql(
        f"""
        SELECT incident_id, title, severity, incident_status, market, opened_at, ops_notes
        FROM {SCHEMA}.mv_incident_dispatch_board
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
else:
    display(spark.sql(f"DESCRIBE EXTENDED {SCHEMA}.mv_incident_dispatch_board"))
