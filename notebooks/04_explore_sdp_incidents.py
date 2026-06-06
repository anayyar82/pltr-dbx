# Databricks notebook source
# MAGIC %md
# MAGIC # Explore SDP incidents (ex-Quiver)
# MAGIC Ad-hoc analysis for ATT Service Delivery Platform gold tables.

# COMMAND ----------

catalog = "users"
gold = "ankur_nayyar"

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * FROM {catalog}.{gold}.metric_sdp_executive_summary
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT market, severity, open_incidents, oldest_opened_at
        FROM {catalog}.{gold}.metric_open_incidents_by_market
        ORDER BY open_incidents DESC
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {catalog}.{gold}.mv_incident_dispatch_board
        WHERE incident_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
        ORDER BY
          CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
          opened_at ASC
        LIMIT 50
    """)
)
