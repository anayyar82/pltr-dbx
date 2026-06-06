# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy ATT SDP ontology DDL
# MAGIC Objects, links, Lakebase writeback tables, and semantic KPI views.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_sdp_dev")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.dropdown("deploy_semantic", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
deploy_semantic = dbutils.widgets.get("deploy_semantic") == "true"

replacements = {
    "${catalog}": catalog,
    "${bronze_schema}": bronze_schema,
    "${silver_schema}": silver_schema,
    "${gold_schema}": gold_schema,
}

# COMMAND ----------

import pathlib

def run_sql_file(relative_path: str) -> None:
    candidates = [
        pathlib.Path(relative_path),
        pathlib.Path("/Workspace/Repos/pltr-dbx") / relative_path,
    ]
    sql_path = next((p for p in candidates if p.exists()), None)
    if sql_path is None:
        raise FileNotFoundError(f"DDL not found: {relative_path}")
    text = sql_path.read_text()
    for k, v in replacements.items():
        text = text.replace(k, v)
    print(f"Executing {relative_path}...")
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        spark.sql(stmt)

# COMMAND ----------

# Schemas
run_sql_file("src/ontology/ddl/00_schemas.sql")
run_sql_file("src/ontology/ddl/att_sdp_objects.sql")
run_sql_file("src/ontology/ddl/att_sdp_links.sql")
run_sql_file("src/ontology/ddl/att_sdp_lakebase.sql")

# COMMAND ----------

if deploy_semantic:
    run_sql_file("src/semantic/metrics/sdp_kpis.sql")
    print("Semantic KPI views deployed.")
else:
    print("Skipping semantic views (set deploy_semantic=true to deploy).")

print("ATT SDP DDL deployment complete.")
