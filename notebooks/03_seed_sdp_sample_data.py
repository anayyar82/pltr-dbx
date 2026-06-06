# Databricks notebook source
# MAGIC %md
# MAGIC # Seed ATT SDP sample data
# MAGIC Loads JSON seed files into bronze volumes for the DLT pipeline demo.
# MAGIC Run once before `sdp_service_delivery_refresh`.

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("bronze_schema", "ankur_nayyar")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")

# COMMAND ----------

import json
import pathlib

SEED_ENTITIES = [
    "customer_accounts",
    "service_incidents",
    "service_orders",
    "field_technicians",
]

def find_seed_file(entity: str) -> pathlib.Path:
    candidates = [
        pathlib.Path(f"data/sdp_seed/{entity}.json"),
        pathlib.Path(f"/Workspace/Repos/pltr-dbx/data/sdp_seed/{entity}.json"),
    ]
    return next(p for p in candidates if p.exists())

# COMMAND ----------

for entity in SEED_ENTITIES:
    seed_path = find_seed_file(entity)
    rows = json.loads(seed_path.read_text())
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in rows]))
    volume_path = f"/Volumes/{catalog}/{bronze}/sdp_exports/{entity}"
    print(f"Writing {len(rows)} rows to {volume_path}")
    df.write.format("delta").mode("overwrite").save(volume_path)

# COMMAND ----------

# Seed dispatch bridge for demo (INC-9003 already dispatched to TECH-301)
spark.sql(f"""
  MERGE INTO {catalog}.gold.bridge_incident_technician AS t
  USING (
    SELECT 'INC-9003' AS incident_id, 'TECH-301' AS technician_id,
           'CONFIRMED' AS assignment_status, current_timestamp() AS assigned_at,
           'seed_script' AS assigned_by
  ) AS s
  ON t.incident_id = s.incident_id AND t.technician_id = s.technician_id
  WHEN NOT MATCHED THEN INSERT *
""")

print("Seed data loaded. Run sdp_service_delivery_refresh job next.")
