# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Clean up ATT SDP schema (drop all tables, views, MV, bronze volumes)
# MAGIC Run before a from-scratch redeploy. Does **not** delete the schema or Lakebase Postgres project.

# COMMAND ----------

dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "ankur_nayyar")
dbutils.widgets.dropdown("confirm_drop", "yes", ["yes", "no"], "Confirm drop all SDP objects")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
CONFIRM = dbutils.widgets.get("confirm_drop")
FQ = f"{CATALOG}.{SCHEMA}"

if CONFIRM != "yes":
    raise ValueError("Set widget confirm_drop=yes to run cleanup")

# SHOW/DROP IN-schema commands require current catalog context (no catalog.schema).
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
print(f"Cleaning {FQ} …")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Materialized views

# COMMAND ----------

try:
    spark.sql("DROP MATERIALIZED VIEW IF EXISTS mv_incident_dispatch_board")
    print("Dropped mv_incident_dispatch_board")
except Exception as exc:
    print(f"MV drop note: {exc}")

def _drop_relation(name: str) -> None:
    """Drop view, materialized view, or table by trying the appropriate command."""
    for stmt in (
        f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.{name}",
        f"DROP VIEW IF EXISTS {SCHEMA}.{name}",
        f"DROP TABLE IF EXISTS {SCHEMA}.{name}",
    ):
        try:
            spark.sql(stmt)
            print(f"Dropped {name} ({stmt.split()[1]})")
            return
        except Exception as exc:
            msg = str(exc)
            if "DROP_COMMAND_TYPE_MISMATCH" in msg or "Cannot drop" in msg:
                continue
            print(f"  skip {name}: {exc}")
            return
    print(f"  could not drop {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Views (KPI / Genie)

# COMMAND ----------

for row in spark.sql(f"SHOW VIEWS IN {SCHEMA}").collect():
    _drop_relation(row.viewName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Tables (gold, DLT bronze/silver, synced mirrors)

# COMMAND ----------

for pass_num in range(1, 4):
    rows = spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()
    if not rows:
        break
    print(f"Drop pass {pass_num}: {len(rows)} table(s)")
    for row in rows:
        _drop_relation(row.tableName)

remaining = spark.sql(f"SHOW TABLES IN {SCHEMA}").collect()
print(f"Remaining tables: {len(remaining)}")
for row in remaining:
    print(f"  - {row.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Bronze volume landing zone

# COMMAND ----------

volume_base = f"/Volumes/{CATALOG}/{SCHEMA}/sdp_exports"
try:
    dbutils.fs.rm(volume_base, recurse=True)
    print(f"Removed volume path {volume_base}")
except Exception as exc:
    print(f"Volume rm note: {exc}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.sdp_exports COMMENT 'SDP bronze export landing'")
print("Volume sdp_exports ready (empty)")

# COMMAND ----------

print(f"""
CLEANUP COMPLETE — {FQ}
Next: run sdp_semantic_setup, then sdp_full_pipeline
""")
