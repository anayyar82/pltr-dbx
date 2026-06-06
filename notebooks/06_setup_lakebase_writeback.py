# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase writeback setup — `att-ankur-demo`
# MAGIC Project: [att-ankur-demo](https://e2-demo-field-eng.cloud.databricks.com/lakebase/projects/52d0022f-9c22-43bd-80fb-0971d8c46080)
# MAGIC
# MAGIC Serverless-safe: uses psycopg2 (not Spark JDBC) for Postgres DDL/seed.
# MAGIC **Run on an all-purpose cluster** — serverless jobs fail Lakebase SASL auth; use direct host (not pooler).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

PROJECT = "att-ankur-demo"
BRANCH = "production"
ENDPOINT = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/primary"
POSTGRES_DB = "sdp_ops"
POSTGRES_SCHEMA = "ankur_nayyar"
HOST = "ep-hidden-cell-d1mr7kp0.database.us-west-2.cloud.databricks.com"
# App service principal — grant OAuth Postgres role (Apps writeback)
APP_SP_CLIENT_ID = "be33de06-36a1-467e-926b-902c55903267"

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
user = w.current_user.me().user_name
token = w.api_client.do(
    "POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT, "database": POSTGRES_DB}
)["token"]
print(f"Credentials for {user} @ {HOST}/{POSTGRES_DB}")

# COMMAND ----------

import psycopg2

ddl = f"""
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('{APP_SP_CLIENT_ID}', 'service_principal');
GRANT CONNECT ON DATABASE {POSTGRES_DB} TO "{APP_SP_CLIENT_ID}";
GRANT CREATE, USAGE ON SCHEMA {POSTGRES_SCHEMA} TO "{APP_SP_CLIENT_ID}";
CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};
CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.service_incident_ops (
  incident_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  assigned_technician_id TEXT,
  ops_notes TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT
);
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {POSTGRES_SCHEMA}.service_incident_ops TO "{APP_SP_CLIENT_ID}";
ALTER TABLE {POSTGRES_SCHEMA}.service_incident_ops REPLICA IDENTITY FULL;
"""
conn = psycopg2.connect(host=HOST, dbname=POSTGRES_DB, user=user, password=token, sslmode="require")
with conn.cursor() as cur:
    cur.execute(ddl)
    cur.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.service_incident_ops")
    count = cur.fetchone()[0]
conn.commit()
conn.close()
print(f"Writable table ready: {POSTGRES_SCHEMA}.service_incident_ops ({count} rows)")

# COMMAND ----------

if count == 0:
    rows = spark.sql("""
        SELECT incident_id, status, assigned_technician_id, ops_notes, updated_at, updated_by
        FROM users.ankur_nayyar.service_incident_ops
    """).collect()
    if rows:
        cred = w.api_client.do(
            "POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT, "database": POSTGRES_DB}
        )
        conn = psycopg2.connect(
            host=HOST, dbname=POSTGRES_DB, user=user, password=cred["token"], sslmode="require"
        )
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    f"""INSERT INTO {POSTGRES_SCHEMA}.service_incident_ops
                        (incident_id, status, assigned_technician_id, ops_notes, updated_at, updated_by)
                        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        r.incident_id,
                        r.status,
                        r.assigned_technician_id,
                        r.ops_notes,
                        r.updated_at,
                        r.updated_by,
                    ),
                )
        conn.commit()
        conn.close()
        print(f"Seeded {len(rows)} rows from UC into Postgres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Enable Lakehouse Sync
# MAGIC 1. Open your [Lakebase project](https://e2-demo-field-eng.cloud.databricks.com/lakebase/projects/52d0022f-9c22-43bd-80fb-0971d8c46080)
# MAGIC 2. Enable **Lakehouse Sync** on schema `ankur_nayyar`
# MAGIC 3. App writeback → Postgres → UC `users.ankur_nayyar.service_incident_ops`
