#!/usr/bin/env python3
"""Grant Lakebase OAuth role to the Ops Console app service principal."""

from __future__ import annotations

import os
import sys

PROJECT = "att-ankur-demo"
BRANCH = "production"
ENDPOINT = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/primary"
POSTGRES_DB = "sdp_ops"
POSTGRES_SCHEMA = "ankur_nayyar"
HOST = "ep-hidden-cell-d1mr7kp0.database.us-west-2.cloud.databricks.com"
APP_SP_CLIENT_ID = os.getenv("APP_SP_CLIENT_ID", "be33de06-36a1-467e-926b-902c55903267")

SQL_STATEMENTS = [
    "CREATE EXTENSION IF NOT EXISTS databricks_auth",
    f"SELECT databricks_create_role('{APP_SP_CLIENT_ID}', 'service_principal')",
    f'GRANT CONNECT ON DATABASE {POSTGRES_DB} TO "{APP_SP_CLIENT_ID}"',
    f'GRANT CREATE, USAGE ON SCHEMA {POSTGRES_SCHEMA} TO "{APP_SP_CLIENT_ID}"',
    f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.service_incident_ops (
      incident_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      assigned_technician_id TEXT,
      ops_notes TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    )
    """.strip(),
    f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {POSTGRES_SCHEMA}.service_incident_ops TO "{APP_SP_CLIENT_ID}"',
    f"ALTER TABLE {POSTGRES_SCHEMA}.service_incident_ops REPLICA IDENTITY FULL",
]


def main() -> int:
    try:
        import psycopg2
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("Install: pip install psycopg2-binary databricks-sdk", file=sys.stderr)
        return 1

    host = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
    w = WorkspaceClient(host=host)
    user = w.current_user.me().user_name
    token = w.api_client.do(
        "POST",
        "/api/2.0/postgres/credentials",
        body={"endpoint": ENDPOINT, "database": POSTGRES_DB},
    )["token"]

    conn = psycopg2.connect(host=HOST, dbname=POSTGRES_DB, user=user, password=token, sslmode="require")
    with conn.cursor() as cur:
        for stmt in SQL_STATEMENTS:
            print(f"Running: {stmt[:80]}...")
            cur.execute(stmt)
    conn.commit()
    conn.close()
    print(f"OAuth role granted for app SP {APP_SP_CLIENT_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
