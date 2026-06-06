#!/usr/bin/env python3
"""Configure Lakebase synced tables and writeback for ATT SDP demo.

Uses project: att-ankur-demo (52d0022f-9c22-43bd-80fb-0971d8c46080)
Docs: https://docs.databricks.com/en/oltp/projects/sync-tables
"""

import json
import subprocess
import sys
import time

PROFILE = "e2-demo-field-eng"
PROJECT_ID = "att-ankur-demo"
BRANCH = f"projects/{PROJECT_ID}/branches/production"
ENDPOINT = f"projects/{PROJECT_ID}/branches/production/endpoints/primary"
POSTGRES_DB = "sdp_ops"
POSTGRES_HOST = "ep-hidden-cell-d1mr7kp0-pooler.database.us-west-2.cloud.databricks.com"
POSTGRES_SCHEMA = "ankur_nayyar"


def api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd.extend(["--json", json.dumps(body)])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def wait_operation(operation: dict, label: str) -> None:
    name = operation.get("name", "")
    if not name:
        return
    for _ in range(30):
        op = api("get", f"/api/2.0/postgres/{name}")
        if op.get("done"):
            if op.get("error"):
                raise RuntimeError(f"{label} failed: {op['error']}")
            print(f"OK  [{label}]")
            return
        time.sleep(5)
    raise RuntimeError(f"{label} timed out")


def create_synced_table(synced_id: str, source: str, pk: list[str], policy: str = "SNAPSHOT") -> None:
    print(f"Creating synced table {synced_id} ({policy})...")
    try:
        body = {
            "spec": {
                "source_table_full_name": source,
                "branch": BRANCH,
                "primary_key_columns": pk,
                "scheduling_policy": policy,
                "postgres_database": POSTGRES_DB,
                "create_database_objects_if_missing": True,
            }
        }
        if policy in ("TRIGGERED", "CONTINUOUS"):
            body["new_pipeline_spec"] = {
                "storage_catalog": "users",
                "storage_schema": "ankur_nayyar",
                "channel": "PREVIEW",
            }
        op = api(
            "post",
            f"/api/2.0/postgres/synced_tables?synced_table_id={synced_id}",
            body,
        )
        wait_operation(op, synced_id)
    except RuntimeError as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            print(f"SKIP [{synced_id}] already exists")
        else:
            raise


def create_writeback_table() -> None:
    """Create writable Postgres table for app writeback (not a synced table)."""
    creds = api("post", "/api/2.0/postgres/credentials", {"endpoint": ENDPOINT, "database": POSTGRES_DB})
    token = creds["token"]
    user = "ankur.nayyar@databricks.com"

    try:
        import psycopg2
    except ImportError:
        print("SKIP [writeback_table] psycopg2 not installed locally — create table via Lakebase SQL editor")
        return

    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};
    CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.service_incident_ops (
      incident_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      assigned_technician_id TEXT,
      ops_notes TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    );
    ALTER TABLE {POSTGRES_SCHEMA}.service_incident_ops REPLICA IDENTITY FULL;
    """
    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=user, password=token, sslmode="require")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    conn.close()
    print("OK  [writeback_table] service_incident_ops in Postgres")


def main():
    synced = [
        ("users.ankur_nayyar.service_incident_pg", "users.ankur_nayyar.service_incident", ["incident_id"], "SNAPSHOT"),
        ("users.ankur_nayyar.service_incident_ops_pg", "users.ankur_nayyar.service_incident_ops", ["incident_id"], "SNAPSHOT"),
    ]
    for synced_id, source, pk, policy in synced:
        create_synced_table(synced_id, source, pk, policy)

    create_writeback_table()

    print(f"""
Lakebase setup complete
=======================
Project : {PROJECT_ID} ({BRANCH})
Postgres: {POSTGRES_HOST}/{POSTGRES_DB}
Synced  : service_incident_pg, service_incident_ops_pg

Writeback: App writes to {POSTGRES_SCHEMA}.service_incident_ops
Enable Lakehouse Sync on schema '{POSTGRES_SCHEMA}' in project UI for UC replication:
  https://e2-demo-field-eng.cloud.databricks.com/lakebase/projects/52d0022f-9c22-43bd-80fb-0971d8c46080
""")


if __name__ == "__main__":
    main()
