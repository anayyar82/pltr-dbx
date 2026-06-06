"""
ATT SDP Ops Console — Databricks App (ex-Foundry Workshop module).

Operational UI for incident dispatch with Lakebase writeback.
Replaces Workshop module: ri.workshop.att.module.sdp-ops-console
"""

from __future__ import annotations

import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from mlflow_tracker import experiment_url, status as mlflow_status, track_event
from viz import build_dashboard, infer_visualization

app = Flask(__name__, template_folder="templates")

CATALOG = os.getenv("DBX_CATALOG", "users")
GOLD = os.getenv("DBX_GOLD_SCHEMA", "ankur_nayyar")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "")
LAKEBASE_POOLER_HOST = os.getenv("LAKEBASE_POOLER_HOST", "")
LAKEBASE_USE_POOLER = os.getenv("LAKEBASE_USE_POOLER", "").lower() in ("1", "true", "yes")
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "sdp_ops")
LAKEBASE_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "ankur_nayyar")
LAKEBASE_PROJECT = os.getenv("LAKEBASE_PROJECT_ID", "att-ankur-demo")
LAKEBASE_BRANCH = os.getenv("LAKEBASE_BRANCH", "production")
LAKEBASE_USER = os.getenv("LAKEBASE_USER", "")
LAKEBASE_ENDPOINT = os.getenv(
    "LAKEBASE_ENDPOINT",
    f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/primary",
)
APP_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "")
GENIE_SPACE_ID = os.getenv("DATABRICKS_GENIE_SPACE_ID") or os.getenv("GENIE_SPACE_ID", "")
SDP_E2E_JOB_NAME = os.getenv("SDP_E2E_JOB_NAME", "sdp_write_refresh")
SDP_SEMANTIC_JOB_NAME = os.getenv("SDP_SEMANTIC_JOB_NAME", "sdp_semantic_setup")
WORKSPACE_ID = os.getenv("DATABRICKS_WORKSPACE_ID", "1444828305810485")
# DLT owns service_incident_ops as a view — app writes to this Delta table instead.
OPS_WRITEBACK_TABLE = os.getenv("DBX_OPS_WRITEBACK_TABLE", "service_incident_ops_writeback")
_writeback_table_ready = False
_latency_demo_table_ready = False
_latency_uc_table_ready = False
OPEN_STATUSES = frozenset({"OPEN", "IN_PROGRESS", "DISPATCHED"})

LIVE_SCENARIOS = [
    {"id": "new_p1_dallas", "label": "New P1 — Dallas fiber cut", "desc": "Critical outage in DALLAS market"},
    {"id": "new_p2_chicago", "label": "New P2 — Chicago", "desc": "Medium severity uverse incident"},
    {"id": "demo_burst", "label": "Demo burst", "desc": "Multiple incidents + order update"},
    {"id": "new_provisioning_order", "label": "New provisioning order", "desc": "Service order in PROVISIONING"},
    {"id": "escalate_chicago_uverse", "label": "Escalate Chicago Uverse", "desc": "Update existing incident severity"},
]

JOB_PIPELINES = {
    "live": {
        "name": SDP_E2E_JOB_NAME,
        "label": "Add live data + refresh",
        "steps": [
            {"key": "write_bronze", "label": "Bronze append", "layer": "bronze"},
            {"key": "run_sdp_dlt", "label": "DLT pipeline", "layer": "gold"},
            {"key": "sync_lakebase", "label": "Lakebase sync", "layer": "lakebase"},
            {"key": "refresh_dispatch_mv", "label": "Semantic MV", "layer": "semantic"},
            {"key": "show_kpis", "label": "KPI verify", "layer": "app"},
        ],
    },
    "semantic": {
        "name": SDP_SEMANTIC_JOB_NAME,
        "label": "Rebuild semantic layer",
        "steps": [
            {"key": "bootstrap_bronze_seed", "label": "Bronze seed", "layer": "bronze"},
            {"key": "run_sdp_dlt_full", "label": "DLT full refresh", "layer": "gold"},
            {"key": "deploy_semantic_layer", "label": "MV + KPI views", "layer": "semantic"},
            {"key": "sync_lakebase", "label": "Lakebase sync", "layer": "lakebase"},
            {"key": "show_kpis", "label": "KPI verify", "layer": "app"},
        ],
    },
}


def _app_host() -> str:
    return os.environ.get("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")


def _workspace_client(use_user_token: bool = False):
    """Databricks Apps: SP creds are injected via env; SDK picks up oauth-m2m automatically."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

    host = _app_host()
    if use_user_token:
        user_token = request.headers.get("X-Forwarded-Access-Token") or request.headers.get(
            "x-forwarded-access-token"
        )
        if user_token:
            return WorkspaceClient(config=Config(host=host, token=user_token, auth_type="pat"))
    return WorkspaceClient(host=host)


def _is_genie_scope_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "invalid scope" in msg or "required scopes: genie" in msg


def _genie_call(w, question: str, conversation_id: str | None):
    if conversation_id:
        message = w.genie.create_message_and_wait(
            space_id=GENIE_SPACE_ID,
            conversation_id=conversation_id,
            content=question,
        )
        return message, conversation_id
    message = w.genie.start_conversation_and_wait(space_id=GENIE_SPACE_ID, content=question)
    return message, message.conversation_id


def _genie_attachment_text(attachment) -> str | None:
    text = getattr(attachment, "text", None)
    if not text:
        return None
    content = getattr(text, "content", None)
    if isinstance(content, list):
        return "".join(content).strip() or None
    return str(content).strip() if content else None


def _genie_attachment_sql(attachment) -> str | None:
    query = getattr(attachment, "query", None)
    if not query:
        return None
    statement = getattr(query, "query", None) or getattr(query, "statement", None)
    if isinstance(statement, list):
        return "".join(statement).strip() or None
    return str(statement).strip() if statement else None


def _attachment_has_query(attachment) -> bool:
    return getattr(attachment, "query", None) is not None or _genie_attachment_sql(attachment) is not None


def _genie_fetch_query_result(w, space_id: str, conversation_id: str, message_id: str, attachment) -> dict:
    attachment_id = getattr(attachment, "attachment_id", None)
    if not attachment_id:
        return {}
    try:
        result = w.genie.get_message_query_result(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id,
        )
    except Exception:
        return {}
    columns = []
    rows = []
    if result.statement_response and result.statement_response.manifest:
        columns = [c.name for c in result.statement_response.manifest.schema.columns]
    if result.statement_response and result.statement_response.result:
        rows = result.statement_response.result.data_array or []
    return {"columns": columns, "rows": rows}


def _genie_collect_datasets(
    w, space_id: str, conversation_id: str, message_id: str, message, question: str,
) -> tuple[list[str], list[str], list[dict]]:
    text_parts: list[str] = []
    sql_parts: list[str] = []
    datasets: list[dict] = []
    seen_sql: set[str] = set()

    for attachment in message.attachments or []:
        att_text = _genie_attachment_text(attachment)
        if att_text:
            text_parts.append(att_text)
        att_sql = _genie_attachment_sql(attachment)
        if att_sql and att_sql not in seen_sql:
            sql_parts.append(att_sql)
            seen_sql.add(att_sql)

        if not (conversation_id and message_id and _attachment_has_query(attachment)):
            continue

        table = _genie_fetch_query_result(w, space_id, conversation_id, message_id, attachment)
        used_fallback = False
        if not table.get("columns") and att_sql:
            table = _execute_sql_as_table(att_sql)
            used_fallback = bool(table.get("columns"))

        if not table.get("columns"):
            continue

        sql_for_ds = att_sql or (sql_parts[-1] if sql_parts else None)
        datasets.append({
            "columns": table["columns"],
            "rows": table["rows"],
            "sql": sql_for_ds,
            "source": "sql_fallback" if used_fallback else "genie_attachment",
        })

    # SQL fallback when Genie returned SQL but no query attachments
    for sql in sql_parts:
        if any(d.get("sql") == sql for d in datasets):
            continue
        table = _execute_sql_as_table(sql)
        if table.get("columns"):
            datasets.append({
                "columns": table["columns"],
                "rows": table["rows"],
                "sql": sql,
                "source": "sql_fallback",
            })

    if not datasets:
        demo_sql = _genie_demo_sql(question)
        if demo_sql:
            table = _execute_sql_as_table(demo_sql)
            if table.get("columns"):
                sql_parts.append(demo_sql)
                datasets.append({
                    "columns": table["columns"],
                    "rows": table["rows"],
                    "sql": demo_sql,
                    "source": "demo_sql",
                })

    return text_parts, sql_parts, datasets


def _genie_demo_sql(question: str) -> str | None:
    """Last-resort SQL for common demo questions when Genie returns text-only."""
    q = question.lower()
    fq = f"{CATALOG}.{GOLD}"
    if "executive summary" in q or "executive kpi" in q:
        return f"SELECT * FROM {fq}.metric_sdp_executive_summary"
    if "across all markets" in q or "by market" in q or "compare open" in q:
        return f"SELECT market, severity, open_incidents FROM {fq}.metric_open_incidents_by_market ORDER BY open_incidents DESC"
    if "severity" in q or "pie" in q:
        return f"SELECT severity, SUM(open_incidents) AS total FROM {fq}.metric_open_incidents_by_market GROUP BY severity"
    if "technician" in q or "utilization" in q:
        return f"SELECT market, skill_level, status, technician_count FROM {fq}.metric_technician_utilization"
    if "mttr" in q or "resolve" in q:
        return f"SELECT market, service_type, avg_hours_to_resolve, incident_count FROM {fq}.metric_incident_mttr"
    if "provisioning" in q or "orders" in q:
        return f"SELECT market, service_type, order_count FROM {fq}.metric_order_fulfillment_sla WHERE status = 'PROVISIONING'"
    if "p1" in q and "dallas" in q:
        return f"SELECT COUNT(*) AS open_p1_dallas FROM {fq}.service_incident WHERE severity = 'P1' AND market = 'DALLAS' AND status IN ('OPEN','IN_PROGRESS','DISPATCHED')"
    if "dispatch" in q or "list" in q:
        return f"SELECT incident_id, title, severity, incident_status, market, technician_name FROM {fq}.mv_incident_dispatch_board WHERE incident_status IN ('OPEN','IN_PROGRESS','DISPATCHED') LIMIT 15"
    return None


def _genie_parse_message(w, space_id: str, message, question: str = "") -> dict:
    conversation_id = getattr(message, "conversation_id", None)
    message_id = getattr(message, "message_id", None) or getattr(message, "id", None)
    text_parts, sql_parts, datasets = _genie_collect_datasets(
        w, space_id, conversation_id, message_id, message, question,
    )

    for ds in datasets:
        ds["visualization"] = infer_visualization(question, ds["columns"], ds["rows"])

    first = datasets[0] if datasets else {}
    dashboard = build_dashboard(question, datasets)

    status = getattr(message, "status", None)
    if hasattr(status, "value"):
        status = status.value
    error = getattr(message, "error", None)
    error_msg = getattr(error, "message", None) if error else None

    return {
        "conversation_id": conversation_id,
        "message_id": message_id,
        "status": status,
        "text": "\n\n".join(text_parts) if text_parts else None,
        "sql": "\n\n".join(sql_parts) if sql_parts else None,
        "columns": first.get("columns", []),
        "rows": first.get("rows", []),
        "datasets": datasets,
        "dashboard": dashboard,
        "error": error_msg,
    }


def _genie_ask(question: str, conversation_id: str | None = None) -> dict:
    if not GENIE_SPACE_ID:
        return {"error": "Genie space not configured"}
    last_error = None
    t0 = time.time()
    for use_user_token in (True, False):
        try:
            w = _workspace_client(use_user_token=use_user_token)
            message, conv_id = _genie_call(w, question, conversation_id)
            parsed = _genie_parse_message(w, GENIE_SPACE_ID, message, question)
            parsed["conversation_id"] = parsed.get("conversation_id") or conv_id
            elapsed_ms = (time.time() - t0) * 1000
            parsed["latency_ms"] = round(elapsed_ms, 1)
            row_count = sum(len(d.get("rows") or []) for d in parsed.get("datasets") or [])
            run_id = track_event(
                "genie_ask",
                params={
                    "question": question[:500],
                    "conversation_id": parsed.get("conversation_id"),
                    "sql": (parsed.get("sql") or "")[:500],
                    "dashboard_layout": (parsed.get("dashboard") or {}).get("layout"),
                },
                metrics={
                    "latency_ms": elapsed_ms,
                    "row_count": row_count,
                    "dataset_count": len(parsed.get("datasets") or []),
                },
                tags={"status": str(parsed.get("status") or "ok")},
            )
            parsed["mlflow_run_id"] = run_id
            parsed["mlflow_experiment_url"] = experiment_url()
            return parsed
        except Exception as exc:
            last_error = exc
            if use_user_token and _is_genie_scope_error(exc):
                continue
            break
    track_event(
        "genie_ask_error",
        params={"question": question[:500]},
        tags={"error": str(last_error)[:250] if last_error else "unknown"},
    )
    return {"error": str(last_error) if last_error else "Genie request failed"}

def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _ops_writeback_fq() -> str:
    return f"{CATALOG}.{GOLD}.{OPS_WRITEBACK_TABLE}"


def _incident_joins(alias: str = "i") -> str:
    """Standard joins + effective status expression for incident queries."""
    return f"""
        LEFT JOIN {_ops_writeback_fq()} w ON {alias}.incident_id = w.incident_id
        LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON {alias}.incident_id = o.incident_id
    """


def _effective_status(alias: str = "i") -> str:
    return f"COALESCE(w.status, o.status, {alias}.status)"


def _lakebase_enabled() -> bool:
    return bool(LAKEBASE_HOST)


def _lakebase_pg_hosts() -> list[str]:
    """Direct endpoint first — pooler often fails SASL for Apps SP OAuth (use pooler only if explicit)."""
    hosts: list[str] = []
    if LAKEBASE_HOST:
        hosts.append(LAKEBASE_HOST)
    if LAKEBASE_USE_POOLER and LAKEBASE_POOLER_HOST and LAKEBASE_POOLER_HOST not in hosts:
        hosts.append(LAKEBASE_POOLER_HOST)
    return hosts


def _lakebase_ops_table() -> str:
    return f"{LAKEBASE_SCHEMA}.service_incident_ops"


def _lakebase_latency_table() -> str:
    return f"{LAKEBASE_SCHEMA}.lakebase_latency_demo"


def _latency_uc_table() -> str:
    return f"{CATALOG}.{GOLD}.lakebase_latency_demo"


def _normalize_path_choice(value: str | None, default: str = "lakebase") -> str:
    v = (value or default).strip().lower()
    return v if v in ("lakebase", "warehouse") else default


def _lakebase_connect():
    import psycopg2

    user, password = _lakebase_credentials()
    if not user or not password:
        raise RuntimeError("Lakebase OAuth credential unavailable")

    last_exc: Exception | None = None
    for host in _lakebase_pg_hosts():
        try:
            return psycopg2.connect(
                host=host,
                dbname=LAKEBASE_DB,
                user=user,
                password=password,
                sslmode="require",
                connect_timeout=15,
            )
        except Exception as exc:
            last_exc = exc
            app.logger.warning("Lakebase connect failed on %s: %s", host, exc)
    raise RuntimeError(str(last_exc) if last_exc else "No Lakebase host configured")


def _lakebase_query(sql: str, params: tuple | None = None) -> tuple[list[dict], str | None]:
    """Run SQL on Lakebase Postgres (low-latency OLTP path)."""
    rows, err, _ = _lakebase_query_timed(sql, params)
    return rows, err


def _lakebase_query_timed(sql: str, params: tuple | None = None) -> tuple[list[dict], str | None, float]:
    """Run SQL on Lakebase Postgres; returns (rows, error, latency_ms)."""
    if not _lakebase_enabled():
        return [], "Lakebase not configured", 0.0
    t0 = time.perf_counter()
    try:
        from psycopg2.extras import RealDictCursor

        conn = _lakebase_connect()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                if not cur.description:
                    conn.commit()
                    return [], None, round((time.perf_counter() - t0) * 1000, 1)
                rows = [dict(r) for r in cur.fetchall()]
                for row in rows:
                    for key, val in row.items():
                        if isinstance(val, datetime):
                            row[key] = val.isoformat()
                conn.commit()
                return rows, None, round((time.perf_counter() - t0) * 1000, 1)
        finally:
            conn.close()
    except ImportError:
        return [], "psycopg2 not installed", round((time.perf_counter() - t0) * 1000, 1)
    except Exception as exc:
        return [], str(exc), round((time.perf_counter() - t0) * 1000, 1)


def _lakebase_ops_map_from_uc_mirror() -> dict[str, dict]:
    """UC foreign table synced from Lakebase — fallback when direct Postgres auth fails."""
    rows = _execute_sql(
        f"""
        SELECT incident_id, status, ops_notes, updated_at, updated_by
        FROM {CATALOG}.{GOLD}.service_incident_ops_pg
        """,
    )
    return {str(r["incident_id"]): r for r in rows}


def _lakebase_ops_map() -> tuple[dict[str, dict], str | None, str]:
    rows, err = _lakebase_query(
        f"""
        SELECT incident_id, status, ops_notes, updated_at, updated_by
        FROM {_lakebase_ops_table()}
        """,
    )
    if not err:
        return {str(r["incident_id"]): r for r in rows}, None, "lakebase_postgres"
    mirror = _lakebase_ops_map_from_uc_mirror()
    if mirror:
        app.logger.warning("Lakebase direct read failed (%s); using UC mirror", err)
        return mirror, None, "uc_lakebase_mirror"
    return {}, err, "uc_warehouse"


def _ops_status(incident_id: str, base_status: str | None, ops_map: dict[str, dict]) -> str:
    row = ops_map.get(incident_id)
    if row and row.get("status"):
        return str(row["status"]).upper()
    return (base_status or "OPEN").upper()


def _ops_notes(incident_id: str, base_notes, ops_map: dict[str, dict]):
    row = ops_map.get(incident_id)
    if row and row.get("ops_notes") is not None:
        return row["ops_notes"]
    return base_notes


def _lakebase_ops_overlay() -> tuple[dict[str, dict] | None, str | None, str]:
    """Ops overlay from Lakebase; falls back to UC mirror then warehouse joins."""
    ops_map, err, path, _ = _lakebase_ops_overlay_timed("lakebase")
    return ops_map, err, path


def _warehouse_ops_map_timed() -> tuple[dict[str, dict], str | None, float]:
    """Ops overlay via SQL warehouse (UC mirror of Lakebase ops table)."""
    t0 = time.perf_counter()
    rows, err = _run_statement(
        f"""
        SELECT incident_id, status, ops_notes, updated_at, updated_by
        FROM {CATALOG}.{GOLD}.service_incident_ops_pg
        """,
    )
    ms = round((time.perf_counter() - t0) * 1000, 1)
    if err:
        return {}, err, ms
    ops_map: dict[str, dict] = {}
    for row in rows:
        ops_map[str(row["incident_id"])] = row
    return ops_map, None, ms


def _lakebase_ops_overlay_timed(
    read_path: str = "lakebase",
) -> tuple[dict[str, dict] | None, str | None, str, float]:
    """Ops overlay with explicit read path and latency (ms)."""
    read_path = _normalize_path_choice(read_path)
    if read_path == "warehouse":
        ops_map, err, ms = _warehouse_ops_map_timed()
        if err:
            return None, err, "uc_warehouse", ms
        return ops_map, None, "uc_warehouse", ms

    if not _lakebase_enabled():
        ops_map, err, ms = _warehouse_ops_map_timed()
        return (ops_map if not err else None), err, "uc_warehouse", ms

    t0 = time.perf_counter()
    rows, err, lb_ms = _lakebase_query_timed(
        f"""
        SELECT incident_id, status, ops_notes, updated_at, updated_by
        FROM {_lakebase_ops_table()}
        """,
    )
    if not err:
        return {str(r["incident_id"]): r for r in rows}, None, "lakebase_postgres", lb_ms

    mirror = _lakebase_ops_map_from_uc_mirror()
    ms = round((time.perf_counter() - t0) * 1000, 1)
    if mirror:
        app.logger.warning("Lakebase direct read failed (%s); using UC mirror", err)
        return mirror, None, "uc_lakebase_mirror", ms
    return None, err, "uc_warehouse", ms


def _ensure_lakebase_latency_demo_table() -> str | None:
    global _latency_demo_table_ready
    if _latency_demo_table_ready or not _lakebase_enabled():
        return None
    _, err = _lakebase_query(
        f"""
        CREATE TABLE IF NOT EXISTS {_lakebase_latency_table()} (
            run_id TEXT PRIMARY KEY,
            operation TEXT NOT NULL,
            path TEXT NOT NULL,
            latency_ms INTEGER NOT NULL,
            detail TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
    )
    if not err:
        _latency_demo_table_ready = True
    return err


def _ensure_uc_latency_demo_table() -> str | None:
    global _latency_uc_table_ready
    if _latency_uc_table_ready:
        return None
    err = _execute_sql_mutation(
        f"""
        CREATE TABLE IF NOT EXISTS {_latency_uc_table()} (
          run_id STRING NOT NULL,
          operation STRING NOT NULL,
          path STRING NOT NULL,
          latency_ms INT NOT NULL,
          detail STRING,
          created_at TIMESTAMP NOT NULL,
          CONSTRAINT pk_latency_demo PRIMARY KEY (run_id)
        )
        USING DELTA
        COMMENT 'Warehouse latency demo — compare vs Lakebase Postgres inserts'
        """,
    )
    if not err:
        _latency_uc_table_ready = True
    return err


def _latency_demo_history(limit: int = 25) -> list[dict]:
    limit = max(1, min(limit, 100))
    items: list[dict] = []

    if _lakebase_enabled():
        _ensure_lakebase_latency_demo_table()
        rows, err, _ = _lakebase_query_timed(
            f"""
            SELECT run_id, operation, path, latency_ms, detail, created_at
            FROM {_lakebase_latency_table()}
            ORDER BY created_at DESC
            LIMIT {limit}
            """,
        )
        if not err:
            items.extend(rows)

    _ensure_uc_latency_demo_table()
    wh_rows = _execute_sql(
        f"""
        SELECT run_id, operation, path, latency_ms, detail, cast(created_at as string) AS created_at
        FROM {_latency_uc_table()}
        ORDER BY created_at DESC
        LIMIT {limit}
        """,
    )
    items.extend(wh_rows or [])

    items.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    return items[:limit]


def _kpi_open_counts(ops_map: dict[str, dict]) -> dict[str, int]:
    incidents = _execute_sql(
        f"SELECT incident_id, severity, status FROM {CATALOG}.{GOLD}.service_incident",
    )
    open_rows = [
        i for i in incidents
        if _ops_status(i["incident_id"], i["status"], ops_map) in OPEN_STATUSES
    ]
    return {
        "total_open_incidents": len(open_rows),
        "open_p1_incidents": sum(1 for i in open_rows if i.get("severity") == "P1"),
    }


def _run_statement(stmt: str) -> tuple[list[dict], str | None]:
    """Execute SQL; returns (rows, error_message)."""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        return [], "DATABRICKS_WAREHOUSE_ID not set"
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        w = _workspace_client()
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            wait_timeout="50s",
        )
        while resp.status and resp.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            resp = w.statement_execution.get_statement(resp.statement_id)
        if not resp.status or resp.status.state != StatementState.SUCCEEDED:
            err = getattr(resp.status, "error", None)
            return [], str(err.message if err else resp.status.state)
        if not resp.result or not resp.result.data_array:
            return [], None
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in resp.result.data_array], None
    except Exception as exc:
        return [], str(exc)


def _execute_sql(query: str, params: dict | None = None) -> list[dict]:
    """Run SQL via Statement Execution API (OAuth in Databricks Apps)."""
    stmt = query
    if params:
        for key, value in params.items():
            stmt = stmt.replace(f":{key}", _sql_literal(value))
    rows, _ = _run_statement(stmt)
    return rows


def _execute_sql_as_table(sql: str) -> dict:
    """Run Genie SQL via warehouse when Genie attachment results are empty."""
    rows_dict, err = _run_statement(sql.strip())
    if err or not rows_dict:
        return {"error": err}
    cols = list(rows_dict[0].keys())
    rows = [[r.get(c) for c in cols] for r in rows_dict]
    return {"columns": cols, "rows": rows}


def _execute_sql_mutation(query: str) -> str | None:
    """Run DML; returns None on success or an error message."""
    _, err = _run_statement(query)
    return err


def _ensure_ops_writeback_table() -> str | None:
    global _writeback_table_ready
    if _writeback_table_ready:
        return None
    err = _execute_sql_mutation(
        f"""
        CREATE TABLE IF NOT EXISTS {_ops_writeback_fq()} (
          incident_id STRING NOT NULL,
          status STRING NOT NULL,
          ops_notes STRING,
          updated_at TIMESTAMP NOT NULL,
          updated_by STRING,
          CONSTRAINT pk_ops_writeback PRIMARY KEY (incident_id)
        )
        USING DELTA
        COMMENT 'App writeback overlay — writable status overrides (DLT service_incident_ops is a view)'
        """,
    )
    if not err:
        _writeback_table_ready = True
    return err


def _sql_probe() -> tuple[list[dict], str | None]:
    """Health-check SQL; returns rows and optional error message."""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        return [], "DATABRICKS_WAREHOUSE_ID not set"
    stmt = f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD}.service_incident"
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        w = _workspace_client()
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=stmt,
            wait_timeout="50s",
        )
        while resp.status and resp.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            resp = w.statement_execution.get_statement(resp.statement_id)
        if not resp.status or resp.status.state != StatementState.SUCCEEDED:
            err = getattr(resp.status, "error", None)
            return [], str(err.message if err else resp.status.state)
        if not resp.result or not resp.result.data_array:
            return [], "empty result"
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = [dict(zip(cols, row)) for row in resp.result.data_array]
        return rows, None
    except Exception as exc:
        return [], str(exc)


def _find_job_id(name_fragment: str) -> int | None:
    try:
        w = _workspace_client()
        exact = None
        partial = None
        for job in w.jobs.list():
            name = job.settings.name if job.settings else None
            if not name or name_fragment not in name:
                continue
            if name.endswith(name_fragment) or name.endswith(f"] {name_fragment}"):
                exact = job.job_id
                break
            partial = job.job_id
        return exact or partial
    except Exception:
        pass
    return None


def _live_stats() -> dict:
    ops_map, ops_err, ops_path = _lakebase_ops_overlay()

    if ops_map is not None:
        counts = _kpi_open_counts(ops_map)
        aux = _execute_sql(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
               WHERE status = 'PROVISIONING') AS orders_in_provisioning,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
               WHERE status = 'AVAILABLE') AS available_technicians,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident
               WHERE incident_id LIKE 'INC-L%' OR incident_id LIKE 'INC-S%') AS stream_incidents,
              (SELECT MAX(updated_at) FROM {CATALOG}.{GOLD}.service_incident) AS last_gold_update
            """,
        )
        base = {**counts, **(aux[0] if aux else {})}
        incidents = _execute_sql(
            f"SELECT incident_id, market, status FROM {CATALOG}.{GOLD}.service_incident",
        )
        market_counts: dict[str, int] = {}
        for i in incidents:
            if _ops_status(i["incident_id"], i["status"], ops_map) in OPEN_STATUSES:
                m = i.get("market") or "UNKNOWN"
                market_counts[m] = market_counts.get(m, 0) + 1
        markets = [
            {"market": m, "open_count": c}
            for m, c in sorted(market_counts.items(), key=lambda x: -x[1])
        ]
        recent = _execute_sql(
            f"""
            SELECT incident_id, title, severity, market, status, opened_at, updated_at
            FROM {CATALOG}.{GOLD}.service_incident
            WHERE incident_id LIKE 'INC-L%' OR incident_id LIKE 'INC-S%'
            ORDER BY opened_at DESC
            LIMIT 15
            """,
        )
        for row in recent:
            row["status"] = _ops_status(row["incident_id"], row["status"], ops_map)
    else:
        rows = _execute_sql(
            f"""
            WITH open_incidents AS (
              SELECT i.incident_id, i.severity, i.market, i.status, i.opened_at, i.updated_at,
                     COALESCE(w.status, o.status, i.status) AS effective_status
              FROM {CATALOG}.{GOLD}.service_incident i
              LEFT JOIN {_ops_writeback_fq()} w ON i.incident_id = w.incident_id
              LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
            )
            SELECT
              (SELECT COUNT(*) FROM open_incidents
               WHERE effective_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')) AS total_open_incidents,
              (SELECT COUNT(*) FROM open_incidents
               WHERE severity = 'P1'
                 AND effective_status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')) AS open_p1_incidents,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
               WHERE status = 'PROVISIONING') AS orders_in_provisioning,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
               WHERE status = 'AVAILABLE') AS available_technicians,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident
               WHERE incident_id LIKE 'INC-L%' OR incident_id LIKE 'INC-S%') AS stream_incidents,
              (SELECT MAX(updated_at) FROM {CATALOG}.{GOLD}.service_incident) AS last_gold_update
            """,
        )
        base = rows[0] if rows else {}
        markets = _execute_sql(
            f"""
            SELECT i.market, COUNT(*) AS open_count
            FROM {CATALOG}.{GOLD}.service_incident i
            LEFT JOIN {_ops_writeback_fq()} w ON i.incident_id = w.incident_id
            LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
            WHERE COALESCE(w.status, o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
            GROUP BY i.market
            ORDER BY open_count DESC
            """,
        )
        recent = _execute_sql(
            f"""
            SELECT i.incident_id, i.title, i.severity, i.market,
                   COALESCE(w.status, o.status, i.status) AS status,
                   i.opened_at, i.updated_at
            FROM {CATALOG}.{GOLD}.service_incident i
            LEFT JOIN {_ops_writeback_fq()} w ON i.incident_id = w.incident_id
            LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
            WHERE i.incident_id LIKE 'INC-L%' OR i.incident_id LIKE 'INC-S%'
            ORDER BY opened_at DESC
            LIMIT 15
            """,
        )

    return {
        **base,
        "markets": markets,
        "recent_stream_incidents": recent,
        "polled_at": datetime.now(timezone.utc).isoformat(),
        "ops_read_path": ops_path,
    }


def _job_url(job_id: int, run_id: int | None = None) -> str:
    host = _app_host().rstrip("/")
    if run_id:
        return f"{host}/jobs/{job_id}/runs/{run_id}?o={WORKSPACE_ID}"
    return f"{host}/jobs/{job_id}?o={WORKSPACE_ID}"


def _lakebase_snapshot() -> dict:
    """Ops rows from Lakebase (direct Postgres, or UC mirror on auth failure)."""
    gold_count = _execute_sql(
        f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD}.service_incident",
    )
    hosts = _lakebase_pg_hosts()
    base = {
        "lakebase_project": LAKEBASE_PROJECT,
        "postgres_host": hosts[0] if hosts else None,
        "polled_at": datetime.now(timezone.utc).isoformat(),
        "gold_incident_count": gold_count[0]["n"] if gold_count else 0,
    }

    if not _lakebase_enabled():
        pg_rows = _execute_sql(
            f"""
            SELECT incident_id, status, ops_notes, updated_at, updated_by
            FROM {CATALOG}.{GOLD}.service_incident_ops_pg
            ORDER BY updated_at DESC LIMIT 12
            """,
        )
        pg_count = _execute_sql(
            f"SELECT COUNT(*) AS n FROM {CATALOG}.{GOLD}.service_incident_ops_pg",
        )
        return {
            **base,
            "read_path": "uc_foreign_table",
            "pg_ops_rows": pg_rows,
            "writeback_rows": pg_rows,
            "lakebase_ops_count": pg_count[0]["n"] if pg_count else 0,
        }

    ops_map, err, path = _lakebase_ops_map()
    if err:
        return {**base, "read_path": "lakebase_error", "read_error": err, "pg_ops_rows": [], "lakebase_ops_count": 0}

    rows = sorted(
        ops_map.values(),
        key=lambda r: str(r.get("updated_at") or ""),
        reverse=True,
    )[:12]
    return {
        **base,
        "read_path": path,
        "pg_ops_rows": rows,
        "writeback_rows": rows,
        "lakebase_ops_count": len(ops_map),
    }


def _dashboard_data() -> dict:
    """Aggregated insights for executive dashboard."""
    ops_map, ops_err, ops_path = _lakebase_ops_overlay()

    if ops_map is not None:
        counts = _kpi_open_counts(ops_map)
        aux = _execute_sql(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
               WHERE status = 'PROVISIONING') AS orders_in_provisioning,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
               WHERE status = 'AVAILABLE') AS available_technicians,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident
               WHERE incident_id LIKE 'INC-L%' OR incident_id LIKE 'INC-S%') AS stream_incidents
            """,
        )
        kpis = {**counts, **(aux[0] if aux else {})}
        incidents = _execute_sql(
            f"""
            SELECT incident_id, title, severity, market, status, opened_at
            FROM {CATALOG}.{GOLD}.service_incident
            """,
        )
        open_inc = [
            i for i in incidents
            if _ops_status(i["incident_id"], i["status"], ops_map) in OPEN_STATUSES
        ]
        market_agg: dict[str, dict] = {}
        sev_agg: dict[str, int] = {}
        for i in open_inc:
            m = i.get("market") or "UNKNOWN"
            market_agg.setdefault(m, {"market": m, "open_count": 0, "p1_count": 0})
            market_agg[m]["open_count"] += 1
            if i.get("severity") == "P1":
                market_agg[m]["p1_count"] += 1
            s = i.get("severity") or "UNKNOWN"
            sev_agg[s] = sev_agg.get(s, 0) + 1
        markets = sorted(market_agg.values(), key=lambda x: -x["open_count"])
        severity = sorted(
            [{"severity": s, "incident_count": c} for s, c in sev_agg.items()],
            key=lambda x: {"P1": 1, "P2": 2, "P3": 3}.get(x["severity"], 4),
        )
        recent = sorted(open_inc, key=lambda x: x.get("opened_at") or "", reverse=True)[:10]
        for row in recent:
            row["incident_status"] = _ops_status(row["incident_id"], row["status"], ops_map)
    else:
        joins = _incident_joins("i")
        eff = _effective_status("i")
        open_filter = f"{eff} IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')"

        kpis_rows = _execute_sql(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident i {joins}
               WHERE {open_filter}) AS total_open_incidents,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident i {joins}
               WHERE i.severity = 'P1' AND {open_filter}) AS open_p1_incidents,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
               WHERE status = 'PROVISIONING') AS orders_in_provisioning,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
               WHERE status = 'AVAILABLE') AS available_technicians,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident
               WHERE incident_id LIKE 'INC-L%' OR incident_id LIKE 'INC-S%') AS stream_incidents
            """,
        )
        kpis = kpis_rows[0] if kpis_rows else {}
        markets = _execute_sql(
            f"""
            SELECT i.market,
                   COUNT(*) AS open_count,
                   SUM(CASE WHEN i.severity = 'P1' THEN 1 ELSE 0 END) AS p1_count
            FROM {CATALOG}.{GOLD}.service_incident i {joins}
            WHERE {open_filter}
            GROUP BY i.market
            ORDER BY open_count DESC
            """,
        )
        severity = _execute_sql(
            f"""
            SELECT i.severity, COUNT(*) AS incident_count
            FROM {CATALOG}.{GOLD}.service_incident i {joins}
            WHERE {open_filter}
            GROUP BY i.severity
            ORDER BY CASE i.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END
            """,
        )
        recent = _execute_sql(
            f"""
            SELECT i.incident_id, i.title, i.severity, i.market,
                   {eff} AS incident_status, i.opened_at
            FROM {CATALOG}.{GOLD}.service_incident i {joins}
            WHERE {open_filter}
            ORDER BY i.opened_at DESC
            LIMIT 10
            """,
        )

    orders = _execute_sql(
        f"""
        SELECT market, COUNT(*) AS order_count
        FROM {CATALOG}.{GOLD}.service_order
        WHERE status = 'PROVISIONING'
        GROUP BY market
        ORDER BY order_count DESC
        """,
    )
    technicians = _execute_sql(
        f"""
        SELECT market,
               SUM(CASE WHEN status = 'AVAILABLE' THEN 1 ELSE 0 END) AS available_count,
               SUM(CASE WHEN status != 'AVAILABLE' THEN 1 ELSE 0 END) AS busy_count
        FROM {CATALOG}.{GOLD}.field_technician
        GROUP BY market
        ORDER BY available_count DESC
        """,
    )
    return {
        "kpis": kpis,
        "markets": markets,
        "severity": severity,
        "recent_incidents": recent,
        "orders": orders,
        "technicians": technicians,
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "ops_read_path": ops_path,
    }


def _analytics_data() -> dict:
    """Chart-ready datasets for the Analytics Dashboard tab (AI/BI-style visualizations)."""
    dash = _dashboard_data()
    kpis = dash.get("kpis") or {}

    market_severity = _execute_sql(
        f"""
        SELECT market, severity, open_incidents
        FROM {CATALOG}.{GOLD}.metric_open_incidents_by_market
        ORDER BY market, CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END
        """,
    )
    markets_sorted = sorted({r["market"] for r in market_severity if r.get("market")})
    severities = ["P1", "P2", "P3", "P4"]
    ms_matrix: dict[str, dict[str, int]] = {m: {s: 0 for s in severities} for m in markets_sorted}
    for row in market_severity:
        m, s = row.get("market"), row.get("severity")
        if m in ms_matrix and s in ms_matrix[m]:
            ms_matrix[m][s] = int(row.get("open_incidents") or 0)

    mttr = _execute_sql(
        f"""
        SELECT market, ROUND(avg_hours_to_resolve, 1) AS avg_hours, incident_count
        FROM {CATALOG}.{GOLD}.metric_incident_mttr
        ORDER BY avg_hours DESC
        LIMIT 12
        """,
    )

    order_sla = _execute_sql(
        f"""
        SELECT status, SUM(order_count) AS order_count
        FROM {CATALOG}.{GOLD}.metric_order_fulfillment_sla
        GROUP BY status
        ORDER BY order_count DESC
        """,
    )

    tech_util = _execute_sql(
        f"""
        SELECT market, status, SUM(technician_count) AS technician_count
        FROM {CATALOG}.{GOLD}.metric_technician_utilization
        GROUP BY market, status
        ORDER BY market, status
        """,
    )
    tech_markets = sorted({r["market"] for r in tech_util if r.get("market")})
    tech_available = []
    tech_busy = []
    for m in tech_markets:
        avail = sum(int(r["technician_count"] or 0) for r in tech_util if r["market"] == m and r["status"] == "AVAILABLE")
        busy = sum(int(r["technician_count"] or 0) for r in tech_util if r["market"] == m and r["status"] != "AVAILABLE")
        tech_available.append(avail)
        tech_busy.append(busy)

    trend = _execute_sql(
        f"""
        SELECT DATE(opened_at) AS day, COUNT(*) AS incident_count
        FROM {CATALOG}.{GOLD}.service_incident
        WHERE opened_at >= date_sub(current_date(), 14)
        GROUP BY DATE(opened_at)
        ORDER BY day
        """,
    )

    service_mix = _execute_sql(
        f"""
        SELECT service_type, COUNT(*) AS incident_count
        FROM {CATALOG}.{GOLD}.service_incident i
        LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
        WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
        GROUP BY service_type
        ORDER BY incident_count DESC
        """,
    )

    status_rows = _execute_sql(
        f"""
        SELECT COALESCE(o.status, i.status) AS status, COUNT(*) AS cnt
        FROM {CATALOG}.{GOLD}.service_incident i
        LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
        WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED', 'RESOLVED')
        GROUP BY COALESCE(o.status, i.status)
        ORDER BY cnt DESC
        """,
    )

    active_sevs = [s for s in severities if any(ms_matrix[m].get(s, 0) for m in markets_sorted)]

    return {
        "kpis": kpis,
        "refreshed_at": dash.get("refreshed_at"),
        "ops_read_path": dash.get("ops_read_path"),
        "catalog": CATALOG,
        "schema": GOLD,
        "severity_donut": {
            "labels": [r["severity"] for r in dash.get("severity") or []],
            "values": [int(r.get("incident_count") or 0) for r in dash.get("severity") or []],
        },
        "market_bar": {
            "labels": [r["market"] for r in dash.get("markets") or []],
            "open": [int(r.get("open_count") or 0) for r in dash.get("markets") or []],
            "p1": [int(r.get("p1_count") or 0) for r in dash.get("markets") or []],
        },
        "market_severity_stacked": {
            "labels": markets_sorted,
            "series": [
                {"label": s, "data": [ms_matrix[m].get(s, 0) for m in markets_sorted]}
                for s in severities
                if any(ms_matrix[m].get(s, 0) for m in markets_sorted)
            ],
        },
        "status_pie": {
            "labels": [r["status"] for r in status_rows],
            "values": [int(r.get("cnt") or 0) for r in status_rows],
        },
        "orders_bar": {
            "labels": [r["market"] for r in dash.get("orders") or []],
            "values": [int(r.get("order_count") or 0) for r in dash.get("orders") or []],
        },
        "order_status": {
            "labels": [r["status"] for r in order_sla],
            "values": [int(r.get("order_count") or 0) for r in order_sla],
        },
        "tech_stacked": {
            "labels": tech_markets,
            "available": tech_available,
            "busy": tech_busy,
        },
        "mttr_bar": {
            "labels": [r["market"] for r in mttr],
            "values": [float(r.get("avg_hours") or 0) for r in mttr],
            "counts": [int(r.get("incident_count") or 0) for r in mttr],
        },
        "incident_trend": {
            "labels": [str(r.get("day") or "")[:10] for r in trend],
            "values": [int(r.get("incident_count") or 0) for r in trend],
        },
        "service_mix": {
            "labels": [r.get("service_type") or "Unknown" for r in service_mix],
            "values": [int(r.get("incident_count") or 0) for r in service_mix],
        },
        "heatmap": {
            "markets": markets_sorted,
            "severities": active_sevs,
            "matrix": [[ms_matrix[m].get(s, 0) for s in active_sevs] for m in markets_sorted],
        },
    }


def _lakebase_credentials() -> tuple[str, str]:
    """Fresh OAuth DB credential for the app service principal (Lakebase Autoscaling)."""
    if os.environ.get("LAKEBASE_PASSWORD") and LAKEBASE_USER:
        return LAKEBASE_USER, os.environ["LAKEBASE_PASSWORD"]

    w = _workspace_client()
    user = APP_CLIENT_ID or LAKEBASE_USER

    token = None
    try:
        cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
        token = cred.token
        user = APP_CLIENT_ID or getattr(cred, "username", None) or user
    except Exception as exc:
        app.logger.warning("generate_database_credential failed: %s", exc)

    if not token:
        try:
            resp = w.api_client.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": LAKEBASE_ENDPOINT, "database": LAKEBASE_DB},
            )
            token = resp.get("token")
            user = APP_CLIENT_ID or resp.get("username") or user
        except Exception as exc:
            app.logger.warning("postgres/credentials API failed: %s", exc)

    return user, token or ""


def _merge_ops_to_uc(incident_id: str, status: str, ops_notes: str | None, user: str) -> str | None:
    """Persist ops overlay to writable Delta table (DLT service_incident_ops is a view)."""
    ensure_err = _ensure_ops_writeback_table()
    if ensure_err:
        return ensure_err
    return _execute_sql_mutation(
        f"""
        MERGE INTO {_ops_writeback_fq()} AS t
        USING (SELECT {_sql_literal(incident_id)} AS incident_id) AS s
          ON t.incident_id = s.incident_id
        WHEN MATCHED THEN UPDATE SET
            status = {_sql_literal(status)},
            ops_notes = {_sql_literal(ops_notes)},
            updated_at = current_timestamp(),
            updated_by = {_sql_literal(user)}
        WHEN NOT MATCHED THEN INSERT (
            incident_id, status, ops_notes, updated_at, updated_by
        ) VALUES (
            {_sql_literal(incident_id)}, {_sql_literal(status)},
            {_sql_literal(ops_notes)}, current_timestamp(), {_sql_literal(user)}
        )
        """,
    )


def _lakebase_postgres_write_timed(
    incident_id: str, status: str, ops_notes: str | None, user: str
) -> tuple[str | None, float]:
    """Write ops overlay to Lakebase Postgres; returns (error, latency_ms)."""
    if not _lakebase_enabled():
        return "Lakebase not configured", 0.0
    _, err, ms = _lakebase_query_timed(
        f"""
        INSERT INTO {_lakebase_ops_table()}
            (incident_id, status, ops_notes, updated_at, updated_by)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (incident_id) DO UPDATE SET
            status = EXCLUDED.status,
            ops_notes = EXCLUDED.ops_notes,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
        """,
        (incident_id, status, ops_notes, datetime.now(timezone.utc), user),
    )
    return err, ms


def _lakebase_postgres_write(
    incident_id: str, status: str, ops_notes: str | None, user: str
) -> str | None:
    """Write ops overlay to Lakebase Postgres (primary low-latency path)."""
    err, _ = _lakebase_postgres_write_timed(incident_id, status, ops_notes, user)
    return err


def _merge_ops_to_uc_timed(
    incident_id: str, status: str, ops_notes: str | None, user: str
) -> tuple[str | None, float]:
    """Persist ops overlay to writable Delta table; returns (error, latency_ms)."""
    t0 = time.perf_counter()
    err = _merge_ops_to_uc(incident_id, status, ops_notes, user)
    return err, round((time.perf_counter() - t0) * 1000, 1)


def _lakebase_update_incident(
    incident_id: str,
    status: str,
    ops_notes: str | None,
    user: str,
    write_path: str = "lakebase",
) -> dict:
    """
    Lakebase-first writeback: Postgres sync (fast OLTP), UC Delta MERGE in background.
    write_path=lakebase|warehouse forces a single path for latency demos.
    """
    write_path = _normalize_path_choice(write_path)
    hosts = _lakebase_pg_hosts()

    if write_path == "warehouse":
        merge_error, latency_ms = _merge_ops_to_uc_timed(incident_id, status, ops_notes, user)
        if merge_error:
            return {
                "error": merge_error,
                "incident_id": incident_id,
                "write_path": "warehouse",
                "latency_ms": latency_ms,
                "hint": "Grant app SP USE CATALOG + MODIFY on users.ankur_nayyar",
            }
        return {
            "path": "warehouse_primary",
            "write_path": "warehouse",
            "incident_id": incident_id,
            "status": status,
            "table": _ops_writeback_fq(),
            "latency_ms": latency_ms,
            "sync": "uc_only",
        }

    if _lakebase_enabled():
        lb_err, latency_ms = _lakebase_postgres_write_timed(incident_id, status, ops_notes, user)
        if not lb_err:
            result = {
                "path": "lakebase_primary",
                "write_path": "lakebase",
                "incident_id": incident_id,
                "status": status,
                "table": _lakebase_ops_table(),
                "postgres_host": hosts[0] if hosts else None,
                "latency_ms": latency_ms,
                "sync": "uc_delta_background",
            }

            def _bg_delta() -> None:
                merge_err = _merge_ops_to_uc(incident_id, status, ops_notes, user)
                if merge_err:
                    app.logger.warning("UC writeback mirror lagged: %s", merge_err)

            threading.Thread(target=_bg_delta, daemon=True).start()
            return result

        app.logger.warning("Lakebase write failed (%s); falling back to UC Delta", lb_err)
        merge_error, wh_ms = _merge_ops_to_uc_timed(incident_id, status, ops_notes, user)
        if merge_error:
            return {
                "error": f"Lakebase: {lb_err}; UC: {merge_error}",
                "incident_id": incident_id,
                "write_path": "lakebase",
                "latency_ms": wh_ms,
                "hint": "Run scripts/setup_lakebase_app_oauth.py and grant app SP on Lakebase",
            }
        return {
            "path": "delta_writeback_fallback",
            "write_path": "lakebase",
            "incident_id": incident_id,
            "status": status,
            "table": _ops_writeback_fq(),
            "lakebase_error": lb_err,
            "latency_ms": wh_ms,
            "sync": "uc_only",
        }

    merge_error, latency_ms = _merge_ops_to_uc_timed(incident_id, status, ops_notes, user)
    if merge_error:
        return {
            "error": merge_error,
            "incident_id": incident_id,
            "write_path": "warehouse",
            "latency_ms": latency_ms,
            "hint": "Grant app SP USE CATALOG + MODIFY on users.ankur_nayyar",
        }

    return {
        "path": "delta_writeback",
        "write_path": "warehouse",
        "incident_id": incident_id,
        "status": status,
        "table": _ops_writeback_fq(),
        "latency_ms": latency_ms,
        "sync": "uc_only",
    }


AGENT_NAME = "sdp_incident_triage"


def _agent_audit(tool_name: str, query_text: str, result_summary: str, user_id: str | None = None) -> str | None:
    audit_id = str(uuid.uuid4())
    err = _execute_sql_mutation(
        f"""
        INSERT INTO {CATALOG}.{GOLD}.agent_audit_log
        (audit_id, agent_name, tool_name, user_id, query_text, result_summary, created_at)
        VALUES (
            {_sql_literal(audit_id)}, {_sql_literal(AGENT_NAME)}, {_sql_literal(tool_name)},
            {_sql_literal(user_id)}, {_sql_literal(query_text)}, {_sql_literal(result_summary)},
            current_timestamp()
        )
        """,
    )
    return None if err else audit_id


def _agent_search_incidents(
    market: str | None = None,
    severity: str | None = None,
    keyword: str | None = None,
) -> tuple[list[dict], str]:
    where = [
        "COALESCE(w.status, o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')",
    ]
    if market:
        where.append(f"i.market = {_sql_literal(market.upper())}")
    if severity:
        where.append(f"i.severity = {_sql_literal(severity.upper())}")
    if keyword:
        kw = keyword.replace("'", "''").lower()
        where.append(
            f"(LOWER(i.title) LIKE '%{kw}%' OR LOWER(i.description) LIKE '%{kw}%')",
        )
    rows = _execute_sql(
        f"""
        SELECT i.incident_id, i.title, i.severity,
               COALESCE(w.status, o.status, i.status) AS status,
               i.market, i.service_type, i.opened_at
        FROM {CATALOG}.{GOLD}.service_incident i
        {_incident_joins('i')}
        WHERE {' AND '.join(where)}
        ORDER BY
          CASE i.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
          i.opened_at ASC
        LIMIT 25
        """,
    )
    summary = f"Found {len(rows)} incident(s)"
    if market:
        summary += f" in {market.upper()}"
    if severity:
        summary += f" at {severity.upper()}"
    return rows, summary


def _agent_recommend_technician(market: str, min_skill_level: str | None = None) -> tuple[list[dict], str]:
    where = [
        f"market = {_sql_literal(market.upper())}",
        "status = 'AVAILABLE'",
    ]
    if min_skill_level:
        where.append(f"skill_level >= {_sql_literal(min_skill_level.upper())}")
    rows = _execute_sql(
        f"""
        SELECT technician_id, technician_name, market, skill_level, status
        FROM {CATALOG}.{GOLD}.field_technician
        WHERE {' AND '.join(where)}
        ORDER BY skill_level DESC
        LIMIT 5
        """,
    )
    if rows:
        top = rows[0]
        summary = (
            f"Recommend {top['technician_name']} ({top['technician_id']}) — "
            f"{top['skill_level']} in {market.upper()}"
        )
    else:
        summary = f"No available technicians in {market.upper()}"
    return rows, summary


def _agent_lookup_incident(incident_id: str) -> dict | None:
    rows = _execute_sql(
        f"""
        SELECT i.incident_id, i.title, i.severity,
               COALESCE(w.status, o.status, i.status) AS status,
               i.market, i.service_type
        FROM {CATALOG}.{GOLD}.service_incident i
        {_incident_joins('i')}
        WHERE i.incident_id = {_sql_literal(incident_id)}
        LIMIT 1
        """,
    )
    return rows[0] if rows else None


def _agent_lookup_technician(technician_id: str) -> dict | None:
    rows = _execute_sql(
        f"""
        SELECT technician_id, technician_name, market, skill_level, status
        FROM {CATALOG}.{GOLD}.field_technician
        WHERE technician_id = {_sql_literal(technician_id)}
        LIMIT 1
        """,
    )
    return rows[0] if rows else None


def _agent_propose_dispatch(incident_id: str, technician_id: str) -> dict:
    incident = _agent_lookup_incident(incident_id)
    technician = _agent_lookup_technician(technician_id)
    if not incident:
        return {"error": f"Incident {incident_id} not found"}
    if not technician:
        return {"error": f"Technician {technician_id} not found"}
    if incident["market"] != technician["market"]:
        return {
            "error": f"Market mismatch: incident in {incident['market']}, tech in {technician['market']}",
        }
    warning = None
    if technician.get("status") != "AVAILABLE":
        warning = (
            f"{technician['technician_name']} is {technician['status']} — "
            "proposal requires ops approval to override availability"
        )
    reasoning = (
        f"Propose dispatch: assign {technician['technician_name']} ({technician_id}) "
        f"to {incident_id} ({incident['severity']} · {incident['title'][:60]}). "
        f"Updates bridge_incident_technician + status DISPATCHED on approval."
    )
    return {
        "incident": incident,
        "technician": technician,
        "reasoning": reasoning,
        "warning": warning,
        "requires_approval": True,
        "writes": [
            f"{CATALOG}.{GOLD}.bridge_incident_technician",
            _ops_writeback_fq(),
        ],
    }


def _agent_execute_dispatch(
    incident_id: str,
    technician_id: str,
    user: str,
    ops_notes: str | None = None,
) -> dict:
    proposal = _agent_propose_dispatch(incident_id, technician_id)
    if proposal.get("error"):
        return proposal

    bridge_err = _execute_sql_mutation(
        f"""
        MERGE INTO {CATALOG}.{GOLD}.bridge_incident_technician AS t
        USING (
          SELECT {_sql_literal(incident_id)} AS incident_id,
                 {_sql_literal(technician_id)} AS technician_id
        ) AS s
        ON t.incident_id = s.incident_id AND t.technician_id = s.technician_id
        WHEN MATCHED THEN UPDATE SET
            assignment_status = 'CONFIRMED',
            assigned_at = current_timestamp(),
            assigned_by = {_sql_literal(user)}
        WHEN NOT MATCHED THEN INSERT (
            incident_id, technician_id, assignment_status, assigned_at, assigned_by
        ) VALUES (
            s.incident_id, s.technician_id, 'CONFIRMED', current_timestamp(), {_sql_literal(user)}
        )
        """,
    )
    if bridge_err:
        return {"error": bridge_err, "step": "bridge_incident_technician"}

    notes = ops_notes or f"Agent dispatch approved — {technician_id} assigned"
    status_result = _lakebase_update_incident(incident_id, "DISPATCHED", notes, user)
    if status_result.get("error"):
        return {**status_result, "step": "ops_writeback"}

    _agent_audit(
        "assign_technician",
        f"incident_id={incident_id}, technician_id={technician_id}",
        "CONFIRMED assignment written to bridge",
        user,
    )
    _agent_audit(
        "update_incident_status",
        f"incident_id={incident_id}, status=DISPATCHED",
        "Ops overlay updated via writeback path",
        user,
    )

    def _bg_mv() -> None:
        _execute_sql_mutation(
            f"REFRESH MATERIALIZED VIEW {CATALOG}.{GOLD}.mv_incident_dispatch_board",
        )

    threading.Thread(target=_bg_mv, daemon=True).start()

    return {
        "incident_id": incident_id,
        "technician_id": technician_id,
        "assignment_status": "CONFIRMED",
        "incident_status": "DISPATCHED",
        "bridge_table": f"{CATALOG}.{GOLD}.bridge_incident_technician",
        "ops_table": _ops_writeback_fq(),
        "path": status_result.get("path"),
        "message": "Dispatch approved — bridge + ops tables updated",
    }


@app.get("/")
@app.get("/home")
@app.get("/dashboard")
@app.get("/live")
@app.get("/dispatch")
@app.get("/genie")
@app.get("/agent")
@app.get("/analytics")
def index():
    path_tabs = {
        "/": "home",
        "/home": "home",
        "/dashboard": "dashboard",
        "/live": "live",
        "/dispatch": "dispatch",
        "/genie": "genie",
        "/agent": "agent",
        "/analytics": "analytics",
    }
    active_tab = path_tabs.get(request.path) or request.args.get("tab", "home")
    if active_tab not in ("home", "dashboard", "dispatch", "live", "genie", "agent", "analytics"):
        active_tab = "home"
    return render_template(
        "index.html",
        catalog=CATALOG,
        schema=GOLD,
        lakebase_project=LAKEBASE_PROJECT,
        active_tab=active_tab,
        workspace_host=_app_host().rstrip("/"),
        workspace_id=WORKSPACE_ID,
        genie_space_id=GENIE_SPACE_ID or "",
        mlflow_experiment_url=experiment_url() or "",
    )


@app.get("/api/dashboard")
def dashboard_route():
    return jsonify(_dashboard_data())


@app.get("/api/analytics")
def analytics_route():
    return jsonify(_analytics_data())


@app.get("/health")
def health():
    probe, probe_error = _sql_probe()
    lb_ok, lb_error, lb_path = False, "Lakebase not configured", "uc_warehouse"
    if _lakebase_enabled():
        _, lb_error, lb_path = _lakebase_ops_map()
        lb_ok = lb_error is None
    hosts = _lakebase_pg_hosts()
    return jsonify({
        "status": "ok",
        "app": "att-sdp-ops-console",
        "catalog": CATALOG,
        "lakebase_project": LAKEBASE_PROJECT,
        "lakebase_host": LAKEBASE_HOST or None,
        "lakebase_connect_host": hosts[0] if hosts else None,
        "lakebase_use_pooler": LAKEBASE_USE_POOLER,
        "lakebase_ok": lb_ok,
        "lakebase_error": lb_error if not lb_ok else None,
        "ops_read_path": lb_path if lb_ok else "uc_warehouse",
        "write_path": "lakebase_primary" if _lakebase_enabled() else "uc_delta",
        "warehouse_configured": bool(os.getenv("DATABRICKS_WAREHOUSE_ID")),
        "genie_configured": bool(GENIE_SPACE_ID),
        "genie_space_id": GENIE_SPACE_ID or None,
        "sql_ok": probe_error is None,
        "sql_error": probe_error,
        "incident_count": probe[0]["n"] if probe else None,
    })


@app.get("/api/incidents")
def list_incidents():
    market = request.args.get("market")
    severity = request.args.get("severity")
    status = request.args.get("status", "OPEN,IN_PROGRESS,DISPATCHED")
    read_path = _normalize_path_choice(request.args.get("read_path"), "lakebase")
    statuses = [s.strip().upper() for s in status.split(",") if s.strip()]
    statuses = [s for s in statuses if re.fullmatch(r"[A-Z_]+", s)]
    status_set = set(statuses)
    req_t0 = time.perf_counter()

    ops_map, ops_err, ops_path, ops_ms = _lakebase_ops_overlay_timed(read_path)
    if ops_map is not None:
        where = ["TRUE"]
        if market:
            where.append(f"b.market = {_sql_literal(market.upper())}")
        if severity:
            where.append(f"b.severity = {_sql_literal(severity.upper())}")
        board_t0 = time.perf_counter()
        rows = _execute_sql(
            f"""
            SELECT b.incident_id, b.title, b.severity, b.incident_status,
                   b.ops_notes, b.market, b.service_type, b.opened_at,
                   b.account_name, b.technician_name
            FROM {CATALOG}.{GOLD}.mv_incident_dispatch_board b
            WHERE {' AND '.join(where)}
            ORDER BY
              CASE b.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
              b.opened_at ASC
            LIMIT 200
            """,
        )
        board_ms = round((time.perf_counter() - board_t0) * 1000, 1)
        items = []
        for row in rows:
            eff = _ops_status(row["incident_id"], row["incident_status"], ops_map)
            if eff not in status_set:
                continue
            row["incident_status"] = eff
            row["ops_notes"] = _ops_notes(row["incident_id"], row.get("ops_notes"), ops_map)
            items.append(row)
            if len(items) >= 100:
                break
        total_ms = round((time.perf_counter() - req_t0) * 1000, 1)
        return jsonify({
            "items": items,
            "table": f"{CATALOG}.{GOLD}.mv_incident_dispatch_board",
            "read_path": read_path,
            "ops_read_path": ops_path,
            "ops_source": _lakebase_ops_table() if ops_path == "lakebase_postgres" else "service_incident_ops_pg",
            "latency_ms": total_ms,
            "ops_read_ms": ops_ms,
            "board_read_ms": board_ms,
        })

    where = [
        "COALESCE(w.status, o.status, b.incident_status) IN ("
        + ",".join(_sql_literal(s) for s in statuses)
        + ")",
    ]
    if market:
        where.append(f"b.market = {_sql_literal(market.upper())}")
    if severity:
        where.append(f"b.severity = {_sql_literal(severity.upper())}")

    board_t0 = time.perf_counter()
    rows = _execute_sql(
        f"""
        SELECT b.incident_id, b.title, b.severity,
               COALESCE(w.status, o.status, b.incident_status) AS incident_status,
               COALESCE(w.ops_notes, o.ops_notes, b.ops_notes) AS ops_notes,
               b.market, b.service_type, b.opened_at, b.account_name, b.technician_name
        FROM {CATALOG}.{GOLD}.mv_incident_dispatch_board b
        LEFT JOIN {_ops_writeback_fq()} w ON b.incident_id = w.incident_id
        LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON b.incident_id = o.incident_id
        WHERE {' AND '.join(where)}
        ORDER BY
          CASE b.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
          b.opened_at ASC
        LIMIT 100
        """,
    )
    board_ms = round((time.perf_counter() - board_t0) * 1000, 1)
    total_ms = round((time.perf_counter() - req_t0) * 1000, 1)
    return jsonify({
        "items": rows,
        "table": f"{CATALOG}.{GOLD}.mv_incident_dispatch_board",
        "read_path": read_path,
        "ops_read_path": "uc_warehouse",
        "ops_read_error": ops_err,
        "latency_ms": total_ms,
        "ops_read_ms": ops_ms,
        "board_read_ms": board_ms,
    })


@app.get("/api/kpis")
def kpis():
    ops_map, _, ops_path = _lakebase_ops_overlay()
    if ops_map is not None:
        counts = _kpi_open_counts(ops_map)
        aux = _execute_sql(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
               WHERE status = 'PROVISIONING') AS orders_in_provisioning,
              (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
               WHERE status = 'AVAILABLE') AS available_technicians
            """,
        )
        return jsonify({**counts, **(aux[0] if aux else {}), "ops_read_path": ops_path})

    rows = _execute_sql(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident i
           LEFT JOIN {_ops_writeback_fq()} w ON i.incident_id = w.incident_id
           LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
           WHERE COALESCE(w.status, o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
          ) AS total_open_incidents,
          (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_incident i
           LEFT JOIN {_ops_writeback_fq()} w ON i.incident_id = w.incident_id
           LEFT JOIN {CATALOG}.{GOLD}.service_incident_ops o ON i.incident_id = o.incident_id
           WHERE i.severity = 'P1'
             AND COALESCE(w.status, o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
          ) AS open_p1_incidents,
          (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.service_order
           WHERE status = 'PROVISIONING') AS orders_in_provisioning,
          (SELECT COUNT(*) FROM {CATALOG}.{GOLD}.field_technician
           WHERE status = 'AVAILABLE') AS available_technicians
        """,
    )
    return jsonify({**(rows[0] if rows else {
        "total_open_incidents": 0,
        "open_p1_incidents": 0,
        "orders_in_provisioning": 0,
        "available_technicians": 0,
    }), "ops_read_path": "uc_warehouse"})


@app.patch("/api/incidents/<incident_id>")
def update_incident(incident_id: str):
    """Lakebase writeback endpoint — replaces Foundry Action on ServiceIncident."""
    body = request.get_json(force=True)
    status = body.get("status")
    ops_notes = body.get("ops_notes")
    user = body.get("user", "sdp_ops_console")
    write_path = _normalize_path_choice(body.get("write_path"), "lakebase")

    if not status:
        return jsonify({"error": "status is required"}), 400

    result = _lakebase_update_incident(
        incident_id, status.upper(), ops_notes, user, write_path=write_path,
    )
    if result.get("error"):
        return jsonify(result), 503

    def _bg_track() -> None:
        track_event(
            "incident_writeback",
            params={
                "incident_id": incident_id,
                "status": status.upper(),
                "path": result.get("path"),
                "latency_ms": result.get("latency_ms"),
            },
            tags={"user": user, "write_path": write_path},
        )

    threading.Thread(target=_bg_track, daemon=True).start()
    return jsonify(result)


def _simulate_latency_insert(path: str, operation: str, label: str) -> tuple[dict, int]:
    path = _normalize_path_choice(path)
    if path == "lakebase":
        if not _lakebase_enabled():
            return {"error": "Lakebase not configured", "path": path}, 503
        ensure_err = _ensure_lakebase_latency_demo_table()
        if ensure_err:
            return {"error": ensure_err, "path": path}, 503
        run_id = str(uuid.uuid4())
        t0 = time.perf_counter()
        _, err, _ = _lakebase_query_timed(
            f"""
            INSERT INTO {_lakebase_latency_table()}
                (run_id, operation, path, latency_ms, detail, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, operation, "lakebase", 0, label, datetime.now(timezone.utc)),
        )
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        if err:
            return {"error": err, "path": path, "latency_ms": latency_ms}, 503
        _lakebase_query_timed(
            f"UPDATE {_lakebase_latency_table()} SET latency_ms = %s WHERE run_id = %s",
            (int(round(latency_ms)), run_id),
        )
        return {
            "run_id": run_id,
            "operation": operation,
            "path": "lakebase",
            "latency_ms": latency_ms,
            "detail": label,
            "table": _lakebase_latency_table(),
        }, 200

    ensure_err = _ensure_uc_latency_demo_table()
    if ensure_err:
        return {"error": ensure_err, "path": path}, 503
    run_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    err = _execute_sql_mutation(
        f"""
        INSERT INTO {_latency_uc_table()}
            (run_id, operation, path, latency_ms, detail, created_at)
        VALUES (
            {_sql_literal(run_id)}, {_sql_literal(operation)}, 'warehouse',
            0, {_sql_literal(label)}, current_timestamp()
        )
        """,
    )
    latency_ms = round((time.perf_counter() - t0) * 1000, 1)
    if err:
        return {"error": err, "path": path, "latency_ms": latency_ms}, 503
    _execute_sql_mutation(
        f"""
        UPDATE {_latency_uc_table()}
        SET latency_ms = {int(round(latency_ms))}
        WHERE run_id = {_sql_literal(run_id)}
        """,
    )
    return {
        "run_id": run_id,
        "operation": operation,
        "path": "warehouse",
        "latency_ms": latency_ms,
        "detail": label,
        "table": _latency_uc_table(),
    }, 200


@app.get("/api/latency/history")
def latency_history():
    limit = request.args.get("limit", 25, type=int)
    return jsonify({
        "items": _latency_demo_history(limit),
        "lakebase_table": _lakebase_latency_table() if _lakebase_enabled() else None,
        "warehouse_table": _latency_uc_table(),
    })


@app.post("/api/latency/simulate")
def latency_simulate():
    """Insert a demo row and measure write latency (Lakebase vs SQL warehouse)."""
    body = request.get_json(force=True) or {}
    path = _normalize_path_choice(body.get("path"), "lakebase")
    operation = (body.get("operation") or "insert").strip().lower()
    label = (body.get("label") or f"demo_{operation}").strip()[:120]
    payload, status = _simulate_latency_insert(path, operation, label)
    return jsonify(payload), status


@app.post("/api/latency/compare")
def latency_compare():
    """Run Lakebase + warehouse insert simulations back-to-back."""
    body = request.get_json(force=True) or {}
    label = (body.get("label") or "side_by_side").strip()[:120]
    results = []
    for path in ("lakebase", "warehouse"):
        payload, status = _simulate_latency_insert(path, "compare", label)
        results.append({**payload, "ok": status < 400})
    lb = next((r for r in results if r.get("path") == "lakebase"), {})
    wh = next((r for r in results if r.get("path") == "warehouse"), {})
    speedup = None
    if lb.get("latency_ms") and wh.get("latency_ms") and lb["latency_ms"] > 0:
        speedup = round(wh["latency_ms"] / lb["latency_ms"], 1)
    return jsonify({
        "results": results,
        "lakebase_ms": lb.get("latency_ms"),
        "warehouse_ms": wh.get("latency_ms"),
        "speedup_factor": speedup,
        "label": label,
    })


@app.post("/api/incidents/<incident_id>/dispatch")
def dispatch_technician(incident_id: str):
    body = request.get_json(force=True)
    technician_id = body.get("technician_id")
    user = body.get("user", "sdp_ops_console")

    if not technician_id:
        return jsonify({"error": "technician_id is required"}), 400

    result = _agent_execute_dispatch(incident_id, technician_id, user)
    if result.get("error"):
        return jsonify(result), 503
    return jsonify(result)


@app.post("/api/agent/search")
def agent_search_route():
    body = request.get_json(force=True) or {}
    market = body.get("market")
    severity = body.get("severity")
    keyword = body.get("keyword")
    user = body.get("user", "sdp_ops_console")

    rows, summary = _agent_search_incidents(market, severity, keyword)
    query = f"search_incidents market={market} severity={severity} keyword={keyword}"
    audit_id = _agent_audit("search_incidents", query, summary, user)
    return jsonify({
        "tool": "search_incidents",
        "items": rows,
        "summary": summary,
        "reasoning": (
            f"Prioritizing P1 incidents{f' in {market.upper()}' if market else ''}. "
            f"{summary} — review list before dispatch."
        ),
        "audit_id": audit_id,
    })


@app.post("/api/agent/recommend")
def agent_recommend_route():
    body = request.get_json(force=True) or {}
    market = (body.get("market") or "").strip().upper()
    if not market:
        return jsonify({"error": "market is required"}), 400
    user = body.get("user", "sdp_ops_console")
    min_skill = body.get("min_skill_level")

    rows, summary = _agent_recommend_technician(market, min_skill)
    query = f"recommend_technician market={market} min_skill={min_skill}"
    audit_id = _agent_audit("recommend_technician", query, summary, user)
    return jsonify({
        "tool": "recommend_technician",
        "items": rows,
        "summary": summary,
        "recommended": rows[0] if rows else None,
        "reasoning": (
            summary if rows else
            f"No AVAILABLE techs in {market} — check DISPATCHED roster or adjacent markets."
        ),
        "audit_id": audit_id,
    })


@app.post("/api/agent/propose")
def agent_propose_route():
    body = request.get_json(force=True) or {}
    incident_id = (body.get("incident_id") or "").strip()
    technician_id = (body.get("technician_id") or "").strip()
    if not incident_id or not technician_id:
        return jsonify({"error": "incident_id and technician_id are required"}), 400

    proposal = _agent_propose_dispatch(incident_id, technician_id)
    if proposal.get("error"):
        return jsonify(proposal), 400

    user = body.get("user", "sdp_ops_console")
    audit_id = _agent_audit(
        "assign_technician",
        f"PROPOSED incident_id={incident_id}, technician_id={technician_id}",
        proposal["reasoning"][:500],
        user,
    )
    proposal["tool"] = "assign_technician"
    proposal["audit_id"] = audit_id
    proposal["status"] = "pending_approval"
    return jsonify(proposal)


@app.post("/api/agent/approve")
def agent_approve_route():
    body = request.get_json(force=True) or {}
    incident_id = (body.get("incident_id") or "").strip()
    technician_id = (body.get("technician_id") or "").strip()
    user = body.get("user", "sdp_ops_console")
    ops_notes = body.get("ops_notes")

    if not incident_id or not technician_id:
        return jsonify({"error": "incident_id and technician_id are required"}), 400

    result = _agent_execute_dispatch(incident_id, technician_id, user, ops_notes)
    if result.get("error"):
        return jsonify(result), 503

    mlflow_run = track_event(
        "agent_dispatch_approved",
        params={
            "incident_id": incident_id,
            "technician_id": technician_id,
            "agent": AGENT_NAME,
        },
        tags={"user": user},
    )
    result["mlflow_run_id"] = mlflow_run
    result["mlflow_experiment_url"] = experiment_url()
    return jsonify(result)


@app.get("/api/agent/audit")
def agent_audit_route():
    rows = _execute_sql(
        f"""
        SELECT audit_id, tool_name, user_id, query_text, result_summary, created_at
        FROM {CATALOG}.{GOLD}.agent_audit_log
        WHERE agent_name = {_sql_literal(AGENT_NAME)}
        ORDER BY created_at DESC
        LIMIT 15
        """,
    )
    return jsonify({"items": rows, "agent": AGENT_NAME})


@app.get("/api/genie/status")
def genie_status():
    ml = mlflow_status()
    return jsonify({
        "configured": bool(GENIE_SPACE_ID),
        "space_id": GENIE_SPACE_ID or None,
        "space_url": (
            f"{_app_host().rstrip('/')}/genie/rooms/{GENIE_SPACE_ID}?o={WORKSPACE_ID}"
            if GENIE_SPACE_ID else None
        ),
        "mlflow": ml,
    })


@app.get("/api/mlflow/status")
def mlflow_status_route():
    return jsonify(mlflow_status())


@app.post("/api/genie/ask")
def genie_ask_route():
    body = request.get_json(force=True)
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"error": "question is required"}), 400
    result = _genie_ask(question, body.get("conversation_id"))
    if result.get("error"):
        return jsonify(result), 503
    return jsonify(result)


@app.get("/api/technicians")
def list_technicians():
    market = request.args.get("market")
    where = "TRUE"
    if market:
        where = f"market = {_sql_literal(market.upper())}"
    rows = _execute_sql(
        f"""
        SELECT technician_id, technician_name, market, status, skill_level
        FROM {CATALOG}.{GOLD}.field_technician
        WHERE {where}
        ORDER BY status, skill_level DESC
        """,
    )
    return jsonify({"items": rows})


@app.get("/api/live/scenarios")
def live_scenarios_route():
    return jsonify({"scenarios": LIVE_SCENARIOS, "pipelines": JOB_PIPELINES})


@app.get("/api/live/lakebase")
def live_lakebase_route():
    return jsonify(_lakebase_snapshot())


@app.get("/api/live/stats")
def live_stats_route():
    data = _live_stats()
    lb = _lakebase_snapshot()
    data["lakebase"] = {
        "project": LAKEBASE_PROJECT,
        "ops_count": lb.get("lakebase_ops_count"),
    }
    return jsonify(data)


@app.post("/api/live/trigger")
def live_trigger_route():
    body = request.get_json(force=True) or {}
    scenario = body.get("scenario", "new_p1_dallas")
    workflow = body.get("workflow", "live")
    if workflow not in JOB_PIPELINES:
        return jsonify({"error": f"Unknown workflow '{workflow}'"}), 400

    pipeline = JOB_PIPELINES[workflow]
    job_id = _find_job_id(pipeline["name"])
    if not job_id:
        return jsonify({
            "error": f"Job '{pipeline['name']}' not found. Run: databricks bundle deploy",
        }), 404
    try:
        w = _workspace_client()
        params = {"scenario": scenario} if workflow == "live" else None
        run = w.jobs.run_now(
            job_id=job_id,
            job_parameters=params if params else None,
        )
        return jsonify({
            "workflow": workflow,
            "job_id": job_id,
            "job_name": pipeline["name"],
            "job_url": _job_url(job_id),
            "run_id": run.run_id,
            "run_url": _job_url(job_id, run.run_id),
            "scenario": scenario if workflow == "live" else None,
            "steps": pipeline["steps"],
            "mlflow_run_id": track_event(
                "live_job_trigger",
                params={
                    "workflow": workflow,
                    "job_name": pipeline["name"],
                    "scenario": scenario,
                    "job_id": str(job_id),
                    "run_id": str(run.run_id),
                },
            ),
            "mlflow_experiment_url": experiment_url(),
        })
    except Exception as exc:
        return jsonify({
            "error": str(exc),
            "hint": f"Grant app SP CAN_MANAGE_RUN on {pipeline['name']}",
        }), 503


@app.get("/api/live/run/<int:run_id>")
def live_run_status(run_id: int):
    try:
        w = _workspace_client()
        run = w.jobs.get_run(run_id=run_id)
        life_cycle = (
            run.state.life_cycle_state.value
            if run.state and run.state.life_cycle_state
            else "UNKNOWN"
        )
        result = run.state.result_state.value if run.state and run.state.result_state else None
        msg = run.state.state_message if run.state and run.state.state_message else "In progress…"
        tasks = []
        for task in run.tasks or []:
            tstate = task.state
            tasks.append({
                "task_key": task.task_key,
                "life_cycle_state": (
                    tstate.life_cycle_state.value
                    if tstate and tstate.life_cycle_state
                    else None
                ),
                "result_state": (
                    tstate.result_state.value if tstate and tstate.result_state else None
                ),
            })
        done = life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
        job_id = run.job_id if hasattr(run, "job_id") else None
        return jsonify({
            "run_id": run_id,
            "job_id": job_id,
            "run_url": _job_url(job_id, run_id) if job_id else None,
            "state": life_cycle,
            "result": result,
            "message": msg,
            "done": done,
            "success": result == "SUCCESS",
            "tasks": tasks,
        })
    except Exception as exc:
        return jsonify({"run_id": run_id, "state": "ERROR", "message": str(exc), "done": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
