"""
ATT SDP Incident Triage Agent (AgentBricks / Mosaic AI).

Replaces Foundry AIP agent `ri.aip.att.agent.incident-triage`.
Deploy via Databricks AgentBricks or Mosaic AI Agent Framework.

See config/sdp_agent_tools.yaml for tool definitions and governance.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

# Tool implementations are registered with AgentBricks at deploy time.
# This module provides the Python tool handlers for writeback actions.


def assign_technician(
    spark,
    catalog: str,
    gold_schema: str,
    incident_id: str,
    technician_id: str,
    assigned_by: str = "sdp_incident_triage",
) -> dict[str, Any]:
    """Propose technician dispatch — requires human approval in AgentBricks."""
    spark.sql(f"""
        INSERT INTO {catalog}.{gold_schema}.bridge_incident_technician
        (incident_id, technician_id, assignment_status, assigned_at, assigned_by)
        VALUES (
            '{incident_id}',
            '{technician_id}',
            'PROPOSED',
            current_timestamp(),
            '{assigned_by}'
        )
    """)
    return {
        "incident_id": incident_id,
        "technician_id": technician_id,
        "assignment_status": "PROPOSED",
        "message": "Dispatch proposed — awaiting ops approval in SDP Console App",
    }


def update_incident_status(
    spark,
    catalog: str,
    gold_schema: str,
    incident_id: str,
    status: str,
    ops_notes: str | None = None,
    updated_by: str = "sdp_incident_triage",
) -> dict[str, Any]:
    """Write incident status via Lakebase-synced ops table."""
    notes_sql = f"'{ops_notes}'" if ops_notes else "ops_notes"
    if ops_notes:
        spark.sql(f"""
            MERGE INTO {catalog}.{gold_schema}.service_incident_ops AS t
            USING (SELECT '{incident_id}' AS incident_id) AS s
            ON t.incident_id = s.incident_id
            WHEN MATCHED THEN UPDATE SET
                status = '{status}',
                ops_notes = '{ops_notes}',
                updated_at = current_timestamp(),
                updated_by = '{updated_by}'
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        spark.sql(f"""
            MERGE INTO {catalog}.{gold_schema}.service_incident_ops AS t
            USING (SELECT '{incident_id}' AS incident_id) AS s
            ON t.incident_id = s.incident_id
            WHEN MATCHED THEN UPDATE SET
                status = '{status}',
                updated_at = current_timestamp(),
                updated_by = '{updated_by}'
        """)
    return {
        "incident_id": incident_id,
        "status": status,
        "updated_by": updated_by,
        "sync": "Lakebase → Delta sync will propagate to service_incident",
    }


def log_audit(
    spark,
    catalog: str,
    gold_schema: str,
    agent_name: str,
    tool_name: str,
    query_text: str,
    result_summary: str,
    user_id: str | None = None,
) -> None:
    audit_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    spark.sql(f"""
        INSERT INTO {catalog}.{gold_schema}.agent_audit_log
        VALUES (
            '{audit_id}',
            '{agent_name}',
            '{tool_name}',
            {f"'{user_id}'" if user_id else 'NULL'},
            '{query_text.replace("'", "''")}',
            '{result_summary.replace("'", "''")}',
            timestamp '{now}'
        )
    """)


# AgentBricks registration stub — wire at deploy:
# agent = Agent(name="sdp_incident_triage", tools=[search_incidents, ...])
