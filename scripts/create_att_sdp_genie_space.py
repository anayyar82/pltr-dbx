#!/usr/bin/env python3
"""Create ATT SDP Genie space from config/genie_space.yaml manifest."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_genie_config() -> tuple[str, str, str, str]:
    import yaml

    path = ROOT / "config" / "genie_space.yaml"
    if path.exists():
        with path.open(encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
        gs = doc.get("genie_space") or {}
        return (
            gs.get("catalog", "users"),
            gs.get("schema", "ankur_nayyar"),
            gs.get("title", "ATT SDP Service Delivery"),
            gs.get("description", "ATT SDP Genie analytics"),
        )
    return "users", "ankur_nayyar", "ATT SDP Service Delivery", (
        "Natural language analytics for ATT Service Delivery Platform."
    )


CATALOG, SCHEMA, TITLE, DESCRIPTION = _load_genie_config()
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "03560442e95cb440")
PARENT_PATH = os.getenv("GENIE_PARENT_PATH", "/Users/pltr-dbx")


def _table(name: str, description: str, columns: list[str] | None = None) -> dict:
    cfg: dict = {
        "identifier": f"{CATALOG}.{SCHEMA}.{name}",
        "description": [description],
    }
    if columns:
        cfg["column_configs"] = sorted(
            [
                {
                    "column_name": col,
                    "enable_format_assistance": True,
                    "enable_entity_matching": True,
                }
                for col in columns
            ],
            key=lambda c: c["column_name"],
        )
    return cfg


def build_serialized_space() -> str:
    space = {
        "version": 2,
        "config": {
            "sample_questions": sorted(
                [
                    {
                        "id": "4c7d05f8ad8d4e19a68b867415996710",
                        "question": ["How many P1 incidents are open in Dallas?"],
                    },
                    {
                        "id": "48124f89c00147c0a783be438a10fc0a",
                        "question": ["Compare open incidents across all markets"],
                    },
                    {
                        "id": "5730922c52ef4276925767e207784d94",
                        "question": ["Give me the current SDP executive summary"],
                    },
                ],
                key=lambda x: x["id"],
            )
        },
        "data_sources": {
            "tables": sorted(
                [
                    _table("field_technician", "Field technician roster and availability", ["market", "skill_level", "status"]),
                    _table("metric_open_incidents_by_market", "Open incident counts by market and severity", ["market", "severity"]),
                    _table("metric_sdp_executive_summary", "Executive KPI rollup for SDP health"),
                    _table(
                        "mv_incident_dispatch_board",
                        "Operational dispatch board joining incidents, accounts, technicians",
                        ["incident_status", "market", "severity"],
                    ),
                    _table("service_incident", "Network and service incidents with severity and status", ["market", "severity", "status"]),
                    _table("service_order", "Service fulfillment orders and provisioning status", ["market", "status"]),
                ],
                key=lambda t: t["identifier"],
            )
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": "a8aa6a1479f24a0e95325675a674acf5",
                    "content": [
                        "You are an analytics assistant for ATT Service Delivery Operations. ",
                        "Always filter by market when the user mentions a city or region. ",
                        "Severity levels are P1 (critical) through P4 (low). ",
                        "Incident statuses: OPEN, IN_PROGRESS, DISPATCHED, RESOLVED, CLOSED. ",
                        "Prefer mv_incident_dispatch_board for operational dispatch questions. ",
                        "For executive rollups, use metric_sdp_executive_summary.",
                    ],
                }
            ],
            "example_question_sqls": sorted(
                [
                    {
                        "id": "1b8332fb649444d48a6b5189ee813f99",
                        "question": ["Compare open incidents across all markets"],
                        "sql": [
                            "SELECT market, severity, open_incidents\n",
                            f"FROM {CATALOG}.{SCHEMA}.metric_open_incidents_by_market\n",
                            "ORDER BY market, severity",
                        ],
                    },
                    {
                        "id": "bfc0f5c12b0748d5a9d58edf3313439a",
                        "question": ["How many P1 incidents are open in Dallas?"],
                        "sql": [
                            "SELECT COUNT(*) AS open_p1\n",
                            f"FROM {CATALOG}.{SCHEMA}.service_incident\n",
                            "WHERE severity = 'P1'\n",
                            "  AND status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')\n",
                            "  AND market = 'DALLAS'",
                        ],
                    },
                    {
                        "id": "cf684a59849b407ca00afbc99fa2f62c",
                        "question": ["Give me the current SDP executive summary"],
                        "sql": [
                            "SELECT *\n",
                            f"FROM {CATALOG}.{SCHEMA}.metric_sdp_executive_summary",
                        ],
                    },
                ],
                key=lambda x: x["id"],
            ),
        },
    }
    return json.dumps(space)


def main() -> int:
    from databricks.sdk import WorkspaceClient

    host = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
    w = WorkspaceClient(host=host)

    existing = None
    for page in w.genie.list_spaces():
        for space in page.spaces or []:
            if space.title == TITLE:
                existing = space
                break
        if existing:
            break

    serialized = build_serialized_space()
    if existing:
        updated = w.genie.update_space(
            space_id=existing.space_id,
            title=TITLE,
            description=DESCRIPTION,
            warehouse_id=WAREHOUSE_ID,
            serialized_space=serialized,
        )
        print(json.dumps({"action": "updated", "space_id": updated.space_id, "title": updated.title}))
        return 0

    created = w.genie.create_space(
        warehouse_id=WAREHOUSE_ID,
        serialized_space=serialized,
        title=TITLE,
        description=DESCRIPTION,
        parent_path=PARENT_PATH,
    )
    print(json.dumps({"action": "created", "space_id": created.space_id, "title": created.title}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
