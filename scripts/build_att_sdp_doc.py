#!/usr/bin/env python3
"""Build ATT SDP architecture guide for Google Docs import.

Usage:
  python3 scripts/build_att_sdp_doc.py
  # Output: docs/ATT_SDP_GoogleDocs_Import/ATT_SDP_Component_Guide.docx
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "ATT_SDP_GoogleDocs_Import"
OUT_HTML = OUT_DIR / "ATT_SDP_Component_Guide.html"
OUT_DOCX = OUT_DIR / "ATT_SDP_Component_Guide.docx"
OUT_TXT = OUT_DIR / "ATT_SDP_Component_Guide.txt"

DOC_ID = "1D8VVsgzxcIrxunHY56dUiqSmD5GMZGIN74RdwvNdNBs"
DOC_URL = f"https://docs.google.com/document/d/{DOC_ID}/edit"

BODY = """
<h1>ATT Service Delivery Platform — Architecture &amp; Component Guide</h1>
<p><strong>Foundry → Databricks migration demo</strong> · Workspace: e2-demo-field-eng · Catalog: users.ankur_nayyar · Doc v2026-06-06</p>
<p><strong>Live app:</strong> att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com<br>
<strong>Lakebase:</strong> att-ankur-demo / sdp_ops<br>
<strong>Slides deck:</strong> docs/ATT_SDP_GoogleSlides_Import/</p>

<h2>1. Architecture — Three Paths</h2>
<p><strong>① Batch ingest (Foundry Builds → DLT)</strong><br>
Seed / Live NOC (notebooks 01, 07) → UC Volume sdp_exports → DLT sdp_service_delivery_dlt → Unity Catalog Gold → Semantic layer (mv_incident_dispatch_board, metric_* views) → App / Genie / Jobs</p>
<p><strong>② Ops read (Lakebase first)</strong><br>
Lakebase Postgres service_incident_ops (primary) ↔ UC mirror service_incident_ops_pg (fallback) + MV board structure → effective status on Dispatch &amp; KPIs</p>
<p><strong>③ Ops write (Foundry Actions → Lakebase OLTP)</strong><br>
App PATCH / Agent approve → Lakebase Postgres (primary) → UC writeback Delta (background) → Lakehouse sync → MV refresh → Genie</p>
<p><em>Mental model: DLT owns gold · Lakebase owns hot ops · MV joins for UI. DLT does NOT run on every dispatch click.</em></p>

<h2>2. Foundry → Databricks Mapping</h2>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Foundry</th><th>Databricks</th><th>Why</th></tr>
<tr><td>Objects</td><td>UC Delta gold tables</td><td>Governed entities with PK/FK</td></tr>
<tr><td>Links</td><td>bridge_incident_technician + MV</td><td>M:N dispatch relationships</td></tr>
<tr><td>Builds</td><td>DLT + bundle jobs</td><td>Bronze → gold transforms</td></tr>
<tr><td>Slate</td><td>metric_* views + AI/BI</td><td>Certified KPIs for dashboards &amp; Genie</td></tr>
<tr><td>Workshop</td><td>Ops Console App (5 tabs)</td><td>Operational UI with writeback</td></tr>
<tr><td>Quiver</td><td>Notebooks + Genie Space</td><td>NL / exploratory analytics</td></tr>
<tr><td>AIP</td><td>Agent Triage + AgentBricks</td><td>Tool-using agent + approval gate</td></tr>
<tr><td>Actions</td><td>Lakebase Postgres writeback</td><td>Sub-second ops edits</td></tr>
</table>

<h2>3. Component Catalog</h2>

<h3>3.1 Unity Catalog &amp; Storage</h3>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Component</th><th>Why</th><th>Where used</th></tr>
<tr><td>Catalog users · schema ankur_nayyar</td><td>Demo isolation</td><td>All gold, app SQL, Genie</td></tr>
<tr><td>UC Volume sdp_exports</td><td>Bronze JSON landing</td><td>DLT autoloader, notebooks 01/07/08</td></tr>
<tr><td>SQL Warehouse 03560442e95cb440</td><td>MV &amp; gold queries</td><td>App, Genie, agent SQL</td></tr>
<tr><td>Asset Bundle e2_demo</td><td>Deploy jobs &amp; DLT</td><td>CI, bundle run commands</td></tr>
</table>

<h3>3.2 Gold Ontology Tables (Foundry Objects)</h3>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Table</th><th>Purpose</th><th>Consumed by</th></tr>
<tr><td>service_incident</td><td>Core NOC incidents</td><td>DLT, MV, Dashboard, Genie, Agent</td></tr>
<tr><td>service_order</td><td>Fulfillment orders</td><td>Dashboard KPIs, Genie</td></tr>
<tr><td>customer_account</td><td>Account context</td><td>Dispatch MV, Genie</td></tr>
<tr><td>field_technician</td><td>Tech roster</td><td>MV, Agent recommend, Genie</td></tr>
<tr><td>bridge_incident_technician</td><td>M:N incident↔tech link</td><td>MV, Agent approve writes</td></tr>
<tr><td>service_incident_ops</td><td>Ops overlay (DLT VIEW)</td><td>MV definition; Lakebase sync</td></tr>
<tr><td>service_incident_ops_writeback</td><td>Writable Delta overlay</td><td>App background UC mirror</td></tr>
<tr><td>agent_audit_log</td><td>Agent tool audit</td><td>Agent Triage tab</td></tr>
</table>
<p><strong>Note:</strong> App cannot write directly to service_incident_ops (DLT view). Writes go to Lakebase Postgres first.</p>

<h3>3.3 DLT Pipeline (Builds)</h3>
<p><strong>Name:</strong> sdp_service_delivery_dlt · Serverless ADVANCED · Target: users.ankur_nayyar</p>
<p><strong>Why:</strong> Autoload bronze, data quality, materialize gold.<br>
<strong>Triggered by:</strong> sdp_semantic_setup (full), sdp_write_refresh (incremental, Live Demo tab).<br>
<strong>Source:</strong> src/pipelines/dlt/sdp_service_delivery.py</p>

<h3>3.4 Semantic Layer (Slate)</h3>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Asset</th><th>Type</th><th>Where used</th></tr>
<tr><td>mv_incident_dispatch_board</td><td>Materialized view</td><td>Dispatch tab, Genie, /api/incidents</td></tr>
<tr><td>metric_sdp_executive_summary</td><td>View</td><td>Dashboard, Genie</td></tr>
<tr><td>metric_open_incidents_by_market</td><td>View</td><td>Dashboard bars, Genie</td></tr>
<tr><td>metric_incident_mttr</td><td>View</td><td>Genie, notebooks</td></tr>
<tr><td>metric_technician_utilization</td><td>View</td><td>Genie</td></tr>
</table>
<p><strong>Deploy once:</strong> notebook 10_deploy_semantic_layer (sdp_semantic_setup)<br>
<strong>Refresh often:</strong> notebook 05 REFRESH MV only (sdp_write_refresh) — faster than full redeploy</p>

<h3>3.5 Lakebase (Actions / OLTP)</h3>
<p><strong>Project:</strong> att-ankur-demo · <strong>DB:</strong> sdp_ops · <strong>Schema:</strong> ankur_nayyar<br>
<strong>Writable:</strong> service_incident_ops (Postgres) · <strong>Direct host only</strong> (not pooler — SASL fails for Apps SP)</p>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Operation</th><th>Primary</th><th>Fallback</th></tr>
<tr><td>Status PATCH</td><td>Lakebase Postgres</td><td>UC service_incident_ops_writeback</td></tr>
<tr><td>Ops overlay read</td><td>Postgres direct</td><td>UC service_incident_ops_pg mirror</td></tr>
<tr><td>Board structure</td><td>UC mv_incident_dispatch_board</td><td>—</td></tr>
</table>
<p><strong>Sync:</strong> UC→PG synced tables (read replica) · PG→UC Lakehouse sync (writeback)<br>
<strong>Dispatch tab:</strong> Lakebase vs Warehouse latency toggle + insert simulation table</p>

<h3>3.6 Ops Console App (Workshop)</h3>
<p><strong>URL:</strong> att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com · <strong>SP:</strong> be33de06-36a1-467e-926b-902c55903267</p>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Tab</th><th>What it does</th><th>Data sources</th></tr>
<tr><td>Dashboard</td><td>Exec KPIs, markets, orders, techs</td><td>Gold + Lakebase ops overlay</td></tr>
<tr><td>Dispatch</td><td>Status buttons + latency demo</td><td>MV + Lakebase overlay; PATCH writeback</td></tr>
<tr><td>Agent Triage</td><td>Search → recommend → approve → write</td><td>Gold read; bridge + Lakebase write</td></tr>
<tr><td>Live Demo</td><td>Scenario picker + job stepper</td><td>sdp_write_refresh; Lakebase feed</td></tr>
<tr><td>Genie</td><td>NL analytics → charts/KPIs</td><td>Genie Space on warehouse</td></tr>
</table>

<h3>3.7 Genie Space (Quiver / Slate NL)</h3>
<p><strong>Space:</strong> att_sdp_service_delivery · <strong>ID:</strong> 01f160fc477b16c6a0f63d8164cff930<br>
<strong>Why:</strong> NL questions on governed UC assets only (read-only).<br>
<strong>Where:</strong> App Genie tab, standalone Genie UI, demo Part B.</p>

<h3>3.8 Agent Triage (AIP)</h3>
<p><strong>Agent:</strong> sdp_incident_triage · <strong>Config:</strong> config/sdp_agent_tools.yaml</p>
<p><strong>Tools:</strong> search_incidents, recommend_technician, assign_technician (writes after approval)<br>
<strong>Flow:</strong> P1 Dallas search → recommend tech → propose → human approve → bridge + Lakebase DISPATCHED<br>
<strong>Audit:</strong> agent_audit_log + MLflow</p>

<h3>3.9 Bundle Jobs</h3>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Job</th><th>When</th><th>Why</th></tr>
<tr><td>sdp_cleanup</td><td>Reset demo</td><td>Drop tables, clear volumes</td></tr>
<tr><td>sdp_semantic_setup</td><td>First deploy / after cleanup</td><td>DLT full + MV create + Lakebase sync (~8–20 min)</td></tr>
<tr><td>sdp_write_refresh</td><td>Live Demo default</td><td>Incremental bronze→DLT→sync→MV (~3–5 min)</td></tr>
<tr><td>sdp_full_pipeline</td><td>One-shot deploy</td><td>End-to-end automation</td></tr>
</table>
<p><strong>sdp_write_refresh order:</strong> 07 bronze append → DLT → 09 Lakebase sync → 05 MV refresh → 05c KPIs</p>

<h2>4. Data Flows</h2>
<h3>4.1 Dispatch status update</h3>
<p>User clicks status → PATCH /api/incidents/{id} → Lakebase Postgres MERGE → background UC writeback → MLflow → board shows effective status (MV + overlay)</p>
<h3>4.2 Live Demo</h3>
<p>Scenario → sdp_write_refresh → bronze append → DLT → Lakebase sync → MV refresh → App polls job + Lakebase feed</p>
<h3>4.3 Agent approve</h3>
<p>Search → recommend → propose → approve → bridge CONFIRMED + Lakebase DISPATCHED + audit log</p>
<h3>4.4 Genie</h3>
<p>NL question → Genie Space SQL → charts/KPIs in app → MLflow tracked</p>

<h2>5. Environment Reference</h2>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Item</th><th>Value</th></tr>
<tr><td>Workspace</td><td>e2-demo-field-eng</td></tr>
<tr><td>Catalog.schema</td><td>users.ankur_nayyar</td></tr>
<tr><td>SQL warehouse</td><td>03560442e95cb440</td></tr>
<tr><td>App SP</td><td>be33de06-36a1-467e-926b-902c55903267</td></tr>
<tr><td>Genie space ID</td><td>01f160fc477b16c6a0f63d8164cff930</td></tr>
</table>

<h2>6. Common Misconceptions</h2>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Myth</th><th>Reality</th></tr>
<tr><td>App reads everything from Lakebase</td><td>Board structure from UC MV; only ops overlay is Lakebase-first</td></tr>
<tr><td>DLT runs on every dispatch</td><td>Dispatch is OLTP via Lakebase; DLT runs on batch jobs</td></tr>
<tr><td>Genie writes data</td><td>Genie is read-only</td></tr>
<tr><td>Use Lakebase pooler for Apps</td><td>Direct Postgres host required (pooler → SASL failure)</td></tr>
</table>

<h2>7. Who Owns What</h2>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Concern</th><th>Owner</th></tr>
<tr><td>Entity definitions</td><td>UC gold DDL + DLT</td></tr>
<tr><td>Batch transforms</td><td>DLT pipeline</td></tr>
<tr><td>Operational writes</td><td>Lakebase Postgres</td></tr>
<tr><td>Fast ops reads</td><td>Lakebase (+ UC mirror fallback)</td></tr>
<tr><td>Dispatch board shape</td><td>Materialized view</td></tr>
<tr><td>Executive KPIs</td><td>Semantic metric_* views</td></tr>
<tr><td>Operational UI</td><td>Ops Console App</td></tr>
<tr><td>NL analytics</td><td>Genie Space</td></tr>
<tr><td>Agentic triage</td><td>Agent Triage tab</td></tr>
<tr><td>Pipeline orchestration</td><td>Bundle jobs</td></tr>
<tr><td>Audit</td><td>agent_audit_log, MLflow</td></tr>
</table>

<h2>8. Related Repo Docs</h2>
<p>ATT_SDP_RUNBOOK.md · ATT_SDP_COMPONENTS.md · ATT_SDP_GoogleSlides_Import/ · docs/GOOGLE_OAUTH_SETUP.md</p>
<p><strong>Commands:</strong><br>
databricks bundle deploy -t e2_demo -p e2-demo-field-eng<br>
databricks bundle run sdp_write_refresh -t e2_demo -p e2-demo-field-eng</p>
"""


def build_html() -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>ATT SDP Component Guide</title>
<style>
body {{ font-family: Arial, sans-serif; font-size: 11pt; line-height: 1.45; max-width: 8in; margin: 1in; }}
h1 {{ font-size: 20pt; color: #0a2540; }}
h2 {{ font-size: 14pt; color: #0095da; margin-top: 1.2em; border-bottom: 1px solid #ccc; }}
h3 {{ font-size: 12pt; color: #333; }}
table {{ border-collapse: collapse; width: 100%; margin: 0.5em 0; font-size: 10pt; }}
th {{ background: #0a2540; color: white; text-align: left; }}
td, th {{ border: 1px solid #ccc; padding: 6px; vertical-align: top; }}
</style></head><body>
{BODY}
</body></html>"""


def html_to_docx(html_path: Path, docx_path: Path) -> bool:
    r = subprocess.run(
        ["textutil", "-convert", "docx", str(html_path), "-output", str(docx_path)],
        capture_output=True,
        text=True,
    )
    return r.returncode == 0 and docx_path.exists()


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    html = build_html()
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")

    # Plain text fallback
    import re
    text = re.sub(r"<[^>]+>", "\n", BODY)
    text = re.sub(r"\n{3,}", "\n\n", text)
    OUT_TXT.write_text(
        f"ATT SDP Component Guide · v2026-06-06\nGoogle Doc: {DOC_URL}\n\n{text.strip()}\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUT_TXT}")

    if html_to_docx(OUT_HTML, OUT_DOCX):
        print(f"Wrote {OUT_DOCX} ({OUT_DOCX.stat().st_size // 1024} KB)")
    else:
        print("DOCX conversion skipped — use HTML import or copy/paste from .txt")

    import_txt = OUT_DIR / "IMPORT.txt"
    import_txt.write_text(
        f"""ATT SDP Component Guide — Google Docs import
=============================================
Target doc: {DOC_URL}

OPTION 1 — Import DOCX (recommended)
  1. Open the Google Doc link above
  2. File → Import → Upload
  3. Select: ATT_SDP_Component_Guide.docx
  4. Choose "Replace current document" or insert at end

OPTION 2 — Upload via Drive
  1. drive.google.com → Upload ATT_SDP_Component_Guide.docx
  2. Right-click → Open with → Google Docs
  3. Copy content into your target doc if needed

OPTION 3 — Copy/paste
  Open ATT_SDP_Component_Guide.txt → Select all → Paste into Google Doc

Verify: Title shows "Doc v2026-06-06" in section 1 header.

Auto-upload (OAuth): python3 scripts/google_docs_upload.py --build
""",
        encoding="utf-8",
    )
    print(f"Wrote {import_txt}")


if __name__ == "__main__":
    main()
