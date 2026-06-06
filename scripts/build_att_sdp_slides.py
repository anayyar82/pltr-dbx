#!/usr/bin/env python3
"""Generate ATT SDP architecture deck (HTML + PDF + speaker notes).

Usage:
  python3 scripts/build_att_sdp_slides.py
  open presentations/ATT_SDP_Architecture.html
  # Google Slides: File → Import slides → presentations/ATT_SDP_Architecture.pdf → Replace all
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "presentations"
OUT_HTML = OUT_DIR / "ATT_SDP_Architecture.html"
OUT_PDF = OUT_DIR / "ATT_SDP_Architecture.pdf"
OUT_NOTES = OUT_DIR / "ATT_SDP_Speaker_Notes.md"

DIAGRAM_HTML = """
            <div class="diagram">
              <div class="row label">① BATCH INGEST (Foundry Builds → DLT)</div>
              <div class="row flow">
                <div class="box">Seed / Live NOC<br><small>nb 01 · 07</small></div><span>→</span>
                <div class="box">UC Volume<br><small>sdp_exports</small></div><span>→</span>
                <div class="box">DLT Pipeline<br><small>sdp_service_delivery_dlt</small></div><span>→</span>
                <div class="box hi">Unity Catalog Gold</div>
              </div>
              <div class="row flow">
                <div class="box wide">Semantic: mv_incident_dispatch_board · metric_* KPI views · notebook 05/10</div>
              </div>
              <div class="row flow">
                <div class="box hi">Dashboard</div>
                <div class="box hi">Dispatch</div>
                <div class="box hi">Agent Triage</div>
                <div class="box hi">Live Demo</div>
                <div class="box hi">Genie</div>
              </div>
              <div class="row label">② OPS READ — Lakebase first (sub-second overlay)</div>
              <div class="row flow">
                <div class="box wb">Lakebase Postgres<br><small>service_incident_ops</small></div>
                <span>↔ fallback</span>
                <div class="box">UC mirror<br><small>service_incident_ops_pg</small></div>
                <span>+</span>
                <div class="box">MV structure<br><small>mv_incident_dispatch_board</small></div>
                <span>→</span>
                <div class="box hi">Effective status on board &amp; KPIs</div>
              </div>
              <div class="row label">③ WRITEBACK (Foundry Actions → Lakebase OLTP)</div>
              <div class="row flow">
                <div class="box wb">App PATCH · Agent approve</div><span>→</span>
                <div class="box wb">Lakebase Postgres (primary)</div><span>→</span>
                <div class="box">UC writeback Delta<br><small>service_incident_ops_writeback</small></div>
                <span>→</span>
                <div class="box">Lakehouse sync · MV refresh</div>
              </div>
              <div class="row flow">
                <div class="box">Synced UC→PG: service_incident_pg · service_incident_ops_pg</div>
              </div>
            </div>"""


@dataclass
class Slide:
    title: str
    subtitle: str = ""
    body: list[tuple[str, dict]] = field(default_factory=list)
    notes: str = ""
    kind: str = "content"  # content | diagram


def _b(text: str, **kw) -> tuple[str, dict]:
    return (text, {"bullet": True, **kw})


SLIDES: list[Slide] = [
    Slide(
        "ATT Service Delivery Platform",
        "Foundry → Databricks · Live demo stack",
        [
            ("Governed lakehouse + Lakebase OLTP + Genie + Agents + Apps", {"size": 2200, "color": "E8EEF7"}),
            ("", {}),
            ("Workspace: e2-demo-field-eng · Catalog: users.ankur_nayyar", {"size": 1900, "color": "8FA3BF"}),
            ("App: att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com", {"size": 1900, "color": "8FA3BF"}),
            ("Lakebase: att-ankur-demo / sdp_ops · Deck v2026-06-06", {"size": 1900, "color": "8FA3BF"}),
        ],
        notes="Live deployed workspace — not a mockup. Same SDP ontology as Foundry, new consumption layer.",
    ),
    Slide(
        "Agenda",
        "",
        [
            _b("Foundry → Databricks mapping"),
            _b("Architecture: batch · ops read · ops write"),
            _b("Component catalog — what each piece does & where it is used"),
            _b("Gold ontology · DLT · Semantic layer · Lakebase"),
            _b("Ops Console App (5 tabs) · Agent Triage · Genie"),
            _b("Jobs, data flows & demo script"),
        ],
        notes="~25 slides. Deep-dive on components is the new section for technical audiences.",
    ),
    Slide(
        "Foundry → Databricks Mapping",
        "Same capabilities — different platform primitives",
        [
            _b("Objects → UC Delta gold (service_incident, service_order, …)"),
            _b("Links → bridge_incident_technician + dispatch MV joins"),
            _b("Builds → DLT sdp_service_delivery_dlt + bundle jobs"),
            _b("Slate → metric_* views + AI/BI dashboards + Genie"),
            _b("Workshop → Databricks App Ops Console (5 tabs)"),
            _b("Quiver → Notebooks + Genie Space NL analytics"),
            _b("AIP → Agent Triage tab + AgentBricks (sdp_incident_triage)"),
            _b("Actions → Lakebase Postgres writeback + Lakehouse sync"),
        ],
        notes="Anchor for Foundry audiences: Actions are OLTP on Lakebase, not batch MERGE into gold.",
    ),
    Slide(
        "Architecture — Three Paths",
        "What runs when (avoid mixing batch and OLTP)",
        [
            _b("Batch path: Volume → DLT → Gold → MV/KPIs — canonical lakehouse data"),
            _b("Ops read: Lakebase Postgres first for status; UC mirror fallback; MV for board shape"),
            _b("Ops write: App/Agent → Lakebase Postgres → background UC writeback Delta"),
            _b("DLT does NOT run on every dispatch click — only on workflow jobs"),
            ("", {}),
            ("Mental model: DLT owns gold · Lakebase owns hot ops · MV joins for UI", {"size": 1800, "color": "9ECBFF"}),
        ],
        notes="Common misconception: app is not Lakebase-only. Board structure from UC MV; overlay from Lakebase.",
    ),
    Slide(
        "End-to-End Architecture Diagram",
        "Batch (top) · Ops read (middle) · Writeback (bottom)",
        kind="diagram",
        notes="Walk ① batch, ② read overlay, ③ writeback. Point to Live Demo job for ① and Dispatch for ③.",
    ),
    Slide(
        "Component: Unity Catalog & Storage",
        "Why: governed demo isolation · Where: all SQL, Genie, App",
        [
            _b("Catalog users · schema ankur_nayyar — gold + semantic home"),
            _b("UC Volume sdp_exports — bronze JSON landing (seed + live append)"),
            _b("SQL Warehouse 03560442e95cb440 — MV queries, Genie, agent SQL"),
            _b("Asset Bundle databricks.yml target e2_demo — jobs, DLT, deploy"),
            ("Repo: src/ontology/ddl/ · notebooks/01_bootstrap_bronze_seed.py", {"size": 1500, "color": "8FA3BF"}),
        ],
        notes="Volume is the bronze landing zone — same role as Foundry dataset exports.",
    ),
    Slide(
        "Component: Gold Ontology Tables",
        "Why: Foundry Objects with PK/FK · Where: DLT, MV, Genie, Agent",
        [
            _b("service_incident — core NOC incidents → Dashboard, Dispatch, search"),
            _b("service_order — fulfillment → provisioning KPIs, Genie"),
            _b("customer_account · field_technician — context on dispatch board"),
            _b("bridge_incident_technician — M:N link → Agent approve writes"),
            _b("service_incident_ops — DLT VIEW (not directly writable by App)"),
            _b("service_incident_ops_writeback — writable Delta overlay (background mirror)"),
            _b("agent_audit_log — every agent tool call for governance"),
        ],
        notes="App cannot MERGE into service_incident_ops — DLT owns it as a view. Writes go to Lakebase first.",
    ),
    Slide(
        "Component: DLT Pipeline",
        "Why: replaces Foundry Builds · Where: sdp_write_refresh, sdp_semantic_setup",
        [
            _b("Name: sdp_service_delivery_dlt — serverless, ADVANCED"),
            _b("Bronze raw_* autoload from UC Volume → Silver stg_* → Gold tables"),
            _b("Data quality expectations on pipeline (Foundry checks equivalent)"),
            _b("Incremental: sdp_write_refresh (Live Demo tab — ~3–5 min)"),
            _b("Full refresh: sdp_semantic_setup (first deploy / reset — ~8–20 min)"),
            ("Source: src/pipelines/dlt/sdp_service_delivery.py", {"size": 1500, "color": "8FA3BF"}),
        ],
        notes="Live Demo uses incremental DLT. Full semantic setup includes DLT full refresh — that's why rebuild feels slow.",
    ),
    Slide(
        "Component: Semantic Layer",
        "Why: Slate certified metrics · Where: Dashboard, Dispatch, Genie",
        [
            _b("mv_incident_dispatch_board — MV: incident + account + tech + ops join"),
            _b("metric_sdp_executive_summary — single-row exec KPIs"),
            _b("metric_open_incidents_by_market · metric_incident_mttr · metric_technician_utilization"),
            _b("Deploy once: notebook 10 (CREATE MV + KPI views)"),
            _b("Refresh often: notebook 05 REFRESH MV only (sdp_write_refresh job)"),
            _b("App overlays Lakebase ops status ON TOP of MV — effective status in UI"),
        ],
        notes="Don't run notebook 10 every refresh — use 05. KPI views are instant (metadata-only).",
    ),
    Slide(
        "Component: Lakebase (Actions / OLTP)",
        "Why: sub-second ops writes · Where: Dispatch PATCH, Agent approve",
        [
            _b("Project att-ankur-demo · DB sdp_ops · schema ankur_nayyar"),
            _b("Writable: service_incident_ops (Postgres) — primary PATCH target"),
            _b("Direct host only — pooler causes SASL failure for Apps SP OAuth"),
            _b("Synced UC→PG: service_incident_pg, service_incident_ops_pg (read replica)"),
            _b("Lakehouse sync PG→UC keeps lakehouse consistent for DLT/MV/Genie"),
            _b("Dispatch tab: toggle Lakebase vs Warehouse to demo latency (ms)"),
            ("Setup: nb 06 · scripts/setup_lakebase_app_oauth.py", {"size": 1500, "color": "8FA3BF"}),
        ],
        notes="Foundry Actions story. Insert simulation table lakebase_latency_demo shows write speed vs warehouse.",
    ),
    Slide(
        "Component: Ops Console App",
        "Why: Workshop operational UI · Where: att-sdp-ops-ankur Databricks App",
        [
            _b("Dashboard — exec KPIs, markets, orders, techs (gold + Lakebase overlay)"),
            _b("Dispatch — MV board + status buttons + Lakebase/warehouse latency toggle"),
            _b("Agent Triage — search → recommend → propose → human approve → write"),
            _b("Live Demo — scenario picker · sdp_write_refresh stepper · Lakebase feed"),
            _b("Genie — NL questions → auto charts/KPIs · MLflow tracked"),
            _b("SP be33de06-… · /health shows lakebase_ok, ops_read_path, write_path"),
        ],
        notes="Five tabs map to Foundry Workshop + Quiver + AIP in one shell.",
    ),
    Slide(
        "Component: Agent Triage (AIP)",
        "Why: autonomous triage with approval gate · Where: App tab + AgentBricks config",
        [
            _b("Tools: search_incidents · recommend_technician · assign_technician"),
            _b("Demo flow: P1 Dallas → recommend TECH → propose INC-9001 → approve"),
            _b("Writes: bridge_incident_technician CONFIRMED + Lakebase DISPATCHED"),
            _b("Audit: agent_audit_log + MLflow agent_dispatch_approved"),
            _b("Config: config/sdp_agent_tools.yaml · src/agents/sdp_incident_agent.py"),
            _b("Human approval required — no autonomous writes without operator click"),
        ],
        notes="In-app demo is ready today. Workspace AgentBricks agent is optional (runbook Part C).",
    ),
    Slide(
        "Component: Genie Space",
        "Why: NL analytics (Quiver/Slate) · Where: App Genie tab + workspace Genie UI",
        [
            _b("Space att_sdp_service_delivery · ID in config/genie_space.yaml"),
            _b("Governed tables: service_incident, MV, metric_* views only"),
            _b("In-app: POST /api/genie/ask → charts, KPI cards, tables (viz.py)"),
            _b("Demo chips: P1 Dallas · Executive summary · Market bar chart · MTTR"),
            _b("Read-only — Genie never writes; dispatch writes go to Lakebase"),
            _b("Low-code: business users ask questions without SQL"),
        ],
        notes="Open Genie Space link from app for native AI/BI experience.",
    ),
    Slide(
        "Component: Bundle Jobs (Workflows)",
        "Why: orchestrate pipeline · Where: Live Demo tab, CLI, CI",
        [
            _b("sdp_cleanup — reset demo (drop tables, clear volumes)"),
            _b("sdp_semantic_setup — bootstrap + DLT full + nb10 + Lakebase sync (~8–20 min)"),
            _b("sdp_write_refresh — bronze append + DLT incr + sync + MV refresh (~3–5 min)"),
            _b("sdp_full_pipeline — one-shot end-to-end deploy"),
            _b("App Live Demo → sdp_write_refresh · Rebuild semantic → sdp_semantic_setup"),
            ("Task order: 07 bronze → DLT → 09 Lakebase sync → 05 MV refresh → 05c KPIs", {"size": 1500, "color": "9ECBFF"}),
        ],
        notes="Use write_refresh for demos. semantic_setup only after cleanup or schema change.",
    ),
    Slide(
        "Component: MLflow & Observability",
        "Why: demo event tracking · Where: Genie, dispatch, agent, live jobs",
        [
            _b("Experiment /Shared/att-sdp-ops-console"),
            _b("Tracks: incident_writeback · genie_ask · live_job_trigger · agent_dispatch"),
            _b("Genie tab shows MLflow badge with experiment link"),
            _b("agent_audit_log — SQL-auditable tool call history in UC"),
            ("Module: apps/sdp_ops_console/mlflow_tracker.py", {"size": 1500, "color": "8FA3BF"}),
        ],
        notes="Governance story alongside agent audit log.",
    ),
    Slide(
        "Data Flow: Dispatch Status Update",
        "Workshop + Actions pattern",
        [
            _b("User clicks IN_PROGRESS / DISPATCHED / RESOLVED in Dispatch tab"),
            _b("PATCH /api/incidents/{id} → Lakebase Postgres MERGE (primary, ~ms)"),
            _b("Background: UC Delta MERGE service_incident_ops_writeback"),
            _b("Background: MLflow incident_writeback event"),
            _b("App re-reads ops overlay from Lakebase (or UC mirror fallback)"),
            _b("Board shows effective status = MV structure + Lakebase overlay"),
        ],
        notes="Live demo: toggle write path Warehouse vs Lakebase to show latency difference.",
    ),
    Slide(
        "Data Flow: Live Demo Pipeline",
        "Builds pattern — batch, not OLTP",
        [
            _b("User picks scenario (e.g. new_p1_dallas) → POST /api/live/trigger"),
            _b("Job sdp_write_refresh: notebook 07 append JSON to bronze volume"),
            _b("DLT incremental refresh → gold tables updated"),
            _b("Notebook 09 triggers Lakebase synced table pipelines"),
            _b("Notebook 05 REFRESH mv_incident_dispatch_board"),
            _b("App polls job steps + Lakebase feed + KPI cards update"),
        ],
        notes="Show pipeline progress table in Live Demo tab — steps in job order.",
    ),
    Slide(
        "Data Flow: Agent Dispatch Approval",
        "AIP pattern with human gate",
        [
            _b("Agent searches UC gold (search_incidents tool)"),
            _b("Recommends technician by market/skill (recommend_technician)"),
            _b("Proposes assignment — approval card shown to operator"),
            _b("On approve: bridge_incident_technician CONFIRMED"),
            _b("Lakebase ops status DISPATCHED + agent_audit_log entries"),
            _b("Background MV refresh — board reflects assignment"),
        ],
        notes="Run full demo flow chip in Agent Triage tab.",
    ),
    Slide(
        "Low-Code / No-Code in the Demo",
        "What business users can do without notebooks",
        [
            _b("Genie tab — NL questions → auto dashboards (no SQL)"),
            _b("Dashboard tab — certified KPIs from metric_* views"),
            _b("Live Demo — one-click workflow (sdp_write_refresh)"),
            _b("Dispatch — click status buttons (Lakebase writeback)"),
            _b("Agent Triage — conversational chips + approve button"),
            _b("Workspace: Genie Space · Workflows UI · Catalog Explorer · DLT UI"),
        ],
        notes="10-min arc: Dashboard → Genie chips → Live Demo job → Agent approve → Dispatch write.",
    ),
    Slide(
        "Demo Script (45 min)",
        "",
        [
            _b("1 · sdp_write_refresh or Live Demo tab — prove Builds/DLT"),
            _b("2 · Catalog / SQL — show MV + metric_* views"),
            _b("3 · Genie — 'How many P1 in Dallas?' + executive summary"),
            _b("4 · Agent Triage — full demo flow → approve dispatch"),
            _b("5 · Dispatch — status update + Lakebase latency toggle"),
            _b("6 · SQL verify — service_incident_ops + mv_incident_dispatch_board"),
            ("Baseline: 5 open · 2 P1 · 3 provisioning · 5 techs available", {"size": 1800, "color": "9ECBFF"}),
        ],
        notes="Project guide: docs/ATT_SDP_Project_Guide.docx",
    ),
    Slide(
        "Key URLs & Commands",
        "",
        [
            ("App: att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com", {"size": 1500, "bullet": True}),
            ("Lakebase: att-ankur-demo · Genie space: 01f160fc477b16c6a0f63d8164cff930", {"size": 1500, "bullet": True}),
            ("Docs: ATT_SDP_RUNBOOK.md · ATT_SDP_COMPONENTS.md", {"size": 1500, "bullet": True}),
            ("", {}),
            ("databricks bundle deploy -t e2_demo -p e2-demo-field-eng", {"size": 1500, "color": "9ECBFF"}),
            ("databricks bundle run sdp_write_refresh -t e2_demo", {"size": 1500, "color": "9ECBFF"}),
        ],
        notes="Import PDF into Google Slides: File → Import slides → Replace all.",
    ),
    Slide(
        "Summary",
        "Governed lakehouse + Lakebase OLTP + AI analytics",
        [
            _b("Batch: DLT → UC gold → MV + metric_* semantic layer"),
            _b("Ops: Lakebase Postgres read/write (Foundry Actions)"),
            _b("UI: 5-tab Ops Console — Dashboard, Dispatch, Agent, Live, Genie"),
            _b("Agentic: approval-gated dispatch with audit trail"),
            _b("Analytics: Genie NL on certified views — low-code for business users"),
            _b("Each component has a clear why & where — see ATT_SDP_COMPONENTS.md"),
        ],
        notes="Closing slide. Q&A.",
    ),
]


def build_speaker_notes(path: Path) -> None:
    lines = ["# ATT SDP Architecture — Speaker Notes\n"]
    for i, slide in enumerate(SLIDES, start=1):
        lines.append(f"## Slide {i}: {slide.title}\n")
        if slide.subtitle:
            lines.append(f"*{slide.subtitle}*\n")
        if slide.notes:
            lines.append(slide.notes.strip() + "\n")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {path}")


def build_html(path: Path) -> None:
    slides_html = []
    for i, slide in enumerate(SLIDES):
        bullets = "".join(
            f"<li>{escape(text)}</li>"
            for text, opts in slide.body
            if text and opts.get("bullet")
        )
        extra = "".join(
            f"<p class='{'accent' if opts.get('color') == '9ECBFF' else 'meta'}'>{escape(text)}</p>"
            for text, opts in slide.body
            if text and not opts.get("bullet")
        )
        sub = f"<p class='subtitle'>{escape(slide.subtitle)}</p>" if slide.subtitle else ""
        if slide.kind == "diagram":
            content = DIAGRAM_HTML
        else:
            content = f"<ul>{bullets}</ul>{extra}" if bullets or extra else ""
        slides_html.append(
            f'<section class="slide" id="s{i}"><h1>{escape(slide.title)}</h1>{sub}{content}</section>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>ATT SDP Architecture</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", system-ui, sans-serif; background: #0b1220; color: #e8eef7; }}
    .slide {{ display: none; min-height: 100vh; padding: 2rem 3rem; border-bottom: 4px solid #0095da; }}
    .slide.active {{ display: block; }}
    h1 {{ font-size: 1.85rem; margin: 0 0 0.4rem; }}
    .subtitle {{ color: #9ecbff; margin: 0 0 0.75rem; font-size: 0.95rem; }}
    ul {{ font-size: 0.95rem; line-height: 1.45; margin: 0; padding-left: 1.2rem; }}
    li {{ margin: 0.22rem 0; }}
    .meta {{ color: #8fa3bf; font-size: 0.85rem; }}
    .accent {{ color: #9ecbff; font-family: ui-monospace, monospace; font-size: 0.82rem; }}
    .nav {{ position: fixed; bottom: 1rem; right: 1rem; color: #8fa3bf; font-size: 0.9rem; }}
    body.capture .nav {{ display: none !important; }}
    body.capture {{ background: #0b1220; }}
    body.capture .slide {{ display: none !important; min-height: 0; padding: 0; border: none; }}
    body.capture .slide.active {{
      display: block !important;
      width: 1280px; height: 720px; min-height: 720px; max-height: 720px;
      overflow: hidden; padding: 36px 48px; box-sizing: border-box;
    }}
    @media print {{
      @page {{ size: 13.333in 7.5in; margin: 0; }}
      body {{ background: #0b1220; margin: 0; }}
      .slide {{
        display: block !important;
        page-break-after: always;
        break-after: page;
        width: 13.333in;
        height: 7.5in;
        min-height: 7.5in;
        max-height: 7.5in;
        overflow: hidden;
        padding: 0.5in 0.7in;
        box-sizing: border-box;
      }}
      .nav {{ display: none !important; }}
    }}
    .diagram .row {{ display: flex; align-items: center; gap: 0.35rem; flex-wrap: wrap; margin: 0.35rem 0; }}
    .diagram .label {{ color: #9ecbff; font-weight: 600; font-size: 0.78rem; width: 100%; margin-top: 0.25rem; }}
    .diagram .box {{ background: #141e30; border: 1px solid #0095da; border-radius: 8px; padding: 0.4rem 0.6rem; font-size: 0.72rem; line-height: 1.25; }}
    .diagram .box small {{ color: #8fa3bf; font-size: 0.62rem; }}
    .diagram .box.hi {{ background: #003d66; border-color: #43a047; }}
    .diagram .box.wb {{ border-color: #fb8c00; }}
    .diagram .box.wide {{ min-width: 88%; }}
    .diagram span {{ color: #0095da; font-weight: bold; font-size: 0.8rem; }}
  </style>
</head>
<body>
{''.join(slides_html)}
<div class="nav">← → to navigate · slide <span id="num">1</span> / {len(SLIDES)}</div>
<script>
  const slides = [...document.querySelectorAll('.slide')];
  let idx = 0;
  function show(n) {{
    idx = Math.max(0, Math.min(slides.length - 1, n));
    slides.forEach((s, i) => s.classList.toggle('active', i === idx));
    document.getElementById('num').textContent = idx + 1;
  }}
  document.addEventListener('keydown', e => {{
    if (e.key === 'ArrowRight' || e.key === ' ') show(idx + 1);
    if (e.key === 'ArrowLeft') show(idx - 1);
  }});
  const params = new URLSearchParams(location.search);
  if (params.get('capture') === '1') document.body.classList.add('capture');
  const start = parseInt(params.get('slide') || '0', 10);
  show(Number.isFinite(start) ? start : 0);
</script>
</body>
</html>"""
    path.write_text(html, encoding="utf-8")
    print(f"Wrote {path} ({len(SLIDES)} slides)")


def build_pdf(html_path: Path, pdf_path: Path) -> None:
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if not Path(chrome).exists():
        print("Skip PDF — Chrome not found")
        return
    r = subprocess.run(
        [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path}",
            f"file://{html_path.resolve()}",
        ],
        capture_output=True,
        text=True,
    )
    if pdf_path.exists() and pdf_path.stat().st_size > 1000:
        print(f"Wrote {pdf_path} ({pdf_path.stat().st_size // 1024} KB)")
    else:
        print(f"PDF generation failed: {r.stderr[:300]}")


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    build_speaker_notes(OUT_NOTES)
    build_html(OUT_HTML)
    build_pdf(OUT_HTML, OUT_PDF)
