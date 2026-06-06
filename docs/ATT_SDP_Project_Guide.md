# ATT Service Delivery Platform — Project Guide

**Foundry → Databricks migration demo** · Version 2026-06-06

---

## 1. Overview

This project (`pltr-dbx`) implements the **ATT Service Delivery Platform (SDP)** on Databricks — a governed operational lakehouse with low-latency writeback, Genie analytics, agentic triage, and a five-tab Ops Console App. It mirrors Palantir Foundry concepts (Objects, Builds, Workshop, Actions, AIP, Slate, Quiver) using Unity Catalog, DLT, Lakebase, Databricks Apps, and Genie.

### 1.1 Live environment

| Item | Value |
|------|-------|
| Workspace | e2-demo-field-eng |
| Bundle target | e2_demo |
| Catalog.schema | users.ankur_nayyar |
| CLI profile | e2-demo-field-eng |
| SQL warehouse | 03560442e95cb440 |
| Ops Console App | https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/ |
| Lakebase project | att-ankur-demo (DB: sdp_ops, schema: ankur_nayyar) |
| App service principal | be33de06-36a1-467e-926b-902c55903267 |
| Genie space ID | 01f160fc477b16c6a0f63d8164cff930 |
| Baseline KPIs (seed) | 5 open incidents · 2 P1 · 3 provisioning orders · 5 available techs |

---

## 2. Architecture — three data paths

Understanding **which path runs when** is critical for demos and troubleshooting.

### 2.1 Batch ingest path (Foundry Builds → DLT)

**Flow:** Seed / live NOC data → UC Volume `sdp_exports` → DLT `sdp_service_delivery_dlt` → Unity Catalog gold tables → semantic layer (MV + metric_* views) → App / Genie / jobs

**When it runs:** Jobs `sdp_write_refresh` (incremental, ~3–5 min), `sdp_semantic_setup` (full, ~8–20 min), or Live Demo tab in the App.

**Why:** Canonical lakehouse data with data quality expectations. Replaces Foundry pipeline builds.

### 2.2 Ops read path (Lakebase-first overlay)

**Flow:** Lakebase Postgres `service_incident_ops` (primary) ↔ UC mirror `service_incident_ops_pg` (fallback) + MV `mv_incident_dispatch_board` (board structure) → effective status on Dispatch board and KPIs

**When it runs:** Every App dashboard/dispatch load; toggle Lakebase vs Warehouse in Dispatch tab for latency demo.

**Why:** Sub-second reads for operational status without waiting for SQL warehouse on every overlay fetch.

### 2.3 Ops write path (Foundry Actions → Lakebase OLTP)

**Flow:** App PATCH or Agent approve → Lakebase Postgres MERGE → background UC Delta `service_incident_ops_writeback` → Lakehouse sync → optional MV refresh

**When it runs:** User clicks status in Dispatch; Agent Triage approval.

**Why:** Foundry Actions pattern — human ops edits must not require DLT or warehouse MERGE as primary path.

**Key rule:** DLT does **not** run on every dispatch click. Dispatch is OLTP via Lakebase.

---

## 3. Foundry → Databricks mapping

| Foundry | Databricks | Repo location |
|---------|------------|---------------|
| Objects | UC Delta gold tables | src/ontology/ddl/att_sdp_*.sql |
| Links | bridge_incident_technician + MV joins | mv_incident_dispatch_board |
| Builds | DLT + bundle jobs | src/pipelines/dlt/sdp_service_delivery.py |
| Slate | metric_* KPI views + AI/BI | src/semantic/metrics/sdp_kpis.sql |
| Workshop | Databricks App (Ops Console) | apps/sdp_ops_console/ |
| AIP | Agent Triage + AgentBricks | config/sdp_agent_tools.yaml, app.py |
| Quiver | Notebooks + Genie Space | notebooks/, config/genie_space.yaml |
| Actions | Lakebase Postgres writeback | config/lakebase.yaml, notebook 06 |

---

## 4. Component catalog — what we use and why

### 4.1 Unity Catalog & storage

| Component | Why we use it | Where it is used |
|-----------|---------------|------------------|
| Catalog users, schema ankur_nayyar | Isolated demo gold + semantic home | All SQL, Genie, App, agents |
| UC Volume sdp_exports | Bronze JSON landing (seed + live append) | DLT autoloader; notebooks 01, 07, 08 |
| SQL Warehouse 03560442e95cb440 | MV queries, gold joins, Genie SQL | App DATABRICKS_WAREHOUSE_ID; Genie |
| Asset Bundle (databricks.yml, e2_demo) | Deploy jobs, DLT, notebooks to workspace | bundle deploy / bundle run |

### 4.2 Gold ontology tables (Foundry Objects)

| Table | Purpose | Why | Consumed by |
|-------|---------|-----|-------------|
| service_incident | Core NOC incidents | Primary operational entity | DLT, MV, Dashboard, Genie, Agent search |
| service_order | Fulfillment orders | Provisioning KPIs | Dashboard, Genie, live stats |
| customer_account | Enterprise/residential accounts | Dispatch context | MV, Genie |
| field_technician | Tech roster & availability | Dispatch recommendations | MV, Agent recommend_technician |
| bridge_incident_technician | M:N incident ↔ technician | Foundry link type | MV, Agent approve writes |
| service_incident_ops | Ops overlay fields | Canonical ops in lakehouse | MV definition; Lakebase sync source |
| service_incident_ops_writeback | Writable Delta overlay | DLT owns ops as VIEW — app needs writable UC target | Background mirror after Lakebase write |
| agent_audit_log | Agent tool audit trail | Governance (AIP logging) | Agent Triage tab |

**Important:** App cannot MERGE into `service_incident_ops` directly — DLT materializes it as a **view**. Writes go to **Lakebase Postgres** first.

### 4.3 Delta Live Tables pipeline

| Item | Detail |
|------|--------|
| Name | sdp_service_delivery_dlt |
| Type | Serverless DLT, ADVANCED |
| Source | src/pipelines/dlt/sdp_service_delivery.py |
| Target schema | users.ankur_nayyar |

**Why:** Replaces Foundry Builds — autoload bronze JSON, apply expectations, materialize gold.

**Triggered by:**
- sdp_semantic_setup — full refresh (bootstrap, after cleanup)
- sdp_write_refresh — incremental (Live Demo tab default)
- sdp_full_pipeline — end-to-end deploy

### 4.4 Semantic layer (Foundry Slate)

| Asset | Type | Why | Where used |
|-------|------|-----|------------|
| mv_incident_dispatch_board | Materialized view | One join-friendly dispatch object | Dispatch tab, Genie, /api/incidents |
| metric_sdp_executive_summary | View | Single-row exec KPIs | Dashboard, Genie |
| metric_open_incidents_by_market | View | Market/severity breakdown | Dashboard bars, Genie charts |
| metric_incident_mttr | View | MTTR analytics | Genie |
| metric_order_fulfillment_sla | View | Order SLA | Genie |
| metric_technician_utilization | View | Tech utilization | Genie |

**Deploy once:** notebook 10_deploy_semantic_layer (job sdp_semantic_setup)

**Refresh often:** notebook 05 REFRESH MATERIALIZED VIEW only (job sdp_write_refresh) — much faster than full redeploy

**Note:** App applies Lakebase ops overlay **on top of** MV for effective status — not only COALESCE inside MV.

### 4.5 Lakebase (Foundry Actions / OLTP)

| Item | Value |
|------|-------|
| Project | att-ankur-demo |
| Postgres DB | sdp_ops |
| Schema | ankur_nayyar |
| Writable table | ankur_nayyar.service_incident_ops |
| Direct host | ep-hidden-cell-d1mr7kp0.database.us-west-2.cloud.databricks.com |

**Why Lakebase:** Foundry Actions need sub-second operational writes without SQL warehouse round-trips.

**Sync directions:**
- UC → Postgres: synced tables (service_incident_pg, service_incident_ops_pg) for read replica
- Postgres → UC: Lakehouse Sync on writable ops table for lakehouse consistency

**App read/write paths:**

| Operation | Primary | Fallback |
|-----------|---------|----------|
| Status PATCH (Dispatch) | Lakebase Postgres | UC service_incident_ops_writeback |
| Ops overlay read | Postgres direct | UC service_incident_ops_pg mirror |
| Board structure (title, market, tech) | UC mv_incident_dispatch_board | — |

**Critical:** Use **direct** Postgres host for Apps SP OAuth — pooler host causes SASL authentication failures.

**Dispatch tab extras:** Lakebase vs SQL Warehouse latency toggle; insert simulation table `lakebase_latency_demo`.

**Setup:** notebooks/06_setup_lakebase_writeback.py, scripts/setup_lakebase_app_oauth.py, config/lakebase.yaml

### 4.6 Ops Console App (Foundry Workshop)

| Item | Value |
|------|-------|
| App name | att-sdp-ops-ankur |
| Code | apps/sdp_ops_console/ |
| URL | https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/ |

#### App tabs

| Tab | What it does | Data sources | Why |
|-----|--------------|--------------|-----|
| Dashboard | Exec KPIs, markets, orders, techs | Gold + Lakebase ops overlay | Single-pane demo entry |
| Dispatch | Incident board, status buttons, latency demo | MV + Lakebase overlay; PATCH writeback | Workshop dispatch UX |
| Agent Triage | Search → recommend → propose → approve → write | Gold read; bridge + Lakebase write | AIP with human approval gate |
| Live Demo | Scenario picker, job stepper, Lakebase feed | Triggers sdp_write_refresh | End-to-end pipeline demo |
| Genie | NL questions → auto charts/KPIs | Genie Space API | Quiver/Slate NL analytics |

#### Key API routes

| Route | Touches |
|-------|---------|
| GET /api/dashboard | Gold + Lakebase ops |
| GET /api/incidents?read_path= | MV + ops overlay (Lakebase or warehouse) |
| PATCH /api/incidents/{id} | Lakebase Postgres (+ UC mirror) |
| POST /api/agent/* | Gold SQL, bridge, Lakebase |
| POST /api/live/trigger | Job sdp_write_refresh |
| POST /api/genie/ask | Genie Space |
| POST /api/latency/simulate | Lakebase vs warehouse insert benchmark |
| GET /health | Warehouse probe + lakebase_ok |

### 4.7 Genie Space (Quiver + Slate NL)

| Item | Value |
|------|-------|
| Space name | att_sdp_service_delivery |
| Space ID | 01f160fc477b16c6a0f63d8164cff930 |
| Config | config/genie_space.yaml |

**Why:** Business users ask NL questions on governed UC assets only — no ad-hoc SQL.

**Tables exposed:** service_incident, service_order, field_technician, mv_incident_dispatch_board, metric_sdp_executive_summary, metric_open_incidents_by_market

**Where:** App Genie tab; standalone Genie UI. **Read-only** — never writes data.

### 4.8 Agent / AgentBricks (AIP)

| Item | Value |
|------|-------|
| Agent name | sdp_incident_triage |
| Tools config | config/sdp_agent_tools.yaml |
| Handlers | src/agents/sdp_incident_agent.py (also inline in app.py) |

**Why:** Autonomous triage with **human approval** before any write.

**Tools:** search_incidents, recommend_technician, assign_technician (writes after approval)

**Demo flow:** P1 Dallas search → recommend TECH-201 → propose INC-9001 → operator approves → bridge CONFIRMED + Lakebase DISPATCHED

**Audit:** agent_audit_log + MLflow agent_dispatch_approved

### 4.9 MLflow tracking

| Item | Value |
|------|-------|
| Experiment | /Shared/att-sdp-ops-console |
| Module | apps/sdp_ops_console/mlflow_tracker.py |

**Why:** Observability for dispatch writes, Genie asks, live job triggers, agent approvals.

### 4.10 Bundle jobs (Workflows)

| Job | When to run | Steps (summary) | Why |
|-----|-------------|-----------------|-----|
| sdp_cleanup | Reset demo | Drop tables/MVs, clear volumes | Clean slate |
| sdp_semantic_setup | First deploy / after cleanup | Bootstrap → DLT full → nb10 semantic → Lakebase sync | Build ontology + MV |
| sdp_write_refresh | Live Demo default | 07 bronze append → DLT incr → 09 sync → 05 MV refresh | Incremental NOC event |
| sdp_full_pipeline | One-shot deploy | End-to-end automation | CI / full deploy |
| sdp_refresh | After manual bronze append | DLT + Lakebase + MV (no write) | Refresh only |

**sdp_write_refresh task order:** write_bronze (07) → run_sdp_dlt → sync_lakebase (09) → refresh_dispatch_mv (05) → show_kpis (05c)

---

## 5. Data flows by user action

### 5.1 Dispatch status update

1. User clicks IN_PROGRESS / DISPATCHED / RESOLVED in Dispatch tab
2. PATCH /api/incidents/{id} with write_path lakebase or warehouse
3. Lakebase Postgres MERGE on service_incident_ops (primary, ~ms)
4. Background: UC Delta MERGE service_incident_ops_writeback
5. MLflow incident_writeback event
6. App re-reads ops overlay; board shows MV structure + effective status

### 5.2 Live Demo “Add live data”

1. User picks scenario (e.g. new_p1_dallas) → POST /api/live/trigger
2. Job sdp_write_refresh: bronze append → DLT → Lakebase sync → MV refresh
3. App polls job steps + Lakebase feed + KPI cards

### 5.3 Agent dispatch approval

1. Agent search → recommend → propose
2. Human clicks Approve
3. bridge_incident_technician CONFIRMED + Lakebase ops DISPATCHED
4. agent_audit_log entries; background MV refresh

### 5.4 Genie question

1. User asks in Genie tab → Genie Space API → warehouse SQL
2. Results rendered as charts/KPIs/tables (viz.py)
3. MLflow tracks question

---

## 6. Demo runbook (45 minutes)

| Step | Action | Proves |
|------|--------|--------|
| 1 | bundle run sdp_write_refresh OR Live Demo tab | Builds / DLT / batch path |
| 2 | Catalog Explorer: MV + metric_* views | Slate semantic layer |
| 3 | Genie: “How many P1 in Dallas?” + executive summary | Quiver NL analytics |
| 4 | Agent Triage: full demo flow → approve | AIP + approval gate |
| 5 | Dispatch: status update + Lakebase latency toggle | Workshop + Actions |
| 6 | SQL verify ops + dispatch board rows | Writeback landed |

### Prerequisites

```bash
databricks auth login --profile e2-demo-field-eng
cd pltr-dbx
databricks bundle validate -t e2_demo --profile e2-demo-field-eng
databricks bundle deploy -t e2_demo --profile e2-demo-field-eng
```

### Key commands

```bash
# Incremental live demo (preferred)
databricks bundle run sdp_write_refresh -t e2_demo --profile e2-demo-field-eng

# Full reset + rebuild
databricks bundle run sdp_cleanup -t e2_demo --profile e2-demo-field-eng
databricks bundle run sdp_semantic_setup -t e2_demo --profile e2-demo-field-eng

# Redeploy App after code changes
databricks workspace import --file apps/sdp_ops_console/app.py --overwrite \
  "/Users/ankur.nayyar@databricks.com/apps/att-sdp-ops-ankur/app.py" -p e2-demo-field-eng
databricks api post /api/2.0/apps/att-sdp-ops-ankur/deployments -p e2-demo-field-eng \
  --json '{"source_code_path":"/Workspace/Users/ankur.nayyar@databricks.com/apps/att-sdp-ops-ankur","mode":"SNAPSHOT"}'
```

### SQL verification

```sql
SELECT * FROM users.ankur_nayyar.metric_sdp_executive_summary;

SELECT incident_id, title, severity, incident_status, market, technician_name
FROM users.ankur_nayyar.mv_incident_dispatch_board
ORDER BY severity, opened_at;

SELECT incident_id, status, ops_notes, updated_at
FROM users.ankur_nayyar.service_incident_ops_pg
WHERE incident_id = 'INC-9001';
```

---

## 7. Validation URLs

| Surface | URL |
|---------|-----|
| Ops Console | https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/ |
| Health | …/health |
| Live Demo tab | …/live |
| UC schema | https://e2-demo-field-eng.cloud.databricks.com/explore/data/users/ankur_nayyar |
| Genie Space | https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/01f160fc477b16c6a0f63d8164cff930 |
| Lakebase | att-ankur-demo project in workspace |
| Jobs | https://e2-demo-field-eng.cloud.databricks.com/jobs |

Health check: sql_ok true, lakebase_ok true, incident_count ≥ 5.

---

## 8. Repository layout

```
pltr-dbx/
├── databricks.yml              # Bundle target e2_demo
├── config/                     # genie_space, lakebase, agent tools, mapping
├── src/ontology/ddl/           # UC gold DDL
├── src/pipelines/dlt/          # sdp_service_delivery DLT
├── src/semantic/metrics/       # KPI view SQL
├── src/agents/                 # Agent handlers
├── apps/sdp_ops_console/       # Flask App (5 tabs)
├── notebooks/                  # Deploy, live data, Lakebase, semantic layer
├── resources/jobs/             # Workflow definitions
├── data/sdp_seed/              # Baseline JSON
├── scripts/                    # Deploy helpers, slide generator
├── docs/ATT_SDP_Project_Guide.docx   # This document
└── presentations/              # Architecture slides (HTML/PDF)
```

---

## 9. Common misconceptions

| Myth | Reality |
|------|---------|
| App reads everything from Lakebase | Board structure from UC MV; only ops overlay is Lakebase-first |
| DLT runs on every dispatch | Dispatch is OLTP via Lakebase; DLT runs on batch jobs |
| Genie writes data | Genie is read-only analytics |
| Use Lakebase pooler for Apps | Direct Postgres host required (pooler → SASL failure) |
| notebook 10 needed every refresh | Use notebook 05 REFRESH MV only for routine updates |

---

## 10. Who owns what (quick reference)

| Concern | Owner component |
|---------|-----------------|
| Entity definitions & PK/FK | UC gold DDL + DLT |
| Batch transforms & DQ | DLT pipeline |
| Operational writes | Lakebase Postgres |
| Fast ops reads | Lakebase (+ UC mirror fallback) |
| Dispatch board shape | Materialized view |
| Executive KPIs | metric_* semantic views |
| Human operational UI | Ops Console App |
| NL analytics | Genie Space |
| Autonomous triage | Agent Triage tab |
| Pipeline orchestration | Bundle jobs |
| Demo reset | sdp_cleanup |
| Audit & observability | agent_audit_log, MLflow |

---

## 11. Troubleshooting

| Symptom | Fix |
|---------|-----|
| App shows 0 KPIs / empty board | Check /health → sql_ok; grant App SP on users.ankur_nayyar + warehouse CAN_USE |
| Lakebase SASL auth failed | Use direct host in app.yaml; remove pooler; run setup_lakebase_app_oauth.py |
| Dispatch board stale after write | Normal — overlay updates via Lakebase immediately; run notebook 05 for MV join refresh |
| semantic_setup very slow | DLT full refresh dominates; use sdp_write_refresh for demos |
| Genie wrong counts | Re-run sdp_write_refresh; confirm Genie space uses users.ankur_nayyar tables |
| Live Demo job permission error | Grant App SP Can Manage Run on sdp_write_refresh job |

---

*Generated by scripts/build_project_guide.py — regenerate after architecture changes.*
