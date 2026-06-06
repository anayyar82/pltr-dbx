# ATT Service Delivery Platform — Foundry → Databricks Demo

Operational **Service Delivery Platform** demo migrated from Palantir Foundry to a governed Databricks lakehouse with Genie, Apps, DLT, and Lakebase writeback.

**Live workspace:** `e2-demo-field-eng` · catalog `users.ankur_nayyar` · [Ops Console App](https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/)

## Documentation

| Resource | Path |
|----------|------|
| **Project guide (components, architecture, runbook)** | [docs/ATT_SDP_Project_Guide.md](docs/ATT_SDP_Project_Guide.md) · [DOCX](docs/ATT_SDP_Project_Guide.docx) |
| **Architecture design document** | [docs/ATT_SDP_Architecture_Design.md](docs/ATT_SDP_Architecture_Design.md) |
| Regenerate guide | `python3 scripts/build_project_guide.py` |
| **New workspace install** | [install/README.md](install/README.md) · `./install.sh all --config config/deployment.yaml` |
| Architecture slides (HTML/PDF) | `presentations/` · `python3 scripts/build_att_sdp_slides.py` |

## Foundry → Databricks mapping

| Foundry | Databricks | This repo |
|---------|------------|-----------|
| Objects | Unity Catalog Delta tables | `src/ontology/ddl/att_sdp_*.sql` |
| Links | Bridge tables + MVs | `mv_incident_dispatch_board` |
| Builds | DLT + Workflows | `src/pipelines/dlt/sdp_service_delivery.py` |
| Slate | AI/BI semantic KPIs | `src/semantic/metrics/sdp_kpis.sql` |
| Workshop | **Databricks App** | `apps/sdp_ops_console/` |
| AIP | **AgentBricks** | `src/agents/sdp_incident_agent.py` |
| Quiver | Notebooks + **Genie** | `notebooks/`, `config/genie_space.yaml` |
| Actions | **Lakebase** writeback | `config/lakebase.yaml` |

## Repository layout

```
pltr-dbx/
├── databricks.yml                 # Bundle (target: e2_demo)
├── config/
│   ├── att_sdp_mapping.yaml       # Foundry → UC inventory
│   ├── genie_space.yaml           # Genie Space manifest
│   ├── lakebase.yaml              # Lakebase writeback + sync
│   └── sdp_agent_tools.yaml       # AgentBricks tools
├── src/
│   ├── ontology/ddl/              # ATT SDP UC DDL
│   ├── pipelines/dlt/             # sdp_service_delivery DLT
│   ├── semantic/metrics/          # KPI views
│   └── agents/                    # Incident triage agent
├── apps/sdp_ops_console/          # Ops Console + Genie tab
├── notebooks/                     # Deploy, live demo, Lakebase setup
├── resources/jobs/                # Refresh & clean-redeploy jobs
├── data/sdp_seed/                 # Baseline seed JSON
├── docs/                          # ATT_SDP_Project_Guide.docx (master doc)
├── install/                       # Workspace installer (./install.sh)
├── presentations/                 # Architecture slides (optional)
└── scripts/                       # Deploy helpers, doc/slide generators
```

## Three workflows

Run in order for a from-scratch redeploy:

| # | Workflow | Purpose |
|---|----------|---------|
| 1 | `sdp_cleanup` | Drop all tables, views, MVs, clear bronze volumes |
| 2 | `sdp_semantic_setup` | Gold ontology, seed data, KPI views, dispatch MV, DLT full refresh |
| 3 | `sdp_full_pipeline` | Autoloader bronze → SDP DLT → Lakebase → App |

```bash
databricks bundle deploy -t e2_demo --profile e2-demo-field-eng
databricks bundle run sdp_cleanup -t e2_demo --profile e2-demo-field-eng
databricks bundle run sdp_semantic_setup -t e2_demo --profile e2-demo-field-eng
databricks bundle run sdp_full_pipeline -t e2_demo --profile e2-demo-field-eng
```

One-command from scratch:

```bash
./scripts/deploy_from_scratch.sh
```

## Incremental refresh (after append writes)

```text
Append write (07/08) → sdp_refresh (DLT + Lakebase + MV) → App / Genie
```

```bash
databricks bundle run sdp_refresh -t e2_demo --profile e2-demo-field-eng
```

**Write + refresh in one command:**

```bash
databricks bundle run sdp_write_refresh -t e2_demo --profile e2-demo-field-eng
```

Use **append** write mode in notebook 07 (default). Do not use snapshot overwrite unless running cleanup first.

Legacy combined job (still available):

```bash
databricks bundle run sdp_clean_redeploy -t e2_demo --profile e2-demo-field-eng
```

## Presentation deck

```bash
python3 scripts/build_att_sdp_slides.py
open presentations/ATT_SDP_Architecture.html   # present in browser (← → keys)
# presentations/ATT_SDP_Architecture.pdf         # import to Google Slides if needed
```
