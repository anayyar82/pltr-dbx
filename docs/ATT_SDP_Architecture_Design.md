# ATT Service Delivery Platform — Architecture Design Document

**Document version:** 1.0  
**Date:** June 2026  
**Repository:** [anayyar82/pltr-dbx](https://github.com/anayyar82/pltr-dbx)  
**Status:** As-built (reference deployment on `e2-demo-field-eng`)

---

## Table of contents

1. [Purpose and scope](#1-purpose-and-scope)
2. [Design goals](#2-design-goals)
3. [System context](#3-system-context)
4. [Logical architecture](#4-logical-architecture)
5. [Data architecture](#5-data-architecture)
6. [Component design](#6-component-design)
7. [Integration and data flows](#7-integration-and-data-flows)
8. [Foundry parity mapping](#8-foundry-parity-mapping)
9. [Security and governance](#9-security-and-governance)
10. [Deployment architecture](#10-deployment-architecture)
11. [Non-functional characteristics](#11-non-functional-characteristics)
12. [Design decisions and trade-offs](#12-design-decisions-and-trade-offs)
13. [Appendix](#13-appendix)

---

## 1. Purpose and scope

### 1.1 Purpose

This document describes the **as-built architecture** of the ATT Service Delivery Platform (SDP) demo migrated from Palantir Foundry to Databricks. It is intended for architects, field engineers, and customer stakeholders evaluating:

- How Foundry concepts map to Databricks primitives
- How batch analytics and operational (OLTP-style) workloads coexist
- How to deploy the same pattern to a new workspace

### 1.2 In scope

| Area | Description |
|------|-------------|
| Batch ingest | Bronze → DLT → Gold → Semantic layer |
| Operational read/write | Lakebase Postgres + UC mirrors |
| Human-facing UI | Databricks App (Ops Console) |
| NL analytics | Genie Space |
| Agentic workflows | Agent Triage with human approval |
| Orchestration | Databricks Asset Bundle + Workflow jobs |

### 1.3 Out of scope

- Production ATT network systems (NOC feeds are simulated JSON)
- Multi-region HA and DR runbooks
- Enterprise SSO / SCIM beyond workspace defaults
- Cost modeling and capacity planning

### 1.4 Related documents

| Document | Location |
|----------|----------|
| Project guide (components, runbook) | [docs/ATT_SDP_Project_Guide.md](ATT_SDP_Project_Guide.md) |
| Workspace installer | [install/README.md](../install/README.md) |
| Foundry inventory | [config/att_sdp_mapping.yaml](../config/att_sdp_mapping.yaml) |

---

## 2. Design goals

| Goal | Design response |
|------|-----------------|
| **Foundry parity** | Explicit 1:1 mapping: Objects → UC, Builds → DLT, Workshop → App, Actions → Lakebase |
| **Governed analytics** | All reads go through Unity Catalog; Genie exposes certified tables/views only |
| **Low-latency ops** | Dispatch writes go to Lakebase Postgres (OLTP), not through DLT |
| **Separation of concerns** | Three distinct data paths: batch, ops-read, ops-write |
| **Demo repeatability** | Bundle jobs + installer CLI for clean redeploy to any workspace |
| **Human-in-the-loop AI** | Agent proposes actions; operator must approve before any write |
| **Observability** | MLflow events for writes, Genie asks, job triggers, agent approvals |

---

## 3. System context

### 3.1 Context diagram

```mermaid
flowchart TB
    subgraph Users
        NOC[NOC Operator]
        Analyst[Business Analyst]
        Exec[Executive]
    end

    subgraph Databricks["Databricks Workspace"]
        App[Ops Console App]
        Genie[Genie Space]
        Jobs[Workflow Jobs]
        DLT[DLT Pipeline]
        UC[(Unity Catalog)]
        WH[(SQL Warehouse)]
        LB[(Lakebase Postgres)]
    end

    NOC --> App
    Analyst --> Genie
    Exec --> App
    Exec --> Genie

    App --> UC
    App --> WH
    App --> LB
    App --> Jobs
    App --> Genie

    Genie --> WH
    Genie --> UC

    Jobs --> DLT
    Jobs --> UC
    Jobs --> LB

    DLT --> UC
    LB <-->|Synced Tables / Lakehouse Sync| UC
```

### 3.2 Actors

| Actor | Primary interface | Typical actions |
|-------|-------------------|-----------------|
| NOC operator | Ops Console — Dispatch, Agent Triage | Update incident status, approve agent dispatch |
| Demo presenter | Live Demo tab | Trigger pipeline scenarios |
| Business analyst | Genie Space | Ask NL questions on governed data |
| Platform engineer | Bundle CLI, installer | Deploy, bootstrap, configure workspace |
| App service principal | Background API calls | SQL, Lakebase OAuth, job triggers |

### 3.3 Reference deployment

| Item | Value |
|------|-------|
| Workspace | `e2-demo-field-eng` |
| Catalog.schema | `users.ankur_nayyar` |
| Ops Console App | [att-sdp-ops-ankur](https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/) |
| Lakebase project | `att-ankur-demo` / DB `sdp_ops` |
| Bundle target | `e2_demo` |

---

## 4. Logical architecture

The platform is organized into **five layers**. Each layer has a single responsibility; cross-layer calls follow the three data paths defined in Section 5.

```mermaid
flowchart TB
    subgraph Presentation["Presentation Layer"]
        Dashboard[Dashboard Tab]
        Dispatch[Dispatch Tab]
        Agent[Agent Triage Tab]
        LiveDemo[Live Demo Tab]
        GenieTab[Genie Tab]
    end

    subgraph Application["Application Layer"]
        Flask[Flask App — app.py]
        APIs[REST APIs]
        Viz[viz.py — chart inference]
        MLflow[mlflow_tracker.py]
    end

    subgraph Semantic["Semantic Layer"]
        MV[mv_incident_dispatch_board]
        KPIs[metric_* views]
    end

    subgraph Processing["Processing Layer"]
        DLTpipe[sdp_service_delivery_dlt]
        NB[Notebooks — deploy, sync, refresh]
        WF[Workflow Jobs]
    end

    subgraph Storage["Storage Layer"]
        Volume[UC Volume sdp_exports]
        Gold[Gold Delta tables]
        LBpg[Lakebase Postgres]
        LBMirror[UC synced mirrors]
    end

    Presentation --> Application
    Application --> Semantic
    Application --> Storage
    Semantic --> Storage
    Processing --> Storage
    WF --> Processing
```

### 4.1 Layer responsibilities

| Layer | Technology | Responsibility |
|-------|------------|----------------|
| Presentation | HTML/JS templates | Five-tab Ops Console UX |
| Application | Flask on Databricks Apps | API orchestration, path selection (Lakebase vs warehouse) |
| Semantic | SQL views + materialized view | KPI rollups, dispatch board join object |
| Processing | DLT + Workflows | Batch ingest, transforms, MV refresh, Lakebase sync |
| Storage | UC Delta + Lakebase Postgres | System of record (lakehouse) + ops OLTP store |

---

## 5. Data architecture

### 5.1 Medallion layout

```mermaid
flowchart LR
    subgraph Bronze
        Vol[sdp_exports Volume\nJSON files]
    end

    subgraph Silver
        SI[silver service_incident]
        SO[silver service_order]
        FT[silver field_technician]
    end

    subgraph Gold
        GI[service_incident]
        GO[service_order]
        GC[customer_account]
        GT[field_technician]
        BR[bridge_incident_technician]
        OPS[service_incident_ops VIEW]
        WB[service_incident_ops_writeback]
    end

    subgraph Semantic
        MV[mv_incident_dispatch_board]
        M1[metric_sdp_executive_summary]
        M2[metric_open_incidents_by_market]
    end

    Vol -->|Autoloader| SI
    Vol --> SO
    Vol --> FT
    SI --> GI
    SO --> GO
    FT --> GT
    GI --> MV
    GO --> M1
    GT --> MV
    BR --> MV
    OPS --> MV
```

> **Note:** In this demo, bronze/silver/gold share one schema (`users.ankur_nayyar`) for simplicity. Production deployments typically split schemas.

### 5.2 Three data paths

This is the **central architectural invariant**. Misunderstanding which path applies causes most operational confusion.

```mermaid
flowchart TB
    subgraph Path1["Path 1 — Batch Ingest (Foundry Builds)"]
        direction LR
        A1[Seed / Live JSON] --> A2[UC Volume]
        A2 --> A3[DLT Pipeline]
        A3 --> A4[Gold Tables]
        A4 --> A5[MV + KPI Views]
    end

    subgraph Path2["Path 2 — Ops Read (Lakebase-first)"]
        direction LR
        B1[mv_incident_dispatch_board] --> B3[Effective dispatch row]
        B2[Lakebase service_incident_ops] --> B3
        B4[UC mirror fallback] -.-> B2
    end

    subgraph Path3["Path 3 — Ops Write (Foundry Actions)"]
        direction LR
        C1[App PATCH / Agent approve] --> C2[Lakebase Postgres MERGE]
        C2 --> C3[UC writeback Delta]
        C3 --> C4[Lakehouse Sync]
    end
```

| Path | Trigger | Latency target | Must NOT |
|------|---------|----------------|----------|
| **Batch** | Job `sdp_write_refresh`, Live Demo tab | Minutes | Run on every UI click |
| **Ops read** | Dashboard/Dispatch load | Sub-second | Replace MV for board structure |
| **Ops write** | Status button, agent approval | Sub-second | Block on DLT or full MV rebuild |

### 5.3 Entity model (gold ontology)

```mermaid
erDiagram
    CUSTOMER_ACCOUNT ||--o{ SERVICE_INCIDENT : has
    CUSTOMER_ACCOUNT ||--o{ SERVICE_ORDER : has
    SERVICE_INCIDENT ||--o{ BRIDGE_INCIDENT_TECHNICIAN : assigns
    FIELD_TECHNICIAN ||--o{ BRIDGE_INCIDENT_TECHNICIAN : assigned
    SERVICE_INCIDENT ||--o| SERVICE_INCIDENT_OPS : overlays

    CUSTOMER_ACCOUNT {
        string account_id PK
        string account_name
        string segment
        string market
    }

    SERVICE_INCIDENT {
        string incident_id PK
        string account_id FK
        string severity
        string status
        string market
        timestamp opened_at
    }

    SERVICE_INCIDENT_OPS {
        string incident_id PK
        string status
        string assigned_technician_id
        string ops_notes
        timestamp updated_at
    }

    FIELD_TECHNICIAN {
        string technician_id PK
        string name
        string skill_level
        string market
        string status
    }

    BRIDGE_INCIDENT_TECHNICIAN {
        string incident_id FK
        string technician_id FK
        string assignment_status
    }

    SERVICE_ORDER {
        string order_id PK
        string account_id FK
        string status
        string market
    }
```

### 5.4 Lakebase sync topology

```mermaid
flowchart LR
    subgraph UC["Unity Catalog"]
        SI[service_incident]
        OPS[service_incident_ops]
        SIPG[service_incident_pg]
        OPSPG[service_incident_ops_pg]
        WB[service_incident_ops_writeback]
    end

    subgraph PG["Lakebase Postgres sdp_ops"]
        PSI[service_incident synced]
        POP[service_incident_ops writable]
    end

    SI -->|Synced Table TRIGGERED| PSI
    OPS -->|Synced Table TRIGGERED| POP
    POP -->|Lakehouse Sync| WB
    POP -->|App MERGE primary| POP
```

| Direction | Mechanism | Purpose |
|-----------|-----------|---------|
| UC → Postgres | Synced Tables | Low-latency read replica for App overlay |
| Postgres → UC | Lakehouse Sync | Keep lakehouse consistent after ops writes |
| App → Postgres | Direct OAuth MERGE | Primary write path (Foundry Actions) |
| App → UC | Background MERGE to `service_incident_ops_writeback` | Writable Delta fallback when Lakebase unavailable |

**Critical constraint:** Databricks Apps must use the **direct** Postgres host. The pooler host causes SASL OAuth failures for the App service principal.

---

## 6. Component design

### 6.1 Delta Live Tables — `sdp_service_delivery_dlt`

| Attribute | Value |
|-----------|-------|
| Source | `src/pipelines/dlt/sdp_service_delivery.py` |
| Mode | Serverless, ADVANCED, non-continuous |
| Input | UC Volume `sdp_exports` (JSON) |
| Output | Gold managed tables in target schema |
| Expectations | Row-level data quality on bronze/silver |

**Design rationale:** DLT replaces Foundry Builds with declarative pipelines, built-in lineage, and expectations. Incremental runs (`sdp_write_refresh`) append bronze and refresh gold without full redeploy.

### 6.2 Semantic layer

| Asset | Type | Refresh | Consumers |
|-------|------|---------|-----------|
| `mv_incident_dispatch_board` | Materialized View | `REFRESH MATERIALIZED VIEW` (nb 05) | Dispatch, Genie, APIs |
| `metric_sdp_executive_summary` | View | On deploy / DLT refresh | Dashboard, Genie |
| `metric_open_incidents_by_market` | View | On deploy / DLT refresh | Dashboard, Genie |
| `metric_incident_mttr` | View | On deploy | Genie |
| `metric_order_fulfillment_sla` | View | On deploy | Genie |
| `metric_technician_utilization` | View | On deploy | Genie |

**Design rationale:** Materialized view pre-joins incidents, accounts, technicians, and bridge for dispatch UX. KPI views provide Slate-equivalent certified metrics. App applies Lakebase ops overlay **at read time** on top of MV rows.

### 6.3 Ops Console App

| Attribute | Value |
|-----------|-------|
| Runtime | Databricks Apps (Flask) |
| Code | `apps/sdp_ops_console/` |
| Auth | App service principal (OAuth M2M) + optional user token for Genie |

#### Tab architecture

```mermaid
flowchart TB
    subgraph Tabs
        T1[Dashboard]
        T2[Dispatch]
        T3[Agent Triage]
        T4[Live Demo]
        T5[Genie]
    end

    subgraph Backend
        API[Flask REST API]
    end

    subgraph DataSources
        WH[(SQL Warehouse)]
        LB[(Lakebase)]
        Jobs[Workflow Jobs]
        GenieAPI[Genie Space API]
    end

    T1 --> API
    T2 --> API
    T3 --> API
    T4 --> API
    T5 --> API

    API --> WH
    API --> LB
    API --> Jobs
    API --> GenieAPI
```

| Tab | Key APIs | Write? |
|-----|----------|--------|
| Dashboard | `GET /api/dashboard` | No |
| Dispatch | `GET/PATCH /api/incidents` | Yes (Lakebase) |
| Agent Triage | `POST /api/agent/*` | Yes (after approval) |
| Live Demo | `POST /api/live/trigger` | Indirect (job) |
| Genie | `POST /api/genie/ask` | No |

#### Dispatch latency demo

The Dispatch tab supports toggling **read path** (Lakebase vs SQL Warehouse) and **write path** for side-by-side latency comparison. A simulation table (`lakebase_latency_demo`) demonstrates insert latency differences.

### 6.4 Agent Triage (AIP pattern)

```mermaid
sequenceDiagram
    participant Op as Operator
    participant App as Ops Console
    participant WH as SQL Warehouse
    participant LB as Lakebase
    participant Audit as agent_audit_log

    Op->>App: Search P1 Dallas
    App->>WH: search_incidents SQL
    WH-->>App: Incident rows
    Op->>App: Recommend technician
    App->>WH: recommend_technician SQL
    WH-->>App: TECH-201
    Op->>App: Propose assignment
    App-->>Op: Proposal (pending approval)
    Op->>App: Approve
    App->>LB: MERGE service_incident_ops DISPATCHED
    App->>WH: UPDATE bridge CONFIRMED
    App->>Audit: Log tool calls
```

**Design rationale:** Mirrors Foundry AIP with a mandatory human approval gate. No autonomous writes.

### 6.5 Genie Space

| Attribute | Value |
|-----------|-------|
| Config | `config/genie_space.yaml` |
| Create script | `scripts/create_att_sdp_genie_space.py` |
| Tables exposed | 6 governed UC assets |
| Mode | Read-only NL → SQL |

### 6.6 Workflow jobs

| Job | Purpose | Duration |
|-----|---------|----------|
| `sdp_cleanup` | Drop tables, MVs, clear volumes | ~1 min |
| `sdp_semantic_setup` | Full bootstrap + DLT full refresh + semantic | ~8–20 min |
| `sdp_write_refresh` | Incremental live demo (bronze → DLT → sync → MV) | ~3–5 min |
| `sdp_full_pipeline` | End-to-end deploy automation | Varies |
| `sdp_refresh` | Refresh only (no bronze write) | ~2–4 min |

```mermaid
flowchart LR
    subgraph sdp_write_refresh
        W1[07 write bronze] --> W2[DLT incremental]
        W2 --> W3[09 sync lakebase]
        W3 --> W4[05 refresh MV]
        W4 --> W5[05c show KPIs]
    end
```

---

## 7. Integration and data flows

### 7.1 Dispatch status update

```mermaid
sequenceDiagram
    participant UI as Dispatch UI
    participant API as Flask API
    participant LB as Lakebase Postgres
    participant UC as Unity Catalog
    participant ML as MLflow

    UI->>API: PATCH /api/incidents/INC-9001
    API->>LB: MERGE service_incident_ops
    LB-->>API: OK (~ms)
    API->>UC: Background MERGE writeback table
    API->>ML: Track incident_writeback
    API-->>UI: Updated effective status
    Note over UI: Re-read applies LB overlay on MV row
```

### 7.2 Live Demo pipeline trigger

```mermaid
sequenceDiagram
    participant UI as Live Demo Tab
    participant API as Flask API
    participant Job as sdp_write_refresh
    participant DLT as DLT Pipeline
    participant LB as Lakebase

    UI->>API: POST /api/live/trigger scenario=new_p1_dallas
    API->>Job: Run job (async)
    Job->>Job: Append bronze JSON
    Job->>DLT: Incremental update
    Job->>LB: Sync tables
    Job->>Job: REFRESH MATERIALIZED VIEW
    UI->>API: Poll job status + KPIs
    API-->>UI: Step progress + updated counts
```

### 7.3 Genie question

```mermaid
sequenceDiagram
    participant User as Analyst
    participant App as Genie Tab
    participant Genie as Genie Space
    participant WH as SQL Warehouse

    User->>App: "How many P1 in Dallas?"
    App->>Genie: Start conversation + message
    Genie->>WH: Generated SQL
    WH-->>Genie: Result set
    Genie-->>App: SQL + data
    App->>App: viz.py infer chart type
    App-->>User: KPI / chart / table
```

---

## 8. Foundry parity mapping

| Foundry concept | Databricks implementation | Repo artifact |
|-----------------|---------------------------|---------------|
| **Objects** | UC managed Delta tables | `src/ontology/ddl/att_sdp_objects.sql` |
| **Links** | Bridge table + MV | `bridge_incident_technician`, `mv_incident_dispatch_board` |
| **Builds** | DLT + Workflows | `src/pipelines/dlt/sdp_service_delivery.py` |
| **Slate** | Semantic KPI views + AI/BI | `src/semantic/metrics/sdp_kpis.sql` |
| **Workshop** | Databricks App | `apps/sdp_ops_console/` |
| **Actions** | Lakebase Postgres writeback | `config/lakebase.yaml`, nb 06 |
| **AIP** | Agent Triage + tools | `config/sdp_agent_tools.yaml` |
| **Quiver** | Notebooks + Genie | `notebooks/`, `config/genie_space.yaml` |

Full inventory: [config/att_sdp_mapping.yaml](../config/att_sdp_mapping.yaml)

---

## 9. Security and governance

### 9.1 Identity and access

| Principal | Access |
|-----------|--------|
| Human users | Workspace login; App UI; Genie via user token or space ACLs |
| App service principal | UC schema (SELECT/MODIFY), warehouse CAN_USE, Lakebase OAuth role, job CAN_MANAGE_RUN |
| Job compute | Serverless DLT / job cluster SP (workspace default) |

### 9.2 Data governance

- All analytics assets registered in **Unity Catalog**
- Genie Space exposes **allow-listed tables/views only**
- Agent writes require **explicit human approval**
- Tool calls logged to **`agent_audit_log`**
- MLflow tracks dispatch writes, Genie queries, job triggers

### 9.3 Secrets

| Secret | Storage |
|--------|---------|
| Lakebase credentials | Injected by Databricks Apps runtime (OAuth) |
| Google OAuth (optional slides upload) | `config/google_credentials.json` (gitignored) |
| Deployment config | `config/deployment.yaml` (local; not committed with secrets) |

---

## 10. Deployment architecture

### 10.1 Repository layout

```
pltr-dbx/
├── databricks.yml              # Asset Bundle definition
├── config/                     # Lakebase, Genie, agent, deployment
├── src/ontology/ddl/           # Gold DDL
├── src/pipelines/dlt/          # DLT pipeline
├── src/semantic/metrics/       # KPI SQL
├── apps/sdp_ops_console/       # Ops Console App
├── notebooks/                  # Deploy, sync, live data
├── resources/jobs/             # Workflow definitions
├── install/                    # New workspace installer CLI
└── docs/                       # Architecture + project guide
```

### 10.2 New workspace deployment

Use the installer package:

```bash
cp config/deployment.example.yaml config/deployment.yaml
# Edit workspace, schema, warehouse, Lakebase, app name
./install.sh all --config config/deployment.yaml
```

The installer patches `databricks.yml`, `app.yaml`, `lakebase.yaml`, and job notebook parameters for the target workspace.

### 10.3 Deployment topology

```mermaid
flowchart TB
    subgraph DevMachine["Developer Laptop"]
        Git[Git clone pltr-dbx]
        CLI[Databricks CLI + install.sh]
    end

    subgraph Workspace["Target Databricks Workspace"]
        Bundle[Asset Bundle sync]
        Jobs[Workflow Jobs]
        DLT[DLT Pipeline]
        AppDeploy[Apps deployment]
        UC[(Unity Catalog)]
        LB[(Lakebase Project)]
    end

    Git --> CLI
    CLI -->|bundle deploy| Bundle
    Bundle --> Jobs
    Bundle --> DLT
    CLI -->|bootstrap job| Jobs
    CLI -->|deploy-app| AppDeploy
    Jobs --> UC
    Jobs --> LB
    DLT --> UC
    AppDeploy --> AppRuntime[Ops Console runtime]
    AppRuntime --> UC
    AppRuntime --> LB
```

---

## 11. Non-functional characteristics

| Characteristic | Target (demo) | Notes |
|----------------|---------------|-------|
| Dispatch write latency | < 500 ms | Lakebase direct Postgres |
| Dispatch read latency | < 200 ms | Lakebase overlay read |
| Batch refresh | 3–5 min incremental | `sdp_write_refresh` |
| Full bootstrap | 8–20 min | DLT full refresh dominates |
| Availability | Best-effort | Demo workspace, no SLA |
| Concurrent users | ~10 | Flask App on Apps runtime |

---

## 12. Design decisions and trade-offs

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Lakebase for ops writes | Sub-second OLTP; Foundry Actions parity | Extra infra (Lakebase project setup) |
| MV + runtime overlay vs single table | Keeps batch and ops concerns separate | Two sources for effective status |
| Single schema for bronze/silver/gold | Simpler demo | Not production medallion isolation |
| DLT owns `service_incident_ops` as VIEW | Pipeline controls canonical ops shape | App writes to separate writeback table |
| Direct Postgres host (no pooler) | Apps SP OAuth compatibility | No connection pooling benefit for App |
| Human approval for agent writes | Governance / demo safety | Extra click for operators |
| Genie read-only | Prevents accidental writes from NL | Cannot action from Genie |

---

## 13. Appendix

### 13.1 Key URLs (reference deployment)

| Surface | URL |
|---------|-----|
| Ops Console | https://att-sdp-ops-ankur-1444828305810485.aws.databricksapps.com/ |
| Health check | …/health |
| Live Demo | …/live |
| GitHub repo | https://github.com/anayyar82/pltr-dbx |

### 13.2 Validation SQL

```sql
SELECT * FROM users.ankur_nayyar.metric_sdp_executive_summary;

SELECT incident_id, title, severity, incident_status, market, technician_name
FROM users.ankur_nayyar.mv_incident_dispatch_board
ORDER BY severity, opened_at;
```

### 13.3 Document history

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | June 2026 | Platform team | Initial architecture design document |

---

*This document is maintained in the [pltr-dbx](https://github.com/anayyar82/pltr-dbx) repository. For operational procedures see [ATT_SDP_Project_Guide.md](ATT_SDP_Project_Guide.md).*
