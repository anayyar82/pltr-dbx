# ATT Service Delivery Platform
## Business Overview — Problem, Solution, and Why These Components

**Audience:** Business stakeholders, NOC leadership, executives, customer sponsors  
**Purpose:** Explain what problem this demo solves and why each Databricks component was chosen  
**Version:** 1.0 · June 2026  
**Repository:** [github.com/anayyar82/pltr-dbx](https://github.com/anayyar82/pltr-dbx)

---

## 1. Executive summary

Telecom service delivery operations depend on **fast, accurate information** and **safe, auditable actions** — when a fiber cut hits Dallas, operators must see the right incidents, assign the right technician, and update status in seconds, not minutes. Analysts and executives need trusted KPIs without writing SQL. AI can help triage — but only if a human stays in control.

This project demonstrates how **ATT’s Service Delivery Platform**, originally built on Palantir Foundry, can run on **Databricks** with the same business outcomes:

| Business need | What we deliver |
|---------------|-----------------|
| Single view of service health | Executive dashboard with live KPIs |
| Fast incident dispatch | Dispatch board with one-click status updates |
| Safe automation | AI agent that **proposes** actions — operator **approves** |
| Self-serve analytics | Ask questions in plain English, get charts and answers |
| Governed data | One catalog, certified metrics, full lineage |
| Operational writeback | Status changes land instantly — no batch delay |

**Bottom line:** We are not replacing how operators work — we are modernizing the platform so the same workflows run on an open lakehouse with better AI, analytics, and cloud-native operations.

---

## 2. The business problem

### 2.1 What ATT Service Delivery Operations does every day

Network and service operations teams manage:

- **Service incidents** — outages, degradations, customer-impacting events (P1–P4 severity)
- **Field technicians** — who is available, skilled, and closest to the problem
- **Service orders** — provisioning and fulfillment status
- **Customer accounts** — enterprise vs residential context for prioritization

Operators dispatch technicians. Analysts track SLA and market performance. Executives need a rollup: *How many P1s are open? Where are we exposed?*

### 2.2 Pain points this architecture addresses

| Pain point | Business impact |
|------------|-----------------|
| **Slow status updates** | Operators wait for batch pipelines before the board reflects reality — delays dispatch decisions |
| **Split systems** | Analytics in one tool, operations in another — inconsistent numbers in meetings |
| **Risky automation** | AI that writes data without approval creates compliance and customer-trust risk |
| **Analyst bottleneck** | Business users depend on engineers for every report |
| **Migration uncertainty** | Moving off Foundry raises fear of losing Objects, Actions, Workshop, and AIP patterns |
| **No governed self-serve** | Ad-hoc SQL on raw tables produces conflicting KPIs |

### 2.3 What “success” looks like for the business

1. **Operator** opens one console, sees all open incidents, updates status in under a second  
2. **Analyst** asks *“How many P1 incidents are open in Dallas?”* and gets a trusted answer  
3. **AI** recommends TECH-201 for INC-9001 — operator reviews and approves  
4. **Executive** sees one KPI row: open incidents, P1 count, provisioning backlog, available techs  
5. **Platform team** can redeploy the same demo to any Databricks workspace for customer proof

---

## 3. The solution in business terms

We built a **Service Delivery Operations platform** on Databricks that mirrors how ATT already thinks about the problem — but on a modern lakehouse stack.

```text
┌─────────────────────────────────────────────────────────────────┐
│                    OPS CONSOLE (one screen)                      │
│  Dashboard │ Dispatch │ Agent Triage │ Live Demo │ Genie        │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   Trusted KPIs         Fast dispatch         Ask in English
   (executive view)     (operator actions)    (analyst self-serve)
         │                    │                    │
         └────────────────────┴────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     Governed data lake                 Operational store
     (history, analytics, AI)           (instant status updates)
```

**Three simple rules the business should remember:**

1. **Batch updates** feed the lakehouse — new incidents, orders, and KPIs (runs on a schedule or demo trigger)  
2. **Operational clicks** go straight to the operational database — no waiting for overnight jobs  
3. **AI never writes alone** — it recommends; a person approves

---

## 4. Why we use each component — business lens

This section answers: *“Why did we pick this? What does it do for the business?”*

### 4.1 Unity Catalog — “One trusted inventory of our data”

| | |
|---|---|
| **Business problem** | Different teams report different numbers; no one knows which table is official |
| **What it does** | Central registry of all tables, views, and permissions — like a governed catalog of business objects |
| **Why we use it** | Every dashboard, Genie question, and agent search reads from the **same certified data** |
| **Foundry equivalent** | Objects + ACLs |
| **Business outcome** | Executives trust the KPIs; compliance can audit who accessed what |

---

### 4.2 Delta Live Tables (DLT) — “Reliable pipelines that keep data fresh”

| | |
|---|---|
| **Business problem** | Incident and order data arrives continuously; bad or late data causes wrong dispatch |
| **What it does** | Automated pipeline from raw feeds → cleaned → business-ready tables, with quality checks |
| **Why we use it** | Replaces Foundry **Builds** — runs incrementally in minutes for demos, not hours |
| **Foundry equivalent** | Builds |
| **Business outcome** | Dispatch board and KPIs reflect the latest NOC feed without manual intervention |

---

### 4.3 Semantic layer (KPI views + dispatch board) — “Certified metrics everyone agrees on”

| | |
|---|---|
| **Business problem** | Every analyst defines “open P1” differently; meetings debate definitions |
| **What it does** | Pre-built, approved views: executive summary, incidents by market, dispatch board |
| **Why we use it** | Replaces Foundry **Slate** — one definition of MTTR, open incidents, SLA |
| **Foundry equivalent** | Slate dashboards + derived datasets |
| **Business outcome** | Same number in the app, in Genie, and in the board deck |

**Key views the business sees:**

| View | Business question it answers |
|------|------------------------------|
| Executive summary | How healthy is service delivery right now? |
| Open incidents by market | Where are we exposed geographically? |
| Dispatch board | Who is assigned to what, and what is the live status? |

---

### 4.4 Lakebase — “Instant operational actions”

| | |
|---|---|
| **Business problem** | Operators change incident status dozens of times per hour — they cannot wait for a batch job |
| **What it does** | Operational database (Postgres) optimized for **sub-second reads and writes** |
| **Why we use it** | Replaces Foundry **Actions** — writeback without touching the analytics pipeline |
| **Foundry equivalent** | Actions (update incident status) |
| **Business outcome** | Click *DISPATCHED* → board updates immediately; customer-facing teams see current state |

**Why not write directly to the data lake?**  
The lake is optimized for analytics and history. Operations need OLTP speed. Lakebase gives operators speed; the lake keeps the full record for reporting.

---

### 4.5 Ops Console App — “The operator’s single screen”

| | |
|---|---|
| **Business problem** | Operators juggle multiple tools; context switching slows incident response |
| **What it does** | One web app: dashboard, dispatch board, AI triage, demo controls, Genie |
| **Why we use it** | Replaces Foundry **Workshop** module — purpose-built UI for NOC workflows |
| **Foundry equivalent** | Workshop |
| **Business outcome** | Train once, work in one place; faster mean time to dispatch |

**Five tabs — who uses what:**

| Tab | Primary user | Business value |
|-----|--------------|----------------|
| **Dashboard** | Executive, ops lead | At-a-glance health — open P1s, orders, tech availability |
| **Dispatch** | NOC operator | Act on incidents; compare Lakebase vs warehouse speed in demos |
| **Agent Triage** | Operator + AI | AI finds incidents and recommends techs; human approves |
| **Live Demo** | Presenter, platform team | Show end-to-end pipeline live in customer meetings |
| **Genie** | Analyst, executive | Natural language analytics without SQL |

---

### 4.6 Genie — “Analytics for everyone, no SQL required”

| | |
|---|---|
| **Business problem** | Analyst backlog; executives wait days for simple market comparisons |
| **What it does** | Ask questions in English → Genie generates SQL on **governed tables only** → charts and tables |
| **Why we use it** | Net-new capability beyond classic Foundry — lowers barrier for business users |
| **Foundry equivalent** | Quiver + Slate (exploration), simplified |
| **Business outcome** | *“How many P1 incidents are open in Dallas?”* answered in seconds, on trusted data |

**Governance note:** Genie can only query tables we explicitly allow — no access to raw or experimental data.

---

### 4.7 Agent Triage — “AI that assists, not replaces”

| | |
|---|---|
| **Business problem** | High-volume triage burns operator time; full automation is too risky |
| **What it does** | AI searches incidents, recommends technicians, **proposes** assignment — operator must **Approve** |
| **Why we use it** | Replaces Foundry **AIP** with a human approval gate built in |
| **Foundry equivalent** | AIP / ontology functions |
| **Business outcome** | Faster triage, full audit trail, no silent autonomous writes |

**Demo story for the business:**

1. Search: *P1 incidents in Dallas*  
2. AI recommends TECH-201 (L2 fiber, available, same market)  
3. AI proposes: assign TECH-201 to INC-9001  
4. Operator clicks **Approve** → status becomes DISPATCHED  
5. Audit log records every step

---

### 4.8 Workflow jobs — “Repeatable, schedulable operations”

| | |
|---|---|
| **Business problem** | Manual pipeline steps don’t scale; demos fail when someone forgets a step |
| **What it does** | One-click jobs: refresh data, rebuild metrics, run live demo scenario |
| **Why we use it** | Replaces Foundry scheduled **Builds** + operational runbooks |
| **Foundry equivalent** | Scheduled builds |
| **Business outcome** | Presenter clicks “Add live data” in the app → entire pipeline runs reliably |

---

## 5. Before and after — what changes for the business

| Scenario | Before (typical pain) | After (this platform) |
|----------|----------------------|------------------------|
| Operator updates status | Wait for batch / refresh | Instant on dispatch board |
| Executive asks for KPIs | Request to analytics team | Open dashboard or ask Genie |
| AI suggests dispatch | Not available or not trusted | Recommend + approve workflow |
| New market onboarding | Custom integration | Redeploy bundle to new workspace |
| Audit “who changed what?” | Scattered logs | Agent audit log + MLflow events |
| Migration from Foundry | Fear of losing patterns | 1:1 mapping proven in demo |

---

## 6. How the pieces work together — one incident story

**Scenario:** New P1 fiber cut in Dallas during a customer demo.

| Step | What happens | Component | Business meaning |
|------|--------------|-----------|------------------|
| 1 | Presenter triggers “New P1 — Dallas” | Live Demo → Workflow job | Simulates NOC feed arriving |
| 2 | Raw JSON lands in storage | Unity Catalog Volume | Secure landing zone |
| 3 | Pipeline cleans and loads gold tables | DLT | Data is validated and current |
| 4 | KPI views and dispatch board refresh | Semantic layer | Everyone sees updated counts |
| 5 | Lakebase sync copies ops-relevant rows | Lakebase sync | Operational store is ready |
| 6 | Dashboard shows 6 open incidents, 3 P1 | Ops Console | Executive sees impact immediately |
| 7 | Operator opens Dispatch, sees INC-L* row | Materialized view + Lakebase overlay | Full context on one row |
| 8 | Analyst asks Genie about Dallas P1s | Genie | Self-serve answer in seconds |
| 9 | Agent recommends technician | Agent Triage | AI accelerates decision |
| 10 | Operator approves → DISPATCHED | Lakebase writeback | Action is instant and auditable |

**Total demo time:** ~3–5 minutes for pipeline refresh; operator actions are **immediate**.

---

## 7. Talking points for your presentation

### Opening (30 seconds)

> “ATT service delivery teams need one trusted view of incidents, orders, and technicians — and they need to act in seconds, not after a batch job. This demo shows the same Foundry-style workflows on Databricks: governed data, instant dispatch, AI with human approval, and analytics anyone can use.”

### When they ask “Why Databricks?”

> “We keep the business semantics — objects, actions, dashboards, AI — but gain an open lakehouse, native AI/BI, and operational Postgres through Lakebase. It’s migration without reinventing how NOC works.”

### When they ask “Why so many components?”

> “Each solves one business job: Catalog for trust, DLT for fresh data, semantic layer for agreed KPIs, Lakebase for speed, App for operators, Genie for analysts, Agent for assisted triage. None of them alone replaces the platform — together they do.”

### When they ask “Is AI safe?”

> “The agent never writes without approval. Every search, recommendation, and approval is logged. That’s by design — speed with accountability.”

### Closing (30 seconds)

> “You’ve seen batch and operations separated correctly: analytics stays governed, actions stay fast. That’s the architecture ATT needs for a Foundry-to-Databricks migration that the business can actually adopt.”

---

## 8. Component summary card (print-friendly)

| Component | Business job | Replaces (Foundry) | One-line why |
|-----------|--------------|-------------------|--------------|
| **Unity Catalog** | Trust & governance | Objects + ACLs | One source of truth |
| **DLT** | Fresh, clean data | Builds | Automated pipelines with quality |
| **Semantic views** | Agreed KPIs | Slate | Same metrics everywhere |
| **Lakebase** | Instant actions | Actions | Sub-second dispatch writes |
| **Ops Console App** | Operator UI | Workshop | One screen for NOC |
| **Genie** | Self-serve analytics | Quiver / exploration | English → trusted answers |
| **Agent Triage** | Assisted decisions | AIP | AI proposes, human approves |
| **Workflow jobs** | Reliable automation | Scheduled builds | Repeatable demo & production |

---

## 9. What we are NOT claiming

Be transparent with business audiences:

- This is a **demonstration environment** with seed and simulated live data — not production ATT network feeds  
- **High availability and disaster recovery** are out of scope for the demo  
- **Genie** answers depend on data freshness — after a live demo trigger, wait for the job to complete  
- **Lakebase** requires a one-time project setup per workspace  
- Full **Foundry feature parity** (e.g. every Workshop widget) is not the goal — **business workflow parity** is

---

## 10. Next steps for stakeholders

| Role | Suggested action |
|------|------------------|
| **Executive sponsor** | Review Dashboard + Genie executive summary question |
| **NOC / ops lead** | Walk through Dispatch + Agent approval flow |
| **Analytics lead** | Review semantic KPI definitions and Genie table allow-list |
| **Architecture / platform** | Review [Architecture Design](ATT_SDP_Architecture_Design.md) for technical depth |
| **Field engineering** | Use [installer](../install/README.md) to deploy to customer workspace |

---

## Related documents

| Document | Audience | Link |
|----------|----------|------|
| Business overview (this doc) | Executives, business sponsors | [ATT_SDP_Business_Overview.md](ATT_SDP_Business_Overview.md) |
| Architecture design | Architects, engineers | [ATT_SDP_Architecture_Design.md](ATT_SDP_Architecture_Design.md) |
| Project guide | Implementers, demo runners | [ATT_SDP_Project_Guide.md](ATT_SDP_Project_Guide.md) |
| Workspace installer | Platform / field eng | [install/README.md](../install/README.md) |

---

*Maintained in [pltr-dbx](https://github.com/anayyar82/pltr-dbx). For technical implementation details see the Architecture Design and Project Guide.*
