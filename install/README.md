# ATT SDP Installer

Deploy the ATT Service Delivery Platform demo to a **new Databricks workspace** using a config file and CLI commands.

## Quick start

```bash
# 1. Clone and install Python deps
git clone https://github.com/anayyar82/pltr-dbx.git
cd pltr-dbx
pip install -r requirements.txt

# 2. Log in to your target workspace
databricks auth login --profile my-workspace

# 3. Create deployment config (interactive or copy example)
./install.sh init --interactive
# OR:
cp config/deployment.example.yaml config/deployment.yaml
# edit config/deployment.yaml

# 4. Full install
./install.sh all --config config/deployment.yaml
```

## Commands

| Command | Description |
|---------|-------------|
| `./install.sh init --interactive` | Prompt for workspace settings; writes `config/deployment.yaml` |
| `./install.sh configure --config config/deployment.yaml` | Patch `databricks.yml`, `app.yaml`, `lakebase.yaml`, `genie_space.yaml` |
| `./install.sh validate` | Check CLI auth + `databricks bundle validate` |
| `./install.sh deploy` | `databricks bundle deploy` |
| `./install.sh bootstrap` | Run `sdp_semantic_setup` job (gold + DLT + MV + Lakebase sync) |
| `./install.sh bootstrap --skip-cleanup` | Bootstrap without dropping existing tables |
| `./install.sh deploy-app` | Upload Ops Console App and trigger deployment |
| `./install.sh all --config config/deployment.yaml` | configure → validate → deploy → bootstrap → deploy-app |
| `./install.sh status` | Show active deployment config |

## What gets configured

The installer writes workspace-specific values into:

| File | Purpose |
|------|---------|
| `config/deployment.local.yaml` | Saved deployment state (gitignored) |
| `config/environments.local.yaml` | Environment overrides (gitignored) |
| `databricks.yml` | Bundle target, catalog, schema, warehouse, app SP |
| `apps/sdp_ops_console/app.yaml` | App env vars (catalog, Lakebase, Genie, warehouse) |
| `config/lakebase.yaml` | Lakebase project, Postgres host, synced tables |
| `config/genie_space.yaml` | Genie catalog/schema and table manifest |

## Prerequisites (target workspace)

1. **Unity Catalog** — `USE CATALOG` on your catalog; `CREATE SCHEMA` on your schema (or use existing)
2. **SQL Warehouse** — Serverless or Pro warehouse; note the warehouse ID
3. **Lakebase project** (optional but recommended) — Create project + `sdp_ops` database; note **direct** Postgres host
4. **Databricks Apps** enabled in workspace
5. **Permissions** — Your user can create jobs, pipelines, and apps

## Post-install checklist

After `./install.sh all`:

1. **App health** — `https://<app-name>-<workspace-id>.aws.databricksapps.com/health`
2. **Grant App SP** — `USE CATALOG`, `USE SCHEMA`, `MODIFY` on your schema; `CAN_USE` on warehouse
3. **Job permissions** — App SP needs `CAN_MANAGE_RUN` on `sdp_write_refresh` (Live Demo tab)
4. **Genie space** — If `genie.space_id` was empty:
   ```bash
   export DATABRICKS_WAREHOUSE_ID=<warehouse_id>
   python3 scripts/create_att_sdp_genie_space.py --profile my-workspace
   ```
   Then add `space_id` to `deployment.yaml` and re-run `./install.sh configure`
5. **Lakebase OAuth** — `python3 scripts/setup_lakebase_app_oauth.py` (uses App SP from deployment config)

## Incremental deploy (existing workspace)

```bash
./install.sh configure --config config/deployment.yaml
./install.sh deploy
databricks bundle run sdp_write_refresh -t <target> --profile <profile>
```

## Config reference

See `config/deployment.example.yaml` for all fields:

- `deployment.target` — bundle target name (`-t` flag)
- `deployment.profile` — Databricks CLI profile
- `workspace.host` / `workspace.workspace_id` — workspace URL and org ID (`?o=` in browser)
- `unity_catalog.catalog` / `unity_catalog.schema` — UC location for all gold tables
- `sql.warehouse_id` — SQL warehouse for MV refresh and Genie
- `app.name` / `app.user_email` — App name and workspace user for app folder path
- `lakebase.*` — Lakebase project and Postgres connection (set `enabled: false` to skip)
- `service_principal.app_client_id` — fill after first app deploy for job trigger permissions

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `bundle validate` fails | Check profile: `databricks auth login --profile <name>` |
| App deploy 404 | Create app once in UI or ensure `app.name` is unique |
| Lakebase SASL error | Use **direct** Postgres host, not `-pooler` host |
| Live Demo job fails | Grant App SP `CAN_MANAGE_RUN` on `sdp_write_refresh`; re-run `configure` with SP ID |
| Empty dispatch board | Run `./install.sh bootstrap` or `databricks bundle run sdp_semantic_setup` |
