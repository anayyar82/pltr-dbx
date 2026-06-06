#!/usr/bin/env python3
"""
ATT SDP workspace installer — configure and deploy to a new Databricks workspace.

Usage:
  python3 install/sdp_install.py configure --config config/deployment.yaml
  python3 install/sdp_install.py validate
  python3 install/sdp_install.py deploy
  python3 install/sdp_install.py bootstrap [--skip-cleanup]
  python3 install/sdp_install.py deploy-app
  python3 install/sdp_install.py all --config config/deployment.yaml
  python3 install/sdp_install.py status
  python3 install/sdp_install.py init --interactive
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("PyYAML required: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONFIG = ROOT / "config" / "deployment.local.yaml"
EXAMPLE_CONFIG = ROOT / "config" / "deployment.example.yaml"
DATABRICKS_YML = ROOT / "databricks.yml"


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: Path | None = None) -> dict[str, Any]:
    if path and path.exists():
        with path.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    elif LOCAL_CONFIG.exists():
        with LOCAL_CONFIG.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        raise FileNotFoundError(
            f"No deployment config. Copy {EXAMPLE_CONFIG} to config/deployment.yaml "
            "or run: python3 install/sdp_install.py init --interactive"
        )
    return cfg


def save_local_config(cfg: dict[str, Any]) -> None:
    LOCAL_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    with LOCAL_CONFIG.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)
    print(f"Saved → {LOCAL_CONFIG}")


def _fq(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        cwd=ROOT,
        check=check,
        capture_output=capture,
        text=True,
    )


def _databricks_api(profile: str, method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method.lower(), path, "-p", profile, "--output", "json"]
    if body is not None:
        cmd.extend(["--json", json.dumps(body)])
    r = _run(cmd, capture=True)
    return json.loads(r.stdout) if r.stdout.strip() else {}


def prompt(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"{label}{suffix}: ").strip()
    return val or default


def cmd_init(args: argparse.Namespace) -> None:
    if not args.interactive and not args.config:
        print("Use --interactive or pass --config after editing deployment.example.yaml")
        sys.exit(1)

    if args.config:
        cfg = load_config(Path(args.config))
        save_local_config(cfg)
        cmd_configure(argparse.Namespace(config=str(args.config), no_validate=True))
        return

    print("\n=== ATT SDP Installer — interactive setup ===\n")
    cfg: dict[str, Any] = {
        "deployment": {},
        "workspace": {},
        "unity_catalog": {},
        "sql": {},
        "app": {},
        "lakebase": {"enabled": True},
        "genie": {"enabled": True},
        "service_principal": {},
        "install": {
            "run_cleanup_before_bootstrap": False,
            "run_semantic_setup": True,
            "deploy_app": True,
        },
    }
    cfg["deployment"]["target"] = prompt("Bundle target name", "my_sdp")
    cfg["deployment"]["profile"] = prompt("Databricks CLI profile", cfg["deployment"]["target"])
    cfg["deployment"]["bundle_root"] = prompt("Bundle root in workspace", "~/pltr-dbx")
    cfg["workspace"]["host"] = prompt("Workspace URL", "https://YOUR-WORKSPACE.cloud.databricks.com")
    cfg["workspace"]["workspace_id"] = prompt("Workspace org ID (?o= in URL)")
    cfg["unity_catalog"]["catalog"] = prompt("Unity Catalog", "users")
    cfg["unity_catalog"]["schema"] = prompt("Schema name (bronze/silver/gold)", "sdp_demo")
    cfg["sql"]["warehouse_id"] = prompt("SQL warehouse ID")
    cfg["app"]["name"] = prompt("App name", "att-sdp-ops-demo")
    cfg["app"]["user_email"] = prompt("Your Databricks user email")
    cfg["app"]["mlflow_experiment"] = prompt("MLflow experiment", "/Shared/att-sdp-ops-console")
    lb = prompt("Configure Lakebase? (y/n)", "y").lower().startswith("y")
    cfg["lakebase"]["enabled"] = lb
    if lb:
        cfg["lakebase"]["project_name"] = prompt("Lakebase project name", "att-sdp-demo")
        cfg["lakebase"]["branch"] = prompt("Lakebase branch", "production")
        cfg["lakebase"]["postgres_host"] = prompt("Lakebase direct Postgres host (not pooler)")
        cfg["lakebase"]["postgres_database"] = prompt("Postgres database", "sdp_ops")
        cfg["lakebase"]["postgres_schema"] = cfg["unity_catalog"]["schema"]
    cfg["genie"]["space_id"] = prompt("Genie space ID (blank to create later)", "")
    cfg["service_principal"]["app_client_id"] = prompt("App SP client ID (blank until after app deploy)", "")

    out = ROOT / "config" / "deployment.yaml"
    with out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)
    print(f"\nWrote {out}")
    save_local_config(cfg)
    cmd_configure(argparse.Namespace(config=str(out), no_validate=True))


def render_environments_local(cfg: dict) -> dict:
    dep = cfg["deployment"]
    uc = cfg["unity_catalog"]
    ws = cfg["workspace"]
    schema = uc["schema"]
    return {
        "environments": {
            dep["target"]: {
                "workspace_host": ws["host"].rstrip("/"),
                "catalog": uc["catalog"],
                "bronze_schema": schema,
                "silver_schema": schema,
                "gold_schema": schema,
            }
        }
    }


def render_lakebase(cfg: dict) -> dict:
    uc = cfg["unity_catalog"]
    lb = cfg["lakebase"]
    ws = cfg["workspace"]
    cat, schema = uc["catalog"], uc["schema"]
    project = lb["project_name"]
    branch = lb.get("branch", "production")
    return {
        "lakebase": {
            "workspace_host": ws["host"].rstrip("/"),
            "project_id": project,
            "branch_id": branch,
            "branch_resource": f"projects/{project}/branches/{branch}",
            "endpoint_resource": f"projects/{project}/branches/{branch}/endpoints/primary",
            "postgres_host": lb["postgres_host"],
            "postgres_pooler_host": lb.get("postgres_pooler_host", ""),
            "postgres_database": lb["postgres_database"],
            "postgres_schema": lb["postgres_schema"],
        },
        "catalog": cat,
        "gold_schema": schema,
        "synced_tables": [
            {
                "synced_table_id": _fq(cat, schema, "service_incident_pg"),
                "source_table": _fq(cat, schema, "service_incident"),
                "primary_key": ["incident_id"],
                "scheduling_policy": "TRIGGERED",
                "purpose": "Low-latency incident reads for Ops Console",
            },
            {
                "synced_table_id": _fq(cat, schema, "service_incident_ops_pg"),
                "source_table": _fq(cat, schema, "service_incident_ops"),
                "primary_key": ["incident_id"],
                "scheduling_policy": "TRIGGERED",
                "purpose": "Mirror ops-editable fields UC → Postgres",
            },
        ],
        "writeback": {
            "postgres_table": "service_incident_ops",
            "uc_table": _fq(cat, schema, "service_incident_ops"),
            "columns": [
                "incident_id",
                "status",
                "assigned_technician_id",
                "ops_notes",
                "updated_at",
                "updated_by",
            ],
            "foundry_action_rid": "ri.foundry.att.action.update-incident-status",
        },
    }


def render_genie_space(cfg: dict) -> dict:
    uc = cfg["unity_catalog"]
    genie = cfg.get("genie", {})
    cat, schema = uc["catalog"], uc["schema"]
    return {
        "genie_space": {
            "name": "att_sdp_service_delivery",
            "space_id": genie.get("space_id") or "",
            "title": "ATT SDP Service Delivery",
            "description": (
                "Natural language analytics for ATT Service Delivery Platform. "
                "Ask about open incidents, order fulfillment, technician availability, "
                "and market-level SLA performance."
            ),
            "catalog": cat,
            "schema": schema,
            "tables": [
                {"name": "service_incident", "description": "Network and service incidents"},
                {"name": "service_order", "description": "Service fulfillment orders"},
                {"name": "field_technician", "description": "Field technician roster"},
                {"name": "mv_incident_dispatch_board", "description": "Operational dispatch board"},
                {"name": "metric_sdp_executive_summary", "description": "Executive KPI rollup"},
                {"name": "metric_open_incidents_by_market", "description": "Open incidents by market"},
            ],
            "instructions": (
                "You are an analytics assistant for ATT Service Delivery Operations. "
                "Severity levels are P1 (critical) through P4 (low). "
                "Prefer mv_incident_dispatch_board for dispatch questions."
            ),
        }
    }


def render_app_yaml(cfg: dict) -> dict:
    dep = cfg["deployment"]
    uc = cfg["unity_catalog"]
    ws = cfg["workspace"]
    sql = cfg["sql"]
    app = cfg["app"]
    lb = cfg.get("lakebase", {})
    genie = cfg.get("genie", {})
    project = lb.get("project_name", "att-sdp-demo")
    branch = lb.get("branch", "production")
    schema = lb.get("postgres_schema") or uc["schema"]
    host = ws["host"].rstrip("/")

    doc: dict[str, Any] = {
        "name": app["name"],
        "description": (
            "ATT Service Delivery Platform ops console with Lakebase writeback and Genie analytics."
        ),
        "command": ["python", "app.py"],
        "user_api_scopes": ["dashboards.genie", "sql"],
        "resources": [],
        "env": [
            {"name": "DBX_CATALOG", "value": uc["catalog"]},
            {"name": "DBX_GOLD_SCHEMA", "value": uc["schema"]},
            {"name": "DATABRICKS_HOST", "value": host},
            {"name": "DATABRICKS_WAREHOUSE_ID", "value": str(sql["warehouse_id"])},
            {"name": "DATABRICKS_WORKSPACE_ID", "value": str(ws["workspace_id"])},
            {"name": "SDP_E2E_JOB_NAME", "value": "sdp_write_refresh"},
            {"name": "SDP_SEMANTIC_JOB_NAME", "value": "sdp_semantic_setup"},
            {"name": "MLFLOW_EXPERIMENT_NAME", "value": app.get("mlflow_experiment", "/Shared/att-sdp-ops-console")},
        ],
    }

    if genie.get("enabled", True) and genie.get("space_id"):
        doc["resources"].append(
            {
                "name": "sdp-genie",
                "genie_space": {"space_id": str(genie["space_id"]), "permission": "CAN_RUN"},
            }
        )
        doc["env"].append({"name": "DATABRICKS_GENIE_SPACE_ID", "value": str(genie["space_id"])})

    if lb.get("enabled", True):
        doc["resources"].append(
            {
                "name": "sdp_lakebase",
                "database": {
                    "project_name": project,
                    "branch": branch,
                    "database_name": lb["postgres_database"],
                },
            }
        )
        doc["env"].extend(
            [
                {"name": "LAKEBASE_PROJECT_ID", "value": project},
                {"name": "LAKEBASE_BRANCH", "value": branch},
                {"name": "LAKEBASE_HOST", "value": lb["postgres_host"]},
                {
                    "name": "LAKEBASE_ENDPOINT",
                    "value": f"projects/{project}/branches/{branch}/endpoints/primary",
                },
                {"name": "LAKEBASE_DB", "value": lb["postgres_database"]},
                {"name": "LAKEBASE_SCHEMA", "value": schema},
            ]
        )

    return doc


def patch_job_notebook_params() -> None:
    """Ensure all job notebook tasks pass bundle catalog/schema widgets."""
    params = {"catalog": "${var.catalog}", "schema": "${var.gold_schema}"}
    jobs_dir = ROOT / "resources" / "jobs"
    for path in sorted(jobs_dir.glob("*.yml")):
        with path.open(encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
        changed = False
        for job in (doc.get("resources") or {}).get("jobs", {}).values():
            for task in job.get("tasks") or []:
                nt = task.get("notebook_task")
                if not nt:
                    continue
                bp = nt.setdefault("base_parameters", {})
                for k, v in params.items():
                    if bp.get(k) != v:
                        bp[k] = v
                        changed = True
        if changed:
            write_yaml(path, doc)


def patch_databricks_yml(cfg: dict) -> None:
    """Add or update bundle target in databricks.yml from deployment config."""
    dep = cfg["deployment"]
    uc = cfg["unity_catalog"]
    ws = cfg["workspace"]
    sql = cfg["sql"]
    sp = cfg.get("service_principal", {}).get("app_client_id") or "be33de06-36a1-467e-926b-902c55903267"
    target = dep["target"]
    schema = uc["schema"]

    with DATABRICKS_YML.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    doc.setdefault("variables", {})
    doc["variables"].update(
        {
            "catalog": {"description": "Unity Catalog name", "default": uc["catalog"]},
            "bronze_schema": {"default": schema},
            "silver_schema": {"default": schema},
            "gold_schema": {"default": schema},
            "sql_warehouse_id": {
                "description": "SQL warehouse for MV refresh",
                "default": str(sql["warehouse_id"]),
            },
            "app_service_principal_id": {
                "description": "App service principal for job CAN_MANAGE_RUN",
                "default": sp,
            },
        }
    )

    doc.setdefault("targets", {})
    doc["targets"][target] = {
        "mode": "development",
        "default": True,
        "workspace": {
            "host": ws["host"].rstrip("/"),
            "root_path": dep.get("bundle_root", "~/pltr-dbx"),
        },
        "variables": {
            "catalog": uc["catalog"],
            "bronze_schema": schema,
            "silver_schema": schema,
            "gold_schema": schema,
            "sql_warehouse_id": str(sql["warehouse_id"]),
            "app_service_principal_id": sp,
        },
    }

    # Only one default target
    for name, tcfg in doc["targets"].items():
        if name != target and isinstance(tcfg, dict):
            tcfg["default"] = False

    with DATABRICKS_YML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)
    print(f"Updated {DATABRICKS_YML} (target={target})")


def write_yaml(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    print(f"Wrote {path}")


def cmd_configure(args: argparse.Namespace) -> None:
    cfg_path = Path(args.config) if args.config else None
    cfg = load_config(cfg_path)
    save_local_config(cfg)

    write_yaml(ROOT / "config" / "environments.local.yaml", render_environments_local(cfg))
    write_yaml(ROOT / "config" / "genie_space.yaml", render_genie_space(cfg))
    write_yaml(ROOT / "apps" / "sdp_ops_console" / "app.yaml", render_app_yaml(cfg))

    if cfg.get("lakebase", {}).get("enabled", True):
        write_yaml(ROOT / "config" / "lakebase.yaml", render_lakebase(cfg))
    else:
        print("Skipped lakebase.yaml (lakebase.enabled=false)")

    patch_databricks_yml(cfg)
    patch_job_notebook_params()

    dep = cfg["deployment"]
    app = cfg["app"]
    ws = cfg["workspace"]
    print(
        textwrap.dedent(
            f"""
            === Configuration applied ===
            Target:   {dep['target']}
            Profile:  {dep['profile']}
            Catalog:  {cfg['unity_catalog']['catalog']}.{cfg['unity_catalog']['schema']}
            App:      {app['name']}
            App URL:  https://{app['name']}-{ws['workspace_id']}.aws.databricksapps.com/

            Next: python3 install/sdp_install.py validate
                  python3 install/sdp_install.py deploy
                  python3 install/sdp_install.py bootstrap
                  python3 install/sdp_install.py deploy-app
            """
        )
    )

    if not getattr(args, "no_validate", False) and not getattr(args, "skip_validate", False):
        cmd_validate(argparse.Namespace())


def cmd_validate(args: argparse.Namespace) -> None:
    cfg = load_config()
    dep = cfg["deployment"]
    profile, target = dep["profile"], dep["target"]

    try:
        _run(["databricks", "auth", "profiles"], check=True)
    except subprocess.CalledProcessError:
        print(f"Run: databricks auth login --profile {profile}")
        sys.exit(1)

    try:
        _run(["databricks", "bundle", "validate", "-t", target, "--profile", profile])
    except subprocess.CalledProcessError:
        sys.exit(1)

    print("Validation OK")


def cmd_deploy(args: argparse.Namespace) -> None:
    cfg = load_config()
    dep = cfg["deployment"]
    _run(
        [
            "databricks",
            "bundle",
            "deploy",
            "-t",
            dep["target"],
            "--profile",
            dep["profile"],
            "--auto-approve",
        ]
    )


def cmd_bootstrap(args: argparse.Namespace) -> None:
    cfg = load_config()
    dep = cfg["deployment"]
    profile, target = dep["profile"], dep["target"]
    skip_cleanup = args.skip_cleanup or not cfg.get("install", {}).get(
        "run_cleanup_before_bootstrap", False
    )

    if not skip_cleanup:
        print("==> Cleanup (drop tables, views, MV, bronze volumes)")
        _run(["databricks", "bundle", "run", "sdp_cleanup", "-t", target, "--profile", profile])

    print("==> Semantic setup (gold + seed + DLT full refresh + MV + Lakebase sync)")
    _run(["databricks", "bundle", "run", "sdp_semantic_setup", "-t", target, "--profile", profile])

    print("Bootstrap complete. Deploy the App next: python3 install/sdp_install.py deploy-app")


def cmd_deploy_app(args: argparse.Namespace) -> None:
    cfg = load_config()
    dep = cfg["deployment"]
    app = cfg["app"]
    profile = dep["profile"]
    app_name = app["name"]
    user_email = app["user_email"]
    workspace_app_dir = f"/Workspace/Users/{user_email}/apps/{app_name}"

    # Create app resource if it does not exist
    try:
        _databricks_api(profile, "GET", f"/api/2.0/apps/{app_name}")
    except subprocess.CalledProcessError:
        print(f"Creating app {app_name}...")
        _databricks_api(
            profile,
            "POST",
            "/api/2.0/apps",
            {
                "name": app_name,
                "description": "ATT SDP Ops Console with Lakebase writeback and Genie",
            },
        )

    app_src = ROOT / "apps" / "sdp_ops_console"
    files = [
        ("app.py", "SOURCE"),
        ("app.yaml", "SOURCE"),
        ("viz.py", "SOURCE"),
        ("mlflow_tracker.py", "SOURCE"),
        ("requirements.txt", "SOURCE"),
        ("templates/index.html", "SOURCE"),
    ]

    for rel, fmt in files:
        local = app_src / rel
        remote = f"{workspace_app_dir}/{rel}"
        _run(
            [
                "databricks",
                "workspace",
                "import",
                "--file",
                str(local),
                "--format",
                fmt,
                "--overwrite",
                remote,
                "-p",
                profile,
            ]
        )

    _run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/apps/{}/deployments".format(app_name),
            "-p",
            profile,
            "--json",
            json.dumps(
                {
                    "source_code_path": workspace_app_dir,
                    "mode": "SNAPSHOT",
                }
            ),
        ]
    )

    ws = cfg["workspace"]
    print(
        f"\nApp deployment triggered.\n"
        f"URL: https://{app_name}-{ws['workspace_id']}.aws.databricksapps.com/\n"
        f"Health: https://{app_name}-{ws['workspace_id']}.aws.databricksapps.com/health\n"
        f"\nAfter deploy succeeds, copy the App service principal ID into deployment.yaml "
        f"(service_principal.app_client_id) and re-run configure + deploy for job permissions."
    )


def cmd_status(args: argparse.Namespace) -> None:
    cfg = load_config()
    dep = cfg["deployment"]
    uc = cfg["unity_catalog"]
    app = cfg["app"]
    ws = cfg["workspace"]
    print(yaml.safe_dump(cfg, default_flow_style=False, sort_keys=False))
    print(
        f"App URL: https://{app['name']}-{ws['workspace_id']}.aws.databricksapps.com/\n"
        f"Bundle:  databricks bundle deploy -t {dep['target']} --profile {dep['profile']}\n"
        f"Schema:  {uc['catalog']}.{uc['schema']}"
    )


def cmd_all(args: argparse.Namespace) -> None:
    if args.config:
        cmd_configure(argparse.Namespace(config=args.config, skip_validate=True))
    elif not LOCAL_CONFIG.exists():
        print("Pass --config config/deployment.yaml or run init first")
        sys.exit(1)

    cfg = load_config()
    cmd_validate(argparse.Namespace())

    if cfg.get("install", {}).get("run_semantic_setup", True):
        cmd_deploy(argparse.Namespace())
        skip = not cfg.get("install", {}).get("run_cleanup_before_bootstrap", False)
        cmd_bootstrap(argparse.Namespace(skip_cleanup=skip))
    else:
        cmd_deploy(argparse.Namespace())

    if cfg.get("install", {}).get("deploy_app", True):
        cmd_deploy_app(argparse.Namespace())

    cmd_status(argparse.Namespace())
    print(
        textwrap.dedent(
            """
            === Install complete ===
            1. Open App /health — confirm sql_ok and lakebase_ok
            2. Grant App SP USE CATALOG + MODIFY on your schema + CAN_USE on warehouse
            3. Grant App SP CAN_MANAGE_RUN on job sdp_write_refresh (for Live Demo tab)
            4. Create Genie space if needed: DATABRICKS_WAREHOUSE_ID=... python3 scripts/create_att_sdp_genie_space.py
            5. Run Lakebase OAuth setup: python3 scripts/setup_lakebase_app_oauth.py
            """
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ATT SDP installer — deploy to a new Databricks workspace",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Create config/deployment.yaml interactively")
    p_init.add_argument("--interactive", action="store_true", help="Prompt for all values")
    p_init.add_argument("--config", type=Path, help="Use existing YAML instead of prompts")

    p_cfg = sub.add_parser("configure", help="Apply deployment.yaml to repo config files")
    p_cfg.add_argument("--config", type=Path, help="Path to deployment.yaml")
    p_cfg.add_argument("--no-validate", action="store_true")

    sub.add_parser("validate", help="Validate CLI auth and bundle")
    sub.add_parser("deploy", help="databricks bundle deploy")
    p_boot = sub.add_parser("bootstrap", help="Run semantic setup job (optionally cleanup first)")
    p_boot.add_argument("--skip-cleanup", action="store_true")
    sub.add_parser("deploy-app", help="Upload and deploy Ops Console App")
    sub.add_parser("status", help="Show current deployment config")
    p_all = sub.add_parser("all", help="configure → validate → deploy → bootstrap → deploy-app")
    p_all.add_argument("--config", type=Path, help="Path to deployment.yaml")

    args = parser.parse_args()
    handlers = {
        "init": cmd_init,
        "configure": cmd_configure,
        "validate": cmd_validate,
        "deploy": cmd_deploy,
        "bootstrap": cmd_bootstrap,
        "deploy-app": cmd_deploy_app,
        "status": cmd_status,
        "all": cmd_all,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
