# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Deploy Ops Console App
# MAGIC Copies bundle app sources to the Apps workspace folder and triggers deployment.

# COMMAND ----------

import base64
import json
import urllib.parse
import urllib.request

dbutils.widgets.text("app_name", "att-sdp-ops-ankur")
dbutils.widgets.text(
    "workspace_app_dir",
    "/Workspace/Users/ankur.nayyar@databricks.com/apps/att-sdp-ops-ankur",
)
dbutils.widgets.text("bundle_root", "")

APP_NAME = dbutils.widgets.get("app_name")
WORKSPACE_APP_DIR = dbutils.widgets.get("workspace_app_dir").rstrip("/")
BUNDLE_ROOT = dbutils.widgets.get("bundle_root").strip()

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = ctx.apiUrl().get().rstrip("/")
TOKEN = ctx.apiToken().get()


def _api(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        f"{HOST}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def _resolve_bundle_root() -> str:
    if BUNDLE_ROOT:
        return BUNDLE_ROOT.rstrip("/")
    for candidate in (
        "/Workspace/Users/ankur.nayyar@databricks.com/pltr-dbx/files",
        "/Workspace/Users/ankur.nayyar@databricks.com/pltr-dbx",
    ):
        try:
            _api("GET", f"/api/2.0/workspace/get-status?path={urllib.parse.quote(candidate + '/apps/sdp_ops_console/app.py')}")
            return candidate
        except Exception:
            continue
    raise FileNotFoundError("Set bundle_root widget to the synced bundle path (…/pltr-dbx/files)")


def _export_workspace_file(path: str) -> bytes:
    result = _api("GET", f"/api/2.0/workspace/export?path={urllib.parse.quote(path)}&format=SOURCE")
    return base64.b64decode(result["content"])


def _import_workspace_file(path: str, content: bytes, *, language: str | None = None) -> None:
    payload = {
        "path": path,
        "format": "SOURCE" if language else "AUTO",
        "overwrite": True,
        "content": base64.b64encode(content).decode("utf-8"),
    }
    if language:
        payload["language"] = language
    _api("POST", "/api/2.0/workspace/import", payload)


bundle_root = _resolve_bundle_root()
app_src_dir = f"{bundle_root}/apps/sdp_ops_console"
print(f"Bundle root : {bundle_root}")
print(f"App source  : {app_src_dir}")
print(f"Deploy path : {WORKSPACE_APP_DIR}")

# COMMAND ----------

for filename, language in (
    ("app.py", "PYTHON"),
    ("app.yaml", None),
    ("requirements.txt", None),
):
    src = f"{app_src_dir}/{filename}"
    dst = f"{WORKSPACE_APP_DIR}/{filename}"
    content = _export_workspace_file(src)
    if language:
        _import_workspace_file(dst, content, language=language)
    else:
        _import_workspace_file(dst, content)
    print(f"Copied {filename}")

# COMMAND ----------

result = _api(
    "POST",
    f"/api/2.0/apps/{APP_NAME}/deployments",
    {"source_code_path": WORKSPACE_APP_DIR, "mode": "SNAPSHOT"},
)

print(json.dumps(result, indent=2))
print(f"""
APP DEPLOY TRIGGERED
====================
Name : {APP_NAME}
Path : {WORKSPACE_APP_DIR}
URL  : https://{APP_NAME}-1444828305810485.aws.databricksapps.com/
""")
