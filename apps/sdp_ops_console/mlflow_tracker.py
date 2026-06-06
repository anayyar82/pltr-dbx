"""MLflow tracking for ATT SDP Ops Console demo events."""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "/Shared/att-sdp-ops-console")
WORKSPACE_HOST = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com").rstrip("/")
WORKSPACE_ID = os.getenv("DATABRICKS_WORKSPACE_ID", "1444828305810485")

_mlflow_ready: bool | None = None
_experiment_id: str | None = None


def _init_mlflow() -> bool:
    global _mlflow_ready, _experiment_id
    if _mlflow_ready is not None:
        return _mlflow_ready
    try:
        import mlflow

        mlflow.set_tracking_uri("databricks")
        exp = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
        if exp is None:
            _experiment_id = mlflow.create_experiment(EXPERIMENT_NAME)
        else:
            _experiment_id = exp.experiment_id
        mlflow.set_experiment(experiment_id=_experiment_id)
        _mlflow_ready = True
    except Exception:
        _mlflow_ready = False
    return _mlflow_ready


def experiment_url() -> str | None:
    if not _init_mlflow() or not _experiment_id:
        return None
    return f"{WORKSPACE_HOST}/ml/experiments/{_experiment_id}?o={WORKSPACE_ID}"


def status() -> dict:
    ready = _init_mlflow()
    return {
        "enabled": ready,
        "experiment_name": EXPERIMENT_NAME,
        "experiment_id": _experiment_id,
        "experiment_url": experiment_url() if ready else None,
    }


def track_event(
    event_type: str,
    params: dict[str, Any] | None = None,
    metrics: dict[str, float] | None = None,
    tags: dict[str, str] | None = None,
) -> str | None:
    """Log one MLflow run per app event. Returns run_id or None."""
    if not _init_mlflow():
        return None
    try:
        import mlflow

        run_name = f"{event_type}-{int(time.time())}"
        with mlflow.start_run(run_name=run_name, experiment_id=_experiment_id):
            mlflow.set_tag("event_type", event_type)
            mlflow.set_tag("app", "att-sdp-ops-console")
            mlflow.set_tag("trace_id", str(uuid.uuid4())[:8])
            for key, value in (tags or {}).items():
                mlflow.set_tag(str(key)[:250], str(value)[:250])
            for key, value in (params or {}).items():
                if value is not None:
                    mlflow.log_param(str(key)[:250], str(value)[:500])
            for key, value in (metrics or {}).items():
                try:
                    mlflow.log_metric(str(key)[:250], float(value))
                except (TypeError, ValueError):
                    pass
            return mlflow.active_run().info.run_id
    except Exception:
        return None
