"""Infer chart types from Genie SQL result sets for in-app dashboards."""

from __future__ import annotations

import re


def _is_numeric(value) -> bool:
    if value is None or value == "":
        return False
    try:
        float(str(value).replace(",", ""))
        return True
    except (TypeError, ValueError):
        return False


def _to_float(value) -> float:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _question_hints(question: str) -> dict:
    q = (question or "").lower()
    return {
        "line": any(w in q for w in ("trend", "over time", "timeline", "mttr", "history", "daily", "weekly")),
        "pie": any(w in q for w in ("share", "percent", "distribution", "breakdown", "proportion", "mix")),
        "compare": any(w in q for w in ("compare", "across", "by market", "by severity", "versus", "vs ")),
        "summary": any(w in q for w in ("summary", "executive", "overview", "how many", "total", "count")),
        "table": any(w in q for w in ("list", "show all", "details", "which incidents", "dispatch board")),
    }


def infer_visualization(question: str, columns: list[str], rows: list[list]) -> dict:
    """Return a chart spec for the frontend (Chart.js compatible)."""
    if not columns or not rows:
        return {"type": "empty"}

    hints = _question_hints(question)
    ncol = len(columns)
    numeric_idxs = [
        i for i in range(ncol)
        if any(_is_numeric(r[i]) for r in rows[:8] if i < len(r))
    ]
    text_idxs = [i for i in range(ncol) if i not in numeric_idxs]

    # Single-row rollup → KPI cards
    if len(rows) == 1 and numeric_idxs:
        items = []
        for i, col in enumerate(columns):
            val = rows[0][i] if i < len(rows[0]) else None
            if _is_numeric(val):
                items.append({"label": col.replace("_", " ").title(), "value": _to_float(val)})
            elif val is not None and str(val).strip():
                items.append({"label": col.replace("_", " ").title(), "value": str(val), "text": True})
        if items:
            return {"type": "kpi", "items": items}

    # Pick dimension + measure columns
    if not numeric_idxs:
        return {"type": "table", "columns": columns, "rows": rows}

    label_idx = text_idxs[0] if text_idxs else 0
    value_idx = numeric_idxs[0] if numeric_idxs[0] != label_idx else (numeric_idxs[1] if len(numeric_idxs) > 1 else numeric_idxs[0])

    labels = [str(r[label_idx]) if label_idx < len(r) else "" for r in rows[:24]]
    values = [_to_float(r[value_idx]) if value_idx < len(r) else 0 for r in rows[:24]]
    measure_name = columns[value_idx].replace("_", " ").title()

    if hints["table"] or (ncol > 3 and len(rows) > 12):
        return {"type": "table", "columns": columns, "rows": rows}

    chart_type = "bar"
    if hints["line"] or any(re.search(r"(date|time|hour|day|month|week|opened|updated)", c, re.I) for c in columns):
        chart_type = "line"
    elif hints["pie"] and len(rows) <= 10:
        chart_type = "pie"
    elif len(rows) >= 2:
        chart_type = "bar"

    # Multi-series: extra numeric columns
    extra_series = []
    for idx in numeric_idxs[1:3]:
        if idx == label_idx:
            continue
        extra_series.append({
            "label": columns[idx].replace("_", " ").title(),
            "data": [_to_float(r[idx]) if idx < len(r) else 0 for r in rows[:24]],
        })

    datasets = [{"label": measure_name, "data": values}] + extra_series

    return {
        "type": chart_type,
        "title": measure_name,
        "labels": labels,
        "datasets": datasets,
        "columns": columns,
        "rows": rows,
    }


def build_dashboard(question: str, datasets: list[dict]) -> dict:
    """Combine multiple Genie result sets into a dashboard layout."""
    panels = []
    for i, ds in enumerate(datasets):
        cols = ds.get("columns") or []
        rows = ds.get("rows") or []
        viz = ds.get("visualization") or infer_visualization(question, cols, rows)
        viz["panel_id"] = i
        if ds.get("sql"):
            viz["sql"] = ds["sql"]
        panels.append(viz)

    if not panels:
        return {"layout": "empty", "panels": []}

    primary = panels[0]
    layout = "single"
    if len(panels) > 1:
        layout = "multi"
    elif primary.get("type") == "kpi":
        layout = "kpi"
    elif primary.get("type") in ("bar", "line", "pie"):
        layout = "chart"

    return {"layout": layout, "panels": panels, "question": question}
