#!/usr/bin/env python3
"""Build business overview DOCX from docs/ATT_SDP_Business_Overview.md.

Usage:
  python3 scripts/build_business_overview.py
"""

from __future__ import annotations

import html
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IN_MD = ROOT / "docs" / "ATT_SDP_Business_Overview.md"
OUT_HTML = ROOT / "docs" / ".build_business_overview.html"
OUT_DOCX = ROOT / "docs" / "ATT_SDP_Business_Overview.docx"


def md_to_html(md: str) -> str:
    lines = md.split("\n")
    out: list[str] = []
    in_table = False
    in_code = False

    css = """
    body { font-family: Calibri, Arial, sans-serif; font-size: 11pt; line-height: 1.45; margin: 0.75in; color: #1a1a1a; }
    h1 { font-size: 24pt; color: #0a2540; border-bottom: 3px solid #0095da; padding-bottom: 8px; }
    h2 { font-size: 16pt; color: #0095da; margin-top: 1.5em; border-bottom: 1px solid #ddd; padding-bottom: 4px; }
    h3 { font-size: 13pt; color: #333; margin-top: 1.2em; }
    h4 { font-size: 11pt; color: #555; font-weight: bold; }
    table { border-collapse: collapse; width: 100%; margin: 0.8em 0; font-size: 10pt; }
    th { background: #0a2540; color: white; text-align: left; padding: 7px 10px; }
    td { border: 1px solid #ccc; padding: 6px 10px; vertical-align: top; }
    pre { background: #f6f8fa; padding: 12px; font-size: 9pt; border: 1px solid #ddd; font-family: Menlo, monospace; }
    hr { border: none; border-top: 1px solid #ddd; margin: 1.5em 0; }
    p { margin: 0.5em 0; }
    blockquote { border-left: 4px solid #0095da; margin: 1em 0; padding: 0.5em 1em; background: #f0f8ff; font-style: italic; }
    """

    for line in lines:
        if line.strip().startswith("```"):
            if in_code:
                out.append("</pre>")
                in_code = False
            else:
                out.append("<pre>")
                in_code = True
            continue
        if in_code:
            out.append(html.escape(line) + "\n")
            continue

        if line.startswith("|") and "|" in line[1:]:
            cells = [c.strip() for c in line.strip("|").split("|")]
            if all(re.match(r"^[-:]+$", c) for c in cells):
                continue
            tag = "th" if not in_table else "td"
            if not in_table:
                out.append("<table>")
                in_table = True
            row = "".join(f"<{tag}>{html.escape(c)}</{tag}>" for c in cells)
            out.append(f"<tr>{row}</tr>")
            continue
        elif in_table:
            out.append("</table>")
            in_table = False

        if line.startswith("# "):
            out.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            out.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("### "):
            out.append(f"<h3>{html.escape(line[4:])}</h3>")
        elif line.startswith("#### "):
            out.append(f"<h4>{html.escape(line[5:])}</h4>")
        elif line.strip() == "---":
            out.append("<hr/>")
        elif line.strip().startswith("> "):
            out.append(f"<blockquote><p>{html.escape(line.strip()[2:])}</p></blockquote>")
        elif line.strip().startswith("- "):
            out.append(f"<p>• {html.escape(line.strip()[2:])}</p>")
        elif line.strip():
            esc = html.escape(line)
            esc = re.sub(r"`([^`]+)`", r"<code>\1</code>", esc)
            esc = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", esc)
            out.append(f"<p>{esc}</p>")
        else:
            out.append("<br/>")

    if in_table:
        out.append("</table>")
    if in_code:
        out.append("</pre>")

    return f"<!DOCTYPE html><html><head><meta charset='utf-8'><style>{css}</style></head><body>{''.join(out)}</body></html>"


def main() -> None:
    if not IN_MD.exists():
        raise SystemExit(f"Missing {IN_MD}")
    md = IN_MD.read_text(encoding="utf-8")
    OUT_HTML.write_text(md_to_html(md), encoding="utf-8")
    r = subprocess.run(
        ["textutil", "-convert", "docx", str(OUT_HTML), "-output", str(OUT_DOCX)],
        capture_output=True,
        text=True,
    )
    OUT_HTML.unlink(missing_ok=True)
    if r.returncode == 0 and OUT_DOCX.exists():
        print(f"Wrote {OUT_DOCX} ({OUT_DOCX.stat().st_size // 1024} KB)")
    else:
        print(f"DOCX failed: {r.stderr}")
        print(f"Use {IN_MD} directly")


if __name__ == "__main__":
    main()
