"""
Report output writers.

Converts a Report model into JSON / CSV / HTML files and generates
summary text for notification channels (Feishu, etc.).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .models import Report, Status


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(report: Report, out_dir: str) -> Path:
    """Write report.json with full structured results."""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.json"
    fp.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    return fp


def write_csv(report: Report, out_dir: str) -> Path:
    """Write report.csv with a flat row per check."""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.csv"
    with fp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["generated_at", "cluster", "check", "status", "summary", "duration_ms"])
        for r in report.results:
            w.writerow([report.generated_at, report.cluster, r.name, r.status.value, r.summary, r.duration_ms or ""])
    return fp


def write_html(report: Report, out_dir: str) -> Path:
    """Write report.html for human review."""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.html"
    rows: list[str] = []
    for r in report.results:
        cls = r.status.value.lower()
        rows.append(
            "<tr>"
            f"<td>{_esc(r.name)}</td>"
            f"<td class='{cls}'>{_esc(r.status.value)}</td>"
            f"<td>{_esc(r.summary)}</td>"
            f"<td>{'' if r.duration_ms is None else r.duration_ms}</td>"
            "</tr>"
        )

    summary = report.to_dict().get("summary", {})
    html = f"""<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>K8s Inspection Report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, "Noto Sans", sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #fafafa; text-align: left; }}
    .pass {{ color: #0a7d30; font-weight: 600; }}
    .warn {{ color: #a66a00; font-weight: 600; }}
    .fail {{ color: #b00020; font-weight: 600; }}
    .skip {{ color: #666; font-weight: 600; }}
    .meta {{ margin: 0 0 12px 0; color: #444; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #f3f4f6; margin-right: 8px; }}
  </style>
</head>
<body>
  <h1>K8s Inspection Report</h1>
  <p class="meta">
    <span class="pill">generated_at: {_esc(report.generated_at)}</span>
    <span class="pill">cluster: {_esc(report.cluster)}</span>
    <span class="pill">total: {summary.get("total", 0)}</span>
    <span class="pill pass">pass: {summary.get("pass", 0)}</span>
    <span class="pill warn">warn: {summary.get("warn", 0)}</span>
    <span class="pill fail">fail: {summary.get("fail", 0)}</span>
    <span class="pill skip">skip: {summary.get("skip", 0)}</span>
  </p>
  <table>
    <thead>
      <tr><th>Check</th><th>Status</th><th>Summary</th><th>Duration(ms)</th></tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</body>
</html>
"""
    fp.write_text(html, encoding="utf-8")
    return fp


def _esc(s: Any) -> str:
    """Minimal HTML escaping to prevent injection."""
    text = "" if s is None else str(s)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def format_failed_summary(report: Report) -> str:
    """
    Generate a compact summary text for notification channels.
    Shows up to 10 FAIL and 10 WARN items to avoid flooding.
    """
    fails = [r for r in report.results if r.status == Status.FAIL]
    warns = [r for r in report.results if r.status == Status.WARN]
    lines = [f"Cluster: {report.cluster}", f"Time: {report.generated_at}"]
    if fails:
        lines.append(f"FAIL: {len(fails)}")
        for r in fails[:10]:
            lines.append(f"- {r.name}: {r.summary}")
    if warns:
        lines.append(f"WARN: {len(warns)}")
        for r in warns[:10]:
            lines.append(f"- {r.name}: {r.summary}")
    return "\n".join(lines)
