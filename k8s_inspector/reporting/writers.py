"""
Report output writers.

Converts a Report model into JSON / CSV / HTML files and generates
summary text for notification channels (Feishu, etc.).
"""

from __future__ import annotations

import csv
import json
import io
from pathlib import Path
from typing import Any

from k8s_inspector.core.models import Report, Status

from k8s_inspector.core.files import atomic_write_text, _ensure_dir


def write_json(report: Report, out_dir: str) -> Path:
    """Write report.json with full structured results."""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.json"
    atomic_write_text(fp, json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return fp


def write_csv(report: Report, out_dir: str) -> Path:
    """Write report.csv with a flat row per check."""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.csv"
    buffer = io.StringIO(newline="")
    w = csv.writer(buffer)
    columns = ["generated_at", "cluster", "check", "status", "summary", "duration_ms"]
    if report.schema_version == 2:
        columns += ['schema_version', 'run_id', 'rule_id', 'execution_status', 'observed_at', 'coverage_complete',
                    'resource_kind', 'namespace', 'resource_name', 'resource_uid', 'container', 'reason', 'evidence']
    w.writerow(columns)
    rows = report.results
    if report.schema_version==2:
        from k8s_inspector.core.observations import CheckEvaluation, to_legacy_result
        rows = [to_legacy_result(CheckEvaluation(e,tuple(f for f in report.findings if f.rule_id==e.rule_id),
            tuple(o for o in report.observations if o.rule_id==e.rule_id))) for e in report.executions]
    for r in rows:
        base = [report.generated_at, report.cluster, r.name, r.status.value, r.summary, r.duration_ms or ""]
        if report.schema_version != 2:
            w.writerow(base)
            continue
        execution = next((e for e in report.executions if e.rule_id == r.name), None)
        finding_rows = [f for f in report.findings if f.rule_id == r.name] or [None]
        for f in finding_rows:
            resource = f.resource if f else None
            extra = [2, report.run_id, r.name, execution.execution_status.value if execution else '',
                execution.observed_at.isoformat() if execution else '',
                execution.coverage.complete if execution else '',
                resource.kind if resource else '', resource.namespace if resource else '',
                resource.name if resource else '', resource.uid if resource else '',
                resource.container if resource else '', f.reason if f else '',
                json.dumps(f.evidence, ensure_ascii=False) if f else '']
            w.writerow(base + extra)
    atomic_write_text(fp, buffer.getvalue())
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
    if report.schema_version == 2:
        html = html.replace('</body>', _v2_html(report) + '</body>')
    atomic_write_text(fp, html)
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


def _v2_html(report):
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote
    rows = []
    for execution in report.executions:
        c = execution.coverage
        health = next((o.health.value for o in report.observations if o.rule_id == execution.rule_id
                       and o.resource_uid is None), 'UNKNOWN')
        coverage = f"observed={c.observed}; expected={c.expected if c.expected is not None else 'unknown'}; complete={c.complete}"
        rows.append('<tr>' + ''.join(f'<td>{_esc(v)}</td>' for v in (
            execution.rule_id, execution.execution_status.value, health,
            execution.observed_at.isoformat(), coverage, execution.error_kind or '')) + '</tr>')
    evidence = []
    for f in report.findings:
        ref = f.resource
        anchor = 'object-' + quote(ref.uid or f.fingerprint, safe='')
        label = f"{ref.kind} {ref.namespace or ''}/{ref.name} (UID={ref.uid or 'weak identity'})"
        evidence.append(f'<li id="{_esc(anchor)}"><a href="#{_esc(anchor)}">{_esc(label)}</a> '
            f'{_esc(f.rule_id)} [{_esc(f.severity.value)}] {_esc(f.summary)} '
            f'<pre>{_esc(json.dumps(f.evidence, ensure_ascii=False, indent=2))}</pre></li>')
    local = datetime.fromisoformat(report.generated_at.replace('Z', '+00:00')).astimezone(timezone(timedelta(hours=8)))
    return (f'<h2>Execution quality / 采集质量</h2><p>schema=2; run_id={_esc(report.run_id)}; '
            f'profile={_esc(report.profile)}; Asia/Shanghai: {_esc(local.isoformat())}</p>'
            '<table><tr><th>Rule</th><th>Execution</th><th>Health</th><th>Observed UTC</th><th>Coverage</th><th>Error</th></tr>'
            + ''.join(rows) + '</table><h2>Current / historical evidence</h2><p>pods.history 为历史证据；UNKNOWN 不代表恢复。</p><ul>'
            + ''.join(evidence) + '</ul>')


def snapshot_is_compatible(data, *, cluster, profile, scope, now, max_age_seconds):
    """Only unscoped cluster background is attachable without team ownership proof."""
    from datetime import datetime
    from k8s_inspector.core.observations import utc
    if scope != 'cluster' or not cluster or data.get('cluster') != cluster:
        return False
    if data.get('schema_version', 1) not in (1, 2):
        return False
    if data.get('schema_version') == 2:
        if data.get('profile') != profile or not data.get('run_id'):
            return False
        observations = data.get('observations')
        if not isinstance(observations, list) or any(not isinstance(o, dict) or
                o.get('cluster_id') != cluster or (o.get('resource_uid') is None and o.get('scope') != scope)
                for o in observations):
            return False
    try:
        timestamp = utc(datetime.fromisoformat(data['generated_at'].replace('Z', '+00:00')))
        age = (utc(now) - timestamp).total_seconds()
        return -60 <= age <= max_age_seconds
    except (KeyError, ValueError, TypeError, AttributeError):
        return False
