"""
Alertmanager → Feishu webhook bridge (long-running HTTP service).

Both delivery modes notify existing groups. Chat policy is fixed; notification
groupKey values do not create chats or drive incident recovery.
- webhook: Send text to a fixed group's bot webhook.
- app: Send cards to a fallback chat and optional label-routed responsibility chats.

Run:
  python -m k8s_inspector.alertmanager_bridge --mode app --feishu-chat-id oc_xxx
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any

from .alert_routing import (
    Alert, AlertBatch, FixedChatRoute, load_fixed_chat_routes, normalize_payload, route_alerts,
)
from .feishu_app import FeishuAppClient, build_alert_card, load_feishu_app_config_from_env
from .notify import send_feishu_text

logger = logging.getLogger(__name__)
MAX_WEBHOOK_BYTES = 1024 * 1024
REPORT_MAX_AGE = {"5m": 900, "10m": 1800, "hourly": 7200, "daily": 129600, "full": 129600}


@dataclass
class BridgeConfig:
    mode: str
    feishu_webhook: str | None = None
    feishu_chat_id: str | None = None
    chat_routes: tuple[FixedChatRoute, ...] = ()
    chat_policy: str = "fixed"
    report_dir: str = "/data/out"
    report_max_results: int = 5
    report_max_items: int = 3
    listen_host: str = "0.0.0.0"
    listen_port: int = 8000
    timeout_s: float = 5.0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="k8s-inspector-alert-bridge")
    p.add_argument("--listen-host", default="0.0.0.0")
    p.add_argument("--listen-port", default=8000, type=int)
    p.add_argument("--report-dir", default=None, help="Inspection report root directory")
    p.add_argument("--report-max-results", default=None, type=int)
    p.add_argument("--report-max-items", default=None, type=int)
    p.add_argument("--feishu-webhook", default=None, help="Feishu bot webhook (also FEISHU_WEBHOOK env)")
    p.add_argument(
        "--mode", default=None, choices=["webhook", "app"],
        help="Feishu push mode: webhook=bot webhook; app=enterprise app bot",
    )
    p.add_argument("--feishu-chat-id", default=None, help="Existing fallback chat_id (also FEISHU_CHAT_ID)")
    p.add_argument("--routes-file", default=None, help="JSON/YAML exact-label routing file (also FEISHU_ROUTES_FILE)")
    p.add_argument(
        "--chat-policy", default=None, choices=["fixed"],
        help="Chat policy (default: fixed); automatic chat creation is disabled",
    )
    p.add_argument("--timeout", default=5.0, type=float)
    return p


def alertmanager_payload_to_text(payload: dict[str, Any]) -> str:
    return "\n".join(_notification_lines(normalize_payload(payload)))


def _notification_lines(batch: AlertBatch) -> list[str]:
    firing, resolved = batch.firing, batch.resolved
    lines = ["Alertmanager 通知", f"FIRING: {len(firing)} | RESOLVED: {len(resolved)}"]
    if batch.truncated_alerts:
        lines.append(f"注意：上游截断了 {batch.truncated_alerts} 条告警，本通知不完整。")
    if firing:
        lines.append("正在告警：")
        lines.extend(_format_alerts(firing, limit=8))
    if resolved:
        lines.append("收到恢复通知：")
        lines.extend(_format_alerts(resolved, limit=8))
        lines.append("恢复仅描述所列告警；事件结案仍需核验其他告警和有效巡检结果。")
    return lines


def _format_alerts(alerts: tuple[Alert, ...], limit: int) -> list[str]:
    out: list[str] = []
    for a in alerts[:limit]:
        labels, ann = a.labels, a.annotations
        name = labels.get("alertname") or "Alert"
        cluster = labels.get("cluster")
        profile = labels.get("profile")
        check = labels.get("check")
        sev = labels.get("severity")
        summary = ann.get("summary") or ann.get("message") or ""
        parts = [_trim_line(str(name), 40)]
        if sev:
            parts.append(f"severity={sev}")
        if cluster:
            parts.append(f"cluster={cluster}")
        if profile:
            parts.append(f"profile={profile}")
        if check:
            parts.append(f"check={check}")
        for field in ("owner_team", "namespace", "service", "resource", "node"):
            if labels.get(field):
                parts.append(f"{field}={labels[field]}")
        header = " | ".join(parts)
        if summary:
            out.append(_trim_line(f"- {header}: {summary}", 180))
        else:
            out.append(_trim_line(f"- {header}", 180))
    if len(alerts) > limit:
        out.append(f"- ... and {len(alerts) - limit} more")
    return out


class _ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """HTTPServer with thread-per-request concurrency."""
    daemon_threads = True
    allow_reuse_address = True


def _validate_config(cfg: BridgeConfig) -> None:
    if cfg.mode not in {"app", "webhook"}:
        raise ValueError("mode must be webhook or app")
    if cfg.chat_policy != "fixed":
        raise ValueError("only fixed chat policy is supported; incident creation is not enabled")
    if cfg.mode == "app":
        load_fixed_chat_routes(None, cfg.feishu_chat_id)
        for route in cfg.chat_routes:
            if not route.match or any(not k or not v for k, v in route.match.items()):
                raise ValueError("chat routes require non-empty exact label matches")
            load_fixed_chat_routes(None, route.chat_id)
    if cfg.mode == "webhook" and not cfg.feishu_webhook:
        raise ValueError("webhook mode requires FEISHU_WEBHOOK (or --feishu-webhook)")
    if not 0 <= cfg.listen_port <= 65535 or not math.isfinite(cfg.timeout_s) or cfg.timeout_s <= 0:
        raise ValueError("invalid listen port or timeout")
    if cfg.report_max_results < 0 or cfg.report_max_items < 0:
        raise ValueError("report limits must be non-negative")


def _notification_detail_lines(batch: AlertBatch, cfg: BridgeConfig) -> list[str]:
    # Reports have no ownership model yet; attaching them to a team/namespace
    # notification could include unrelated objects. Only use a homogeneous,
    # unscoped cluster/profile snapshot, and label it as background context.
    if cfg.chat_routes or any(
        a.labels.get(k) for a in batch.alerts for k in ("owner_team", "namespace", "service")
    ):
        return []
    contexts = {(a.labels.get("cluster", ""), a.labels.get("profile", "")) for a in batch.alerts}
    if len(contexts) != 1 or cfg.report_max_results == 0:
        return []
    cluster, profile = next(iter(contexts))
    checks = {a.labels.get("check", "") for a in batch.alerts}
    return _build_report_detail_lines(
        report_dir=cfg.report_dir, profile=profile, cluster=cluster,
        check_hint=next(iter(checks)) if len(checks) == 1 else "",
        max_results=cfg.report_max_results, max_items=cfg.report_max_items,
    )


def handle_alert_payload(
    payload: Any, cfg: BridgeConfig, feishu_app: FeishuAppClient | None = None,
) -> None:
    _validate_config(cfg)
    batch = normalize_payload(payload)
    if cfg.mode == "webhook":
        lines = _compose_notification_lines(batch, cfg)
        send_feishu_text(cfg.feishu_webhook, text="\n".join(lines), timeout_s=cfg.timeout_s)
        return
    if feishu_app is None:
        raise RuntimeError("Feishu app client not initialized")
    failures: list[Exception] = []
    for chat_id, notification in route_alerts(batch, cfg.feishu_chat_id, cfg.chat_routes).items():
        lines = _compose_notification_lines(notification, cfg)
        has_firing = bool(notification.firing)
        card = build_alert_card(
            title="K8s 告警通知" if has_firing else "K8s 告警状态变更",
            lines=lines,
            template="red" if has_firing else "orange",
        )
        try:
            feishu_app.send_card_message(chat_id, card)
        except Exception as exc:
            failures.append(exc)
            logger.warning("card delivery failed for a destination; continuing remaining destinations")
    if failures:
        raise RuntimeError(f"delivery failed for {len(failures)} destination(s)") from failures[0]


def _compose_notification_lines(batch: AlertBatch, cfg: BridgeConfig) -> list[str]:
    lines = _notification_lines(batch)
    for line in _notification_detail_lines(batch, cfg):
        if len("\n".join(lines)) + len(line) > 3700:
            lines.append("巡检背景较长，后续背景内容已省略。")
            break
        lines.append(line)
    return lines


def create_server(
    cfg: BridgeConfig, feishu_app: FeishuAppClient | None = None,
) -> _ThreadingHTTPServer:
    _validate_config(cfg)
    if cfg.mode == "app" and feishu_app is None:
        feishu_app = FeishuAppClient(load_feishu_app_config_from_env())
    logger.info(
        "alert-bridge starting: mode=%s chat_policy=%s listen=%s:%d routes=%d",
        cfg.mode, cfg.chat_policy, cfg.listen_host, cfg.listen_port, len(cfg.chat_routes),
    )

    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(cfg.timeout_s)

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {"/", "/alert"}:
                self.send_error(404)
                return
            if self.headers.get("Transfer-Encoding"):
                self.send_error(400, "Transfer-Encoding is not supported")
                return
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
                self.send_error(400, "a single valid Content-Length is required")
                return
            if len(lengths[0]) > 8:
                self.send_error(413, "oversized Content-Length")
                return
            length = int(lengths[0])
            if length <= 0 or length > MAX_WEBHOOK_BYTES:
                self.send_error(413, "invalid or oversized webhook body")
                return
            try:
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ValueError("incomplete webhook body")
                payload = json.loads(raw.decode("utf-8"))
                normalize_payload(payload)
            except (ValueError, UnicodeError):
                self.send_error(400, "invalid Alertmanager payload")
                return
            except TimeoutError:
                self.send_error(408, "webhook body read timed out")
                return
            try:
                handle_alert_payload(payload, cfg, feishu_app)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
            except Exception:
                logger.exception("alert-bridge error processing webhook")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"internal error")

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
                return
            self.send_error(404)

        def log_message(self, _format: str, *_args: Any) -> None:
            # Suppress default HTTP request logging; we log at the application level.
            return

    return _ThreadingHTTPServer((cfg.listen_host, cfg.listen_port), Handler)


def run_server(cfg: BridgeConfig) -> _ThreadingHTTPServer:
    httpd = create_server(cfg)
    logger.info("alert-bridge listening on %s:%d", cfg.listen_host, cfg.listen_port)

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("received signal %d, shutting down", signum)
        # shutdown() must run outside the thread executing serve_forever().
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
    return httpd


def _config_from_args(args: argparse.Namespace) -> BridgeConfig:
    mode = (args.mode or os.getenv("FEISHU_MODE") or "webhook").strip().lower()
    webhook = args.feishu_webhook or os.getenv("FEISHU_WEBHOOK")
    report_dir = (args.report_dir or os.getenv("INSPECTOR_REPORT_DIR") or "/data/out").strip() or "/data/out"

    def integer_option(cli_value: int | None, env: str, default: int) -> int:
        return cli_value if cli_value is not None else int(os.getenv(env, str(default)))

    chat_policy = (args.chat_policy or os.getenv("FEISHU_CHAT_POLICY") or "fixed").strip().lower()
    chat_id, routes = None, ()
    if mode == "app":
        chat_id, routes = load_fixed_chat_routes(
            args.routes_file or os.getenv("FEISHU_ROUTES_FILE"),
            args.feishu_chat_id or os.getenv("FEISHU_CHAT_ID"),
        )
    cfg = BridgeConfig(
        mode=mode,
        feishu_webhook=webhook,
        feishu_chat_id=chat_id,
        chat_routes=routes,
        chat_policy=chat_policy,
        report_dir=str(report_dir),
        report_max_results=integer_option(args.report_max_results, "INSPECTOR_REPORT_MAX_RESULTS", 5),
        report_max_items=integer_option(args.report_max_items, "INSPECTOR_REPORT_MAX_ITEMS", 3),
        listen_host=args.listen_host,
        listen_port=int(args.listen_port),
        timeout_s=float(args.timeout),
    )
    _validate_config(cfg)
    return cfg


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = _config_from_args(args)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    run_server(cfg)
    return 0


def _build_report_detail_lines(
    report_dir: str, profile: str, check_hint: str, max_results: int, max_items: int,
    cluster: str,
) -> list[str]:
    report, _ = _try_load_latest_report_json(report_dir=report_dir, profile=profile)
    if report is None:
        return []
    if not cluster or report.get("cluster") != cluster or profile not in REPORT_MAX_AGE:
        return []
    try:
        timestamp = datetime.fromisoformat(str(report.get("generated_at", "")).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            return []
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        if age < -60 or age > REPORT_MAX_AGE[profile]:
            return []
    except (ValueError, TypeError):
        return []

    lines: list[str] = []
    generated_at = report.get("generated_at")
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    results = report.get("results") if isinstance(report.get("results"), list) else []

    lines.append("---- 最新巡检快照（辅助背景，不作为事件恢复依据） ----")
    if isinstance(generated_at, str) and generated_at:
        lines.append(f"generated_at: {generated_at}")
    if isinstance(summary, dict) and summary:
        parts: list[str] = []
        for k in ["total", "pass", "warn", "fail", "skip"]:
            v = summary.get(k)
            if isinstance(v, int):
                parts.append(f"{k}={v}")
        if parts:
            lines.append("summary: " + " ".join(parts))

    issue_results: list[dict[str, Any]] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        st = r.get("status")
        if st in {"FAIL", "WARN"}:
            issue_results.append(r)

    if check_hint:
        issue_results.sort(key=lambda r: 0 if r.get("name") == check_hint else 1)

    if not issue_results:
        lines.append("checks: (no FAIL/WARN in report)")
        return lines

    lines.append("checks:")
    shown = 0
    for r in issue_results:
        if shown >= max_results:
            break
        name = r.get("name") if isinstance(r.get("name"), str) else "unknown"
        st = r.get("status") if isinstance(r.get("status"), str) else "UNKNOWN"
        s = r.get("summary") if isinstance(r.get("summary"), str) else ""
        header = f"- [{st}] {name}"
        if s:
            header += f": {s}"
        lines.append(_trim_line(header, 180))

        data = r.get("data") if isinstance(r.get("data"), dict) else {}
        data_line = _summarize_check_data(name=name, data=data, max_items=max_items)
        if data_line:
            lines.append(_trim_line("  " + data_line, 220))
        shown += 1

    if len(issue_results) > shown:
        lines.append(f"- ... and {len(issue_results) - shown} more")
    return lines


def _try_load_latest_report_json(report_dir: str, profile: str) -> tuple[dict[str, Any] | None, str | None]:
    report_dir = (report_dir or "").strip()
    profile = (profile or "").strip()
    if not report_dir or profile not in REPORT_MAX_AGE:
        return None, None

    candidates: list[str] = []
    if profile:
        candidates.append(os.path.join(report_dir, "latest", profile, "report.json"))
        if profile in {"hourly", "daily"}:
            archive_root = os.path.join(report_dir, "archive", profile)
            latest = _pick_latest_subdir(archive_root)
            if latest:
                candidates.append(os.path.join(archive_root, latest, "report.json"))
    root = Path(report_dir).resolve()
    for fp in candidates:
        try:
            if not Path(fp).resolve().is_relative_to(root):
                continue
            if os.path.isfile(fp):
                with open(fp, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                if isinstance(obj, dict):
                    return obj, fp
        except Exception:
            continue
    return None, None


def _pick_latest_subdir(root: str) -> str | None:
    try:
        if not os.path.isdir(root):
            return None
        subs = [n for n in os.listdir(root) if os.path.isdir(os.path.join(root, n))]
        if not subs:
            return None
        subs.sort()
        return subs[-1]
    except Exception:
        return None


def _summarize_check_data(name: str, data: dict[str, Any], max_items: int) -> str:
    if not isinstance(data, dict) or not data:
        return ""

    exclude = {"utilization"}
    prefer_keys: list[str] = []
    if name == "nodes":
        prefer_keys = ["not_ready", "over_cpu", "over_memory", "noschedule_taints"]
    elif name == "pods":
        prefer_keys = ["bad", "restart_heavy", "restart_threshold"]
    elif name == "pvcs":
        prefer_keys = ["pending"]
    elif name == "events":
        prefer_keys = ["count", "sample"]
    elif name == "control-plane":
        prefer_keys = ["apiserver_healthz", "error"]
    else:
        prefer_keys = []

    keys: list[str] = []
    for k in prefer_keys:
        if k in data:
            keys.append(k)
    for k in data.keys():
        if k in exclude:
            continue
        if k not in keys:
            keys.append(k)
        if len(keys) >= 3:
            break

    parts: list[str] = []
    for k in keys:
        v = data.get(k)
        if isinstance(v, list):
            parts.append(f"{k}={len(v)}{_preview_list(v, max_items=max_items)}")
        elif isinstance(v, dict):
            parts.append(f"{k}={_preview_dict(v)}")
        elif isinstance(v, (str, int, float, bool)) or v is None:
            parts.append(f"{k}={v}")
        else:
            parts.append(f"{k}={type(v).__name__}")
    return "; ".join(parts)


def _preview_list(v: list[Any], max_items: int) -> str:
    if max_items <= 0 or not v:
        return ""
    items = []
    for it in v[:max_items]:
        items.append(_compact_item(it))
    s = ", ".join(items)
    return f" ({s})" if s else ""


def _compact_item(it: Any) -> str:
    if isinstance(it, str):
        return _trim_line(it, 80)
    if isinstance(it, (int, float, bool)) or it is None:
        return str(it)
    if isinstance(it, dict):
        for keys in [
            ("namespace", "name", "issue"),
            ("namespace", "name"),
            ("name", "issue"),
            ("node",),
        ]:
            vals = []
            ok = True
            for k in keys:
                v = it.get(k)
                if not isinstance(v, str) or not v:
                    ok = False
                    break
                vals.append(v)
            if ok and vals:
                return "/".join(vals)
        try:
            s = json.dumps(it, ensure_ascii=False, separators=(",", ":"))
            return _trim_line(s, 120)
        except Exception:
            return "{...}"
    try:
        s = str(it)
        return _trim_line(s, 120)
    except Exception:
        return "<item>"


def _preview_dict(d: dict[str, Any]) -> str:
    try:
        keys = list(d.keys())
        if len(keys) <= 6:
            s = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
            return _trim_line(s, 140)
        return "{...}"
    except Exception:
        return "{...}"


def _trim_line(s: str, max_len: int) -> str:
    s = s.replace("\n", " ").replace("\r", " ")
    if len(s) <= max_len:
        return s
    return s[:max(0, max_len - 3)] + "..."


if __name__ == "__main__":
    raise SystemExit(main())
