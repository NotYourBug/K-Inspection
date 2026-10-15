"""
Command-line entry point.

Subcommands:
  run       Execute inspection and output reports
  cleanup   Remove expired archive directories
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .checks import run_all
from .config import load_config
from .kube import load_kube
from .metrics import PushGatewayConfig, push_report_to_gateway
from .models import Report
from .notify import send_feishu_text
from .report import format_failed_summary, write_csv, write_html, write_json

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def _should_persist(report: Report, persist_level: str) -> bool:
    p = (persist_level or "all").strip().lower()
    if p in {"none", "never", "off"}:
        return False
    if p in {"issue", "warn", "fail"}:
        return report.has_warn or report.has_fail
    return True


def _resolve_output_dir(base_out_dir: str, archive: str, now: datetime) -> str:
    a = (archive or "none").strip().lower()
    if a in {"none", "flat"}:
        return base_out_dir
    p = Path(base_out_dir)
    if a == "hourly":
        return str(p / now.strftime("%Y%m%d%H"))
    if a == "daily":
        return str(p / now.strftime("%Y%m%d"))
    return base_out_dir


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="k8s-inspector")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Execute inspection")
    run.add_argument("--kubeconfig", default=None)
    run.add_argument("--context", default=None)
    run.add_argument("--in-cluster", action="store_true", default=False)
    run.add_argument("--config", default=None, help="JSON/YAML config file path (optional)")
    run.add_argument(
        "--cluster",
        default=None,
        help="Cluster identifier (priority over INSPECTOR_CLUSTER env var)",
    )
    run.add_argument(
        "--profile",
        default="full",
        choices=["5m", "10m", "hourly", "daily", "full"],
        help="Execution frequency preset",
    )
    run.add_argument("--output-dir", default="./out")
    run.add_argument(
        "--formats",
        default="json",
        help="Output formats, comma-separated: json,csv,html",
    )
    run.add_argument(
        "--persist-level",
        default="all",
        choices=["all", "issue", "none"],
        help="Persistence level: all=always write; issue=only on WARN/FAIL; none=never",
    )
    run.add_argument(
        "--archive",
        default="none",
        choices=["none", "hourly", "daily"],
        help="Archive mode: none=flat; hourly/daily=subdirectory by time",
    )
    run.add_argument(
        "--fail-on",
        default="fail",
        choices=["fail", "warn"],
        help="Exit code policy: fail=non-zero only on FAIL; warn=non-zero on WARN or FAIL",
    )
    run.add_argument(
        "--feishu-webhook",
        default=None,
        help="Feishu bot webhook URL (also settable via FEISHU_WEBHOOK env var)",
    )
    run.add_argument(
        "--pushgateway-url",
        default=None,
        help="Pushgateway URL (also settable via PUSHGATEWAY_URL env var)",
    )
    run.add_argument(
        "--pushgateway-job",
        default=None,
        help="Pushgateway job name (also settable via PUSHGATEWAY_JOB, default k8s-inspector)",
    )
    run.add_argument(
        "--pushgateway-timeout",
        default=5.0,
        type=float,
        help="Pushgateway push timeout in seconds (default 5)",
    )
    run.add_argument(
        "--exit-mode",
        default=None,
        choices=["strict", "always0"],
        help="Exit code mode: strict=normal 0/1/2; always0=always exit 0 (for Prometheus alerts)",
    )

    cleanup = sub.add_parser("cleanup", help="Remove expired archive directories")
    cleanup.add_argument(
        "--root",
        default="./out/archive",
        help="Archive root directory (default ./out/archive)",
    )
    cleanup.add_argument("--hourly-days", default=7, type=int, help="Hourly archive retention days (default 7)")
    cleanup.add_argument("--daily-days", default=30, type=int, help="Daily archive retention days (default 30)")

    return p


def cmd_run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    kube = load_kube(kubeconfig=args.kubeconfig, context=args.context, in_cluster=args.in_cluster)

    results = run_all(
        core=kube.core,
        storage=kube.storage,
        custom=kube.custom,
        api_client=kube.api_client,
        cfg=cfg,
        profile=args.profile,
    )

    now = _now_dt()
    cluster_name = (
        str(args.cluster).strip()
        if args.cluster and str(args.cluster).strip()
        else (os.getenv("INSPECTOR_CLUSTER") or "").strip()
    )
    report_cluster = cluster_name or kube.display_name
    report = Report(
        generated_at=now.isoformat(timespec="seconds"),
        cluster=report_cluster,
        results=results,
    )

    formats = {f.strip().lower() for f in str(args.formats).split(",") if f.strip()}
    if _should_persist(report, str(args.persist_level)):
        out_dir = _resolve_output_dir(str(args.output_dir), str(args.archive), now)
        if "json" in formats:
            write_json(report, out_dir)
        if "csv" in formats:
            write_csv(report, out_dir)
        if "html" in formats:
            write_html(report, out_dir)

    pushgateway_url = args.pushgateway_url or os.getenv("PUSHGATEWAY_URL")
    if pushgateway_url:
        job = args.pushgateway_job or os.getenv("PUSHGATEWAY_JOB") or "k8s-inspector"
        try:
            push_report_to_gateway(
                report=report,
                profile=args.profile,
                cfg=PushGatewayConfig(
                    url=pushgateway_url, job=job, timeout_s=float(args.pushgateway_timeout)
                ),
            )
        except Exception as e:
            logger.warning("Pushgateway push failed: %s", e)

    webhook = args.feishu_webhook or os.getenv("FEISHU_WEBHOOK")
    if webhook and (report.has_fail or report.has_warn):
        text = format_failed_summary(report)
        send_feishu_text(webhook, text=text)

    exit_mode = (
        (args.exit_mode or os.getenv("INSPECTOR_EXIT_MODE") or "strict").strip().lower()
    )
    if exit_mode == "always0":
        return 0
    if report.has_fail:
        return 2
    if args.fail_on == "warn" and report.has_warn:
        return 1
    return 0


def _cleanup_subdirs(root: Path, keep_days: int, fmt: str) -> int:
    if keep_days <= 0:
        return 0
    if not root.exists():
        return 0
    now = _now_dt().replace(minute=0, second=0, microsecond=0)
    cutoff = now - timedelta(days=keep_days)
    removed = 0
    for p in root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if not name.isdigit():
            continue
        try:
            dt = datetime.strptime(name, fmt)
        except Exception:
            continue
        dt = dt.replace(tzinfo=_now_dt().tzinfo)
        if dt < cutoff:
            shutil.rmtree(p, ignore_errors=True)
            removed += 1
    return removed


def cmd_cleanup(args: argparse.Namespace) -> int:
    root = Path(str(args.root))
    hourly = root / "hourly"
    daily = root / "daily"
    removed_hourly = _cleanup_subdirs(hourly, int(args.hourly_days), "%Y%m%d%H")
    removed_daily = _cleanup_subdirs(daily, int(args.daily_days), "%Y%m%d")
    logger.info("cleanup done: hourly_removed=%d daily_removed=%d", removed_hourly, removed_daily)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = build_parser()
    args = p.parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "cleanup":
        return cmd_cleanup(args)
    raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
