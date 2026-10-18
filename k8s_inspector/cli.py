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
import stat
import sys
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from pathlib import Path

import time
from .runner import run_inspection
from .config import read_config, resolve_config
from .kube import load_kube
from .metrics import PushGatewayConfig, push_report_to_gateway
from .models import Report
from .observation_state import ObservationStateStore, profile_run_lock
from .notify import send_feishu_text
from .report import format_failed_summary, write_csv, write_html, write_json
from .sinks import run_sinks, write_run_status, write_run_manifest

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _should_persist(report: Report, persist_level: str) -> bool:
    p = (persist_level or "all").strip().lower()
    if p in {"none", "never", "off"}:
        return False
    if p in {"issue", "warn", "fail"}:
        return report.has_warn or report.has_fail
    return True


def _resolve_output_dir(base_out_dir: str, archive: str, now: datetime, *, run_id: str | None = None) -> str:
    a = (archive or "none").strip().lower()
    if a in {"none", "flat"}:
        return base_out_dir
    p = Path(base_out_dir)
    now = now.astimezone(timezone.utc)
    if a in {"hourly", "daily"}:
        bucket = now.strftime("%Y%m%d%H" if a == "hourly" else "%Y%m%d")
        return str(p / bucket / (now.strftime("%Y%m%dT%H%M%SZ") + "_" + (run_id or uuid4().hex)))
    return base_out_dir


def _output_locations(base: Path, profile: str, archive: str) -> tuple[Path, Path]:
    """Accept both a general output root and the deployed canonical paths."""
    if base.name == profile and base.parent.name == "latest":
        root = base.parent.parent
        return base, (root / "archive" / archive if archive != "none" else base)
    if archive != "none" and base.name == archive and base.parent.name == "archive":
        return base.parent.parent / "latest" / profile, base
    return base / "latest" / profile, (base / "archive" / archive if archive != "none" else base)


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
        default=None,
        choices=["5m", "10m", "hourly", "daily", "full"],
        help="Execution frequency preset",
    )
    run.add_argument("--state-dir", default=None, help="Persistent observation state root (or INSPECTOR_STATE_DIR)")
    run.add_argument("--output-dir", default="./out")
    run.add_argument("--output-policy", default=None, choices=["best-effort", "strict"],
                     help="strict returns 1 if any configured output fails")
    run.add_argument(
        "--formats",
        default="json",
        help="Output formats, comma-separated: json,csv,html",
    )
    run.add_argument(
        "--persist-level",
        default="all",
        choices=["all", "issue", "none"],
        help="Extra output/archive policy; canonical latest JSON is always refreshed",
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
    cleanup.add_argument("--dry-run", action="store_true", help="List expired archives without deleting")

    return p


def cmd_run(args: argparse.Namespace) -> int:
    try:
        cfg = resolve_config(read_config(args.config), os.environ,
            {'cluster_id': args.cluster, 'profile': args.profile, 'output_policy': args.output_policy})
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    if args.in_cluster and not cfg.cluster_id:
        print('Configuration error: in-cluster runs require --cluster or INSPECTOR_CLUSTER (or config cluster_id)', file=sys.stderr)
        return 1
    formats = {f.strip().lower() for f in str(args.formats).split(",") if f.strip()}
    if not formats or formats - {"json", "csv", "html"}:
        print("Configuration error: formats must contain json, csv or html", file=sys.stderr)
        return 1

    latest, _ = _output_locations(Path(args.output_dir), cfg.profile, args.archive)
    try:
        with profile_run_lock(latest):
            return _cmd_run_locked(args, cfg, formats)
    except OSError as exc:
        print(f'Run startup/storage error: {type(exc).__name__}', file=sys.stderr)
        return 1


def _cmd_run_locked(args, cfg, formats):
    latest, _ = _output_locations(Path(args.output_dir), cfg.profile, args.archive)
    state_root = args.state_dir or os.getenv('INSPECTOR_STATE_DIR')
    state_path = Path(state_root) / cfg.profile / 'observations.json' if state_root else latest / 'observation-state.json'
    store = ObservationStateStore(state_path)
    last_valid, baseline, confirmed = (), (), ()
    def read_state():
        nonlocal last_valid, baseline, confirmed
        last_valid = store.read()
        confirmed = store.read_confirmed()
        baseline = store.event_baseline(cluster_id=cluster_id, profile=cfg.profile)
    kube = load_kube(kubeconfig=args.kubeconfig, context=args.context, in_cluster=args.in_cluster)
    cluster_id = cfg.cluster_id or getattr(kube, 'cluster_identity', None) or kube.display_name
    state_outcomes = run_sinks({'observation_state_read': read_state}, frozenset({'observation_state_read'}))

    kube.event_baseline = baseline
    now = _now_dt()
    args.profile = cfg.profile
    report = run_inspection(kube, cfg, cfg.profile, cluster_id=cluster_id,
                            now=now, monotonic=time.monotonic)
    logger.info('Effective inspection configuration: profile=%s cluster=%s timeouts=%s deadline=%s output_policy=%s',
                cfg.profile, report.cluster, cfg.timeouts, cfg.deadlines[cfg.profile], cfg.output_policy)

    latest, extra_base = _output_locations(Path(args.output_dir), args.profile, args.archive)
    def update_state():
        nonlocal last_valid, confirmed
        last_valid = store.update(report)
        confirmed = store.read_confirmed()
    outcomes = state_outcomes + run_sinks({'observation_state': update_state}, frozenset({'observation_state'}))
    report.sink_results = outcomes
    actions = {"latest_json": lambda: write_json(report, str(latest))}
    if _should_persist(report, str(args.persist_level)):
        out_dir = _resolve_output_dir(str(extra_base), str(args.archive), now, run_id=report.run_id)
        if "json" in formats and Path(out_dir) != latest:
            actions["json"] = lambda: write_json(report, out_dir)
        if "csv" in formats:
            actions["csv"] = lambda: write_csv(report, out_dir)
        if "html" in formats:
            actions["html"] = lambda: write_html(report, out_dir)

    webhook = args.feishu_webhook or os.getenv("FEISHU_WEBHOOK")
    if webhook and (report.has_fail or report.has_warn):
        actions["notification"] = lambda: send_feishu_text(
            webhook, text=format_failed_summary(report))
    outcomes += run_sinks(actions, required=frozenset(actions))

    pushgateway_url = args.pushgateway_url or os.getenv("PUSHGATEWAY_URL")
    if pushgateway_url:
        job = args.pushgateway_job or os.getenv("PUSHGATEWAY_JOB") or "k8s-inspector"
        outcomes += run_sinks({"metrics": lambda: push_report_to_gateway(
                report=report,
                profile=args.profile,
                cfg=PushGatewayConfig(
                    url=pushgateway_url, job=job, timeout_s=float(args.pushgateway_timeout)
                ),
                sink_results=outcomes, last_valid=last_valid, confirmed=confirmed,
            )}, required=frozenset({"metrics"}))
    outcomes += run_sinks({'manifest': lambda: write_run_manifest(report, outcomes, latest)},
                          required=frozenset({'manifest'}))
    outcomes += run_sinks({"run_status": lambda: write_run_status(
        report, args.profile, outcomes, latest / "run-status.json")},
        required=frozenset({"run_status"}))
    for outcome in outcomes:
        if not outcome.success:
            logger.error("Output %s failed (%s)", outcome.name, outcome.error_kind)

    exit_mode = (
        (args.exit_mode or os.getenv("INSPECTOR_EXIT_MODE") or "strict").strip().lower()
    )
    if exit_mode == "always0":
        return 0
    output_policy = args.output_policy or cfg.output_policy
    if output_policy == "strict" and any(not x.success and x.required for x in outcomes):
        return 1
    if report.has_fail:
        return 2
    if args.fail_on == "warn" and report.has_warn:
        return 1
    return 0


def _is_reparse(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & 0x400)


def _safe_archive(root: Path, target: Path) -> None:
    if _is_reparse(root) or _is_reparse(target):
        raise ValueError("Refusing symlink/reparse archive target")
    resolved_root, resolved_target = root.resolve(), target.resolve()
    resolved_target.relative_to(resolved_root)
    if resolved_target == resolved_root or resolved_target.parent != resolved_root:
        raise ValueError("Archive target is not a direct child of root")
    def walk_error(exc: OSError) -> None:
        raise exc
    for parent, directories, filenames in os.walk(target, followlinks=False, onerror=walk_error):
        for name in directories + filenames:
            child = Path(parent) / name
            if _is_reparse(child):
                raise ValueError("Refusing archive containing symlink/reparse entries")
            child.resolve().relative_to(resolved_root)


def _cleanup_subdirs(root: Path, keep_days: int, fmt: str, *, dry_run: bool = False,
                     error_sink: list[str] | None = None) -> int:
    errors = error_sink if error_sink is not None else []
    if keep_days < 0:
        raise ValueError("Retention days must be non-negative")
    if keep_days <= 0:
        return 0
    if not root.exists():
        return 0
    if _is_reparse(root):
        errors.append(f"Refusing symlink/reparse root: {root}")
        return 0
    now = _now_dt().astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
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
        if dt.strftime(fmt) != name:
            continue
        dt = dt.replace(tzinfo=timezone.utc)
        if dt < cutoff:
            try:
                _safe_archive(root, p)
                if dry_run:
                    logger.info("Would remove archive: %s", p)
                else:
                    shutil.rmtree(p)
                    removed += 1
            except (OSError, ValueError) as exc:
                errors.append(f"Archive removal failed: {p}: {type(exc).__name__}")
                logger.error("%s", errors[-1])
    return removed


def cmd_cleanup(args: argparse.Namespace) -> int:
    root = Path(str(args.root))
    hourly = root / "hourly"
    daily = root / "daily"
    errors: list[str] = []
    try:
        if root.exists() and _is_reparse(root):
            raise ValueError("Refusing symlink/reparse archive root")
        removed_hourly = _cleanup_subdirs(hourly, int(args.hourly_days), "%Y%m%d%H",
                                          dry_run=args.dry_run, error_sink=errors)
        removed_daily = _cleanup_subdirs(daily, int(args.daily_days), "%Y%m%d",
                                         dry_run=args.dry_run, error_sink=errors)
    except (OSError, ValueError) as exc:
        logger.error("Cleanup failed: %s", exc)
        return 1
    logger.info("cleanup done: hourly_removed=%d daily_removed=%d", removed_hourly, removed_daily)
    return 1 if errors else 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = build_parser()
    args = p.parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "cleanup":
        return cmd_cleanup(args)
    raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
