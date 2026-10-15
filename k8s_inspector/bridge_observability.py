"""Low-cardinality local health and metrics. Never enqueue self-monitoring alerts."""
from datetime import datetime

from prometheus_client import CollectorRegistry, Gauge, generate_latest

from .bridge_store import DeliveryStore, StorageUnavailable, epoch


def bridge_ready(store: DeliveryStore, worker_last_heartbeat: datetime | None, now: datetime) -> bool:
    if worker_last_heartbeat is None or not 0 <= epoch(now) - epoch(worker_last_heartbeat) <= 30:
        return False
    try:
        store.probe_write(now)
        return store.stats(now).backlog_count < store.max_backlog
    except StorageUnavailable:
        return False


def render_metrics(store: DeliveryStore | None, worker_last_heartbeat: datetime | None, now: datetime) -> bytes:
    registry = CollectorRegistry()
    def gauge(name, description, value):
        Gauge("inspector_bridge_" + name, description, registry=registry).set(value)
    gauge("async_enabled", "Whether this process uses durable async delivery", int(store is not None))
    if store is None:
        return generate_latest(registry)
    age = epoch(now) - epoch(worker_last_heartbeat) if worker_last_heartbeat is not None else float("inf")
    gauge("worker_heartbeat_age_seconds", "Age of actual worker progress; inf when stopped", age)
    try:
        store.probe_write(now)
        writable = 1
    except StorageUnavailable:
        writable = 0
    gauge("database_writable", "Last write probe result", writable)
    try:
        stats = store.stats(now)
    except StorageUnavailable:
        return generate_latest(registry)
    jobs = Gauge("inspector_bridge_jobs", "Retained jobs by bounded delivery state", ["state"], registry=registry)
    for state, count in stats.state_counts.items():
        jobs.labels(state=state).set(count)
    gauge("oldest_pending_age_seconds", "Age of oldest active delivery", stats.oldest_pending_age)
    gauge("unknown_jobs", "Jobs retaining unresolved unknown-result evidence", stats.unknown_count)
    gauge("backlog_ratio", "Uncompleted jobs including unhandled dead divided by capacity", stats.backlog_count / store.max_backlog)
    gauge("last_commit_timestamp_seconds", "Last persisted receipt or delivery event", epoch(stats.last_commit) if stats.last_commit else 0)
    return generate_latest(registry)
