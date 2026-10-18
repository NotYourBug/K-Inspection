"""Logical alert cycles and order-independent notification content identity."""
import hashlib
import json

from .alert_routing import Alert, AlertBatch


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def alert_cycle_key(source_id: str, alert: Alert) -> tuple[str, str, str]:
    fingerprint = alert.fingerprint or "labels:" + hashlib.sha256(canonical_json(alert.labels).encode()).hexdigest()
    return source_id, fingerprint, alert.starts_at


def identity_quality(alert: Alert) -> tuple[str, ...]:
    return tuple(flag for flag, present in (("missing_fingerprint", alert.fingerprint),
                                           ("missing_starts_at", alert.starts_at)) if not present)


def alert_cycle_ids(source_id: str, batch: AlertBatch) -> tuple[str, ...]:
    return tuple(sorted({hashlib.sha256(canonical_json(alert_cycle_key(source_id, alert)).encode()).hexdigest()
                         for alert in batch.alerts}))


def canonical_content_hash(batch: AlertBatch) -> str:
    alerts = []
    for alert in batch.alerts:
        item = {"status": alert.status, "labels": alert.labels, "annotations": alert.annotations,
                "fingerprint": alert_cycle_key("", alert)[1], "starts_at": alert.starts_at}
        if alert.status == "resolved":
            item["ends_at"] = alert.ends_at
        alerts.append(canonical_json(item))
    content = {"alerts": sorted(alerts), "truncated_alerts": batch.truncated_alerts}
    return hashlib.sha256(canonical_json(content).encode()).hexdigest()
