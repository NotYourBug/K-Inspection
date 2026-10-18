from datetime import datetime, timezone

from k8s_inspector.notifications.bridge.routing import Alert, AlertBatch

NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)


def batch(status="firing", fingerprint="f1", starts_at="2026-10-15T00:00:00Z", **kwargs):
    return AlertBatch(alerts=(Alert(status, {"alertname": "NodeDown", "cluster": "prod"},
                                   {"summary": "unready"}, fingerprint, starts_at,
                                   kwargs.pop("ends_at", "")),), **kwargs)


def target(destination_id="team-a", content_hash="hash-a"):
    from k8s_inspector.notifications.delivery.models import DeliveryTarget
    return DeliveryTarget(destination_id, content_hash, {"mode": "app", "chat_id": "oc_a", "card": {}})
