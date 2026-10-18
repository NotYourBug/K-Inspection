"""Validate Alertmanager notifications and route alerts to existing chats.

Notification groups are transport metadata, never chat or incident identities.
Routes use exact label matches, in file order, with a required fallback chat.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Alert:
    status: str
    labels: dict[str, str]
    annotations: dict[str, str]
    fingerprint: str = ""
    starts_at: str = ""
    ends_at: str = ""


@dataclass(frozen=True)
class AlertBatch:
    alerts: tuple[Alert, ...]
    group_key: str = ""
    receiver: str = ""
    truncated_alerts: int = 0

    @property
    def firing(self) -> tuple[Alert, ...]:
        return tuple(a for a in self.alerts if a.status == "firing")

    @property
    def resolved(self) -> tuple[Alert, ...]:
        return tuple(a for a in self.alerts if a.status == "resolved")


@dataclass(frozen=True)
class FixedChatRoute:
    match: dict[str, str]
    chat_id: str


def _string_map(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in value.items()
    ):
        raise ValueError(f"{field} must be an object of string values")
    return dict(value)


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def normalize_payload(payload: Any) -> AlertBatch:
    if not isinstance(payload, dict):
        raise ValueError("webhook body must be a JSON object")
    raw_alerts = payload.get("alerts")
    if not isinstance(raw_alerts, list) or not raw_alerts:
        raise ValueError("alerts must be a non-empty array")
    truncated = payload.get("truncatedAlerts", 0)
    if type(truncated) is not int or truncated < 0:
        raise ValueError("truncatedAlerts must be a non-negative integer")

    alerts = []
    for index, raw in enumerate(raw_alerts):
        field = f"alerts[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{field} must be an object")
        if raw.get("status") not in ("firing", "resolved"):
            raise ValueError(f"{field}.status must be firing or resolved")
        alerts.append(Alert(
            status=raw["status"],
            labels=_string_map(raw.get("labels", {}), f"{field}.labels"),
            annotations=_string_map(raw.get("annotations", {}), f"{field}.annotations"),
            fingerprint=_string(raw.get("fingerprint", ""), f"{field}.fingerprint"),
            starts_at=_string(raw.get("startsAt", ""), f"{field}.startsAt"),
            ends_at=_string(raw.get("endsAt", ""), f"{field}.endsAt"),
        ))
    return AlertBatch(
        alerts=tuple(alerts),
        group_key=_string(payload.get("groupKey", ""), "groupKey"),
        receiver=_string(payload.get("receiver", ""), "receiver"),
        truncated_alerts=truncated,
    )


def _chat_id(value: Any, field: str) -> str:
    chat_id = _string(value, field).strip()
    if not chat_id or chat_id.startswith("YOUR_"):
        raise ValueError(f"{field} must specify an existing Feishu chat_id")
    return chat_id


def load_fixed_chat_routes(
    path: str | None, default_chat_id: str | None,
) -> tuple[str, tuple[FixedChatRoute, ...]]:
    config: dict[str, Any] = {}
    if path:
        config_path = Path(path)
        text = config_path.read_text(encoding="utf-8")
        if config_path.suffix.lower() in {".yaml", ".yml"}:
            import yaml

            try:
                raw = yaml.safe_load(text)
            except yaml.YAMLError as exc:
                raise ValueError("routes file contains invalid YAML") from exc
        else:
            raw = json.loads(text)
        if not isinstance(raw, dict):
            raise ValueError("routes file must contain an object")
        if set(raw) - {"default_chat_id", "routes"}:
            raise ValueError("routes file supports only default_chat_id and routes")
        config = raw

    fallback = _chat_id(
        default_chat_id or config.get("default_chat_id", ""), "default_chat_id",
    )
    raw_routes = config.get("routes", [])
    if not isinstance(raw_routes, list):
        raise ValueError("routes must be an array")
    routes = []
    for index, raw_route in enumerate(raw_routes):
        field = f"routes[{index}]"
        if not isinstance(raw_route, dict) or set(raw_route) != {"match", "chat_id"}:
            raise ValueError(f"{field} requires exactly match and chat_id")
        match = _string_map(raw_route["match"], f"{field}.match")
        if not match or any(not k or not v for k, v in match.items()):
            raise ValueError(f"{field}.match must contain non-empty exact label matches")
        routes.append(FixedChatRoute(
            match=match, chat_id=_chat_id(raw_route["chat_id"], f"{field}.chat_id"),
        ))
    return fallback, tuple(routes)


def route_alerts(
    batch: AlertBatch, default_chat_id: str, routes: tuple[FixedChatRoute, ...],
) -> dict[str, AlertBatch]:
    grouped: dict[str, list[Alert]] = {}
    for alert in batch.alerts:
        chat_id = default_chat_id
        for route in routes:
            if all(alert.labels.get(k) == v for k, v in route.match.items()):
                chat_id = route.chat_id
                break
        grouped.setdefault(chat_id, []).append(alert)
    return {
        chat_id: AlertBatch(
            alerts=tuple(alerts), group_key=batch.group_key, receiver=batch.receiver,
            # A truncated notification cannot identify the owners of omitted alerts.
            truncated_alerts=batch.truncated_alerts,
        )
        for chat_id, alerts in grouped.items()
    }
