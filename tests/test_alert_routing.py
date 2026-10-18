"""Routing contracts: exact ownership, fallback delivery, no chat identity inference."""

import json

import pytest

from k8s_inspector.notifications.bridge.routing import FixedChatRoute, load_fixed_chat_routes, normalize_payload, route_alerts


def alert(status="firing", **labels):
    return {"status": status, "labels": labels, "annotations": {"summary": "inspection finding"}}


def test_profiles_and_rules_share_existing_destination():
    batch = normalize_payload({"groupKey": "arbitrary-notification-key", "alerts": [
        alert(cluster="prod", profile="5m", alertname="PendingPVC"),
        alert(cluster="prod", profile="hourly", alertname="CheckFail"),
        alert("resolved", cluster="prod", profile="daily", alertname="HasFail"),
    ]})
    routed = route_alerts(batch, "oc_platform", ())
    assert list(routed) == ["oc_platform"]
    assert {a.labels["profile"] for a in routed["oc_platform"].alerts} == {"5m", "hourly", "daily"}
    assert len(routed["oc_platform"].firing) == 2
    assert len(routed["oc_platform"].resolved) == 1


def test_first_exact_match_and_missing_owner_fallback():
    batch = normalize_payload({"truncatedAlerts": 2, "alerts": [
        alert(cluster="prod", owner_team="storage"),
        alert(cluster="dev", owner_team="storage"),
        alert(cluster="prod", owner_team="storage-extra"),
        alert(cluster="prod"),
    ]})
    routes = (
        FixedChatRoute({"cluster": "prod", "owner_team": "storage"}, "oc_prod_storage"),
        FixedChatRoute({"owner_team": "storage"}, "oc_storage"),
    )
    routed = route_alerts(batch, "oc_platform", routes)
    assert len(routed["oc_prod_storage"].alerts) == 1
    assert len(routed["oc_storage"].alerts) == 1
    assert len(routed["oc_platform"].alerts) == 2
    assert all(part.truncated_alerts == 2 for part in routed.values())


def test_routes_to_same_chat_are_consolidated():
    batch = normalize_payload({"alerts": [alert(owner_team="a"), alert(owner_team="b"), alert()]})
    routed = route_alerts(batch, "oc_platform", (
        FixedChatRoute({"owner_team": "a"}, "oc_platform"),
        FixedChatRoute({"owner_team": "b"}, "oc_platform"),
    ))
    assert len(routed) == 1
    assert len(routed["oc_platform"].alerts) == 3


def test_keeps_alert_identity_fields_for_future_event_membership():
    raw = alert(cluster="prod")
    raw.update(fingerprint="abc", startsAt="2026-10-02T01:00:00Z", endsAt="0001-01-01T00:00:00Z")
    batch = normalize_payload({"receiver": "bridge", "groupKey": "transport", "alerts": [raw]})
    assert batch.alerts[0].fingerprint == "abc"
    assert batch.alerts[0].starts_at == "2026-10-02T01:00:00Z"
    assert batch.receiver == "bridge"
    assert batch.group_key == "transport"


@pytest.mark.parametrize("payload", [
    [], {}, {"alerts": []}, {"alerts": [None]},
    {"alerts": [{"status": "unknown"}]},
    {"alerts": [{"status": "firing", "labels": {"cluster": 123}}]},
    {"alerts": [{"status": "firing", "annotations": []}]},
    {"alerts": [alert()], "truncatedAlerts": -1},
    {"alerts": [alert()], "truncatedAlerts": True},
    {"alerts": [alert()], "groupKey": {}},
])
def test_malformed_notifications_are_rejected(payload):
    with pytest.raises(ValueError):
        normalize_payload(payload)


@pytest.mark.parametrize("suffix", [".yaml", ".json"])
def test_load_routes_and_cli_fallback_override(tmp_path, suffix):
    config = {"default_chat_id": "oc_file", "routes": [
        {"match": {"owner_team": "storage"}, "chat_id": "oc_storage"},
    ]}
    path = tmp_path / ("routes" + suffix)
    # JSON is also valid YAML.
    path.write_text(json.dumps(config), encoding="utf-8")
    fallback, routes = load_fixed_chat_routes(str(path), None)
    assert fallback == "oc_file"
    assert routes[0].chat_id == "oc_storage"
    assert load_fixed_chat_routes(str(path), "oc_cli")[0] == "oc_cli"


@pytest.mark.parametrize("config", [
    {"default_chat_id": "YOUR_FIXED_CHAT_ID"},
    {"default_chat_id": "oc_platform", "chat_policy": "auto"},
    {"default_chat_id": "oc_platform", "routes": {}},
    {"default_chat_id": "oc_platform", "routes": [{"match": {}, "chat_id": "oc_team"}]},
    {"default_chat_id": "oc_platform", "routes": [{"match": {"owner_team": ""}, "chat_id": "oc_team"}]},
    {"default_chat_id": "oc_platform", "routes": [{"match": {"owner_team": "a"}, "chat_id": ""}]},
])
def test_bad_routes_fail_before_startup(tmp_path, config):
    path = tmp_path / "routes.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        load_fixed_chat_routes(str(path), None)


def test_malformed_yaml_has_actionable_error(tmp_path):
    path = tmp_path / "routes.yaml"
    path.write_text("routes: [", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid YAML"):
        load_fixed_chat_routes(str(path), "oc_platform")
