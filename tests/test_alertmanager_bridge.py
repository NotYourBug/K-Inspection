"""Offline tests for fixed-chat delivery and the real loopback HTTP receiver."""

import http.client
import json
import signal
import threading
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from k8s_inspector.notifications.bridge import server as bridge
from k8s_inspector.notifications.bridge.routing import FixedChatRoute


def payload(*profiles, status="firing", cluster="prod"):
    return {"groupKey": "arbitrary-key", "alerts": [
        {"status": status, "labels": {
            "cluster": cluster, "profile": profile, "alertname": "CheckFail", "check": "pvcs",
        }, "annotations": {"summary": "Pending PVC"}}
        for profile in (profiles or ("5m",))
    ]}


def card_text(card):
    return card["elements"][0]["text"]["content"]


@pytest.fixture
def cfg(tmp_path):
    return bridge.BridgeConfig(
        mode="app", feishu_chat_id="oc_platform", report_dir=str(tmp_path),
        listen_host="127.0.0.1", listen_port=0,
    )


@pytest.fixture
def clean_env(monkeypatch):
    for key in (
        "FEISHU_MODE", "FEISHU_CHAT_ID", "FEISHU_ROUTES_FILE", "FEISHU_WEBHOOK", "FEISHU_CHAT_POLICY",
        "INSPECTOR_REPORT_DIR", "INSPECTOR_REPORT_MAX_RESULTS", "INSPECTOR_REPORT_MAX_ITEMS",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.mark.parametrize("status", ["firing", "resolved"])
def test_existing_chat_delivery_never_creates_or_renames(cfg, status):
    client = MagicMock()
    bridge.handle_alert_payload(payload("5m", "hourly", "daily", status=status), cfg, client)
    client.send_card_message.assert_called_once()
    chat_id, card = client.send_card_message.call_args.args
    assert chat_id == "oc_platform"
    text = card_text(card)
    assert all(f"profile={profile}" in text for profile in ("5m", "hourly", "daily"))
    client.create_chat.assert_not_called()
    client.rename_chat.assert_not_called()


def test_mixed_status_notification_keeps_partial_recovery(cfg):
    data = payload("5m", "hourly")
    data["alerts"][1]["status"] = "resolved"
    data["truncatedAlerts"] = 4
    client = MagicMock()
    bridge.handle_alert_payload(data, cfg, client)
    card = client.send_card_message.call_args.args[1]
    text = card_text(card)
    assert "FIRING: 1 | RESOLVED: 1" in text
    assert "截断了 4 条" in text
    assert "事件结案仍需核验" in text
    assert card["header"]["template"] == "red"


def test_route_ownership_isolated_and_missing_owner_uses_fallback(cfg):
    cfg.chat_routes = (FixedChatRoute({"owner_team": "storage"}, "oc_storage"),)
    data = payload("5m", "hourly")
    data["alerts"][0]["labels"]["owner_team"] = "storage"
    data["alerts"][0]["annotations"]["summary"] = "storage-only"
    data["alerts"][1]["annotations"]["summary"] = "fallback-only"
    client = MagicMock()
    bridge.handle_alert_payload(data, cfg, client)
    messages = {call.args[0]: card_text(call.args[1]) for call in client.send_card_message.call_args_list}
    assert "storage-only" in messages["oc_storage"]
    assert "fallback-only" not in messages["oc_storage"]
    assert "fallback-only" in messages["oc_platform"]
    assert "storage-only" not in messages["oc_platform"]


def test_failed_card_delivery_is_not_acknowledged(cfg):
    client = MagicMock()
    client.send_card_message.side_effect = RuntimeError("simulated SDK rejection")
    with pytest.raises(RuntimeError, match="delivery failed"):
        bridge.handle_alert_payload(payload(), cfg, client)


def test_failed_destination_does_not_block_other_responsibility_chats(cfg):
    cfg.chat_routes = (FixedChatRoute({"owner_team": "storage"}, "oc_storage"),)
    data = payload("5m", "hourly")
    data["alerts"][0]["labels"]["owner_team"] = "storage"
    client = MagicMock()
    client.send_card_message.side_effect = [RuntimeError("storage unavailable"), None]
    with pytest.raises(RuntimeError, match="1 destination"):
        bridge.handle_alert_payload(data, cfg, client)
    assert [call.args[0] for call in client.send_card_message.call_args_list] == ["oc_storage", "oc_platform"]


def test_webhook_mode_uses_same_mixed_source_formatter(cfg, monkeypatch):
    cfg.mode = "webhook"
    cfg.feishu_webhook = "https://example.invalid/hook"
    sender = MagicMock()
    monkeypatch.setattr(bridge, "send_feishu_text", sender)
    data = payload("5m", "hourly")
    data["alerts"][1]["status"] = "resolved"
    bridge.handle_alert_payload(data, cfg)
    text = sender.call_args.kwargs["text"]
    assert "profile=5m" in text and "profile=hourly" in text
    assert "FIRING: 1 | RESOLVED: 1" in text


def write_report(tmp_path, cluster="prod", age=0, timestamp=None, long=False):
    path = tmp_path / "latest" / "5m" / "report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "cluster": cluster,
        "generated_at": timestamp or (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat(),
        "summary": {"fail": 1},
        "results": [{"name": "pvcs", "status": "FAIL", "summary": "snapshot-evidence", "data": {}}],
    }
    if long:
        report["results"] *= 100
    path.write_text(json.dumps(report), encoding="utf-8")


def test_snapshot_is_cluster_bound_fresh_and_explicit_background(cfg, tmp_path):
    write_report(tmp_path)
    client = MagicMock()
    bridge.handle_alert_payload(payload(), cfg, client)
    text = card_text(client.send_card_message.call_args.args[1])
    assert "snapshot-evidence" in text
    assert "辅助背景，不作为事件恢复依据" in text


@pytest.mark.parametrize("report_options", [
    {"cluster": "another-cluster"}, {"age": 901}, {"age": -120},
    {"timestamp": "not-a-timestamp"}, {"timestamp": "2026-10-02T01:00:00"},
])
def test_mismatched_or_invalid_snapshot_is_not_attached(cfg, tmp_path, report_options):
    write_report(tmp_path, **report_options)
    client = MagicMock()
    bridge.handle_alert_payload(payload(), cfg, client)
    assert "snapshot-evidence" not in card_text(client.send_card_message.call_args.args[1])


@pytest.mark.parametrize("label", ["owner_team", "namespace", "service"])
def test_scoped_notification_does_not_attach_whole_cluster_report(cfg, tmp_path, label):
    write_report(tmp_path)
    data = payload()
    data["alerts"][0]["labels"][label] = "team-scope"
    client = MagicMock()
    bridge.handle_alert_payload(data, cfg, client)
    assert "snapshot-evidence" not in card_text(client.send_card_message.call_args.args[1])


def test_mixed_profiles_do_not_use_first_alert_report(cfg, tmp_path):
    write_report(tmp_path)
    client = MagicMock()
    bridge.handle_alert_payload(payload("5m", "hourly"), cfg, client)
    assert "snapshot-evidence" not in card_text(client.send_card_message.call_args.args[1])


def test_check_routed_team_does_not_receive_other_checks_in_snapshot(cfg, tmp_path):
    write_report(tmp_path)
    cfg.chat_routes = (FixedChatRoute({"check": "pvcs"}, "oc_storage"),)
    client = MagicMock()
    bridge.handle_alert_payload(payload(), cfg, client)
    assert client.send_card_message.call_args.args[0] == "oc_storage"
    assert "snapshot-evidence" not in card_text(client.send_card_message.call_args.args[1])


def test_recovery_notice_survives_large_background(cfg, tmp_path):
    write_report(tmp_path, long=True)
    cfg.report_max_results = 100
    client = MagicMock()
    data = payload(*(["5m"] * 16))
    for item in data["alerts"]:
        item["annotations"]["summary"] = "x" * 500
    for item in data["alerts"][8:]:
        item["status"] = "resolved"
    bridge.handle_alert_payload(data, cfg, client)
    text = card_text(client.send_card_message.call_args.args[1])
    assert "FIRING: 8 | RESOLVED: 8" in text
    assert "事件结案仍需核验" in text
    assert "背景内容已省略" in text
    assert len(text) < 4000


@pytest.mark.parametrize("profile", ["../", "../../other", "C:\\other", "", "custom"])
def test_untrusted_profile_cannot_choose_report_path(tmp_path, profile):
    assert bridge._try_load_latest_report_json(str(tmp_path), profile) == (None, None)


def test_app_config_no_longer_requires_invited_user_ids(clean_env, monkeypatch):
    monkeypatch.setenv("FEISHU_CHAT_ID", "oc_platform")
    monkeypatch.setenv("INSPECTOR_REPORT_MAX_RESULTS", "17")
    args = bridge.build_parser().parse_args([
        "--mode", "app", "--listen-host", "127.0.0.1", "--report-max-results", "0",
    ])
    cfg = bridge._config_from_args(args)
    assert cfg.feishu_chat_id == "oc_platform"
    assert cfg.chat_policy == "fixed"
    assert cfg.listen_host == "127.0.0.1"
    assert cfg.report_max_results == 0


def test_missing_fallback_fails_before_server_or_sdk(clean_env, monkeypatch, capsys):
    server = MagicMock()
    monkeypatch.setattr(bridge, "run_server", server)
    with pytest.raises(SystemExit) as exc:
        bridge.main(["--mode", "app"])
    assert exc.value.code == 2
    assert "default_chat_id" in capsys.readouterr().err
    server.assert_not_called()


def test_cli_overrides_environment_destination(clean_env, monkeypatch):
    monkeypatch.setenv("FEISHU_CHAT_ID", "oc_env")
    args = bridge.build_parser().parse_args(["--mode", "app", "--feishu-chat-id", "oc_cli"])
    assert bridge._config_from_args(args).feishu_chat_id == "oc_cli"


def test_unsupported_policy_cannot_reenable_automatic_chat_creation(clean_env, monkeypatch):
    monkeypatch.setenv("FEISHU_CHAT_POLICY", "auto")
    monkeypatch.setenv("FEISHU_CHAT_ID", "oc_platform")
    with pytest.raises(ValueError, match="only fixed"):
        bridge._config_from_args(bridge.build_parser().parse_args(["--mode", "app"]))


@pytest.fixture
def http_receiver(cfg):
    client = MagicMock()
    server = bridge.create_server(cfg, client)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    try:
        yield server, client
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
        assert not worker.is_alive()


def request(server, body, headers=None, path="/alert"):
    conn = http.client.HTTPConnection(*server.server_address, timeout=3)
    try:
        conn.request("POST", path, body=body, headers=headers or {})
        response = conn.getresponse()
        response.read()
        return response.status
    finally:
        conn.close()


def test_http_delivery_success_and_sdk_failure(http_receiver):
    server, client = http_receiver
    assert server.server_address[0] == "127.0.0.1"
    assert request(server, json.dumps(payload())) == 200
    client.send_card_message.assert_called_once()
    client.send_card_message.side_effect = RuntimeError("simulated failure")
    assert request(server, json.dumps(payload())) == 500


def test_http_webhook_business_error_is_500(cfg, monkeypatch):
    cfg.mode = "webhook"
    cfg.feishu_webhook = "https://example.invalid/hook"
    response = MagicMock()
    response.status = 200
    response.read.return_value = b'{"code": 19024, "msg": "keyword rejected"}'
    urlopen = MagicMock()
    urlopen.return_value.__enter__.return_value = response
    monkeypatch.setattr("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen", urlopen)
    server = bridge.create_server(cfg)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    try:
        assert request(server, json.dumps(payload())) == 500
        urlopen.assert_called_once()
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
        assert not worker.is_alive()


@pytest.mark.parametrize("body", [b'[]', b'{}', b'{"alerts": []}', b'{invalid', b'\xff'])
def test_http_invalid_body_is_400_and_has_no_side_effects(http_receiver, body):
    server, client = http_receiver
    assert request(server, body) == 400
    client.send_card_message.assert_not_called()


def test_http_rejects_oversize_and_bad_content_length(http_receiver):
    server, client = http_receiver
    # These requests must be rejected from headers before reading any body.
    # Sending an unread byte can race a TCP reset on Windows when the server closes.
    assert request(server, b"", {"Content-Length": str(bridge.MAX_WEBHOOK_BYTES + 1)}) == 413
    assert request(server, b"", {"Content-Length": "-1"}) == 400
    assert request(server, b"", {"Content-Length": "invalid"}) == 400
    assert request(server, b"", {"Content-Length": "9" * 5000}) == 413
    assert request(server, b"", path="/unsupported") == 404
    client.send_card_message.assert_not_called()


def test_shutdown_is_called_from_another_thread_and_socket_closed(cfg, monkeypatch):
    handlers = {}
    shutdown_complete = threading.Event()
    shutdown_threads = []
    server = MagicMock()

    def serve():
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        assert shutdown_complete.wait(timeout=2)

    def shutdown():
        shutdown_threads.append(threading.get_ident())
        shutdown_complete.set()

    server.serve_forever.side_effect = serve
    server.shutdown.side_effect = shutdown
    monkeypatch.setattr(bridge, "create_server", lambda config: server)
    monkeypatch.setattr(bridge.signal, "signal", lambda sig, handler: handlers.update({sig: handler}))
    bridge.run_server(cfg)
    assert shutdown_threads[0] != threading.get_ident()
    server.server_close.assert_called_once()
