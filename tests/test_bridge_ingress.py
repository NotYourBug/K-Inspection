import http.client
import importlib
import json
import socket
import sqlite3
import threading
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from k8s_inspector import alertmanager_bridge as bridge
from k8s_inspector.alert_routing import FixedChatRoute
from k8s_inspector.bridge_store import DeliveryStore
from tests.delivery_factories import NOW

TOKEN = "test-token-with-32-characters-0001"
PAYLOAD = {"source_id": "untrusted", "alerts": [
    {"status": "firing", "fingerprint": "f1", "startsAt": "2026-10-15T00:00:00Z",
     "labels": {"owner_team": "storage"}, "annotations": {"summary": "storage-only"}},
    {"status": "firing", "fingerprint": "f2", "startsAt": "2026-10-15T00:00:00Z",
     "labels": {}, "annotations": {"summary": "fallback-only"}}]}


def credentials():
    module = importlib.import_module("k8s_inspector.bridge_auth")
    return (module.SourceCredential("prod-am", TOKEN),)


@contextmanager
def serve(tmp_path, **overrides):
    cfg = bridge.BridgeConfig(mode="app", feishu_chat_id="oc_platform", listen_host="127.0.0.1",
        listen_port=0, report_dir=str(tmp_path), delivery_mode="async", state_db=str(tmp_path / "bridge.db"),
        source_credentials=credentials(), chat_routes=(FixedChatRoute({"owner_team": "storage"}, "oc_storage"),),
        **overrides)
    provider = MagicMock()
    server = bridge.create_server(cfg, provider)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
    thread.start()
    try:
        yield server, provider, cfg
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


def post(server, data=PAYLOAD, token=TOKEN, headers=None):
    conn = http.client.HTTPConnection(*server.server_address, timeout=3)
    body = json.dumps(data).encode()
    hdr = {"Content-Type": "application/json", **(headers or {})}
    if token is not None:
        hdr["Authorization"] = "Bearer " + token
    try:
        conn.request("POST", "/alert", body, hdr)
        resp = conn.getresponse()
        return resp.status, resp.read().decode()
    finally:
        conn.close()


def test_202_means_persisted_not_sent_and_body_cannot_override_source(tmp_path):
    with serve(tmp_path) as (server, provider, _):
        status, body = post(server)
        assert status == 202
        receipt = json.loads(body)
        assert receipt["receipt_id"] and receipt["new_jobs"] == 2 and receipt["reused_jobs"] == 0
        jobs = server.delivery_store.list_jobs()
        assert len(jobs) == 2 and {j.source_id for j in jobs} == {"prod-am"}
        assert {j.state for j in jobs} == {"PENDING"}
        assert provider.method_calls == []
        messages = {j.payload["chat_id"]: json.dumps(j.payload) for j in jobs}
        assert "fallback-only" not in messages["oc_storage"] and "storage-only" not in messages["oc_platform"]
        assert TOKEN not in json.dumps([j.payload for j in jobs])
        assert json.loads(post(server)[1])["reused_jobs"] == 2


@pytest.mark.parametrize("token", [None, "wrong-token"])
def test_missing_or_wrong_token_is_401(tmp_path, token):
    with serve(tmp_path) as (server, _, _):
        assert post(server, token=token)[0] == 401
        assert server.delivery_store.list_jobs() == ()


@pytest.mark.parametrize("failure", ["trigger", "capacity", "lock"])
def test_storage_failure_never_acknowledges(tmp_path, failure):
    with serve(tmp_path, max_backlog=1 if failure == "capacity" else 10000) as (server, _, _):
        db = sqlite3.connect(server.delivery_store.path, isolation_level=None)
        try:
            if failure == "trigger":
                db.execute("""create trigger disk_full before insert on outbox
                    when new.payload like '%oc_platform%' begin select raise(ABORT,'secret-token'); end""")
            if failure == "lock":
                db.execute("BEGIN IMMEDIATE")
            status, body = post(server)
            assert status == 503 and "secret-token" not in body
            assert server.delivery_store.list_jobs() == ()
        finally:
            db.close()


def test_body_limit_and_invalid_payload(tmp_path):
    with serve(tmp_path) as (server, _, _):
        assert post(server, {"alerts": []})[0] == 400
        assert post(server, "x" * (1024 * 1024))[0] == 413


def test_concurrency_limit_returns_503_before_new_handler(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = getattr(bridge, "handle_alert_request", None)
    def blocking(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    monkeypatch.setattr(bridge, "handle_alert_request", blocking, raising=False)
    with serve(tmp_path, max_requests=1) as (server, _, _):
        result = []
        first = threading.Thread(target=lambda: result.append(post(server)))
        first.start()
        try:
            assert entered.wait(2)
            assert post(server)[0] == 503
        finally:
            release.set()
            first.join(3)
        assert result[0][0] == 202


def test_async_startup_requires_database_and_auth(tmp_path):
    cfg = bridge.BridgeConfig(mode="webhook", feishu_webhook="https://example.invalid/hook",
                              delivery_mode="async")
    with pytest.raises(ValueError, match="state|auth"):
        bridge.create_server(cfg)


def test_auth_rotation_keeps_logical_source_and_rejects_ambiguous_token():
    auth = importlib.import_module("k8s_inspector.bridge_auth")
    sources = credentials() + (auth.SourceCredential("prod-am", "rotated-token-with-32-characters-1"),)
    assert auth.authenticate({"Authorization": "Bearer " + TOKEN}, sources) == "prod-am"
    assert auth.authenticate({"Authorization": "Bearer rotated-token-with-32-characters-1"}, sources) == "prod-am"
    assert TOKEN not in repr(sources)
    with pytest.raises(ValueError):
        auth.validate_sources(credentials() + (auth.SourceCredential("other", TOKEN),))


def test_cli_reads_mounted_sources_and_environment_without_exposing_token(tmp_path, monkeypatch):
    path = tmp_path / "sources.json"
    path.write_text(json.dumps([{"source_id": "prod-am", "token": TOKEN}]), encoding="utf-8")
    monkeypatch.setenv("BRIDGE_SOURCES_FILE", str(path))
    args = bridge.build_parser().parse_args(["--mode", "webhook", "--feishu-webhook",
        "https://example.invalid/secret", "--delivery-mode", "async", "--state-db", str(tmp_path / "bridge.db")])
    cfg = bridge._config_from_args(args)
    assert cfg.source_credentials[0].source_id == "prod-am"
    assert TOKEN not in repr(cfg) and "example.invalid" not in repr(cfg)


def test_async_http_worker_delivery_and_readiness_end_to_end(tmp_path, monkeypatch):
    with serve(tmp_path) as (server, provider, _):
        from tests.test_bridge_observability import get
        provider.send_card_message.return_value = "om_confirmed"
        finished = threading.Event()
        original = server.delivery_store.finish
        count = []
        def finish(*args, **kwargs):
            result = original(*args, **kwargs)
            count.append(result)
            if len(count) == 2:
                finished.set()
            return result
        monkeypatch.setattr(server.delivery_store, "finish", finish)
        server.delivery_worker.start()
        assert post(server)[0] == 202
        assert finished.wait(3)
        assert len(server.delivery_store.list_jobs(state="SENT")) == 2
        assert get(server, "/readyz")[0] == 200
        assert provider.create_chat.call_count == provider.rename_chat.call_count == 0
