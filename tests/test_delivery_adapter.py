import importlib
import json
import socket
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from k8s_inspector.bridge_store import DeliveryStore
from k8s_inspector.feishu_app import FeishuAppClient, FeishuAppConfig
from tests.delivery_factories import NOW, batch, target


def adapter(**kwargs):
    return importlib.import_module("k8s_inspector.delivery_adapter").DeliveryAdapter(**kwargs)


@pytest.fixture
def job(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    receipt = store.accept("am", batch(), (target(),), NOW)
    return store.get_job(receipt.new_job_ids[0])


def test_same_delivery_reuses_sdk_uuid_and_timeout_reaches_transport(job, monkeypatch):
    from lark_oapi.core.token.manager import TokenManager
    from lark_oapi.core.http import transport
    seen = []
    monkeypatch.setattr(TokenManager, "get_self_tenant_token", lambda _cfg: "fake-token")
    def request(*args, **kwargs):
        seen.append(kwargs)
        return SimpleNamespace(status_code=200, headers={}, content=b'{"code":0,"data":{"message_id":"om_confirmed"}}')
    monkeypatch.setattr(transport.requests, "request", request)
    client = FeishuAppClient(FeishuAppConfig("app", "secret"))
    sender = adapter(feishu_app=client)
    assert sender.send(job).external_id == "om_confirmed"
    assert sender.send(job).success
    assert [json.loads(x["data"])["uuid"] for x in seen] == [job.delivery_id] * 2
    assert all(x["timeout"] == 10 for x in seen)
    assert job.delivery_id in json.loads(seen[0]["data"])["content"]


@pytest.mark.parametrize("code,status,retryable", [(230002, 400, False), (99991672, 400, False),
    (99991400, 200, True), (999, 200, True), (999, 503, True)])
def test_business_or_http_failure_is_never_success(job, monkeypatch, code, status, retryable):
    from k8s_inspector import feishu_app
    client = FeishuAppClient.__new__(FeishuAppClient)
    client._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(
        create=lambda _req: SimpleNamespace(success=lambda: False, code=code, msg="secret-token",
            raw=SimpleNamespace(status_code=status, headers={"Retry-After": "45"}))))))
    result = adapter(feishu_app=client).send(job)
    assert not result.success and result.retryable == retryable
    assert result.error_code == str(code)
    assert "secret-token" not in repr(result)
    if retryable:
        assert result.retry_after_seconds == 45


def test_webhook_business_failure_timeout_and_credentials_are_redacted(job, monkeypatch):
    from k8s_inspector import notify
    job = replace(job, payload={"mode": "webhook", "text": "alert"})
    sender = adapter(webhooks={job.destination_id: "https://example.invalid/secret-token"})
    class Response:
        status = 200
        def read(self, *args): return b'{"code":999,"msg":"secret-token"}'
        def __enter__(self): return self
        def __exit__(self, *args): pass
    seen = []
    def open_(_req, timeout):
        seen.append(timeout)
        return Response()
    monkeypatch.setattr(notify.urllib.request, "urlopen", open_)
    result = sender.send(job)
    assert not result.success and result.error_code == "999" and seen == [10]
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(socket.timeout("secret-token")))
    result = sender.send(job)
    assert (result.success, result.retryable, result.result_unknown) == (False, True, True)
    assert "secret-token" not in repr(result)


def test_webhook_http_rate_limit_and_missing_credentials(job, monkeypatch):
    from k8s_inspector import notify
    job = replace(job, payload={"mode": "webhook", "text": "alert"})
    assert adapter().send(job).error_kind == "configuration"
    def fail(*args, **kwargs):
        raise HTTPError("https://example.invalid/secret-token", 429, "secret-token", {"Retry-After": "20"}, BytesIO())
    monkeypatch.setattr(notify.urllib.request, "urlopen", fail)
    result = adapter(webhooks={job.destination_id: "https://example.invalid/hook"}).send(job)
    assert result.retryable and result.retry_after_seconds == 20


def test_old_two_argument_sdk_call_remains_supported(monkeypatch):
    client = FeishuAppClient.__new__(FeishuAppClient)
    seen = []
    def create(req):
        seen.append(req)
        return SimpleNamespace(success=lambda: True, code=0, raw=SimpleNamespace(status_code=200), data=None)
    client._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(create=create))))
    assert client.send_card_message("oc_a", {}) is None
    assert seen[0].body.uuid is None


def test_token_configuration_failure_is_permanent(job):
    from lark_oapi.core.exception import ObtainAccessTokenException
    class Client:
        def send_card_message(self, *args, **kwargs):
            raise ObtainAccessTokenException("token", 99991543, "secret-token")
    result = adapter(feishu_app=Client()).send(job)
    assert not result.retryable and not result.result_unknown
    assert result.error_code == "99991543" and "secret-token" not in repr(result)
