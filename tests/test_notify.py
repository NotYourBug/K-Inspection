"""Tests for k8s_inspector.notifications.providers.webhook."""

import json
from unittest.mock import MagicMock, patch

import pytest

from k8s_inspector.notifications.providers.webhook import send_feishu_text


class TestSendFeishuText:
    def test_sends_correct_payload(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"code": 0}'

        with patch("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            result = send_feishu_text("https://open.feishu.cn/hook/xxx", "Hello cluster: prod")

        assert result["status"] == 200
        assert result["body"] == '{"code": 0}'
        # Verify the request was created with correct URL
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://open.feishu.cn/hook/xxx"
        assert req.method == "POST"

    def test_truncated_response_body(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps({"code": 0, "padding": "x" * 3000}).encode()

        with patch("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            result = send_feishu_text("https://open.feishu.cn/hook/xxx", "test")

        assert result["status"] == 200
        assert len(result["body"]) <= 2000

    def test_empty_text(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"code": 0}'

        with patch("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            result = send_feishu_text("https://open.feishu.cn/hook/xxx", "")

        assert result["status"] == 200

    @pytest.mark.parametrize("body", [
        b'{"code": 19024}', b'{"code": false}', b'{"code": "0"}',
        b'{}', b'[]', b'ok', b'{"code": 19024, "StatusCode": 0}',
    ])
    def test_unconfirmed_or_failed_delivery_raises(self, body):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = body
        with patch("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            with pytest.raises(RuntimeError):
                send_feishu_text("https://example.invalid/hook", "alert")

    def test_legacy_success_response(self):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"StatusCode": 0, "StatusMessage": "success"}'
        with patch("k8s_inspector.notifications.providers.webhook.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__.return_value = mock_resp
            assert send_feishu_text("https://example.invalid/hook", "alert")["status"] == 200
