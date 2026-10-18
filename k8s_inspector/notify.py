"""
Notification module — Feishu bot webhook sender.

Uses stdlib urllib to keep dependencies minimal.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from .provider_error import ProviderError, retry_after_seconds


def send_feishu_text(webhook_url: str, text: str, timeout_s: float = 5.0) -> dict[str, Any]:
    """
    Send a text message via Feishu bot webhook.

    Returns {"status": http_status, "body": response_body_prefix}.
    Raises RuntimeError for an unconfirmed delivery or non-zero Feishu code.
    """
    payload = {"msg_type": "text", "content": {"text": text}}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read(65537).decode("utf-8", errors="replace")
        if len(body.encode("utf-8")) > 65536:
            raise ProviderError(unknown=True)
        try:
            result = json.loads(body)
        except ValueError as exc:
            raise ProviderError(unknown=True) from exc
        if not isinstance(result, dict):
            raise ProviderError(unknown=True)
        code = result.get("code") if "code" in result else result.get("StatusCode")
        if type(code) is not int or code != 0 or not 200 <= resp.status < 300:
            raise ProviderError(code, status=resp.status, unknown=type(code) is not int,
                                retry_after=retry_after_seconds(getattr(resp, "headers", {})))
        return {"status": resp.status, "body": body[:2000]}
