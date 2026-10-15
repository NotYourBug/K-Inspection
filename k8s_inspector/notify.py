"""
Notification module — Feishu bot webhook sender.

Uses stdlib urllib to keep dependencies minimal.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any


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
        body = resp.read().decode("utf-8", errors="replace")
        try:
            result = json.loads(body)
        except ValueError as exc:
            raise RuntimeError("Feishu webhook returned invalid JSON; delivery not confirmed") from exc
        if not isinstance(result, dict):
            raise RuntimeError("Feishu webhook returned an invalid response; delivery not confirmed")
        code = result.get("code") if "code" in result else result.get("StatusCode")
        if type(code) is not int or code != 0 or not 200 <= resp.status < 300:
            raise RuntimeError(f"Feishu webhook delivery failed (code={code!r})")
        return {"status": resp.status, "body": body[:2000]}
