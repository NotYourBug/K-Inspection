"""
Feishu enterprise app bot client (lark-oapi SDK).

Provides IM group-chat management capabilities that the plain
webhook bot does not support: create chats, invite members, rename chats.

Used by alertmanager_bridge in "app" mode.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from k8s_inspector.notifications.providers.errors import ProviderError, retry_after_seconds


@dataclass
class FeishuAppConfig:
    app_id: str
    app_secret: str
    user_id_type: str = "open_id"


def load_feishu_app_config_from_env() -> FeishuAppConfig:
    app_id = os.getenv("FEISHU_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("FEISHU_APP_ID and FEISHU_APP_SECRET are required")
    user_id_type = os.getenv("FEISHU_USER_ID_TYPE", "open_id").strip() or "open_id"
    return FeishuAppConfig(app_id=app_id, app_secret=app_secret, user_id_type=user_id_type)


class FeishuAppClient:
    """Minimal Feishu IM API client."""

    def __init__(self, cfg: FeishuAppConfig):
        self._cfg = cfg
        self._client = _build_lark_client(cfg.app_id, cfg.app_secret)

    def create_chat(self, name: str, description: str, user_open_ids: list[str]) -> str:
        """Create a group chat and invite members. Returns chat_id."""
        im_v1 = _import_im_v1()
        req = (
            im_v1.CreateChatRequest.builder()
            .user_id_type(self._cfg.user_id_type)
            .request_body(
                im_v1.CreateChatRequestBody.builder()
                .name(name)
                .description(description)
                .user_id_list(user_open_ids)
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.chat.create(req)
        if not resp.success():
            raise RuntimeError(f"Failed to create chat: {resp.msg}, code: {resp.code}")
        return resp.data.chat_id

    def send_card_message(self, chat_id: str, card: dict[str, Any],
                          message_uuid: str | None = None) -> str | None:
        """Send an interactive card message to a chat."""
        im_v1 = _import_im_v1()
        lark = _import_lark()
        content = lark.JSON.marshal(card)
        req = (
            im_v1.CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                im_v1.CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(content)
                .uuid(message_uuid)
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.message.create(req)
        raw = getattr(resp, "raw", None)
        status = getattr(raw, "status_code", None)
        if (type(resp.code) is not int or resp.code != 0 or not resp.success()
                or status is None or not 200 <= status < 300):
            raise ProviderError(resp.code, status=status, unknown=type(resp.code) is not int,
                                retry_after=retry_after_seconds(getattr(raw, "headers", {})))
        return getattr(getattr(resp, "data", None), "message_id", None)

    def send_text_message(self, chat_id: str, text: str) -> None:
        """Send a plain text message to a chat."""
        im_v1 = _import_im_v1()
        payload = {"text": text}
        req = (
            im_v1.CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                im_v1.CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps(payload, ensure_ascii=False))
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.message.create(req)
        if not resp.success():
            raise RuntimeError(f"Failed to send text message: {resp.msg}, code: {resp.code}")

    def rename_chat(self, chat_id: str, new_name: str) -> None:
        """Rename a group chat. Tries patch first, then update for SDK compatibility."""
        im_v1 = _import_im_v1()
        patch_cls = getattr(im_v1, "PatchChatRequest", None)
        patch_body_cls = getattr(im_v1, "PatchChatRequestBody", None)
        if patch_cls and patch_body_cls:
            req = (
                patch_cls.builder()
                .chat_id(chat_id)
                .request_body(patch_body_cls.builder().name(new_name).build())
                .build()
            )
            resp = self._client.im.v1.chat.patch(req)
            if not resp.success():
                raise RuntimeError(f"Failed to rename chat: {resp.msg}, code: {resp.code}")
            return

        update_cls = getattr(im_v1, "UpdateChatRequest", None)
        update_body_cls = getattr(im_v1, "UpdateChatRequestBody", None)
        if update_cls and update_body_cls:
            req = (
                update_cls.builder()
                .chat_id(chat_id)
                .request_body(update_body_cls.builder().name(new_name).build())
                .build()
            )
            resp = self._client.im.v1.chat.update(req)
            if not resp.success():
                raise RuntimeError(f"Failed to rename chat: {resp.msg}, code: {resp.code}")
            return

        raise RuntimeError("No chat rename API found in current lark-oapi version")


def _build_lark_client(app_id: str, app_secret: str):
    lark = _import_lark()
    return lark.Client.builder().app_id(app_id).app_secret(app_secret).timeout(10).build()


def _import_lark():
    try:
        import lark_oapi as lark  # type: ignore
        return lark
    except Exception as e:
        raise RuntimeError("lark-oapi is required for Feishu app bot mode") from e


def _import_im_v1():
    try:
        import importlib
        return importlib.import_module("lark_oapi.api.im.v1")
    except Exception as e:
        raise RuntimeError("lark-oapi im.v1 module missing") from e


def build_alert_card(title: str, lines: list[str], template: str = "red") -> dict[str, Any]:
    """Build a minimal Feishu interactive card payload for alert display."""
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "plain_text", "content": "\n".join(lines)[:4000]}},
        ],
    }
