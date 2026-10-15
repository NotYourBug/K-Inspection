"""
Lightweight state store for alert group-chat tracking.

In "app" mode, the Alertmanager bridge needs to remember which Feishu group
chat corresponds to which alert group key, so it can reuse chats instead of
creating duplicates, and rename groups when alerts resolve.

Uses a JSON file — sufficient for small-scale use. Swap for Redis/DB in production.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ChatRecord:
    """A Feishu group chat associated with an alert group."""
    chat_id: str
    base_name: str
    resolved: bool


class JsonStateStore:
    """
    JSON-backed state store mapping group_key → ChatRecord.

    Uses atomic write (tmp file + os.replace) to avoid corruption on crash.

    File structure:
      {"<group_key>": {"chat_id": "...", "base_name": "...", "resolved": false}}
    """

    def __init__(self, path: str):
        self._path = Path(path)

    def get_chat(self, key: str) -> ChatRecord | None:
        data = self._load()
        it = data.get(key)
        if not isinstance(it, dict):
            return None
        chat_id = it.get("chat_id")
        base_name = it.get("base_name")
        resolved = it.get("resolved")
        if not isinstance(chat_id, str) or not isinstance(base_name, str):
            return None
        return ChatRecord(chat_id=chat_id, base_name=base_name, resolved=bool(resolved))

    def upsert_chat(self, key: str, record: ChatRecord) -> None:
        data = self._load()
        data[key] = {"chat_id": record.chat_id, "base_name": record.base_name, "resolved": record.resolved}
        self._save(data)

    def mark_resolved(self, key: str, resolved: bool) -> None:
        data = self._load()
        it = data.get(key)
        if not isinstance(it, dict):
            return
        it["resolved"] = bool(resolved)
        data[key] = it
        self._save(data)

    def _load(self) -> dict[str, Any]:
        try:
            if not self._path.exists():
                return {}
            raw = self._path.read_text(encoding="utf-8")
            obj = json.loads(raw) if raw.strip() else {}
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def _save(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self._path)
