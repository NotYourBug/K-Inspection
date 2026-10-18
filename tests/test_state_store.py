"""Tests for k8s_inspector.notifications.bridge.legacy_state."""

import json
import tempfile
from pathlib import Path

from k8s_inspector.notifications.bridge.legacy_state import ChatRecord, JsonStateStore


class TestJsonStateStore:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = str(Path(self.tmpdir) / "state.json")

    def test_get_missing_key_returns_none(self):
        store = JsonStateStore(self.path)
        assert store.get_chat("nonexistent") is None

    def test_upsert_and_get(self):
        store = JsonStateStore(self.path)
        record = ChatRecord(chat_id="oc_123", base_name="Test Chat", resolved=False)
        store.upsert_chat("group_key_1", record)

        got = store.get_chat("group_key_1")
        assert got is not None
        assert got.chat_id == "oc_123"
        assert got.base_name == "Test Chat"
        assert got.resolved is False

    def test_upsert_overwrites(self):
        store = JsonStateStore(self.path)
        r1 = ChatRecord(chat_id="oc_1", base_name="Old", resolved=False)
        r2 = ChatRecord(chat_id="oc_2", base_name="New", resolved=True)
        store.upsert_chat("k", r1)
        store.upsert_chat("k", r2)
        got = store.get_chat("k")
        assert got.chat_id == "oc_2"
        assert got.base_name == "New"
        assert got.resolved is True

    def test_mark_resolved(self):
        store = JsonStateStore(self.path)
        store.upsert_chat("k", ChatRecord(chat_id="oc_x", base_name="T", resolved=False))
        store.mark_resolved("k", True)
        assert store.get_chat("k").resolved is True

        store.mark_resolved("k", False)
        assert store.get_chat("k").resolved is False

    def test_mark_resolved_nonexistent_does_not_crash(self):
        store = JsonStateStore(self.path)
        store.mark_resolved("nope", True)

    def test_persistence_across_instances(self):
        store1 = JsonStateStore(self.path)
        store1.upsert_chat("k", ChatRecord(chat_id="oc_p", base_name="Persist", resolved=False))

        store2 = JsonStateStore(self.path)
        got = store2.get_chat("k")
        assert got is not None
        assert got.chat_id == "oc_p"

    def test_multiple_keys(self):
        store = JsonStateStore(self.path)
        store.upsert_chat("a", ChatRecord(chat_id="1", base_name="A", resolved=False))
        store.upsert_chat("b", ChatRecord(chat_id="2", base_name="B", resolved=True))
        assert store.get_chat("a").chat_id == "1"
        assert store.get_chat("b").chat_id == "2"

    def test_corrupted_file_does_not_crash(self):
        Path(self.path).write_text("not valid json {{{")
        store = JsonStateStore(self.path)
        assert store.get_chat("k") is None
        # Should still be able to upsert after corruption
        store.upsert_chat("k", ChatRecord(chat_id="oc", base_name="X", resolved=False))
        assert store.get_chat("k").chat_id == "oc"
