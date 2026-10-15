"""Reproductions from the independent B review, before its one fix pass."""
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from k8s_inspector import bridge_store as storage
from k8s_inspector.bridge_store import DeliveryStore, StorageUnavailable
from k8s_inspector.delivery_adapter import DeliveryAdapter
from k8s_inspector.delivery_identity import canonical_content_hash
from k8s_inspector.delivery_models import DeliveryResult
from k8s_inspector.feishu_app import FeishuAppClient, FeishuAppConfig
from tests.delivery_factories import NOW, batch, target
from tests.test_delivery_worker import Clock, worker


def content(summary="A", fingerprint="f1", status="firing"):
    original = batch(fingerprint=fingerprint, status=status)
    return replace(original, alerts=(replace(original.alerts[0], annotations={"summary": summary}),))


def accept(store, value, now=NOW, destination="team-a"):
    return store.accept("am", value, (target(destination, canonical_content_hash(value)),), now)


def mark_sent(store, now=NOW):
    job = store.claim_due(now)
    assert store.finish(job.delivery_id, job.lease_token, DeliveryResult(True), now)


def test_old_firing_retry_cannot_overtake_resolved_but_other_destination_progresses(tmp_path):
    store, clock = DeliveryStore(tmp_path / "bridge.db"), Clock()
    firing, resolved = content(), content(status="resolved")
    firing_id = accept(store, firing).new_job_ids[0]
    calls = []
    class Provider:
        def send(self, job):
            calls.append(job.delivery_id)
            if job.delivery_id == firing_id and calls.count(firing_id) == 1:
                return DeliveryResult(False, True, retry_after_seconds=600)
            return DeliveryResult(True)
    w = worker(store, Provider(), clock)
    assert w.run_once()
    clock.advance(300)
    resolved_id = accept(store, resolved, clock()).new_job_ids[0]
    assert not w.run_once(), "resolved must wait behind the earlier accepted firing"
    other_id = accept(store, firing, clock(), "team-b").new_job_ids[0]
    assert w.run_once() and calls[-1] == other_id
    clock.advance(300)
    w = worker(DeliveryStore(store.path), Provider(), clock)  # Restart retains queue order.
    assert w.run_once() and w.run_once()
    assert calls[-2:] == [firing_id, resolved_id]


@pytest.mark.parametrize("already_sent", [False, True])
def test_annotation_a_b_a_is_new_transition_not_an_old_duplicate(tmp_path, already_sent):
    store = DeliveryStore(tmp_path / "bridge.db")
    first = accept(store, content("1"))
    if already_sent:
        mark_sent(store)
    second = accept(store, content("0"), NOW + timedelta(seconds=60))
    if already_sent:
        mark_sent(store, NOW + timedelta(seconds=60))
    third = accept(store, content("1"), NOW + timedelta(seconds=120))
    assert len(third.new_job_ids) == 1 and not third.reused_job_ids
    assert third.new_job_ids[0] not in first.new_job_ids + second.new_job_ids
    assert store.get_job(third.new_job_ids[0]).generation == 2


def test_unrelated_interleaving_does_not_defeat_active_or_ha_dedup(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    first = accept(store, content("1"))
    accept(store, content("other", fingerprint="other"), NOW + timedelta(seconds=1))
    repeat = accept(store, content("1"), NOW + timedelta(seconds=2))
    assert repeat.reused_job_ids == first.new_job_ids and not repeat.new_job_ids


@pytest.mark.parametrize("code", [False, 0.0])
def test_sdk_non_integer_zero_cannot_confirm_delivery(tmp_path, monkeypatch, code):
    from lark_oapi.core.token.manager import TokenManager
    from lark_oapi.core.http import transport
    monkeypatch.setattr(TokenManager, "get_self_tenant_token", lambda _cfg: "fake-token")
    monkeypatch.setattr(transport.requests, "request", lambda *a, **kw:
        SimpleNamespace(status_code=200, headers={}, content=json.dumps({"code": code}).encode()))
    store = DeliveryStore(tmp_path / "bridge.db")
    job_id = accept(store, content()).new_job_ids[0]
    result = DeliveryAdapter(feishu_app=FeishuAppClient(FeishuAppConfig("app", "secret"))).send(store.get_job(job_id))
    assert not result.success and result.result_unknown


def test_dead_can_be_disposed_with_reason_without_erasing_history(tmp_path):
    from k8s_inspector.bridge_admin import main
    store = DeliveryStore(tmp_path / "bridge.db", max_backlog=1)
    job_id = accept(store, content()).new_job_ids[0]
    job = store.claim_due(NOW)
    store.finish(job_id, job.lease_token, DeliveryResult(False, error_code="230002"), NOW)
    assert store.stats(NOW).dead_count == store.stats(NOW).backlog_count == 1
    assert main(["--db", str(store.path), "resolve-dead", "--delivery-id", job_id,
                 "--reason", "obsolete destination verified; delivery waived"]) == 0
    stats = store.stats(NOW)
    assert stats.dead_count == stats.backlog_count == 0
    assert store.get_job(job_id).state == "DEAD" and store.get_job(job_id).error_code == "230002"
    assert store.events(job_id)[-1]["event_type"] == "dead_handled"
    assert accept(store, content(), NOW, "team-b").new_job_ids
    mark_sent(store)
    from k8s_inspector.bridge_observability import render_metrics
    assert b"inspector_bridge_unhandled_dead_jobs 0.0" in render_metrics(store, NOW, NOW)
    # Requeue is explicit and starts a new unresolved handling cycle if it fails.
    store.requeue(job_id, "retry specifically authorized", NOW)
    again = store.claim_due(NOW)
    store.finish(job_id, again.lease_token, DeliveryResult(False), NOW)
    assert store.stats(NOW).dead_count == 1


def test_unknown_can_be_acknowledged_with_audit_and_retention_from_confirmation(tmp_path):
    from k8s_inspector.bridge_admin import main
    store = DeliveryStore(tmp_path / "bridge.db")
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=31)
    job_id = accept(store, content(), old).new_job_ids[0]
    first = store.claim_due(old)
    store.finish(job_id, first.lease_token, DeliveryResult(False, True, True), old,
                 retry_at=old + timedelta(seconds=5))
    mark_sent(store, old + timedelta(seconds=5))
    assert store.stats(now).unknown_count == 1
    assert main(["--db", str(store.path), "acknowledge-unknown", "--delivery-id", job_id,
                 "--reason", "supplier history checked; duplicate impact accepted"]) == 0
    assert store.stats(now).unknown_count == 0
    assert store.get_job(job_id).result_unknown  # Historical fact is preserved.
    assert any(event["result_unknown"] for event in store.events(job_id))
    assert store.prune_sent(now - timedelta(days=30)) == 0  # New maintenance audit must age too.
    assert store.prune_sent(now + timedelta(days=31)) == 1


def test_backup_never_reports_success_when_directory_sync_fails(tmp_path, monkeypatch):
    store = DeliveryStore(tmp_path / "bridge.db")
    accept(store, content())
    def fail_sync(_path):
        raise OSError("directory durability failed")
    monkeypatch.setattr(storage, "_sync_directory", fail_sync, raising=False)
    monkeypatch.setattr(storage, "_publish_backup", lambda a, b: storage._publish_posix(a, b), raising=False)
    with pytest.raises(StorageUnavailable):
        store.backup(tmp_path / "snapshot.db")


def test_v2_migration_backfills_order_and_cycle_dedup(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    accept(store, content("1"))
    mark_sent(store)
    accept(store, content("0"), NOW + timedelta(seconds=60))
    mark_sent(store, NOW + timedelta(seconds=60))
    # This fixture is precisely the previous reviewed schema, with no ordering index.
    with sqlite3.connect(store.path) as db:
        db.execute("drop table if exists delivery_members")
        db.execute("drop table if exists delivery_order")
        db.execute("delete from migration_history where version>2")
        db.execute("update schema_version set version=2")
    reopened = DeliveryStore(store.path)
    assert accept(reopened, content("1"), NOW + timedelta(seconds=120)).new_job_ids


def test_delayed_delivery_shows_original_receipt_time(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    job_id = accept(store, content()).new_job_ids[0]
    messages = []
    class Provider:
        def send_card_message(self, chat_id, card, message_uuid=None):
            messages.append(card)
            return "om_confirmed"
    assert DeliveryAdapter(feishu_app=Provider()).send(store.get_job(job_id)).success
    assert NOW.isoformat() in json.dumps(messages)


def test_new_unknown_after_manual_ack_is_unresolved_again(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    job_id = accept(store, content()).new_job_ids[0]
    claimed = store.claim_due(NOW)
    store.finish(job_id, claimed.lease_token, DeliveryResult(False, result_unknown=True), NOW)
    store.acknowledge_unknown(job_id, "supplier outcome checked", NOW)
    assert store.stats(NOW).unknown_count == 0
    store.requeue(job_id, "new attempt authorized", NOW)
    claimed = store.claim_due(NOW)
    store.finish(job_id, claimed.lease_token, DeliveryResult(False, result_unknown=True), NOW)
    assert store.stats(NOW).unknown_count == 1


def test_failed_backup_publish_prevents_schema_upgrade(tmp_path, monkeypatch):
    store = DeliveryStore(tmp_path / "bridge.db")
    accept(store, content())
    with sqlite3.connect(store.path) as db:
        db.execute("drop table delivery_members")
        db.execute("drop table delivery_order")
        db.execute("delete from migration_history where version>2")
        db.execute("update schema_version set version=2")
    def fail_sync(_path):
        raise OSError("directory durability failed")
    monkeypatch.setattr(storage, "_sync_directory", fail_sync)
    monkeypatch.setattr(storage, "_publish_backup", storage._publish_posix)
    with pytest.raises(StorageUnavailable):
        storage.migrate(store.path, storage.SCHEMA_VERSION)
    with sqlite3.connect(store.path) as db:
        assert db.execute("select version from schema_version").fetchone()[0] == 2


def test_backup_requires_prepared_existing_destination_directory(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    accept(store, content())
    unprepared = tmp_path / "new" / "nested"
    with pytest.raises(ValueError, match="directory"):
        store.backup(unprepared / "snapshot.db")
    assert not unprepared.exists()
