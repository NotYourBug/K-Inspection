import sqlite3
from pathlib import Path

import pytest

from k8s_inspector import bridge_store as storage
from k8s_inspector.bridge_store import DeliveryStore, StorageUnavailable, UnsupportedSchema
from k8s_inspector.delivery_models import DeliveryResult
from tests.delivery_factories import NOW, batch, target


@pytest.fixture
def store(tmp_path):
    db = DeliveryStore(tmp_path / "bridge.db")
    db.accept("am", batch(), (target(),), NOW)
    return db


def test_backup_restore_preserves_pending_and_schema_including_wal(store, tmp_path):
    # Keep an independent live reader so committed data can still reside in WAL.
    reader = sqlite3.connect(store.path)
    try:
        reader.execute("BEGIN")
        reader.execute("select * from outbox").fetchall()
        store.accept("am", batch(), (target("b"),), NOW)
        backup = store.backup(tmp_path / "backup.db")
        assert len(DeliveryStore(backup).list_jobs(state="PENDING")) == 2
        with sqlite3.connect(backup) as db:
            assert db.execute("pragma integrity_check").fetchone()[0] == "ok"
            assert db.execute("select version from schema_version").fetchone()[0] == storage.SCHEMA_VERSION
    finally:
        reader.close()


def test_backup_never_overwrites_existing_destination_or_source(store, tmp_path):
    destination = tmp_path / "existing.db"
    destination.write_bytes(b"existing-backup")
    with pytest.raises((ValueError, StorageUnavailable)):
        store.backup(destination)
    assert destination.read_bytes() == b"existing-backup"
    with pytest.raises((ValueError, StorageUnavailable)):
        store.backup(store.path)
    assert len(store.list_jobs()) == 1


def downgrade_fixture_to_v1(store):
    with sqlite3.connect(store.path) as db:
        db.execute("drop table if exists migration_history")
        db.execute("update schema_version set version=1")


def test_migration_backups_before_transactional_upgrade(store):
    downgrade_fixture_to_v1(store)
    storage.migrate(store.path, 2)
    restored = DeliveryStore(store.path)
    assert len(restored.list_jobs()) == 1
    backups = list(store.path.parent.glob("bridge.db.pre-v2-*.bak"))
    assert len(backups) == 1
    with sqlite3.connect(backups[0]) as db:
        assert db.execute("select version from schema_version").fetchone()[0] == 1
    with sqlite3.connect(store.path) as db:
        assert db.execute("select version from migration_history").fetchone()[0] == 2


def test_interrupted_migration_rolls_back_and_can_retry(store, monkeypatch):
    downgrade_fixture_to_v1(store)
    def interrupted(db, backup):
        db.execute("create table incomplete (value TEXT)")
        raise sqlite3.OperationalError("interrupted")
    with monkeypatch.context() as scoped:
        scoped.setitem(storage.MIGRATIONS, 2, interrupted)
        with pytest.raises(StorageUnavailable):
            storage.migrate(store.path, 2)
    with sqlite3.connect(store.path) as db:
        assert db.execute("select version from schema_version").fetchone()[0] == 1
        assert not db.execute("select name from sqlite_master where name='incomplete'").fetchall()
    storage.migrate(store.path, 2)
    assert len(DeliveryStore(store.path).list_jobs()) == 1


def test_newer_or_incomplete_schema_fails_closed(store):
    with sqlite3.connect(store.path) as db:
        db.execute("update schema_version set version=999")
    with pytest.raises(UnsupportedSchema):
        DeliveryStore(store.path)
    with pytest.raises(UnsupportedSchema):
        storage.migrate(store.path, 2)
    with sqlite3.connect(store.path) as db:
        db.execute("update schema_version set version=?", (storage.SCHEMA_VERSION,))
        db.execute("drop table inbox_jobs")
    with pytest.raises(UnsupportedSchema):
        DeliveryStore(store.path)


def test_restoring_old_backup_can_replay_a_confirmed_external_send(store, tmp_path):
    backup = store.backup(tmp_path / "pending-backup.db")
    sent = store.claim_due(NOW)
    store.finish(sent.delivery_id, sent.lease_token, DeliveryResult(True), NOW)
    assert store.list_jobs()[0].state == "SENT"
    replay = DeliveryStore(backup).claim_due(NOW)
    assert replay.delivery_id == sent.delivery_id  # Same UUID, still at-least-once boundary.


def test_local_admin_backup_uses_consistent_snapshot(store, tmp_path, capsys):
    from k8s_inspector.bridge_admin import main
    backup = tmp_path / "admin-backup.db"
    assert main(["--db", str(store.path), "backup", "--destination", str(backup)]) == 0
    assert len(DeliveryStore(backup).list_jobs()) == 1
