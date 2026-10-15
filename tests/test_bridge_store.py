import importlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from tests.delivery_factories import NOW, batch, target


def api():
    return importlib.import_module("k8s_inspector.bridge_store")


def test_pending_jobs_survive_reopen(tmp_path):
    path = tmp_path / "bridge.db"
    receipt = api().DeliveryStore(path).accept("prod-am", batch(), (target(), target("team-b")), NOW)
    store = api().DeliveryStore(path)
    assert {j.delivery_id for j in store.list_jobs()} == set(receipt.new_job_ids)
    job = store.get_job(receipt.new_job_ids[0])
    assert (job.state, job.attempt_count, job.next_attempt_at) == ("PENDING", 0, NOW)
    with sqlite3.connect(path) as db:
        assert db.execute("select count(*) from inbox_jobs").fetchone()[0] == 2


def test_all_destinations_are_atomic(tmp_path):
    store = api().DeliveryStore(tmp_path / "bridge.db")
    with sqlite3.connect(store.path) as db:
        db.execute("""create trigger reject_b before insert on outbox
                      when new.destination_id='team-b' begin select raise(ABORT,'disk full'); end""")
    with pytest.raises(api().StorageUnavailable):
        store.accept("prod-am", batch(), (target(), target("team-b")), NOW)
    assert store.list_jobs() == ()
    with sqlite3.connect(store.path) as db:
        assert db.execute("select count(*) from inbox").fetchone()[0] == 0


def test_concurrent_accepts_reuse_one_set_of_jobs(tmp_path):
    store = api().DeliveryStore(tmp_path / "bridge.db")
    with ThreadPoolExecutor(2) as pool:
        receipts = list(pool.map(lambda _: store.accept("prod-am", batch(), (target(),), NOW), range(2)))
    assert sum(len(r.new_job_ids) for r in receipts) == 1
    assert sum(len(r.reused_job_ids) for r in receipts) == 1
    with sqlite3.connect(store.path) as db:
        assert db.execute("select count(*) from inbox_jobs").fetchone()[0] == 2


def test_backlog_is_transactional_and_duplicates_do_not_inflate_it(tmp_path):
    store = api().DeliveryStore(tmp_path / "bridge.db", max_backlog=1)
    first = store.accept("prod-am", batch(), (target(),), NOW)
    second = store.accept("prod-am", batch(), (target(),), NOW + timedelta(hours=1))
    assert second.reused_job_ids == first.new_job_ids
    with pytest.raises(api().BacklogFull):
        store.accept("prod-am", batch(), (target("team-b"),), NOW)
    assert len(store.list_jobs()) == 1


def test_rejects_naive_time_and_newer_schema(tmp_path):
    store = api().DeliveryStore(tmp_path / "bridge.db")
    with pytest.raises(ValueError, match="aware"):
        store.accept("prod-am", batch(), (target(),), NOW.replace(tzinfo=None))
    with sqlite3.connect(store.path) as db:
        db.execute("update schema_version set version=999")
    with pytest.raises(api().UnsupportedSchema):
        api().DeliveryStore(store.path)
