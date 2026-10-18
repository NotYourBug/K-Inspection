import importlib
import sqlite3
import threading
from datetime import timedelta

import pytest

from k8s_inspector.bridge_store import DeliveryStore, StorageUnavailable
from k8s_inspector.delivery_models import DeliveryResult
from tests.delivery_factories import NOW, batch, target


class Clock:
    now = NOW
    def __call__(self): return self.now
    def advance(self, seconds): self.now += timedelta(seconds=seconds)


class Provider:
    def __init__(self, result=DeliveryResult(True)):
        self.result = result
        self.calls = []
        self.accepted_ids = set()
    def send(self, job):
        self.calls.append(job.delivery_id)
        self.accepted_ids.add(job.delivery_id)
        return self.result


def worker(store, provider, clock, **kwargs):
    cls = importlib.import_module("k8s_inspector.delivery_worker").DeliveryWorker
    return cls(store, provider, clock, jitter=lambda: 0, **kwargs)


@pytest.fixture
def store(tmp_path):
    db = DeliveryStore(tmp_path / "bridge.db")
    db.accept("am", batch(), (target(),), NOW)
    return db


def test_old_or_expired_lease_cannot_finish_new_claim(store):
    old = store.claim_due(NOW)
    assert not store.finish(old.delivery_id, old.lease_token, DeliveryResult(True), NOW + timedelta(seconds=60))
    new = store.claim_due(NOW + timedelta(seconds=61))
    assert new.delivery_id == old.delivery_id and new.lease_token != old.lease_token
    assert not store.finish(old.delivery_id, old.lease_token, DeliveryResult(True), NOW)
    assert store.get_job(old.delivery_id).lease_token == new.lease_token


@pytest.mark.parametrize("deduplicating", [True, False])
def test_external_success_before_local_commit_is_recoverable(store, deduplicating):
    clock, provider = Clock(), Provider()
    w = worker(store, provider, clock)
    with sqlite3.connect(store.path) as db:
        db.execute("""create trigger reject_finish before update on outbox when new.state='SENT'
                   begin select raise(ABORT,'disk full'); end""")
    assert w.run_once()
    assert store.list_jobs()[0].state == "SENDING"
    with sqlite3.connect(store.path) as db:
        db.execute("drop trigger reject_finish")
    clock.advance(61)
    assert worker(DeliveryStore(store.path), provider, clock).run_once()
    job = store.list_jobs()[0]
    assert job.state == "SENT" and job.attempt_count == 2 and job.result_unknown
    # UUID-capable provider dedups; webhook sees two sends with the same visible ID.
    accepted_count = len(provider.accepted_ids) if deduplicating else len(provider.calls)
    assert accepted_count == (1 if deduplicating else 2)
    assert len(set(provider.calls)) == 1
    with sqlite3.connect(store.path) as db:
        assert db.execute("select count(*) from delivery_events where result_unknown=1").fetchone()[0] >= 1


def test_partial_destination_failure_does_not_block_other_job(store):
    store.accept("am", batch(), (target("team-b"),), NOW)
    class Partial:
        def send(self, job):
            return DeliveryResult(job.destination_id == "team-b", retryable=True)
    w = worker(store, Partial(), Clock())
    assert w.run_once() and w.run_once()
    assert {j.destination_id: j.state for j in store.list_jobs()} == {"team-a": "RETRY_WAIT", "team-b": "SENT"}
    receipt = store.accept("am", batch(), (target(), target("team-b")), NOW)
    assert len(receipt.reused_job_ids) == 2 and not receipt.new_job_ids


def test_sixth_failure_exhausts_budget_and_keeps_unknown_evidence(store):
    clock, provider = Clock(), Provider(DeliveryResult(False, True, True, error_kind="timeout"))
    w = worker(store, provider, clock)
    for delay in (5, 15, 45, 120, 300, 1):
        assert w.run_once()
        clock.advance(delay)
    job = store.list_jobs()[0]
    assert job.state == "DEAD" and job.attempt_count == 6 and job.result_unknown
    clock.advance(10000)
    assert not w.run_once() and len(provider.calls) == 6


def test_permanent_error_goes_dead_without_retry(store):
    w = worker(store, Provider(DeliveryResult(False, error_code="230002", error_kind="provider_permanent")), Clock())
    assert w.run_once()
    assert store.list_jobs()[0].state == "DEAD"
    assert not w.run_once()


def test_retry_after_cannot_exceed_total_deadline(store):
    clock = Clock()
    w = worker(store, Provider(DeliveryResult(False, True, retry_after_seconds=10000)), clock)
    assert w.run_once()
    assert store.list_jobs()[0].state == "DEAD"


def test_provider_runs_outside_transaction(store):
    class ProviderWithDBWrite:
        def send(self, job):
            store.accept("am", batch(), (target("team-b"),), NOW)
            return DeliveryResult(True)
    worker(store, ProviderWithDBWrite(), Clock()).run_once()
    assert len(store.list_jobs()) == 2 and store.list_jobs(state="SENT")


def test_shutdown_preserves_inflight_and_stops_new_claims(store):
    entered, release = threading.Event(), threading.Event()
    class Blocking:
        def send(self, job):
            entered.set()
            assert release.wait(3)
            return DeliveryResult(True)
    w = worker(store, Blocking(), Clock())
    w.start()
    try:
        assert entered.wait(2)
        w.stop(grace_seconds=0)
        assert store.list_jobs()[0].state == "SENDING"
    finally:
        release.set()
        w.stop(grace_seconds=3)
    assert store.list_jobs()[0].state == "SENT" and w.last_heartbeat is None


def test_lease_expiration_cannot_send_a_seventh_attempt(store):
    for attempt in range(6):
        assert store.claim_due(NOW + timedelta(seconds=61 * attempt))
    assert store.claim_due(NOW + timedelta(seconds=61 * 6)) is None
    assert store.list_jobs()[0].state == "DEAD" and store.list_jobs()[0].result_unknown


def test_retry_deadline_survives_restart_but_does_not_expire_queued_first_attempt(store):
    clock, provider = Clock(), Provider(DeliveryResult(False, True))
    clock.advance(2000)  # Backlog waiting is not part of a not-yet-started retry budget.
    assert worker(store, provider, clock).run_once()
    assert len(provider.calls) == 1
    clock.advance(901)
    assert worker(DeliveryStore(store.path), provider, clock).run_once()
    assert len(provider.calls) == 1
    assert store.list_jobs()[0].state == "DEAD"
    assert store.list_jobs()[0].error_kind == "retry_deadline"
