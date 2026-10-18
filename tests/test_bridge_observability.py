import importlib
import json
import sqlite3
from datetime import timedelta

import pytest
from prometheus_client.parser import text_string_to_metric_families

from k8s_inspector.notifications.delivery.store import DeliveryStore
from k8s_inspector.notifications.delivery.models import DeliveryResult
from tests.delivery_factories import NOW, batch, target
from tests.test_bridge_ingress import serve, post, PAYLOAD, TOKEN


def obs():
    return importlib.import_module("k8s_inspector.notifications.bridge.observability")


def get(server, path):
    import http.client
    conn = http.client.HTTPConnection(*server.server_address, timeout=3)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        return response.status, response.read().decode()
    finally:
        conn.close()


@pytest.mark.parametrize("heartbeat,expected", [(None, False), (-31, False), (-30, True), (0, True), (1, False)])
def test_stopped_or_stale_worker_is_not_ready(tmp_path, heartbeat, expected):
    store = DeliveryStore(tmp_path / "bridge.db")
    hb = NOW + timedelta(seconds=heartbeat) if heartbeat is not None else None
    assert obs().bridge_ready(store, hb, NOW) == expected


def test_ready_requires_writable_database_and_capacity(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db", max_backlog=1)
    with sqlite3.connect(store.path) as db:
        db.execute("""create trigger readonly_probe before insert on store_health
                   begin select raise(ABORT,'secret-token'); end""")
    assert not obs().bridge_ready(store, NOW, NOW)
    with sqlite3.connect(store.path) as db:
        db.execute("drop trigger readonly_probe")
    assert obs().bridge_ready(store, NOW, NOW)
    store.accept("am", batch(), (target(),), NOW)
    assert not obs().bridge_ready(store, NOW, NOW)


def test_prune_retains_unhandled_dead_pending_and_sent_unknown(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    old = NOW - timedelta(days=31)
    ids = {}
    for state in ("DEAD", "SENT_UNKNOWN", "SENT", "PENDING"):
        delivery_id = store.accept("am", batch(), (target(state),), old).new_job_ids[0]
        ids[state] = delivery_id
        if state != "PENDING":
            job = store.claim_due(old)
            assert job.delivery_id == delivery_id
            store.finish(delivery_id, job.lease_token,
                DeliveryResult(state.startswith("SENT"), result_unknown=state == "SENT_UNKNOWN"), old)
    assert store.prune_sent(NOW - timedelta(days=30)) == 1
    assert {j.delivery_id for j in store.list_jobs()} == {ids[s] for s in ("DEAD", "PENDING", "SENT_UNKNOWN")}
    with sqlite3.connect(store.path) as db:
        assert db.execute("select count(*) from delivery_events where result_unknown=1").fetchone()[0] == 1
        assert db.execute("select count(*) from inbox").fetchone()[0] == 3


def test_stats_track_backlog_age_dead_unknown_and_last_commit(tmp_path):
    store = DeliveryStore(tmp_path / "bridge.db")
    store.accept("am", batch(), (target(),), NOW)
    job = store.claim_due(NOW)
    store.finish(job.delivery_id, job.lease_token, DeliveryResult(False, result_unknown=True), NOW)
    store.accept("am", batch(), (target("b"),), NOW)
    stats = store.stats(NOW + timedelta(seconds=10))
    assert stats.state_counts["DEAD"] == 1 and stats.state_counts["PENDING"] == 1
    assert stats.oldest_pending_age == 10 and stats.unknown_count == 1 and stats.dead_count == 1
    assert stats.last_commit == NOW


def test_requeue_requires_reason_and_retains_old_attempt_audit(tmp_path, capsys):
    admin = importlib.import_module("k8s_inspector.notifications.bridge.admin")
    store = DeliveryStore(tmp_path / "bridge.db")
    store.accept("am", batch(), (target(),), NOW)
    job = store.claim_due(NOW)
    store.finish(job.delivery_id, job.lease_token, DeliveryResult(False, error_code="230002"), NOW)
    with pytest.raises(ValueError):
        store.requeue(job.delivery_id, " ", NOW)
    assert admin.main(["--db", str(store.path), "requeue", "--delivery-id", job.delivery_id,
                       "--reason", "membership repaired"]) == 0
    updated = store.get_job(job.delivery_id)
    assert updated.state == "PENDING" and updated.attempt_count == 0 and updated.error_code == "230002"
    with sqlite3.connect(store.path) as db:
        rows = db.execute("select event_type,attempt_no,reason from delivery_events order by event_id").fetchall()
        assert rows[-1] == ("manual_requeue", 1, "membership repaired")
        assert any(row[0] == "dead" for row in rows)
    assert admin.main(["--db", str(store.path), "inspect", "--delivery-id", job.delivery_id]) == 0
    assert "230002" in capsys.readouterr().out


def test_probe_and_metrics_distinguish_liveness_readiness_and_redact(tmp_path):
    with serve(tmp_path) as (server, _, _):
        assert get(server, "/healthz")[0] == 200
        assert get(server, "/readyz")[0] == 503
        post(server)
        status, text = get(server, "/metrics")
        assert status == 200
        assert all(secret not in text for secret in (TOKEN, "oc_platform", "oc_storage", "storage-only", "f1"))
        samples = [sample for family in text_string_to_metric_families(text) for sample in family.samples]
        assert next(x.value for x in samples if x.name == "inspector_bridge_jobs" and x.labels == {"state": "PENDING"}) == 2
        assert all(set(x.labels) <= {"state"} for x in samples)
        with sqlite3.connect(server.delivery_store.path) as db:
            db.execute("""create trigger fail_probe before insert on store_health
                       begin select raise(ABORT,'secret-token'); end""")
        assert get(server, "/healthz")[0] == 200 and get(server, "/readyz")[0] == 503
        assert "inspector_bridge_database_writable 0.0" in get(server, "/metrics")[1]
