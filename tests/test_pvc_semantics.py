from types import SimpleNamespace
from unittest.mock import MagicMock

from kubernetes import client

from k8s_inspector.checks import check_pending_pvcs
from k8s_inspector.config import InspectorConfig
from k8s_inspector.models import Status
from tests.factories import NOW, pod, pvc, storage_class


def apis(claims, pods=(), classes=()):
    core, storage = MagicMock(), MagicMock()
    core.list_persistent_volume_claim_for_all_namespaces.return_value = SimpleNamespace(items=claims)
    core.list_pod_for_all_namespaces.return_value = SimpleNamespace(items=list(pods))
    storage.list_storage_class.return_value = SimpleNamespace(items=list(classes))
    return core, storage


def inspect(core, storage=None):
    return check_pending_pvcs(core, InspectorConfig(), storage, now=NOW)


def test_wait_for_first_consumer_without_consumer_is_not_fail():
    core, storage = apis([pvc("data", phase="Pending", storage_class="delayed")],
                         classes=[storage_class("delayed", "WaitForFirstConsumer")])
    result = inspect(core, storage)
    assert result.status == Status.PASS
    assert result.data["waiting_for_consumer"] == 1
    assert result.data["pending"] == []


def test_lost_pvc_is_fail():
    core, _ = apis([pvc("data", phase="Lost")])
    result = inspect(core)
    assert result.status == Status.FAIL
    assert result.data["lost_count"] == 1


def test_old_pending_with_failed_consumer_has_evidence():
    volume = client.V1Volume(name="data", persistent_volume_claim=
                             client.V1PersistentVolumeClaimVolumeSource(claim_name="data"))
    consumer = pod("app", phase="Pending", ready=False, volumes=[volume])
    core, storage = apis([pvc("data", phase="Pending", storage_class="delayed")],
                         [consumer], [storage_class("delayed", "WaitForFirstConsumer")])
    result = inspect(core, storage)
    assert result.status == Status.FAIL
    assert result.data["pending"][0]["consumers"] == ["app"]
    assert result.data["pending"][0]["binding_mode"] == "WaitForFirstConsumer"


def test_storageclass_forbidden_is_unknown():
    core, storage = apis([pvc("data", phase="Pending", storage_class="delayed")])
    storage.list_storage_class.side_effect = PermissionError("forbidden")
    result = inspect(core, storage)
    assert result.status == Status.WARN
    assert result.data["collection_errors"]
    assert result.data["pending"] == []


def test_recent_pending_is_observed_without_failure():
    core, storage = apis([pvc("data", phase="Pending", age_seconds=10)])
    result = inspect(core, storage)
    assert result.status == Status.PASS
    assert result.data["within_grace"] == 1


def test_missing_association_data_is_not_healthy():
    core, _ = apis([pvc("data", phase="Pending")])
    assert inspect(core).status == Status.WARN


def test_pvc_association_lists_are_reused():
    core, storage = apis([pvc(f"p{x}", phase="Pending", storage_class="immediate")
                         for x in range(10)], classes=[storage_class("immediate")])
    assert inspect(core, storage).status == Status.FAIL
    assert core.list_pod_for_all_namespaces.call_count == 1
    assert storage.list_storage_class.call_count == 1


def test_lost_survives_pending_association_failure():
    core, storage = apis([pvc("lost", phase="Lost"),
                         pvc("pending", phase="Pending", storage_class="broken")])
    storage.list_storage_class.side_effect = PermissionError("forbidden")
    result = inspect(core, storage)
    assert result.status == Status.FAIL
    assert result.data["lost_count"] == 1
    assert result.data["collection_errors"]
