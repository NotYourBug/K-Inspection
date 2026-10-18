from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from kubernetes import client

from k8s_inspector import checks
from k8s_inspector.common import pod_issue
from k8s_inspector.config import InspectorConfig
from k8s_inspector.models import Status
from tests.factories import NOW, container_status, pod


def evaluate(value):
    return checks.evaluate_pod_state(value, InspectorConfig(), NOW)


def reasons(state):
    return {x["reason"] for x in state["current_issues"]}


def test_running_ready_pod_with_old_oom_is_not_current_fail():
    value = pod("p", container_statuses=[container_status(old_oom=True)])
    state = evaluate(value)
    assert state["current_issues"] == []
    assert any(x["reason"] == "OOMKilled" for x in state["historical_issues"])
    assert pod_issue(value.status.container_statuses, "Running", 0) is None


def test_running_unready_pod_after_grace_is_current_issue():
    assert "PodNotReady" in reasons(evaluate(pod("p", ready=False)))


def test_recent_readiness_transition_uses_condition_time_not_pod_age():
    value = pod("p", ready=False, age_seconds=86400)
    value.status.conditions[0].last_transition_time = NOW - timedelta(seconds=20)
    assert "PodNotReady" not in reasons(evaluate(value))


def test_unready_without_transition_time_is_unknown():
    value = pod("p", ready=False)
    value.status.conditions[0].last_transition_time = None
    state = evaluate(value)
    assert state["readiness"] == "UNKNOWN"
    assert "PodNotReady" not in reasons(state)


def test_init_crashloop_is_current_issue():
    value = pod("p", init_container_statuses=[
        container_status("setup", waiting="CrashLoopBackOff", ready=False)])
    assert "CrashLoopBackOff" in reasons(evaluate(value))


def test_init_nonzero_exit_is_current_issue():
    value = pod("p", init_container_statuses=[container_status("setup", exit_code=1)])
    assert "InitContainerFailed" in reasons(evaluate(value))


def test_completed_init_is_not_current_issue():
    value = pod("p", init_container_statuses=[container_status("setup", exit_code=0)])
    assert evaluate(value)["current_issues"] == []


def test_current_nonzero_terminated_container_is_failure():
    value = pod("p", container_statuses=[container_status(exit_code=1, ready=False)])
    assert "ContainerFailed" in reasons(evaluate(value))


def test_recent_pending_is_within_grace():
    state = evaluate(pod("p", phase="Pending", ready=False, age_seconds=30))
    assert state["current_issues"] == []
    assert state["readiness"] != "HEALTHY"


def test_old_unschedulable_pending_has_reason():
    value = pod("p", phase="Pending", ready=False)
    value.status.conditions.append(client.V1PodCondition(
        type="PodScheduled", status="False", reason="Unschedulable", message="no capacity"))
    assert "Unschedulable" in reasons(evaluate(value))


def test_missing_ready_is_unknown():
    state = evaluate(pod("p", conditions=[]))
    assert state["readiness"] == "UNKNOWN"


def test_successful_job_is_not_unready_failure():
    assert evaluate(pod("p", phase="Succeeded", ready=False))["current_issues"] == []


def test_failed_job_is_current_failure():
    assert "PodFailed" in reasons(evaluate(pod("p", phase="Failed", ready=False)))


def test_deleting_pod_is_not_readiness_failure():
    state = evaluate(pod("p", ready=False, deletion_timestamp=NOW))
    assert "PodNotReady" not in reasons(state)
    assert state["readiness"] == "TERMINATING"


def test_bad_pod_does_not_hide_restart_evidence():
    value = pod("p", container_statuses=[
        container_status(waiting="CrashLoopBackOff", restarts=8)])
    core = SimpleNamespace(list_pod_for_all_namespaces=lambda **kw: SimpleNamespace(items=[value]))
    result = checks.check_pods(core, InspectorConfig())
    assert result.status == Status.FAIL
    assert len(result.data["bad"]) == 1
    assert len(result.data["restart_heavy"]) == 1


def test_missing_ready_does_not_report_healthy():
    value = pod("p", conditions=[])
    core = SimpleNamespace(list_pod_for_all_namespaces=lambda **kw: SimpleNamespace(items=[value]))
    assert checks.check_pods(core, InspectorConfig()).status == Status.WARN


def test_pod_within_grace_has_no_notification_warning():
    value = pod("p", ready=False, age_seconds=30)
    value.status.conditions[0].last_transition_time = datetime.now(timezone.utc)
    core = SimpleNamespace(list_pod_for_all_namespaces=lambda **kw: SimpleNamespace(items=[value]))
    assert checks.check_pods(core, InspectorConfig()).status == Status.PASS
