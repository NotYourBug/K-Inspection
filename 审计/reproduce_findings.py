"""Offline reproductions for the 2026-10-02 review; no cluster or Feishu calls.

Run in an environment containing the repository's declared dependencies:
    python -B 审计/reproduce_findings.py

These probes describe existing defects, not desired behavior. After remediation,
convert the relevant scenarios into regression tests with corrected expectations.
MCP resource helpers are loaded from their AST to isolate them from FastMCP.
"""
from __future__ import annotations

import ast
import importlib.metadata
import importlib.util
import json
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kubernetes import client
from k8s_inspector import alertmanager_bridge as bridge
from k8s_inspector import checks, cli
from k8s_inspector.common import cpu_to_millicores, memory_to_bytes, format_bytes
from k8s_inspector.config import InspectorConfig, load_config
from k8s_inspector.models import CheckResult, Report, Status
from k8s_inspector.notify import send_feishu_text
from k8s_inspector.state_store import ChatRecord, JsonStateStore

observations = []


def record(identifier, observed, expected):
    observations.append({"id": identifier, "observed": observed, "expected": expected})


def pod(name="p", phase="Running", ready=True, restarts=0, last_oom=False):
    cs = client.V1ContainerStatus(
        name="app", image="example", image_id="example-id", ready=ready,
        restart_count=restarts,
        state=client.V1ContainerState(running=client.V1ContainerStateRunning()),
        last_state=client.V1ContainerState(
            terminated=client.V1ContainerStateTerminated(exit_code=137, reason="OOMKilled")
        ) if last_oom else client.V1ContainerState(),
    )
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace="test", uid="uid-" + name),
        spec=client.V1PodSpec(containers=[client.V1Container(name="app", image="example")]),
        status=client.V1PodStatus(phase=phase, container_statuses=[cs]),
    )


def pod_result(pods):
    core = MagicMock()
    core.list_pod_for_all_namespaces.return_value.items = pods
    return checks.check_pods(core, InspectorConfig())


def node(ready=True, pressure=False):
    return client.V1Node(
        metadata=client.V1ObjectMeta(name="n1"), spec=client.V1NodeSpec(taints=[]),
        status=client.V1NodeStatus(
            conditions=[client.V1NodeCondition(type="Ready", status="True" if ready else "False"),
                        client.V1NodeCondition(type="DiskPressure", status="True" if pressure else "False")],
            capacity={"cpu": "4", "memory": "8Gi"},
        ),
    )


def node_result(nodes, metrics, include_metrics=True):
    core = MagicMock()
    core.list_node.return_value.items = nodes
    with patch.object(checks, "get_node_metrics", return_value=metrics):
        return checks.check_nodes(core, MagicMock(), InspectorConfig(), include_metrics)


cfg = InspectorConfig()
p = pod(phase="Pending")
p.status.container_statuses = []
record("R01_pending_pod", pod_result([p]).status.value, "age-aware pending finding")
record("R02_running_unready", pod_result([pod(ready=False)]).status.value, "readiness finding after grace period")
p = pod(phase="Pending")
p.status.container_statuses = []
p.status.init_container_statuses = [client.V1ContainerStatus(
    name="init", image="example", image_id="example-id", ready=False, restart_count=1,
    state=client.V1ContainerState(waiting=client.V1ContainerStateWaiting(reason="CrashLoopBackOff")),
)]
record("R03_init_crash", pod_result([p]).status.value, "FAIL with init container evidence")
record("R04_historical_oom", pod_result([pod(last_oom=True)]).status.value, "historical evidence separated from current health")

result = pod_result([pod(name="broken", phase="Failed"), pod(name="restarts", restarts=5)])
record("R05_lost_restart_finding", sorted(result.data), "bad and restart_heavy both preserved")
record("R06_empty_metrics", node_result([node()], {}).status.value, "explicit missing metrics coverage")
record("R07_disk_pressure", node_result([node(pressure=True)], {}, False).status.value, "explicit disk pressure finding")
record("R08_notready_then_invalid_metric", node_result(
    [node(ready=False)], {"n1": {"cpu": "malformed", "memory": "1Gi"}}
).status.value, "retain FAIL for NotReady despite metric parse error")

core = MagicMock()
core.list_namespaced_pod.side_effect = RuntimeError("403 Forbidden")
with patch.object(checks, "apiserver_healthz", return_value={"ok": False, "error": "timeout"}):
    record("R09_health_fail_then_list_error", checks.check_control_plane(core, MagicMock(), cfg).status.value,
           "retain API health failure and separately record collection error")

core = MagicMock()
core.list_persistent_volume_claim_for_all_namespaces.return_value.items = [client.V1PersistentVolumeClaim(
    metadata=client.V1ObjectMeta(name="lost", namespace="test"),
    status=client.V1PersistentVolumeClaimStatus(phase="Lost"),
)]
result = checks.check_pending_pvcs(core)
record("R10_lost_pvc", {"status": result.status.value, "summary": result.summary}, "FAIL for Lost PVC")

resp = MagicMock()
resp.status = 200
resp.read.return_value = b'{"code":123,"msg":"business rejection"}'
with patch("k8s_inspector.notify.urllib.request.urlopen") as opener:
    opener.return_value.__enter__.return_value = resp
    record("R11_feishu_business_error", send_feishu_text("https://example.invalid/hook", "probe"),
           "delivery error surfaced; all HTTP calls mocked")

with tempfile.TemporaryDirectory(prefix="inspector-review-") as temp:
    temp = Path(temp)
    config_path = temp / "invalid-config.json"
    config_path.write_text(json.dumps({"thresholds": {"node_cpu_utilization": 1.5, "pod_restart_count": -3}}), encoding="utf-8")
    invalid = load_config(str(config_path))
    record("R12_invalid_thresholds", {"cpu": invalid.thresholds.node_cpu_utilization,
                                     "restarts": invalid.thresholds.pod_restart_count}, "reject invalid ranges")

    args = cli.build_parser().parse_args(["run", "--profile", "5m", "--persist-level", "issue",
                                          "--output-dir", str(temp / "latest"), "--cluster", "test"])
    fake_kube = NS(core=MagicMock(), storage=MagicMock(), custom=MagicMock(), api_client=MagicMock(), display_name="test")
    first = datetime(2026, 10, 2, tzinfo=timezone.utc)
    with patch.dict(os.environ, {}, clear=True), patch.object(cli, "load_kube", return_value=fake_kube), \
         patch.object(cli, "run_all", side_effect=[[CheckResult("pods", Status.FAIL, "old fault")],
                                                   [CheckResult("pods", Status.PASS, "recovered")]]), \
         patch.object(cli, "_now_dt", side_effect=[first, first + timedelta(minutes=5)]):
        cli.cmd_run(args)
        cli.cmd_run(args)
    stale = json.loads((temp / "latest" / "report.json").read_text(encoding="utf-8"))
    record("R13_stale_latest_after_recovery", stale["results"][0]["status"], "latest reflects PASS after recovery")

    state = JsonStateStore(str(temp / "state.json"))
    state.upsert_chat("key", ChatRecord("oc_mock", "Test", False))
    bridge_cfg = bridge.BridgeConfig("app", None, "Test", "[resolved]", ["ou_mock"], str(temp / "state.json"), str(temp))
    feishu = MagicMock()
    feishu.send_card_message.side_effect = RuntimeError("mock delivery failure")
    payload = {"groupKey": "key", "status": "resolved", "alerts": [{"status": "resolved", "labels": {}}]}
    try:
        bridge._handle_alert_payload_app_mode(payload, bridge_cfg, state, feishu)
    except RuntimeError:
        pass
    feishu.send_card_message.side_effect = None
    bridge._handle_alert_payload_app_mode(payload, bridge_cfg, state, feishu)
    record("R14_resolved_retry_skipped", {"send_attempts": feishu.send_card_message.call_count,
                                         "marked_resolved": state.get_chat("key").resolved},
           "retry failed recovery delivery; durable pending/sent state")

    reports = temp / "reports"
    other = temp / "outside-reports"
    other.mkdir()
    (other / "report.json").write_text('{"cluster":"outside-report-root"}', encoding="utf-8")
    loaded, selected = bridge._try_load_latest_report_json(str(reports), str(other))
    record("R15_profile_path_escape", loaded, "reject paths and allow only known profile names")

    concurrent = JsonStateStore(str(temp / "concurrent.json"))
    original_load = concurrent._load
    original_save = concurrent._save
    barrier = threading.Barrier(2)
    serial_save = threading.Lock()

    def synchronized_load():
        snapshot = original_load()
        barrier.wait(timeout=5)
        return snapshot

    def serialized_save(data):
        with serial_save:
            original_save(data)

    with patch.object(concurrent, "_load", side_effect=synchronized_load), \
         patch.object(concurrent, "_save", side_effect=serialized_save), ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(concurrent.upsert_chat, key, ChatRecord(key, key, False)) for key in ["a", "b"]]
        for future in futures:
            future.result(timeout=10)
    record("R16_state_lost_update", {"retained_keys": len(original_load())},
           "both keys retained; read-modify-write must be one transaction")

with patch.dict(os.environ, {}, clear=True), patch.object(bridge, "run_server") as server:
    bridge.main(["--mode", "webhook", "--feishu-webhook", "https://example.invalid/hook", "--listen-host", "127.0.0.1"])
    record("R17_ignored_listen_host", server.call_args.args[0].listen_host, "127.0.0.1")

# Isolate the actual MCP resource helper definitions; no modified algorithm.
source = (ROOT / "k8s_inspector" / "mcp_server.py").read_text(encoding="utf-8")
tree = ast.parse(source)
names = {"MCPPodResource", "_sum_container_resources", "_pod_effective_resources"}
body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)]
body += [item for item in tree.body if isinstance(item, (ast.FunctionDef, ast.ClassDef)) and item.name in names]
from dataclasses import dataclass
namespace = {"dataclass": dataclass, "cpu_to_millicores": cpu_to_millicores, "memory_to_bytes": memory_to_bytes,
             "__name__": __name__}
exec(compile(ast.fix_missing_locations(ast.Module(body=body, type_ignores=[])), "isolated_mcp_helpers", "exec"), namespace)
resource_pod = pod()
resource_pod.spec.containers[0].resources = client.V1ResourceRequirements(requests={"cpu": "100m"})
resource_pod.spec.init_containers = [client.V1Container(name=name, resources=client.V1ResourceRequirements(
    requests={"cpu": "800m"})) for name in ["init1", "init2"]]
record("R18_init_resources_sum", namespace["_pod_effective_resources"](resource_pod).cpu_request_m,
       "800m for two regular sequential init containers; observed unit is millicores")
record("R19_fastmcp_missing", importlib.util.find_spec("fastmcp") is None,
       "MCP extra declares fastmcp and compatible pydantic/Python versions")
record("R20_format_bytes_boundary", format_bytes(1024 ** 6), "1.0Ei")

packages = {name: importlib.metadata.version(name) for name in
            ["kubernetes", "prometheus-client", "PyYAML", "lark-oapi", "pytest"]}
output = {"review_date": "2026-10-02", "python": sys.version, "packages": packages,
          "scope": "offline mocks and isolated MCP helper AST; no live integrations", "observations": observations}
result_path = Path(__file__).with_name("复现结果.json")
result_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(output, ensure_ascii=True, indent=2))
