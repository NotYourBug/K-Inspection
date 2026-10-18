"""
FastMCP-based K8s inspection MCP Server.

Exposes K8s troubleshooting tools to LLMs: node status, resource auditing,
Pod diagnosis, and cluster-wide problem scanning.

This module is intentionally self-contained so it can be deployed independently
without depending on the rest of the k8s_inspector package.

Run:
  python -m k8s_inspector.mcp_server
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastmcp import FastMCP
from kubernetes import client, config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException
from pydantic import Field

from .common import (
    BAD_POD_REASONS,
    cpu_to_millicores,
    deep_get,
    format_bytes,
    format_cpu_m,
    memory_to_bytes,
    parse_quantity,
    pod_issue,
    ratio,
)

logger = logging.getLogger(__name__)

mcp = FastMCP("K8s Check MCP")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class MCPPodResource:
    """Effective resource profile for a Pod (scheduling perspective)."""
    cpu_request_m: int = 0
    cpu_limit_m: int = 0
    memory_request_bytes: int = 0
    memory_limit_bytes: int = 0


@dataclass
class MCPKubeClients:
    """K8s API client collection for the MCP server."""
    display_name: str
    api_client: ApiClient
    core: client.CoreV1Api
    custom: client.CustomObjectsApi


@dataclass
class Thresholds:
    node_cpu_utilization: float = 0.80
    node_memory_utilization: float = 0.85
    pod_restart_count: int = 3
    requests_overcommit_ratio: float = 1.0
    limits_overcommit_ratio: float = 1.5


@dataclass
class InspectorConfig:
    thresholds: Thresholds = field(default_factory=Thresholds)


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


# ---------------------------------------------------------------------------
# API rate limiter (token bucket, thread-safe)
# ---------------------------------------------------------------------------

class _TokenBucketLimiter:
    """Limits K8s API call rate with three layers: RPS, burst, and max in-flight."""

    def __init__(self, rps: float, burst: int, max_in_flight: int) -> None:
        self._rps = float(rps)
        self._capacity = max(int(burst), 1)
        self._tokens = float(self._capacity)
        self._updated_at = time.monotonic()
        self._lock = threading.Lock()
        self._sem = threading.Semaphore(max(int(max_in_flight), 1))

    def acquire(self) -> None:
        self._sem.acquire()
        try:
            if self._rps <= 0:
                return
            while True:
                wait_s = 0.0
                with self._lock:
                    now = time.monotonic()
                    elapsed = max(now - self._updated_at, 0.0)
                    if elapsed > 0:
                        self._tokens = min(self._capacity, self._tokens + elapsed * self._rps)
                        self._updated_at = now
                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return
                    wait_s = (1.0 - self._tokens) / self._rps
                time.sleep(max(wait_s, 0.001))
        except Exception:
            self._sem.release()
            raise

    def release(self) -> None:
        self._sem.release()


_API_LIMITER: _TokenBucketLimiter | None = None


def _get_api_limiter() -> _TokenBucketLimiter:
    global _API_LIMITER
    if _API_LIMITER is not None:
        return _API_LIMITER
    rps = _env_float("MCP_K8S_API_RPS", 5.0)
    burst = _env_int("MCP_K8S_API_BURST", 10)
    max_in_flight = _env_int("MCP_K8S_API_MAX_IN_FLIGHT", 3)
    _API_LIMITER = _TokenBucketLimiter(rps=rps, burst=burst, max_in_flight=max_in_flight)
    return _API_LIMITER


def _k8s_api_call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    limiter = _get_api_limiter()
    limiter.acquire()
    try:
        return fn(*args, **kwargs)
    finally:
        limiter.release()


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_config_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    raw = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError("PyYAML is required to read YAML config") from e
        data = yaml.safe_load(raw) or {}
        return data if isinstance(data, dict) else {}
    data = json.loads(raw) if raw.strip() else {}
    return data if isinstance(data, dict) else {}


def _get_runtime_config() -> InspectorConfig:
    """Load MCP server config. Priority: MCP_CONFIG file > env vars > defaults."""
    cfg = InspectorConfig()
    data = _load_config_file((os.getenv("MCP_CONFIG") or "").strip() or None)

    t = cfg.thresholds
    t.node_cpu_utilization = float(
        deep_get(data, ["thresholds", "node_cpu_utilization"], t.node_cpu_utilization)
    )
    t.node_memory_utilization = float(
        deep_get(data, ["thresholds", "node_memory_utilization"], t.node_memory_utilization)
    )
    t.pod_restart_count = int(deep_get(data, ["thresholds", "pod_restart_count"], t.pod_restart_count))
    t.requests_overcommit_ratio = float(
        deep_get(data, ["thresholds", "requests_overcommit_ratio"], t.requests_overcommit_ratio)
    )
    t.limits_overcommit_ratio = float(
        deep_get(data, ["thresholds", "limits_overcommit_ratio"], t.limits_overcommit_ratio)
    )

    t.node_cpu_utilization = _env_float("MCP_THRESHOLD_NODE_CPU_UTILIZATION", t.node_cpu_utilization)
    t.node_memory_utilization = _env_float("MCP_THRESHOLD_NODE_MEMORY_UTILIZATION", t.node_memory_utilization)
    t.pod_restart_count = _env_int("MCP_THRESHOLD_POD_RESTART_COUNT", t.pod_restart_count)
    t.requests_overcommit_ratio = _env_float("MCP_THRESHOLD_REQUESTS_OVERCOMMIT_RATIO", t.requests_overcommit_ratio)
    t.limits_overcommit_ratio = _env_float("MCP_THRESHOLD_LIMITS_OVERCOMMIT_RATIO", t.limits_overcommit_ratio)

    return cfg


# ---------------------------------------------------------------------------
# K8s client init
# ---------------------------------------------------------------------------


def init_k8s_client() -> MCPKubeClients:
    """Initialize K8s client. Supports both kubeconfig and in-cluster modes.

    Env vars:
      MCP_K8S_IN_CLUSTER=true/false
      MCP_K8S_KUBECONFIG=/path/to/config
      MCP_K8S_CONTEXT=xxx
      MCP_K8S_VERIFY_SSL=true/false
    """
    in_cluster = _env_bool("MCP_K8S_IN_CLUSTER", False)
    kubeconfig = (os.getenv("MCP_K8S_KUBECONFIG") or "").strip() or None
    context = (os.getenv("MCP_K8S_CONTEXT") or "").strip() or None
    verify_ssl = _env_bool("MCP_K8S_VERIFY_SSL", False)

    if in_cluster:
        config.load_incluster_config()
        display_name = "in-cluster"
    else:
        config.load_kube_config(config_file=kubeconfig, context=context)
        display_name = context or "kubeconfig"

    cfg = client.Configuration.get_default_copy()
    cfg.verify_ssl = verify_ssl
    api_client = client.ApiClient(cfg)
    return MCPKubeClients(
        display_name=display_name,
        api_client=api_client,
        core=client.CoreV1Api(api_client),
        custom=client.CustomObjectsApi(api_client),
    )


# ---------------------------------------------------------------------------
# Time / name helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _format_age(creation_timestamp: datetime | None) -> str:
    if creation_timestamp is None:
        return ""
    delta = _now_utc() - creation_timestamp.astimezone(timezone.utc)
    total_seconds = int(delta.total_seconds())
    if total_seconds < 3600:
        return f"{max(total_seconds // 60, 0)}m"
    if total_seconds < 86400:
        return f"{total_seconds // 3600}h"
    return f"{total_seconds // 86400}d"


def _safe_name(obj: Any) -> str:
    metadata = getattr(obj, "metadata", None)
    return getattr(metadata, "name", "") or ""


# ---------------------------------------------------------------------------
# Node helpers
# ---------------------------------------------------------------------------


def _get_node_addresses(node: client.V1Node) -> dict[str, str]:
    addresses: dict[str, str] = {}
    for addr in node.status.addresses or []:
        addr_type = addr.type or "Unknown"
        addr_value = addr.address or ""
        if addr_value:
            addresses[addr_type] = addr_value
    return addresses


def _get_conditions(node: client.V1Node) -> dict[str, str]:
    conditions: dict[str, str] = {}
    for cond in node.status.conditions or []:
        conditions[cond.type or "Unknown"] = cond.status or "Unknown"
    return conditions


def _taints_to_list(node: client.V1Node) -> list[str]:
    out: list[str] = []
    for taint in node.spec.taints or []:
        key = taint.key or ""
        effect = taint.effect or ""
        value = taint.value or ""
        if value:
            out.append(f"{key}={value}:{effect}")
        else:
            out.append(f"{key}:{effect}")
    return out


def _parse_node(node: client.V1Node) -> dict[str, Any]:
    """Flatten a V1Node into an LLM-friendly dict."""
    conditions = _get_conditions(node)
    addresses = _get_node_addresses(node)
    info = node.status.node_info if node.status else None
    allocatable = node.status.allocatable if node.status else {}
    capacity = node.status.capacity if node.status else {}
    return {
        "name": _safe_name(node),
        "status": "Ready" if conditions.get("Ready") == "True" else "NotReady",
        "age": _format_age(getattr(node.metadata, "creation_timestamp", None)),
        "internal_ip": addresses.get("InternalIP", ""),
        "hostname": addresses.get("Hostname", ""),
        "unschedulable": bool(getattr(node.spec, "unschedulable", False)),
        "taints": _taints_to_list(node),
        "conditions": conditions,
        "os_image": getattr(info, "os_image", "") or "",
        "kernel_version": getattr(info, "kernel_version", "") or "",
        "kubelet_version": getattr(info, "kubelet_version", "") or "",
        "container_runtime": getattr(info, "container_runtime_version", "") or "",
        "capacity": {
            "cpu": str(capacity.get("cpu", "")),
            "memory": str(capacity.get("memory", "")),
            "pods": str(capacity.get("pods", "")),
        },
        "allocatable": {
            "cpu": str(allocatable.get("cpu", "")),
            "memory": str(allocatable.get("memory", "")),
            "pods": str(allocatable.get("pods", "")),
        },
    }


# ---------------------------------------------------------------------------
# Pod helpers
# ---------------------------------------------------------------------------


def _active_pods_on_node(core: client.CoreV1Api, node_name: str) -> list[client.V1Pod]:
    """Return non-terminal Pods scheduled on a node.

    Excludes Succeeded/Failed Pods since they no longer consume scheduling resources.
    """
    pods = _k8s_api_call(
        core.list_pod_for_all_namespaces,
        field_selector=f"spec.nodeName={node_name}",
        watch=False,
    ).items
    out: list[client.V1Pod] = []
    for pod in pods:
        phase = pod.status.phase if pod.status else ""
        if phase in {"Succeeded", "Failed"}:
            continue
        out.append(pod)
    return out


def _sum_container_resources(containers: list[Any]) -> tuple[int, int, int, int]:
    """Sum requests/limits across a list of containers (CPU m / Memory bytes)."""
    cpu_request_m = 0
    cpu_limit_m = 0
    mem_request_b = 0
    mem_limit_b = 0
    for container in containers:
        resources = getattr(container, "resources", None)
        requests = getattr(resources, "requests", None) or {}
        limits = getattr(resources, "limits", None) or {}
        cpu_request_m += cpu_to_millicores(str(requests.get("cpu", "0")))
        cpu_limit_m += cpu_to_millicores(str(limits.get("cpu", "0")))
        mem_request_b += memory_to_bytes(str(requests.get("memory", "0")))
        mem_limit_b += memory_to_bytes(str(limits.get("memory", "0")))
    return cpu_request_m, cpu_limit_m, mem_request_b, mem_limit_b


def _pod_effective_resources(pod: client.V1Pod) -> MCPPodResource:
    """Compute effective Pod Requests/Limits.

    Uses max(containers_sum, max(init_containers)) which approximates the
    K8s scheduler's init-container-serial-execution model.
    """
    containers = pod.spec.containers or []
    init_containers = pod.spec.init_containers or []

    c_req_cpu, c_lim_cpu, c_req_mem, c_lim_mem = _sum_container_resources(containers)
    i_req_cpu, i_lim_cpu, i_req_mem, i_lim_mem = _sum_container_resources(init_containers)

    return MCPPodResource(
        cpu_request_m=max(c_req_cpu, i_req_cpu),
        cpu_limit_m=max(c_lim_cpu, i_lim_cpu),
        memory_request_bytes=max(c_req_mem, i_req_mem),
        memory_limit_bytes=max(c_lim_mem, i_lim_mem),
    )


def _pod_restart_count(pod: client.V1Pod) -> int:
    total = 0
    for cs in pod.status.container_statuses or []:
        total += int(cs.restart_count or 0)
    return total


def _pod_qos_class(pod: client.V1Pod) -> str:
    if pod.status and pod.status.qos_class:
        return pod.status.qos_class
    return "Unknown"


def _pod_basic_view(pod: client.V1Pod) -> dict[str, Any]:
    return {
        "namespace": getattr(pod.metadata, "namespace", "") or "",
        "name": getattr(pod.metadata, "name", "") or "",
        "phase": getattr(pod.status, "phase", "") or "",
        "qos_class": _pod_qos_class(pod),
        "restart_count": _pod_restart_count(pod),
        "issue": _pod_issue_from_obj(pod, restart_threshold=0),
    }


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------


def _node_metric_view(custom: client.CustomObjectsApi, node_name: str) -> dict[str, Any] | None:
    metrics = get_node_metrics(custom)
    if not metrics:
        return None
    usage = metrics.get(node_name)
    if not usage:
        return None
    cpu_m = cpu_to_millicores(str(usage.get("cpu", "0")))
    mem_b = memory_to_bytes(str(usage.get("memory", "0")))
    return {
        "cpu_usage_m": cpu_m,
        "cpu_usage_human": format_cpu_m(cpu_m),
        "memory_usage_bytes": mem_b,
        "memory_usage_human": format_bytes(mem_b),
    }


def get_node_metrics(custom: client.CustomObjectsApi) -> dict[str, dict[str, str]] | None:
    """Read Node metrics from metrics.k8s.io. Returns None if unavailable."""
    try:
        obj = _k8s_api_call(
            custom.list_cluster_custom_object,
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
        )
    except Exception:
        return None

    items = obj.get("items") if isinstance(obj, dict) else None
    if not isinstance(items, list):
        return None

    out: dict[str, dict[str, str]] = {}
    for it in items:
        md = it.get("metadata", {}) if isinstance(it, dict) else {}
        name = md.get("name")
        usage = it.get("usage", {})
        if isinstance(name, str) and isinstance(usage, dict):
            out[name] = {
                "cpu": str(usage.get("cpu", "")),
                "memory": str(usage.get("memory", "")),
            }
    return out


# ---------------------------------------------------------------------------
# Pod issue wrapper (V1Pod → common.pod_issue)
# ---------------------------------------------------------------------------


def _pod_issue_from_obj(pod: client.V1Pod, restart_threshold: int) -> str | None:
    """Check a V1Pod for anomaly signals. Thin wrapper around common.pod_issue."""
    if not pod.status:
        return "missing_status"
    return pod_issue(
        container_statuses=pod.status.container_statuses or [],
        phase=pod.status.phase or "",
        restart_threshold=restart_threshold,
    )


# ---------------------------------------------------------------------------
# Problem node detection
# ---------------------------------------------------------------------------


def _problem_reason_list(
    node: client.V1Node,
    custom: client.CustomObjectsApi,
    cfg: InspectorConfig,
) -> tuple[list[str], dict[str, Any]]:
    """Collect reasons why a node might be problematic, plus optional metrics detail."""
    conditions = _get_conditions(node)
    reasons: list[str] = []
    metric_detail: dict[str, Any] = {}
    node_name = _safe_name(node)

    if conditions.get("Ready") != "True":
        reasons.append("NodeNotReady")
    if conditions.get("DiskPressure") == "True":
        reasons.append("DiskPressure")
    if conditions.get("MemoryPressure") == "True":
        reasons.append("MemoryPressure")
    if conditions.get("PIDPressure") == "True":
        reasons.append("PIDPressure")
    if conditions.get("NetworkUnavailable") == "True":
        reasons.append("NetworkUnavailable")
    if bool(getattr(node.spec, "unschedulable", False)):
        reasons.append("Unschedulable")

    metrics = _node_metric_view(custom, node_name)
    capacity = node.status.capacity if node.status else {}
    if metrics and capacity:
        cpu_cap_m = cpu_to_millicores(str(capacity.get("cpu", "0")))
        mem_cap_b = memory_to_bytes(str(capacity.get("memory", "0")))
        cpu_ratio = ratio(metrics["cpu_usage_m"], cpu_cap_m)
        mem_ratio = ratio(metrics["memory_usage_bytes"], mem_cap_b)
        metric_detail = {
            **metrics,
            "cpu_capacity_m": cpu_cap_m,
            "memory_capacity_bytes": mem_cap_b,
            "cpu_utilization": cpu_ratio,
            "memory_utilization": mem_ratio,
        }
        if cpu_ratio >= cfg.thresholds.node_cpu_utilization:
            reasons.append("HighCPUUsage")
        if mem_ratio >= cfg.thresholds.node_memory_utilization:
            reasons.append("HighMemoryUsage")

    return reasons, metric_detail


def _event_brief(event: client.V1Event) -> dict[str, str]:
    return {
        "reason": event.reason or "",
        "type": event.type or "",
        "message": (event.message or "")[:300],
        "last_timestamp": (
            str(getattr(event, "last_timestamp", "") or getattr(event, "event_time", "") or "")
        ),
    }


def _pod_issue_summary(pod: client.V1Pod, restart_threshold: int) -> str | None:
    issue = _pod_issue_from_obj(pod, restart_threshold=0)
    if issue:
        return issue
    heavy = _pod_issue_from_obj(pod, restart_threshold=restart_threshold)
    if heavy == f"restart_count>={restart_threshold}":
        return heavy
    return None


def _pod_suggestions(issue: str | None, phase: str) -> list[str]:
    """Map common issue/phase to actionable troubleshooting suggestions."""
    if issue is None and phase == "Running":
        return ["No obvious anomaly detected; check dependencies, probes, and application logs."]
    if issue is None and phase == "Pending":
        return ["Check scheduling events, PVC binding, image pull, and node resource availability."]

    out: list[str] = []
    if issue and "CrashLoopBackOff" in issue:
        out.append("Check previous container logs for application startup errors, config issues, or dependency failures.")
        out.append("Verify probes, env vars, Secret/ConfigMap mounts.")
    if issue and ("ImagePullBackOff" in issue or "ErrImagePull" in issue):
        out.append("Verify image name/tag, registry credentials, and node-to-registry network connectivity.")
    if issue and "OOMKilled" in issue:
        out.append("Check container memory limit/request; investigate potential application memory leak.")
    if issue and "restart_count" in issue:
        out.append("Container restarts elevated; check events and logs to determine if transient or persistent.")
    if phase == "Pending":
        out.append("Pending pods: check scheduling events (resource shortage, taints, PVC binding).")
    if not out:
        out.append("Review Pod events, container statuses, and recent logs for root cause.")
    return out


def _find_problem_container_name(pod: client.V1Pod) -> str | None:
    for cs in pod.status.container_statuses or []:
        if cs.state and cs.state.waiting and cs.state.waiting.reason in BAD_POD_REASONS:
            return cs.name
        if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason in BAD_POD_REASONS:
            return cs.name
    return None


def _read_pod_log_excerpt(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str | None,
) -> str:
    """Read a short log excerpt, preferring previous=True for CrashLoopBackOff scenarios."""
    if not container_name:
        return ""

    for previous in (True, False):
        try:
            return _k8s_api_call(
                core.read_namespaced_pod_log,
                name=pod_name,
                namespace=namespace,
                container=container_name,
                previous=previous,
                tail_lines=80,
                timestamps=True,
            )[:4000]
        except ApiException:
            continue
        except Exception:
            continue
    return ""


# ---------------------------------------------------------------------------
# MCP Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations={
        "title": "List Kubernetes Nodes",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def list_nodes(
    label_selector: Annotated[
        str | None,
        Field(description="Optional Kubernetes label selector, e.g. node-role.kubernetes.io/worker"),
    ] = None,
) -> dict[str, Any]:
    """List cluster nodes with LLM-friendly flattened summaries."""
    kube = init_k8s_client()
    nodes = _k8s_api_call(kube.core.list_node, label_selector=label_selector or None).items
    items = [_parse_node(node) for node in nodes]
    return {"cluster": kube.display_name, "count": len(items), "items": items}


@mcp.tool(
    annotations={
        "title": "Get Node Detail",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def get_node_detail(
    node_name: Annotated[str, Field(description="Node name")],
) -> dict[str, Any]:
    """Get detailed info for a single node: attributes, live metrics, and Pod overview."""
    kube = init_k8s_client()
    node = _k8s_api_call(kube.core.read_node, node_name)
    pods = _active_pods_on_node(kube.core, node_name)
    metrics = _node_metric_view(kube.custom, node_name)
    detail = _parse_node(node)
    detail["pod_count"] = len(pods)
    detail["pods_sample"] = [_pod_basic_view(pod) for pod in pods[:20]]
    detail["metrics"] = metrics
    return detail


@mcp.tool(
    annotations={
        "title": "Audit Node Resource Usage",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def get_node_resource_usage(
    node_name: Annotated[str, Field(description="Node name")],
) -> dict[str, Any]:
    """Audit Pod Requests/Limits on a node. Flags overcommit and suggests BestEffort eviction."""
    kube = init_k8s_client()
    cfg = _get_runtime_config()
    node = _k8s_api_call(kube.core.read_node, node_name)
    pods = _active_pods_on_node(kube.core, node_name)
    allocatable = node.status.allocatable if node.status else {}
    capacity = node.status.capacity if node.status else {}

    total_cpu_req_m = 0
    total_cpu_lim_m = 0
    total_mem_req_b = 0
    total_mem_lim_b = 0
    pod_breakdown: list[dict[str, Any]] = []
    best_effort_pods: list[dict[str, str]] = []

    for pod in pods:
        pod_res = _pod_effective_resources(pod)
        total_cpu_req_m += pod_res.cpu_request_m
        total_cpu_lim_m += pod_res.cpu_limit_m
        total_mem_req_b += pod_res.memory_request_bytes
        total_mem_lim_b += pod_res.memory_limit_bytes

        qos_class = _pod_qos_class(pod)
        if qos_class == "BestEffort":
            best_effort_pods.append({
                "namespace": getattr(pod.metadata, "namespace", "") or "",
                "name": getattr(pod.metadata, "name", "") or "",
            })

        pod_breakdown.append({
            "namespace": getattr(pod.metadata, "namespace", "") or "",
            "name": getattr(pod.metadata, "name", "") or "",
            "qos_class": qos_class,
            "cpu_request_m": pod_res.cpu_request_m,
            "cpu_limit_m": pod_res.cpu_limit_m,
            "memory_request_bytes": pod_res.memory_request_bytes,
            "memory_limit_bytes": pod_res.memory_limit_bytes,
            "restart_count": _pod_restart_count(pod),
        })

    alloc_cpu_m = cpu_to_millicores(str(allocatable.get("cpu", "0")))
    alloc_mem_b = memory_to_bytes(str(allocatable.get("memory", "0")))
    cap_cpu_m = cpu_to_millicores(str(capacity.get("cpu", "0")))
    cap_mem_b = memory_to_bytes(str(capacity.get("memory", "0")))

    cpu_req_ratio = ratio(total_cpu_req_m, alloc_cpu_m)
    mem_req_ratio = ratio(total_mem_req_b, alloc_mem_b)
    cpu_lim_ratio = ratio(total_cpu_lim_m, alloc_cpu_m)
    mem_lim_ratio = ratio(total_mem_lim_b, alloc_mem_b)

    metric_view = _node_metric_view(kube.custom, node_name)
    eviction_suggestion = None
    requests_overcommit = (
        cpu_req_ratio > cfg.thresholds.requests_overcommit_ratio
        or mem_req_ratio > cfg.thresholds.requests_overcommit_ratio
    )
    limits_overcommit = (
        cpu_lim_ratio > cfg.thresholds.limits_overcommit_ratio
        or mem_lim_ratio > cfg.thresholds.limits_overcommit_ratio
    )
    limits_warning = None
    if limits_overcommit:
        limits_warning = {
            "reason": "Node Limits ratio is elevated (soft risk). Assess actual load and QoS distribution.",
            "threshold": cfg.thresholds.limits_overcommit_ratio,
        }

    if requests_overcommit:
        eviction_suggestion = {
            "reason": "Requests overcommit detected. Consider migrating or evicting BestEffort Pods.",
            "best_effort_candidates": best_effort_pods,
            "threshold": cfg.thresholds.requests_overcommit_ratio,
        }

    return {
        "node": node_name,
        "cluster": kube.display_name,
        "pod_count": len(pods),
        "allocatable": {
            "cpu_m": alloc_cpu_m, "cpu_human": format_cpu_m(alloc_cpu_m),
            "memory_bytes": alloc_mem_b, "memory_human": format_bytes(alloc_mem_b),
        },
        "capacity": {
            "cpu_m": cap_cpu_m, "cpu_human": format_cpu_m(cap_cpu_m),
            "memory_bytes": cap_mem_b, "memory_human": format_bytes(cap_mem_b),
        },
        "requests": {
            "cpu_m": total_cpu_req_m, "cpu_human": format_cpu_m(total_cpu_req_m),
            "memory_bytes": total_mem_req_b, "memory_human": format_bytes(total_mem_req_b),
            "cpu_ratio": cpu_req_ratio, "memory_ratio": mem_req_ratio,
        },
        "limits": {
            "cpu_m": total_cpu_lim_m, "cpu_human": format_cpu_m(total_cpu_lim_m),
            "memory_bytes": total_mem_lim_b, "memory_human": format_bytes(total_mem_lim_b),
            "cpu_ratio": cpu_lim_ratio, "memory_ratio": mem_lim_ratio,
        },
        "live_usage": metric_view,
        "overcommit": {
            "requests": requests_overcommit,
            "limits": limits_overcommit,
            "requests_threshold": cfg.thresholds.requests_overcommit_ratio,
            "limits_threshold": cfg.thresholds.limits_overcommit_ratio,
        },
        "eviction_suggestion": eviction_suggestion,
        "limits_warning": limits_warning,
        "pod_breakdown": pod_breakdown[:50],
    }


@mcp.tool(
    annotations={
        "title": "Find Problem Nodes",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def find_problem_nodes(
    label_selector: Annotated[
        str | None,
        Field(description="Optional node label filter"),
    ] = None,
) -> dict[str, Any]:
    """Scan for abnormal nodes: NotReady, disk/memory pressure, high load."""
    kube = init_k8s_client()
    cfg = _get_runtime_config()
    nodes = _k8s_api_call(kube.core.list_node, label_selector=label_selector or None).items
    items: list[dict[str, Any]] = []

    for node in nodes:
        reasons, metric_detail = _problem_reason_list(node, kube.custom, cfg)
        if not reasons:
            continue
        items.append({
            "name": _safe_name(node),
            "status": "Ready" if _get_conditions(node).get("Ready") == "True" else "NotReady",
            "internal_ip": _get_node_addresses(node).get("InternalIP", ""),
            "age": _format_age(getattr(node.metadata, "creation_timestamp", None)),
            "reasons": reasons,
            "conditions": _get_conditions(node),
            "metrics": metric_detail,
        })

    return {
        "cluster": kube.display_name,
        "count": len(items),
        "thresholds": {
            "node_cpu_utilization": cfg.thresholds.node_cpu_utilization,
            "node_memory_utilization": cfg.thresholds.node_memory_utilization,
        },
        "items": items,
    }


@mcp.tool(
    annotations={
        "title": "List Node Pods",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def list_node_pods(
    node_name: Annotated[str, Field(description="Node name")],
) -> dict[str, Any]:
    """List active Pods on a specific node with anomaly markers."""
    kube = init_k8s_client()
    pods = _active_pods_on_node(kube.core, node_name)
    cfg = _get_runtime_config()
    items: list[dict[str, Any]] = []
    for pod in pods:
        issue = _pod_issue_summary(pod, cfg.thresholds.pod_restart_count)
        item = _pod_basic_view(pod)
        item["issue"] = issue
        items.append(item)

    return {"cluster": kube.display_name, "node": node_name, "count": len(items), "items": items}


@mcp.tool(
    annotations={
        "title": "Diagnose Pod Issues",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def diagnose_pod_issues(
    pod_name: Annotated[str, Field(description="Pod name")],
    namespace: Annotated[str, Field(description="Pod namespace")],
) -> dict[str, Any]:
    """Diagnose a Pod: events, log excerpts for crash/error states, and actionable suggestions."""
    kube = init_k8s_client()
    cfg = _get_runtime_config()
    pod = _k8s_api_call(kube.core.read_namespaced_pod, name=pod_name, namespace=namespace)

    phase = pod.status.phase if pod.status else ""
    issue = _pod_issue_summary(pod, cfg.thresholds.pod_restart_count)
    problem_container = _find_problem_container_name(pod)

    events = _k8s_api_call(
        kube.core.list_namespaced_event,
        namespace=namespace,
        field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod_name}",
    ).items

    log_excerpt = ""
    if issue and (
        "CrashLoopBackOff" in issue
        or "ImagePullBackOff" in issue
        or "ErrImagePull" in issue
        or "OOMKilled" in issue
    ):
        log_excerpt = _read_pod_log_excerpt(kube.core, namespace, pod_name, problem_container)

    return {
        "cluster": kube.display_name,
        "namespace": namespace,
        "name": pod_name,
        "node": getattr(pod.spec, "node_name", "") or "",
        "phase": phase,
        "qos_class": _pod_qos_class(pod),
        "restart_count": _pod_restart_count(pod),
        "issue": issue,
        "problem_container": problem_container,
        "events": [_event_brief(event) for event in events[:20]],
        "log_excerpt": log_excerpt,
        "suggestions": _pod_suggestions(issue, phase),
    }


# ---------------------------------------------------------------------------
# MCP Resources & Prompts
# ---------------------------------------------------------------------------


@mcp.resource("k8s://cluster/summary")
def cluster_summary() -> dict[str, Any]:
    """Lightweight cluster summary for LLM context before diagnosis."""
    kube = init_k8s_client()
    cfg = _get_runtime_config()
    nodes = _k8s_api_call(kube.core.list_node).items
    problem_nodes = 0
    for node in nodes:
        reasons, _metric = _problem_reason_list(node, kube.custom, cfg)
        if reasons:
            problem_nodes += 1

    return {
        "cluster": kube.display_name,
        "node_count": len(nodes),
        "problem_node_count": problem_nodes,
        "available_tools": [
            "list_nodes",
            "get_node_detail",
            "get_node_resource_usage",
            "find_problem_nodes",
            "list_node_pods",
            "diagnose_pod_issues",
        ],
    }


@mcp.prompt
def node_triage_prompt(
    node_name: Annotated[str, Field(description="Node name to troubleshoot")],
) -> str:
    """SOP prompt for LLM-based node troubleshooting."""
    return f"""
You are a Kubernetes SRE. Troubleshoot node `{node_name}` in this order:

1. Call `get_node_detail` to check node status, conditions, taints, and Pod overview.
2. Call `get_node_resource_usage` to audit allocatable, requests/limits, and overcommit.
3. If abnormal Pods exist, use `list_node_pods` to filter them, then `diagnose_pod_issues` for details.
4. In your conclusion, answer:
   - Is the node truly abnormal, or just under high load / cordoned?
   - Which Pods/workloads are affected?
   - Recommended next steps or mitigation.
"""


@mcp.prompt
def cluster_problem_scan_prompt() -> str:
    """SOP prompt for LLM-based cluster-wide problem scanning."""
    return """
You are a Kubernetes inspection assistant. Follow these steps:

1. Read `k8s://cluster/summary` for overall context.
2. Call `find_problem_nodes` to identify abnormal nodes, sorted by severity:
   - NodeNotReady
   - DiskPressure / MemoryPressure / PIDPressure
   - HighCPUUsage / HighMemoryUsage
3. For each problem node:
   - Use `get_node_detail` for detailed status;
   - Use `get_node_resource_usage` for resource audit and overcommit;
   - Use `list_node_pods` / `diagnose_pod_issues` for deeper investigation if needed.
4. Final output should include:
   - List of problem nodes
   - Key anomaly reason per node
   - Whether specific Pods/workloads need priority attention
   - Recommended remediation priority
"""


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    mcp.run(transport="sse", host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
