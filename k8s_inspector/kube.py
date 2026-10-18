"""
Kubernetes client layer (thin wrapper around the Python Kubernetes SDK).

Purpose:
- Isolate K8s config loading (kubeconfig / in-cluster) from inspection logic.
- Centralize low-level API calls (/healthz, metrics.k8s.io) that go beyond
  the standard resource CRUD methods.
"""

from __future__ import annotations

import time
import hashlib
import json
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from kubernetes import client, config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException

from .common import parse_quantity


@dataclass
class KubeContext:
    """Collection of K8s API clients scoped to a single cluster connection."""
    display_name: str
    api_client: ApiClient
    core: client.CoreV1Api
    storage: client.StorageV1Api
    custom: client.CustomObjectsApi
    event_baseline: tuple[dict[str, Any], ...] = ()
    cluster_identity: str | None = None


def load_kube(
    kubeconfig: str | None = None,
    context: str | None = None,
    in_cluster: bool = False,
) -> KubeContext:
    """
    Initialize Kubernetes SDK clients.

    - in_cluster=True: uses the Pod ServiceAccount token (for CronJob runs).
    - in_cluster=False: reads a local kubeconfig file.
    """
    identity = None
    if in_cluster:
        config.load_incluster_config()
        display = "in-cluster"
    else:
        config.load_kube_config(config_file=kubeconfig, context=context)
        contexts, active = config.list_kube_config_contexts(config_file=kubeconfig)
        selected = next((c for c in contexts or () if c['name'] == context), None) if context else active
        if not selected:
            raise ValueError('selected kubeconfig context is unavailable')
        display = selected['name']
        from kubernetes.config.kube_config import KUBE_CONFIG_DEFAULT_LOCATION
        sources = kubeconfig or KUBE_CONFIG_DEFAULT_LOCATION
        binding = {'sources': [str(Path(p).expanduser().resolve()) for p in sources.split(os.pathsep)],
                   'context': display, 'cluster': selected['context']['cluster'],
                   'server': client.Configuration.get_default_copy().host.rstrip('/')}
        identity = 'kube-' + hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()[:24]

    # All API clients share one ApiClient so they reuse the same connection pool.
    configuration = client.Configuration.get_default_copy()
    # The collector owns the sole retry budget. urllib3 defaults retry reads too.
    configuration.retries = 0
    if hasattr(configuration, "client_go_retries"):
        configuration.client_go_retries = False
    api_client = client.ApiClient(configuration=configuration)
    return KubeContext(
        display_name=display,
        cluster_identity=identity,
        api_client=api_client,
        core=client.CoreV1Api(api_client),
        storage=client.StorageV1Api(api_client),
        custom=client.CustomObjectsApi(api_client),
    )


def _healthz_response(api_client, timeout_s):
    """Adapt the two supported SDK call APIs without dropping authentication."""
    if callable(getattr(type(api_client), 'param_serialize', None)):
        params = api_client.param_serialize('GET', '/healthz', auth_settings=['BearerToken'])
        response = api_client.call_api(*params, _request_timeout=timeout_s)
        try:
            data = response.response.read(4096) if response.status==200 else b''
            return data.decode('utf-8', errors='replace'), response.status, response.headers
        finally:
            response.response.close()
            response.response.release_conn()
    return api_client.call_api('/healthz', 'GET', response_type='str', auth_settings=['BearerToken'],
        _request_timeout=timeout_s, _preload_content=True)


def apiserver_healthz(api_client: ApiClient, timeout_s: float = 2.0) -> dict[str, Any]:
    """
    Call apiserver /healthz and return structured health data.

    Returns:
      ok: True if status=200 and body="ok".
      status_code: HTTP status code.
      latency_ms: Request duration in ms.
      body/error: Truncated response or error message.
    """
    t0 = time.perf_counter()
    try:
        data, status_code, _headers = _healthz_response(api_client, timeout_s)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        ok = (status_code == 200) and (str(data).strip().lower() == "ok")
        return {
            "ok": ok,
            "status_code": status_code,
            "latency_ms": latency_ms,
            "body": (str(data).strip()[:200] if data is not None else ""),
        }
    except ApiException as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {
            "ok": False,
            "status_code": getattr(e, "status", None),
            "latency_ms": latency_ms,
            "error": str(e),
        }
    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "latency_ms": latency_ms, "error": str(e)}


def get_node_metrics(
    custom: client.CustomObjectsApi,
) -> dict[str, dict[str, str]] | None:
    """
    Read Node metrics from metrics.k8s.io.

    Returns None if metrics-server is not installed or permission is denied.
    Callers must degrade gracefully.
    """
    try:
        obj = custom.list_cluster_custom_object(
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
