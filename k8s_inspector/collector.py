"""Paged SDK requests share one run budget and preserve partial evidence."""
from datetime import datetime, timezone
from typing import Callable

from .observation_models import ExecutionStatus as E
from .snapshot import Collection, RunSnapshot


class KubeCollector:
    def __init__(self, context, deadline: float, clock: Callable[[], float]):
        self.context, self.deadline, self.clock = context, deadline, clock
        self.started_at = datetime.now(timezone.utc)
        self._cache = {}

    def _timeout(self):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise TimeoutError('DEADLINE')
        connect = min(3.0, remaining / 2)
        return connect, min(10.0, remaining - connect)

    def collect_list(self, key, call, **kwargs):
        if key in self._cache:
            return self._cache[key]
        items, token, restarts, retries = [], None, 0, 0
        seen_tokens = set()
        status, error_kind, reason = E.OK, None, None
        while True:
            try:
                params = dict(kwargs, limit=500, _request_timeout=self._timeout())
                if token:
                    params['_continue'] = token
                response = call(**params)
                if isinstance(response, dict):
                    page, metadata = response.get('items'), response.get('metadata') or {}
                    next_token = metadata.get('continue')
                else:
                    page = getattr(response, 'items', None)
                    metadata = getattr(response, 'metadata', None)
                    next_token = getattr(metadata, '_continue', None)
                if not isinstance(page, (list, tuple)) or (next_token is not None and not isinstance(next_token, str)):
                    raise ValueError('INVALID_RESPONSE')
                items.extend(page)
                retries = 0
                if not next_token:
                    break
                if next_token in seen_tokens:
                    raise ValueError('INVALID_RESPONSE')
                seen_tokens.add(next_token)
                token = next_token
            except Exception as exc:
                code = getattr(exc, 'status', None)
                remaining = self.deadline - self.clock()
                if code == 410 and restarts == 0 and remaining > 0:
                    # A new list has a new consistency boundary. Discard old pages.
                    items, token, seen_tokens, retries = [], None, set(), 0
                    restarts += 1
                    continue
                kind = ('DEADLINE' if remaining <= 0 else
                        'FORBIDDEN' if code == 403 else
                        'UNSUPPORTED' if code == 404 else
                        'EXPIRED' if code == 410 else
                        'INVALID_RESPONSE' if isinstance(exc, (ValueError, TypeError)) else 'TEMPORARY')
                temporary = code == 429 or (isinstance(code, int) and code >= 500) or (
                    code in (None, 0) and not isinstance(exc, (ValueError, TypeError, AttributeError)))
                if temporary and retries < 2 and remaining > 0:
                    retries += 1
                    continue
                status = E.PARTIAL if items else E.UNSUPPORTED if code == 404 else E.ERROR
                error_kind, reason = kind, f'{key}: {kind}'
                break
        result = Collection(tuple(items), status, datetime.now(timezone.utc),
                            status == E.OK, error_kind, reason)
        self._cache[key] = result
        return result

    def _binding(self, key):
        ctx = self.context
        if key == 'nodes':
            return ctx.core.list_node, {}
        if key == 'pods':
            return ctx.core.list_pod_for_all_namespaces, {}
        if key == 'pvcs':
            return ctx.core.list_persistent_volume_claim_for_all_namespaces, {}
        if key == 'storage_classes':
            return ctx.storage.list_storage_class, {}
        if key == 'events':
            return ctx.core.list_event_for_all_namespaces, {'field_selector': 'type=Warning'}
        if key == 'node_metrics':
            return ctx.custom.list_cluster_custom_object, {
                'group': 'metrics.k8s.io', 'version': 'v1beta1', 'plural': 'nodes'}
        if key in ('deployments', 'daemonsets'):
            from kubernetes.client import AppsV1Api
            apps = getattr(ctx, 'apps', None) or AppsV1Api(ctx.api_client)
            return (apps.list_deployment_for_all_namespaces if key == 'deployments'
                    else apps.list_daemon_set_for_all_namespaces), {}
        raise ValueError(f'unknown collection key: {key}')

    def snapshot(self, required: frozenset[str], run_id: str, cluster_id: str):
        for key in sorted(required):
            if key not in self._cache:
                call, kwargs = self._binding(key)
                self.collect_list(key, call, **kwargs)
        return RunSnapshot(run_id, cluster_id, self.started_at,
                           {k: self._cache[k] for k in required})
