"""Rule/profile ownership is defined once here, including notification primaries."""
from dataclasses import dataclass
from typing import Callable
from .config import InspectorConfig
from .observation_models import CheckEvaluation
from .profiles import PROFILES, ProfileSpec, normalize_profile
from .snapshot import RunSnapshot
from .rule_evaluation import evaluation


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    profiles: frozenset[str]
    primary_profile: str
    collection_keys: frozenset[str]
    evaluate: Callable[[RunSnapshot, InspectorConfig], CheckEvaluation]


def domain_rule(module, function, rule_id):
    def run(snapshot, cfg):
        import importlib
        domain = importlib.import_module(f'.{module}', __package__)
        matches = [e for e in getattr(domain, function)(snapshot, cfg) if e.execution.rule_id == rule_id]
        if len(matches) != 1:
            raise ValueError('domain must return exactly one matching rule')
        return matches[0]
    return run


def spec(rule_id, primary, profiles, keys, domain):
    return RuleSpec(rule_id, frozenset(profiles), primary, frozenset(keys),
                    domain_rule(f'rules_{domain}', f'evaluate_{domain}', rule_id))


DATA = ('5m', 'hourly', 'daily', 'full')
INFRA = ('hourly', 'daily', 'full')
CONTROL = ('10m', 'hourly', 'daily', 'full')
CATALOG = (
    spec('nodes.ready', '5m', DATA, ('nodes',), 'nodes'),
    spec('pods.current', '5m', DATA, ('pods',), 'pods'),
    spec('pvc.binding', '5m', DATA, ('pvcs', 'pods', 'storage_classes'), 'storage'),
    spec('control.api', '10m', CONTROL, ('api_health',), 'control'),
    spec('control.components', '10m', CONTROL, ('pods',), 'control'),
    spec('nodes.utilization', 'hourly', INFRA, ('nodes', 'node_metrics'), 'nodes'),
    spec('nodes.pressure', 'hourly', INFRA, ('nodes',), 'nodes'),
    spec('pods.history', 'daily', ('daily', 'full'), ('pods',), 'pods'),
    spec('events.warning', 'daily', ('daily', 'full'), ('events',), 'events'),
    spec('addons.ready', 'daily', ('daily', 'full'), ('pods', 'deployments', 'daemonsets'), 'control'),
)


def collection_keys(profile):
    profile = normalize_profile(profile)
    return frozenset(key for rule in CATALOG if profile in rule.profiles for key in rule.collection_keys)
