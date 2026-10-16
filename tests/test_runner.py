from dataclasses import replace
from types import SimpleNamespace as NS
from unittest.mock import Mock
import pytest
from k8s_inspector.config import InspectorConfig
from k8s_inspector.observation_models import ExecutionStatus as E, Severity
from tests.test_collector import page
from tests.test_rule_observations import NOW, node


def context():
    from kubernetes.client import ApiClient
    return NS(core=NS(list_node=Mock(return_value=page([node()])),
        list_pod_for_all_namespaces=Mock(return_value=page()),
        list_persistent_volume_claim_for_all_namespaces=Mock(return_value=page())),
        storage=NS(list_storage_class=Mock(return_value=page())),
        custom=NS(list_cluster_custom_object=Mock(return_value=page())), api_client=ApiClient())


def test_unknown_profile_is_rejected():
    from k8s_inspector.runner import run_inspection
    ctx = context()
    with pytest.raises(ValueError, match='profile'):
        run_inspection(ctx, InspectorConfig(), 'typo', cluster_id='prod', now=NOW, monotonic=lambda: 0)
    assert not ctx.core.list_node.called


def test_profile_collections_match_existing_frequency_contract():
    from k8s_inspector.rule_catalog import collection_keys, normalize_profile
    assert collection_keys('5m') == frozenset({'nodes', 'pods', 'pvcs', 'storage_classes'})
    assert collection_keys('10m') == frozenset({'api_health', 'pods'})
    assert 'node_metrics' in collection_keys('hourly')
    assert 'events' not in collection_keys('hourly')
    assert normalize_profile('1h') == 'hourly'


def test_run_deadline_marks_remaining_rules_unknown():
    from k8s_inspector.runner import run_inspection
    ticks = iter([0] + [10000] * 100)
    report = run_inspection(context(), InspectorConfig(), '5m', cluster_id='prod',
                            now=NOW, monotonic=lambda: next(ticks))
    assert report.schema_version == 2 and report.run_id
    assert report.executions and all(e.execution_status == E.ERROR for e in report.executions)
    assert not any(o.complete for o in report.observations)


def test_known_failure_survives_later_rule_error(monkeypatch):
    from k8s_inspector import runner
    from k8s_inspector.rule_catalog import RuleSpec
    from k8s_inspector.rules_nodes import evaluate_nodes
    def broken(s, cfg):
        raise RuntimeError('credential must not leak')
    catalog = (
        RuleSpec('nodes.ready', frozenset({'5m'}), '5m', frozenset({'nodes'}), lambda s,c: evaluate_nodes(s,c)[0]),
        RuleSpec('broken', frozenset({'5m'}), '5m', frozenset(), broken),
    )
    monkeypatch.setattr(runner, 'CATALOG', catalog)
    report = runner.run_inspection(context(), InspectorConfig(), '5m', cluster_id='prod', now=NOW, monotonic=lambda: 0)
    assert report.has_fail and report.findings[0].severity == Severity.FAIL
    assert report.executions[-1].execution_status == E.ERROR
    assert 'credential' not in str(report.to_dict())


def test_catalog_mismatched_identity_is_execution_error(monkeypatch):
    from k8s_inspector import runner
    from k8s_inspector.rule_catalog import RuleSpec
    from tests.observation_factories import failed_evaluation
    monkeypatch.setattr(runner, 'CATALOG', (
        RuleSpec('wanted', frozenset({'5m'}), '5m', frozenset(), lambda s,c: failed_evaluation()),))
    report = runner.run_inspection(context(), InspectorConfig(), '5m', cluster_id='prod', now=NOW, monotonic=lambda: 0)
    assert report.executions[0].rule_id == 'wanted' and report.executions[0].execution_status == E.ERROR
