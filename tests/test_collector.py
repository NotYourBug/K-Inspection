from types import SimpleNamespace as NS
from unittest.mock import Mock
from kubernetes.client.exceptions import ApiException
import pytest

from k8s_inspector.core.observations import ExecutionStatus as E


def make_collector(deadline=30, clock=lambda: 0):
    from k8s_inspector.inspection.collector import KubeCollector
    return KubeCollector(NS(), deadline, clock)


def page(items=(), token=None):
    return NS(items=list(items), metadata=NS(_continue=token))


def test_second_page_error_is_partial():
    c = make_collector()
    sdk = Mock(side_effect=[page(['n1'], 'next'), ApiException(status=403)])
    result = c.collect_list('nodes', sdk)
    assert (result.complete, result.execution_status, result.items) == (False, E.PARTIAL, ('n1',))
    assert result.error_kind == 'FORBIDDEN'
    assert sdk.call_args_list[1].kwargs['_continue'] == 'next'


def test_deadline_stops_next_page():
    tick = [0.0]
    c = make_collector(1, lambda: tick[0])
    def sdk(**kwargs):
        connect, read = kwargs['_request_timeout']
        assert connect > 0 and read > 0 and connect + read <= 1
        tick[0] = 2
        return page(['n1'], 'next')
    r = c.collect_list('nodes', sdk)
    assert r.items == ('n1',) and not r.complete and r.error_kind == 'DEADLINE'


def test_forbidden_is_not_retried():
    sdk = Mock(side_effect=ApiException(status=403))
    r = make_collector().collect_list('pods', sdk)
    assert r.execution_status == E.ERROR and sdk.call_count == 1


@pytest.mark.parametrize('status, expected', [(404, E.UNSUPPORTED), (429, E.ERROR), (503, E.ERROR)])
def test_classification_and_bounded_retries(status, expected):
    sdk = Mock(side_effect=ApiException(status=status))
    r = make_collector().collect_list('pods', sdk)
    assert r.execution_status == expected and not r.complete
    assert sdk.call_count == (1 if status == 404 else 3)


def test_410_does_not_mix_snapshots():
    sdk = Mock(side_effect=[page(['old'], 'p2'), ApiException(status=410), page(['new'])])
    r = make_collector().collect_list('nodes', sdk)
    assert r.complete and r.items == ('new',)
    assert '_continue' not in sdk.call_args_list[2].kwargs


def test_empty_success_differs_from_empty_error():
    assert make_collector().collect_list('x', lambda **k: page()).complete
    assert not make_collector().collect_list('x', Mock(side_effect=TimeoutError())).complete


def test_deadline_before_first_request_and_malformed_response():
    sdk = Mock()
    r = make_collector(0).collect_list('x', sdk)
    assert r.error_kind == 'DEADLINE' and not sdk.called
    r = make_collector().collect_list('x', lambda **k: {'unexpected': []})
    assert not r.complete and r.error_kind == 'INVALID_RESPONSE'
