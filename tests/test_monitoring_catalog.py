from pathlib import Path
import json
import threading
from urllib.request import urlopen
import pytest
import yaml
from prometheus_client.parser import text_string_to_metric_families


def targets():
    from k8s_inspector.monitoring.inventory import ExpectedTarget
    return (ExpectedTarget('prod', '5m', 900, ('nodes.ready', 'pods.current', 'pvc.binding')),)


def test_rule_has_one_primary_notification_profile(tmp_path):
    from tools.render_monitoring import render_monitoring
    from k8s_inspector.inspection.catalog import CATALOG
    files = render_monitoring(CATALOG, targets(), tmp_path)
    rules = yaml.safe_load((tmp_path/'inspection-rules.yaml').read_text(encoding='utf-8'))['groups'][0]['rules']
    business = [r for r in rules if r.get('labels', {}).get('category') == 'business']
    assert len(business) == 10
    for rule in CATALOG:
        matches = [r for r in business if r['labels']['rule_id'] == rule.rule_id]
        assert len(matches) == 1 and rule.primary_profile in rule.profiles
        assert matches[0]['labels']['profile'] == rule.primary_profile
    assert {r['labels']['profile'] for r in business if r['labels']['rule_id']=='nodes.ready'} == {'5m'}


def test_monitoring_generation_is_reproducible_and_checked_in(tmp_path):
    from tools.render_monitoring import render_monitoring
    from k8s_inspector.monitoring.inventory import load_expected_targets
    from k8s_inspector.inspection.catalog import CATALOG
    expected = load_expected_targets(Path('deploy/monitoring/expected-targets.example.yaml'))
    a = render_monitoring(CATALOG, expected, tmp_path/'a')
    b = render_monitoring(CATALOG, expected, tmp_path/'b')
    assert [p.read_bytes() for p in a] == [p.read_bytes() for p in b]
    for p in a:
        assert p.read_bytes() == (Path('deploy/monitoring/generated')/p.name).read_bytes()


def test_inventory_exports_expected_without_any_inspector_run():
    from k8s_inspector.monitoring.inventory import export_inventory
    metrics = list(text_string_to_metric_families(export_inventory(targets()).decode()))
    samples = {s.name: s for m in metrics for s in m.samples}
    assert samples['inspector_expected_target'].value == 1
    assert samples['inspector_expected_max_run_age_seconds'].value == 900
    rule_samples = [s for m in metrics for s in m.samples if s.name=='inspector_expected_rule']
    assert len(rule_samples) == 3 and {s.labels['scope'] for s in rule_samples} == {'cluster'}


@pytest.mark.parametrize('overrides', [
    {'profile': 'typo'}, {'rule_ids': ['events.warning']}, {'max_run_age_seconds': 0}, {'cluster_id': ''},
])
def test_invalid_inventory_cannot_start(tmp_path, overrides):
    from k8s_inspector.monitoring.inventory import load_expected_targets
    data = dict(cluster_id='prod', profile='5m', max_run_age_seconds=900, rule_ids=['nodes.ready'])
    data.update(overrides)
    path = tmp_path/'inventory.json'
    path.write_text(json.dumps({'targets': [data]}), encoding='utf-8')
    with pytest.raises(ValueError):
        load_expected_targets(path)


def test_independent_inventory_http_is_read_only():
    from k8s_inspector.monitoring.inventory import make_inventory_server
    server = make_inventory_server('127.0.0.1', 0, targets())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        root = 'http://127.0.0.1:'+str(server.server_address[1])
        assert b'inspector_expected_target' in urlopen(root+'/metrics').read()
        assert urlopen(root+'/readyz').status == 200
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
