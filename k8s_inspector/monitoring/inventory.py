"""Independent expected inventory exporter; never reads CronJob/Pushgateway state."""
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import json
from pathlib import Path

from k8s_inspector.core.profiles import normalize_profile


@dataclass(frozen=True)
class ExpectedTarget:
    cluster_id: str
    profile: str
    max_run_age_seconds: int
    rule_ids: tuple[str, ...]

    def __post_init__(self):
        from k8s_inspector.inspection.catalog import CATALOG
        if (not isinstance(self.cluster_id, str) or not self.cluster_id.strip()
                or len(self.cluster_id) > 128 or any(ord(c) < 32 for c in self.cluster_id)):
            raise ValueError('inventory cluster_id is invalid')
        if normalize_profile(self.profile) != self.profile:
            raise ValueError('inventory profile must be canonical')
        if type(self.max_run_age_seconds) is not int or self.max_run_age_seconds <= 0:
            raise ValueError('inventory max_run_age_seconds must be a positive integer')
        allowed = {r.rule_id for r in CATALOG if self.profile in r.profiles}
        if (not isinstance(self.rule_ids, tuple) or not self.rule_ids
                or any(not isinstance(r, str) or r not in allowed for r in self.rule_ids)
                or len(set(self.rule_ids)) != len(self.rule_ids)):
            raise ValueError('inventory rule_ids must be unique registered rules for this profile')


def load_expected_targets(path: Path) -> tuple[ExpectedTarget, ...]:
    import yaml
    try:
        data = yaml.safe_load(Path(path).read_text(encoding='utf-8-sig'))
        if not isinstance(data, dict) or set(data) != {'targets'}:
            raise ValueError('expected inventory requires targets')
        rows = data['targets']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
            raise ValueError('inventory must contain 1..1000 targets')
        targets = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'cluster_id','profile','max_run_age_seconds','rule_ids'}:
                raise ValueError('invalid inventory target fields')
            if not isinstance(row['rule_ids'], list):
                raise ValueError('inventory rule_ids must be an array')
            targets.append(ExpectedTarget(**dict(row, rule_ids=tuple(row['rule_ids']))))
        if len({(t.cluster_id,t.profile) for t in targets}) != len(targets):
            raise ValueError('duplicate inventory target')
        return tuple(sorted(targets, key=lambda t: (t.cluster_id,t.profile)))
    except (yaml.YAMLError, TypeError) as exc:
        raise ValueError('invalid expected inventory') from exc


def export_inventory(targets):
    from prometheus_client import CollectorRegistry, Gauge, generate_latest
    registry = CollectorRegistry()
    target = Gauge('inspector_expected_target', 'Independently configured expected target.',
                   ['cluster','profile'], registry=registry)
    age = Gauge('inspector_expected_max_run_age_seconds', 'Configured maximum run/observation age.',
                ['cluster','profile'], registry=registry)
    rule = Gauge('inspector_expected_rule', 'Expected supported aggregate rule observation.',
                 ['cluster','profile','rule_id','scope'], registry=registry)
    for t in targets:
        target.labels(t.cluster_id,t.profile).set(1)
        age.labels(t.cluster_id,t.profile).set(t.max_run_age_seconds)
        for rule_id in t.rule_ids:
            rule.labels(t.cluster_id,t.profile,rule_id,'cluster').set(1)
    return generate_latest(registry)


def make_inventory_server(host, port, targets):
    body = export_inventory(targets)
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def do_GET(self):
            if self.path == '/metrics':
                data, content_type = body, 'text/plain; version=0.0.4; charset=utf-8'
            elif self.path in ('/healthz','/readyz'):
                data, content_type = b'ok\n', 'text/plain'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length',str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass
    return HTTPServer((host,port),Handler)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--listen-host',default='0.0.0.0')
    parser.add_argument('--listen-port',type=int,default=9400)
    args=parser.parse_args(argv)
    targets=load_expected_targets(Path(args.config))
    server=make_inventory_server(args.listen_host,args.listen_port,targets)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
