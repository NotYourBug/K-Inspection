"""Durable last-valid aggregate observations, with OS-level profile exclusion."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path

from .observation_models import ObservationHealth as H, ExecutionStatus as E, Severity, to_json_value, utc
from .report import atomic_write_text


@contextmanager
def profile_run_lock(directory: Path, *, name='.run.lock'):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory/name).open('a+b') as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b'0')
            handle.flush()
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt':
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class ValidRuleState:
    rule_id: str
    cluster_id: str
    profile: str
    scope: str
    health: H
    failure_count: int
    observed_at: datetime
    run_id: str

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))
        if (not all(isinstance(x, str) and x for x in
                    (self.rule_id, self.cluster_id, self.profile, self.scope, self.run_id))
                or self.health not in (H.HEALTHY, H.UNHEALTHY)
                or type(self.failure_count) is not int or self.failure_count < 0
                or (self.health == H.HEALTHY) != (self.failure_count == 0)):
            raise ValueError('invalid observation state')


@dataclass(frozen=True)
class ConfirmedRuleState:
    """Positive evidence may be partial; it never claims complete coverage."""
    rule_id: str
    cluster_id: str
    profile: str
    scope: str
    failure_count: int
    observed_at: datetime
    run_id: str

    def __post_init__(self):
        object.__setattr__(self, 'observed_at', utc(self.observed_at))
        if (not all(isinstance(x, str) and x for x in (self.rule_id, self.cluster_id,
                self.profile, self.scope, self.run_id)) or type(self.failure_count) is not int
                or self.failure_count <= 0):
            raise ValueError('invalid confirmed finding state')


def _key(state):
    return state.cluster_id, state.profile, state.rule_id, state.scope


class ObservationStateStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.marker = self.path.with_name(self.path.name + '.initialized')

    def _load(self):
        if not self.path.exists():
            if self.marker.exists():
                raise ValueError('observation state is missing after initialization')
            return (), (), ()
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
            if type(data.get('schema_version')) is not int or data['schema_version'] != 1:
                raise ValueError('unsupported observation state schema')
            states = tuple(ValidRuleState(**dict(item, health=H(item['health']),
                observed_at=datetime.fromisoformat(item['observed_at']))) for item in data['states'])
            if len({_key(s) for s in states}) != len(states):
                raise ValueError('duplicate observation state identity')
            cursors = tuple(data.get('event_cursors', ()))
            for cursor in cursors:
                if (not isinstance(cursor, dict) or not isinstance(cursor['uid'], str) or not isinstance(cursor['cluster_id'], str) or not isinstance(cursor['profile'], str)
                        or type(cursor['count']) is not int or cursor['count'] < 0):
                    raise ValueError('invalid event cursor state')
                utc(datetime.fromisoformat(cursor['observed_at']))
                utc(datetime.fromisoformat(cursor.get('sampled_at', cursor['observed_at'])))
            confirmed = tuple(ConfirmedRuleState(**dict(item,
                observed_at=datetime.fromisoformat(item['observed_at']))) for item in data.get('confirmed', ()))
            if len({_key(c) for c in confirmed}) != len(confirmed):
                raise ValueError('duplicate confirmed finding state')
            return states, cursors, confirmed
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise ValueError('observation state cannot be read or validated') from exc

    def read(self):
        return self._load()[0]

    def read_confirmed(self):
        return self._load()[2]

    def event_baseline(self, *, cluster_id, profile):
        return tuple(c for c in self._load()[1] if c['cluster_id'] == cluster_id and c['profile'] == profile)

    def update(self, report):
        with profile_run_lock(self.path.parent, name=self.path.name + '.lock'):
            previous, previous_cursors, previous_confirmed = self._load()
            states = {_key(s): s for s in previous}
            executions = {e.rule_id: e for e in report.executions}
            updated_healthy = set()
            for observation in report.observations:
                if observation.cluster_id != report.cluster or observation.run_id != report.run_id:
                    raise ValueError('observation identity does not match report')
                if observation.resource_uid is not None or observation.scope != 'cluster':
                    continue
                execution = executions.get(observation.rule_id)
                if (not observation.complete or observation.health == H.UNKNOWN or not execution
                        or execution.execution_status != E.OK or not execution.coverage.complete):
                    continue
                failures = len({f.fingerprint for f in report.findings if f.rule_id == observation.rule_id
                                and f.severity in (Severity.WARN, Severity.FAIL)})
                state = ValidRuleState(observation.rule_id, observation.cluster_id, report.profile,
                    observation.scope, observation.health, failures, observation.observed_at, report.run_id)
                if _key(state) not in states or states[_key(state)].observed_at <= state.observed_at:
                    states[_key(state)] = state
                    if state.health == H.HEALTHY:
                        updated_healthy.add(_key(state))
            confirmed = {_key(c): c for c in previous_confirmed}
            for execution in report.executions:
                findings = [f for f in report.findings if f.rule_id == execution.rule_id
                            and f.severity in (Severity.WARN, Severity.FAIL)]
                key = (report.cluster, report.profile, execution.rule_id, 'cluster')
                if findings:
                    state = ConfirmedRuleState(execution.rule_id, report.cluster, report.profile,
                        'cluster', len({f.fingerprint for f in findings}),
                        max(f.observed_at for f in findings), report.run_id)
                    valid = states.get(key)
                    newer_healthy = valid and valid.health==H.HEALTHY and valid.observed_at > max(f.observed_at for f in findings)
                    if not newer_healthy and (key not in confirmed or confirmed[key].observed_at <= state.observed_at):
                        confirmed[key] = state
                elif key in updated_healthy:
                    if key in confirmed and confirmed[key].observed_at <= states[key].observed_at:
                        del confirmed[key]
            cursors = {(c['cluster_id'], c['profile'], c['uid']): c for c in previous_cursors}
            for finding in report.findings:
                if finding.rule_id == 'events.warning' and type(finding.evidence.get('count')) is int:
                    c = finding.evidence
                    key = (report.cluster, report.profile, c['event_uid'])
                    incoming = {'uid':c['event_uid'],'count':c['count'],'cluster_id':report.cluster,
                        'profile':report.profile,'observed_at':c['event_observed_at'],
                        'sampled_at':c.get('sampled_at',finding.observed_at.isoformat())}
                    old = cursors.get(key)
                    if not old or utc(datetime.fromisoformat(incoming['sampled_at'])) > utc(
                            datetime.fromisoformat(old.get('sampled_at',old['observed_at']))):
                        cursors[key] = incoming
            # Event API normally expires old objects; retain only the relevant window.
            generated = utc(datetime.fromisoformat(report.generated_at.replace('Z', '+00:00')))
            cursors = tuple(c for c in cursors.values()
                            if (generated - utc(datetime.fromisoformat(c.get('sampled_at', c['observed_at'])))).total_seconds() <= 7200)
            current = tuple(states[k] for k in sorted(states))
            data = {'schema_version': 1, 'states': to_json_value(current), 'event_cursors': cursors,
                    'confirmed': to_json_value(tuple(confirmed[k] for k in sorted(confirmed)))}
            atomic_write_text(self.path, json.dumps(data, ensure_ascii=False, allow_nan=False, indent=2))
            if not self.marker.exists():
                atomic_write_text(self.marker, '1\n')
            return current
