"""Warning events in a bounded observation window; cumulative counts stay explicit."""
from datetime import datetime
from .observation_models import Severity, make_finding, utc
from .rule_evaluation import evaluation
from .observation_models import ResourceRef


def evaluate_events(snapshot, cfg):
    collection = snapshot.collections.get('events')
    recent, unknown = [], False
    baseline = snapshot.collections.get('event_baseline')
    previous = {x['uid']: x for x in baseline.items} if baseline else {}
    for event in collection.items if collection else ():
        if event.type != 'Warning':
            continue
        series = getattr(event, 'series', None)
        timestamp = (getattr(series, 'last_observed_time', None) or event.last_timestamp
                     or event.event_time or event.first_timestamp)
        try:
            age = (snapshot.started_at - utc(timestamp)).total_seconds()
        except (ValueError, TypeError):
            unknown = True
            continue
        if age < 0:
            unknown = True
            continue
        if age > 3600:
            continue
        count = getattr(series, 'count', None) if series else event.count
        old = previous.get(event.metadata.uid)
        delta = max(0, count - old['count']) if old and type(count) is int else None
        if old and delta == 0 and timestamp <= datetime.fromisoformat(old['observed_at']):
            continue
        recent.append((event, timestamp, count, delta))
    findings = []
    if len(recent) > cfg.thresholds.warning_events_max:
        for event, timestamp, count, delta in recent:
            involved = event.involved_object
            ref = ResourceRef(involved.kind or 'Event', involved.name or event.metadata.name,
                involved.namespace or event.metadata.namespace, involved.uid)
            findings.append(make_finding('events.warning', snapshot.cluster_id, ref, Severity.WARN,
                event.reason or 'WarningEvent', (event.message or 'Warning Event')[:1000],
                {'event_uid': event.metadata.uid, 'count': count, 'occurrence_delta': delta,
                 'count_is_cumulative': True, 'event_observed_at': utc(timestamp).isoformat(), 'window_seconds': 3600}, collection.observed_at))
    return (evaluation(snapshot, 'events.warning', ('events',), findings, unknown=unknown),)
