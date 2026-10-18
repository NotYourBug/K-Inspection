"""Warning activity from recent monotonic Event counters, with durable cursors."""
from datetime import datetime
from .observation_models import Severity, ResourceRef, make_finding, utc
from .rule_evaluation import evaluation


WINDOW_SECONDS = 3600


def evaluate_events(snapshot, cfg):
    collection = snapshot.collections.get('events')
    baseline = snapshot.collections.get('event_baseline')
    previous = {x['uid']: x for x in baseline.items} if baseline else {}
    recent, unknown, activity = [], False, 0
    for event in collection.items if collection else ():
        if event.type != 'Warning':
            continue
        series = getattr(event, 'series', None)
        timestamp = (getattr(series, 'last_observed_time', None) or event.last_timestamp
                     or event.event_time or event.first_timestamp)
        try:
            timestamp = utc(timestamp)
            age = (collection.observed_at - timestamp).total_seconds()
        except (ValueError, TypeError):
            unknown = True
            continue
        if age < 0:  # Clock skew beyond the actual collection boundary is explicit unknown.
            unknown = True
            continue
        if age > WINDOW_SECONDS:
            continue
        count = getattr(series, 'count', None) if series else event.count
        uid = event.metadata.uid
        if type(count) is not int or count<1 or not isinstance(uid,str) or not uid:
            unknown = True
            continue
        old = previous.get(uid)
        delta, known, lower_bound = None, False, 1
        if old:
            try:
                sampled = utc(datetime.fromisoformat(old.get('sampled_at', old['observed_at'])))
                observed = utc(datetime.fromisoformat(old['observed_at']))
                elapsed = (collection.observed_at - sampled).total_seconds()
                if (0 <= elapsed <= WINDOW_SECONDS and observed <= sampled and observed <= timestamp
                        and type(old['count']) is int and 0 <= old['count'] <= count):
                    delta = count-old['count']
                    if delta or timestamp==observed:
                        lower_bound, known = delta, True
            except (KeyError, TypeError, ValueError):
                pass
        if not known and not old:
            first = getattr(event, 'first_timestamp', None) or getattr(event, 'event_time', None)
            try:
                first = utc(first)
                known = 0 <= (collection.observed_at-first).total_seconds() <= WINDOW_SECONDS and first<=timestamp
            except (TypeError, ValueError):
                pass
            # One occurrence has no unknown cumulative tail.
            if count==1 or known:
                lower_bound, known = count, True
        unknown |= not known
        activity += lower_bound
        involved = event.involved_object
        ref = ResourceRef(involved.kind or 'Event', involved.name or event.metadata.name,
            involved.namespace or event.metadata.namespace, involved.uid)
        evidence = {'event_uid':uid,'count':count,'occurrence_delta':delta,'activity_count':lower_bound,
            'activity_count_known':known,'count_is_cumulative':True,'event_observed_at':timestamp.isoformat(),
            'sampled_at':collection.observed_at.isoformat(),'window_seconds':WINDOW_SECONDS}
        recent.append((event,ref,evidence))
    findings = []
    if activity > cfg.thresholds.warning_events_max:
        for event,ref,evidence in recent:
            if evidence['activity_count']:
                findings.append(make_finding('events.warning',snapshot.cluster_id,ref,Severity.WARN,
                    event.reason or 'WarningEvent',(event.message or 'Warning Event')[:1000],
                    dict(evidence, window_activity_lower_bound=activity),collection.observed_at))
    # INFO is the serializable observation channel even below the alert threshold.
    for event,ref,evidence in recent:
        findings.append(make_finding('events.warning',snapshot.cluster_id,ref,Severity.INFO,
            'EventCursor','Observed Event counter',evidence,collection.observed_at))
    return (evaluation(snapshot,'events.warning',('events',),findings,unknown=unknown),)
