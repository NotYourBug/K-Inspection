import importlib
from dataclasses import replace

import pytest

from tests.delivery_factories import batch


def identity():
    return importlib.import_module("k8s_inspector.delivery_identity")


def test_alert_order_group_key_and_receiver_are_irrelevant():
    a = batch().alerts[0]
    b = replace(a, fingerprint="f2", labels={"cluster": "other"})
    first = replace(batch(), alerts=(a, b), group_key="replica1", receiver="old")
    second = replace(first, alerts=(b, a), group_key="replica2", receiver="new")
    assert identity().canonical_content_hash(first) == identity().canonical_content_hash(second)


@pytest.mark.parametrize("status,equal", [("firing", True), ("resolved", False)])
def test_rolling_end_is_ignored_only_for_firing(status, equal):
    a = batch(status=status, ends_at="2026-10-15T00:10:00Z")
    b = batch(status=status, ends_at="2026-10-15T00:20:00Z")
    assert (identity().canonical_content_hash(a) == identity().canonical_content_hash(b)) == equal


@pytest.mark.parametrize("field,value", [
    ("annotations", {"summary": "changed"}), ("labels", {"severity": "critical"}),
    ("starts_at", "2026-10-15T01:00:00Z"), ("fingerprint", "f2"), ("status", "resolved"),
])
def test_content_or_cycle_change_has_new_hash(field, value):
    a = batch()
    b = replace(a, alerts=(replace(a.alerts[0], **{field: value}),))
    assert identity().canonical_content_hash(a) != identity().canonical_content_hash(b)


def test_truncation_is_part_of_content():
    assert identity().canonical_content_hash(batch()) != identity().canonical_content_hash(batch(truncated_alerts=1))


def test_fallback_fingerprint_and_identity_quality():
    alert = batch(fingerprint="", starts_at="").alerts[0]
    key = identity().alert_cycle_key("am", alert)
    assert key[0] == "am" and key[1].startswith("labels:") and key[2] == ""
    assert identity().identity_quality(alert) == ("missing_fingerprint", "missing_starts_at")
    assert identity().identity_quality(batch().alerts[0]) == ()
