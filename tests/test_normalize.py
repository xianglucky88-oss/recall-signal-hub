from dataclasses import replace

import pytest

from recall_signal_hub.normalize import NormalizationError, canonical_text, normalize
from recall_signal_hub.reconcile import case_key, case_fingerprint, summarize
from recall_signal_hub.risk import needs_alert


def test_unicode_and_width_normalization():
    assert canonical_text("  ＡＣＭＥ—电水壶  １００ ")=="acme 电水壶 100"
    assert canonical_text("电水壶")!=canonical_text("热水器")


def test_evidence_hash_ignores_transport_cursor(document):
    first=document();second=replace(first,cursor="9")
    assert normalize(first).evidence_hash==normalize(second).evidence_hash
    assert normalize(first).evidence_hash!=normalize(document(hazard="burn")).evidence_hash


@pytest.mark.parametrize("changes",[{"product":""},{"brand":"---"},{"published":"2026-10-01"},{"severity":"urgent"}])
def test_rejects_unusable_records(document,changes):
    with pytest.raises(NormalizationError):normalize(document(**changes))


def test_same_case_across_sources_but_not_markets(document):
    assert case_key(normalize(document()))==case_key(normalize(document(source="manufacturer",brand="NORTHLINE")))
    assert case_key(normalize(document()))!=case_key(normalize(document(market="US")))


def test_merge_order_and_risk_are_stable(document):
    from dataclasses import asdict
    a=asdict(normalize(document(hazard="overheat")))
    b=asdict(normalize(document(source="manufacturer",hazard="fire")))
    first=summarize([a,b]);second=summarize([b,a])
    assert first==second and first["severity"]=="high"
    assert case_fingerprint(first)==case_fingerprint(second)


def test_alert_policy_does_not_page_for_identical_or_decreasing_risk():
    high={"severity":"high","hazards":["fire"]}
    assert needs_alert(None,high)
    assert not needs_alert(high,high)
    assert not needs_alert(high,{"severity":"medium","hazards":["fire"]})
    assert needs_alert(high,{"severity":"critical","hazards":["fire"]})
    assert needs_alert(high,{"severity":"high","hazards":["fire","choking"]})
