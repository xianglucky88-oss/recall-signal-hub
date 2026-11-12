import pytest

from recall_signal_hub.risk import HazardRule, RiskError, RiskPolicy


def test_policy_produces_versioned_reason_and_uses_highest_risk():
    decision=RiskPolicy(version="test-policy").assess("overheat and fire")
    assert (decision.severity,decision.policy_version,decision.reason)==("high","test-policy","ignition")


def test_word_boundaries_avoid_incidental_substrings():
    assert RiskPolicy().assess("fireplace decoration").severity=="low"
    assert RiskPolicy().assess("fire").severity=="high"


def test_configured_policy_is_independent_from_publisher_adapter():
    policy=RiskPolicy((HazardRule("magnet", "critical", "loose magnet"),),version="magnetic-products")
    assert policy.assess("loose magnet").severity=="critical"
    assert policy.assess("fire").severity=="low"


@pytest.mark.parametrize("rules",[
    (HazardRule("same","high","fire"),HazardRule("same","low","label")),
    (HazardRule("bad","urgent","fire"),),
    (HazardRule("bad","high"," "),),
])
def test_invalid_policy_fails_before_collection(rules):
    with pytest.raises(RiskError):RiskPolicy(rules)
