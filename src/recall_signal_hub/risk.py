from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
import re

SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


class RiskError(ValueError):
    pass


@dataclass(frozen=True)
class HazardRule:
    rule_id: str
    severity: str
    phrase: str


@dataclass(frozen=True)
class RiskDecision:
    severity: str
    policy_version: str
    reason: str


DEFAULT_RULES = (
    HazardRule("ignition", "high", "fire"),
    HazardRule("burn", "high", "burn"),
    HazardRule("injury", "high", "injury"),
    HazardRule("shock", "high", "electric shock"),
    HazardRule("choking", "high", "choking"),
    HazardRule("sharp-edge", "medium", "laceration"),
    HazardRule("temperature", "medium", "overheat"),
    HazardRule("fall", "medium", "fall"),
    HazardRule("contamination", "medium", "contamination"),
)

# Every policy version ever used to write evidence is retained here. Old evidence
# packages embed their own rule snapshot, but this registry lets later code still
# replay historical decisions when the live policy has moved on.
POLICY_REGISTRY: dict[str, tuple[HazardRule, ...]] = {"baseline-1": DEFAULT_RULES}


def policy_rules(version: str) -> tuple[HazardRule, ...] | None:
    return POLICY_REGISTRY.get(version)


class RiskPolicy:
    """A versioned, explainable rule policy; no model or external service needed."""
    def __init__(self, rules: tuple[HazardRule, ...] = DEFAULT_RULES, version: str = "baseline-1"):
        if len({r.rule_id for r in rules}) != len(rules):
            raise RiskError("duplicate rule identifiers")
        if any(r.severity not in SEVERITY_RANK or not r.phrase.strip() for r in rules):
            raise RiskError("invalid rule")
        self.rules, self.version = rules, version

    def assess(self, hazard: str, supplied: str | None = None) -> RiskDecision:
        if supplied:
            if supplied not in SEVERITY_RANK:
                raise RiskError("unknown explicit severity")
            return RiskDecision(supplied, self.version, "explicit-publisher-severity")
        matches = [r for r in self.rules if re.search(r"(?<!\w)" + re.escape(r.phrase) + r"(?!\w)", hazard)]
        if not matches:
            return RiskDecision("low", self.version, "no-matching-hazard-rule")
        highest = max(SEVERITY_RANK[r.severity] for r in matches)
        winners = sorted(r.rule_id for r in matches if SEVERITY_RANK[r.severity] == highest)
        return RiskDecision(next(r.severity for r in matches if SEVERITY_RANK[r.severity] == highest),
                            self.version, ",".join(winners))


def needs_alert(previous: Mapping | None, current: Mapping) -> bool:
    if SEVERITY_RANK[current["severity"]] < SEVERITY_RANK["high"]:
        return False
    return previous is None or previous.get("active") is False or (
        SEVERITY_RANK[current["severity"]] > SEVERITY_RANK[previous["severity"]]
        or bool(set(current["hazards"]) - set(previous["hazards"])))
