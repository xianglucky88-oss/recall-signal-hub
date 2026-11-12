from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping, Sequence

from .normalize import canonical_text
from .risk import SEVERITY_RANK, RiskError


class RoutingError(ValueError):
    pass


@dataclass(frozen=True)
class Subscription:
    """A stable recipient declaration.

    ``recipient_id`` never changes for a given recipient: changing it means
    removing one subscription and adding another, so queued tasks cannot be
    silently reassigned.
    """
    recipient_id: str
    markets: frozenset[str]
    min_severity: str
    endpoint: str

    @classmethod
    def create(cls, recipient_id: str, markets: Sequence[str], min_severity: str, endpoint: str) -> "Subscription":
        if not isinstance(recipient_id, str) or not recipient_id.strip():
            raise RoutingError("recipient_id must be nonempty text")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise RoutingError("endpoint must be nonempty text")
        if min_severity not in SEVERITY_RANK:
            raise RoutingError("unknown minimum severity")
        normalized_markets = frozenset(
            canonical_text(m) for m in markets if isinstance(m, str) and m.strip())
        if not normalized_markets or len(normalized_markets) != len(list(markets)):
            raise RoutingError("markets must be nonempty and unique text values")
        return cls(recipient_id.strip(), normalized_markets, min_severity, endpoint.strip())

    def matches(self, alert_summary: Mapping) -> bool:
        return (alert_summary["market"] in self.markets
                and SEVERITY_RANK[alert_summary["severity"]] >= SEVERITY_RANK[self.min_severity])


@dataclass(frozen=True)
class RouteConfig:
    """An immutable, content-addressed set of subscriptions.

    The version is derived only from the declared subscription contents, so the
    same local configuration always resolves to the same version, independently
    of when or how often it is loaded.
    """
    version: str
    subscriptions: tuple[Subscription, ...]

    @classmethod
    def from_subscriptions(cls, subscriptions: Sequence[Subscription]) -> "RouteConfig":
        if len({s.recipient_id for s in subscriptions}) != len(subscriptions):
            raise RoutingError("duplicate recipient identifiers")
        ordered = tuple(sorted(subscriptions, key=lambda s: s.recipient_id))
        canonical = json.dumps(
            [{"recipient_id": s.recipient_id, "markets": sorted(s.markets),
              "min_severity": s.min_severity, "endpoint": s.endpoint} for s in ordered],
            sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        version = hashlib.sha256(canonical.encode()).hexdigest()[:16]
        return cls(version, ordered)

    @classmethod
    def from_dict(cls, data: Mapping) -> "RouteConfig":
        entries = data.get("subscriptions")
        if not isinstance(entries, list):
            raise RoutingError("routing configuration requires a subscriptions list")
        subscriptions = [
            Subscription.create(entry["recipient_id"], entry.get("markets", []),
                                entry.get("min_severity", "low"), entry.get("endpoint", ""))
            for entry in entries]
        return cls.from_subscriptions(subscriptions)

    @classmethod
    def load(cls, path: str) -> "RouteConfig":
        with open(path, encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))

    def subscription(self, recipient_id: str) -> Subscription | None:
        for subscription in self.subscriptions:
            if subscription.recipient_id == recipient_id:
                return subscription
        return None

    def expand(self, alert_summary: Mapping) -> tuple["RouteConfig", tuple[str, ...]]:
        """Resolve recipients solely from the alert's immutable case summary."""
        recipients = tuple(s.recipient_id for s in self.subscriptions if s.matches(alert_summary))
        return self, recipients
