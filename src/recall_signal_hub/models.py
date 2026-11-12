from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class SourceDocument:
    source: str
    source_id: str
    cursor: str
    published_at: str
    payload: Mapping[str, str]


@dataclass(frozen=True)
class RecallSignal:
    source: str
    source_id: str
    published_at: str
    product: str
    brand: str
    market: str
    hazard: str
    severity: str
    evidence_hash: str


@dataclass(frozen=True)
class SourceState:
    source: str
    cursor: str | None
    consecutive_failures: int
    not_before: int


@dataclass(frozen=True)
class Alert:
    alert_id: int
    case_key: str
    revision: int
    severity: str
    payload: str
    lease_id: str
    attempts: int

    @property
    def idempotency_key(self) -> str:
        return f"recall:{self.case_key}:{self.revision}"


@dataclass(frozen=True)
class TargetAlert:
    """One deliverable task for one fixed recipient of one case revision."""
    task_id: int
    alert_id: int
    case_key: str
    revision: int
    severity: str
    payload: str
    recipient_id: str
    endpoint: str
    route_version: str
    lease_id: str
    attempts: int

    @property
    def idempotency_key(self) -> str:
        # Receiver-side identity distinguishes both the case revision and the
        # recipient, so two recipients never share a receiver-side dedup slot.
        return f"recall:{self.case_key}:{self.revision}:{self.recipient_id}"


@dataclass(frozen=True)
class BatchResult:
    accepted: int
    rejected: int
    cases_changed: int
    alerts_created: int
    cursor: str | None


@dataclass(frozen=True)
class RunResult:
    source: str
    status: str
    batch: BatchResult | None = None
    error_type: str | None = None
