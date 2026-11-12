from __future__ import annotations

from dataclasses import dataclass

from .outbox import Outbox
from .routing import RouteConfig


@dataclass(frozen=True)
class TaskDrift:
    """A queued task whose frozen snapshot no longer matches current configuration."""
    task_id: int
    alert_id: int
    recipient_id: str
    kind: str  # "endpoint-changed" or "recipient-removed"
    frozen_endpoint: str
    current_endpoint: str | None


class RouteAdministrator:
    """Deterministic handling of routing configuration changes.

    Loading a new :class:`RouteConfig` never mutates queued tasks: tasks keep
    the recipient, endpoint snapshot and route version they were expanded with.
    Drift is only *reported* here, and every change to a queued task is an
    explicit, audited operator action.
    """
    def __init__(self, outbox: Outbox):
        self.outbox = outbox

    def detect_drift(self, current: RouteConfig, *, include_settled: bool = False) -> list[TaskDrift]:
        rows = self.outbox.connection.execute("SELECT * FROM outbox_targets ORDER BY task_id").fetchall()
        drift: list[TaskDrift] = []
        for row in rows:
            if not include_settled and row["status"] == "delivered":
                continue
            subscription = current.subscription(row["recipient_id"])
            if subscription is None:
                drift.append(TaskDrift(row["task_id"], row["alert_id"], row["recipient_id"],
                                       "recipient-removed", row["endpoint"], None))
            elif subscription.endpoint != row["endpoint"]:
                drift.append(TaskDrift(row["task_id"], row["alert_id"], row["recipient_id"],
                                       "endpoint-changed", row["endpoint"], subscription.endpoint))
        return drift

    def pause_removed_recipients(self, current: RouteConfig, now: int) -> list[int]:
        """Explicit handling for removed recipients: freeze their open tasks."""
        paused = []
        for item in self.detect_drift(current):
            if item.kind != "recipient-removed":
                continue
            try:
                self.outbox.pause_target(item.task_id, now,
                                         f"recipient {item.recipient_id} removed from route {current.version}")
            except RuntimeError:
                continue
            paused.append(item.task_id)
        return paused

    def repoint_changed_endpoints(self, current: RouteConfig, now: int) -> list[int]:
        """Explicit handling for endpoint changes: move open tasks deliberately."""
        repointed = []
        for item in self.detect_drift(current):
            if item.kind != "endpoint-changed":
                continue
            try:
                self.outbox.repoint_target(item.task_id, item.current_endpoint, current.version, now,
                                           f"endpoint changed from route {current.version}")
            except RuntimeError:
                continue
            repointed.append(item.task_id)
        return repointed
