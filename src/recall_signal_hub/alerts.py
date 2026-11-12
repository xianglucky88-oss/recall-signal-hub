from __future__ import annotations

import time
from typing import Callable, Protocol

from .models import Alert, TargetAlert
from .outbox import LeaseLost, Outbox
from .transport import HttpTransport


class AlertSink(Protocol):
    def deliver(self, alert: Alert) -> None: ...


class RecordingSink:
    """A local recipient emulator with receiver-side idempotency."""
    def __init__(self):
        self.received: dict[str, str] = {}

    def deliver(self, alert: Alert) -> None:
        self.received.setdefault(alert.idempotency_key, alert.payload)


class TargetSink(Protocol):
    def deliver(self, task: TargetAlert) -> None: ...


class RecordingTargetSink:
    """A local recipient emulator keyed by (case revision, recipient)."""
    def __init__(self):
        self.received: dict[str, str] = {}

    def deliver(self, task: TargetAlert) -> None:
        self.received.setdefault(task.idempotency_key, task.payload)

    def deliveries_for(self, recipient_id: str) -> dict[str, str]:
        suffix = f":{recipient_id}"
        return {key: payload for key, payload in self.received.items() if key.endswith(suffix)}


class RoutedWebhookSink:
    """Delivers one target task to the endpoint frozen on that task.

    The endpoint comes from the queued task snapshot, never from a freshly
    loaded configuration, so endpoint changes cannot silently reroute old tasks.
    """
    def __init__(self, transport: HttpTransport | None = None):
        self.transport = transport or HttpTransport()

    def deliver(self, task: TargetAlert) -> None:
        self.transport.request(task.endpoint, data=task.payload.encode(), headers={
            "Content-Type": "application/json",
            "Idempotency-Key": task.idempotency_key,
            "X-Recipient": task.recipient_id,
            "X-Route-Version": task.route_version})


class WebhookSink:
    def __init__(self, endpoint: str, transport: HttpTransport | None = None):
        self.endpoint = endpoint
        self.transport = transport or HttpTransport()

    def deliver(self, alert: Alert) -> None:
        self.transport.request(self.endpoint, data=alert.payload.encode(), headers={
            "Content-Type":"application/json", "Idempotency-Key":alert.idempotency_key})


class AlertDispatcher:
    def __init__(self, outbox: Outbox, sink: AlertSink, *, clock: Callable[[], int] | None = None):
        self.outbox, self.sink = outbox, sink
        self.clock = clock or (lambda: int(time.time()))

    def dispatch(self, *, limit: int = 20) -> dict[str, int]:
        result = {"delivered":0, "retry":0, "lease_lost":0}
        for alert in self.outbox.claim(self.clock(), limit=limit):
            try:
                self.sink.deliver(alert)
            except Exception as exc:
                try:
                    self.outbox.retry(alert, self.clock(), type(exc).__name__)
                    result["retry"] += 1
                except LeaseLost:
                    result["lease_lost"] += 1
                continue
            try:
                self.outbox.acknowledge(alert, self.clock())
                result["delivered"] += 1
            except LeaseLost:
                result["lease_lost"] += 1
        return result


class TargetDispatcher:
    """Dispatches per-recipient tasks with independent delivery state."""
    def __init__(self, outbox: Outbox, sink: TargetSink, *, clock: Callable[[], int] | None = None):
        self.outbox, self.sink = outbox, sink
        self.clock = clock or (lambda: int(time.time()))

    def dispatch(self, *, limit: int = 20) -> dict[str, int]:
        result = {"delivered": 0, "retry": 0, "lease_lost": 0}
        for task in self.outbox.claim_targets(self.clock(), limit=limit):
            try:
                self.sink.deliver(task)
            except Exception as exc:
                try:
                    self.outbox.retry_target(task, self.clock(), type(exc).__name__)
                    result["retry"] += 1
                except LeaseLost:
                    result["lease_lost"] += 1
                continue
            try:
                # Confirmation uses the actual completion time, never claim time.
                self.outbox.acknowledge_target(task, self.clock())
                result["delivered"] += 1
            except LeaseLost:
                result["lease_lost"] += 1
        return result
