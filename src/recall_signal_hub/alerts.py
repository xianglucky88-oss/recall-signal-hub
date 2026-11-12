from __future__ import annotations

import time
from typing import Callable, Protocol

from .models import Alert
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
