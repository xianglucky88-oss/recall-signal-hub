from __future__ import annotations

import sqlite3
from uuid import uuid4

from .database import transaction
from .models import Alert


class LeaseLost(RuntimeError):
    pass


class Outbox:
    """A delivery queue boundary sharing only its caller's SQLite transaction."""
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS outbox (
            alert_id INTEGER PRIMARY KEY, case_key TEXT NOT NULL, revision INTEGER NOT NULL,
            severity TEXT NOT NULL, payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', available_at INTEGER NOT NULL DEFAULT 0,
            lease_id TEXT, lease_until INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, UNIQUE(case_key, revision),
            FOREIGN KEY(case_key, revision) REFERENCES case_revisions(case_key, revision));
        CREATE INDEX IF NOT EXISTS outbox_ready ON outbox(status, available_at, lease_until);
        """)

    def enqueue(self, case_key: str, revision: int, severity: str, payload: str, now: int) -> None:
        if not self.connection.in_transaction:
            raise RuntimeError("outbox enqueue requires its caller's transaction")
        self.connection.execute("""INSERT INTO outbox(case_key,revision,severity,payload,available_at)
            VALUES(?,?,?,?,?)""", (case_key, revision, severity, payload, now))

    def claim(self, now: int, *, limit: int = 20, lease_seconds: int = 30) -> list[Alert]:
        if limit < 1 or lease_seconds < 1:
            raise ValueError("claim bounds must be positive")
        with transaction(self.connection):
            rows = self.connection.execute("""SELECT * FROM outbox WHERE
                (status='pending' AND available_at<=?) OR (status='leased' AND lease_until<=?)
                ORDER BY alert_id LIMIT ?""", (now, now, limit)).fetchall()
            claimed = []
            for row in rows:
                lease_id = uuid4().hex
                self.connection.execute("""UPDATE outbox SET status='leased', lease_id=?, lease_until=?,
                   attempts=attempts+1 WHERE alert_id=?""", (lease_id, now + lease_seconds, row["alert_id"]))
                claimed.append(Alert(row["alert_id"],row["case_key"],row["revision"],row["severity"],
                                     row["payload"],lease_id,row["attempts"] + 1))
            return claimed

    def _finish(self, alert: Alert, now: int, error_type: str | None = None) -> None:
        with transaction(self.connection):
            if error_type:
                delay = min(300, 2 ** min(alert.attempts, 9))
                args = ('pending', now + delay, error_type, alert.alert_id, alert.lease_id, now)
            else:
                args = ('delivered', now, None, alert.alert_id, alert.lease_id, now)
            result = self.connection.execute("""UPDATE outbox SET status=?,available_at=?,last_error=?,
                lease_id=NULL,lease_until=NULL WHERE alert_id=? AND status='leased' AND lease_id=? AND lease_until>?""", args)
            if result.rowcount != 1:
                raise LeaseLost("alert lease expired or was reclaimed")

    def acknowledge(self, alert: Alert, now: int) -> None:
        self._finish(alert, now)

    def retry(self, alert: Alert, now: int, error_type: str) -> None:
        self._finish(alert, now, error_type)
