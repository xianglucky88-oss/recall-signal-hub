from __future__ import annotations

import sqlite3
from uuid import uuid4

from .database import transaction
from .models import Alert, TargetAlert


class LeaseLost(RuntimeError):
    pass


class Outbox:
    """A delivery queue boundary sharing only its caller's SQLite transaction.

    Two persistence modes share one state machine:

    * Without routing configuration the queue keeps the original single-target
      ``outbox`` table and its ``Alert`` contract.
    * With routing configuration the immutable ``outbox_alerts`` row is expanded,
      before it can ever be claimed, into one ``outbox_targets`` task per matched
      recipient. Each target owns its own lease, attempts and delivery state.
    """
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
        CREATE TABLE IF NOT EXISTS outbox_alerts (
            alert_id INTEGER PRIMARY KEY, case_key TEXT NOT NULL, revision INTEGER NOT NULL,
            severity TEXT NOT NULL, payload TEXT NOT NULL,
            route_version TEXT, target_count INTEGER NOT NULL DEFAULT 0,
            expanded_at INTEGER,
            UNIQUE(case_key, revision),
            FOREIGN KEY(case_key, revision) REFERENCES case_revisions(case_key, revision));
        CREATE TABLE IF NOT EXISTS outbox_targets (
            task_id INTEGER PRIMARY KEY, alert_id INTEGER NOT NULL
                REFERENCES outbox_alerts(alert_id),
            recipient_id TEXT NOT NULL, endpoint TEXT NOT NULL,
            route_version TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', available_at INTEGER NOT NULL DEFAULT 0,
            lease_id TEXT, lease_until INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            UNIQUE(alert_id, recipient_id));
        CREATE INDEX IF NOT EXISTS targets_ready ON outbox_targets(status, available_at, lease_until);
        CREATE TABLE IF NOT EXISTS target_task_events (
            event_id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL
                REFERENCES outbox_targets(task_id),
            event TEXT NOT NULL, detail TEXT NOT NULL, created_at INTEGER NOT NULL);
        """)

    # ------------------------------------------------------------------ enqueue
    def enqueue(self, case_key: str, revision: int, severity: str, payload: str, now: int) -> None:
        """Legacy single-target enqueue used when no routing configuration exists."""
        if not self.connection.in_transaction:
            raise RuntimeError("outbox enqueue requires its caller's transaction")
        self.connection.execute("""INSERT INTO outbox(case_key,revision,severity,payload,available_at)
            VALUES(?,?,?,?,?)""", (case_key, revision, severity, payload, now))

    def enqueue_routed(self, case_key: str, revision: int, severity: str, payload: str,
                       route_version: str, recipients: tuple[str, ...], endpoints: dict[str, str],
                       now: int) -> int:
        """Persist the immutable alert and freeze its target set in one transaction.

        The recipient list, the endpoint snapshot and the subscription
        configuration version are fixed before any task can become deliverable;
        later subscription changes never touch these rows.
        """
        if not self.connection.in_transaction:
            raise RuntimeError("outbox enqueue requires its caller's transaction")
        cur = self.connection.execute("""INSERT INTO outbox_alerts
            (case_key,revision,severity,payload,route_version,target_count,expanded_at)
            VALUES(?,?,?,?,?,?,?)""",
            (case_key, revision, severity, payload, route_version, len(recipients), now))
        alert_id = cur.lastrowid
        self.connection.executemany("""INSERT INTO outbox_targets
            (alert_id,recipient_id,endpoint,route_version,available_at)
            VALUES(?,?,?,?,?)""",
            [(alert_id, recipient, endpoints[recipient], route_version, now)
             for recipient in dict.fromkeys(recipients)])
        return alert_id

    # ------------------------------------------------------------- legacy queue
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

    # ------------------------------------------------------------- target queue
    def claim_targets(self, now: int, *, limit: int = 20, lease_seconds: int = 30) -> list[TargetAlert]:
        if limit < 1 or lease_seconds < 1:
            raise ValueError("claim bounds must be positive")
        with transaction(self.connection):
            rows = self.connection.execute("""SELECT t.*, a.case_key, a.revision, a.severity, a.payload
                FROM outbox_targets t JOIN outbox_alerts a ON a.alert_id=t.alert_id
                WHERE (t.status='pending' AND t.available_at<=?)
                   OR (t.status='leased' AND t.lease_until<=?)
                ORDER BY t.task_id LIMIT ?""", (now, now, limit)).fetchall()
            claimed = []
            for row in rows:
                lease_id = uuid4().hex
                self.connection.execute("""UPDATE outbox_targets SET status='leased', lease_id=?,
                    lease_until=?, attempts=attempts+1 WHERE task_id=?""",
                    (lease_id, now + lease_seconds, row["task_id"]))
                claimed.append(TargetAlert(
                    row["task_id"], row["alert_id"], row["case_key"], row["revision"], row["severity"],
                    row["payload"], row["recipient_id"], row["endpoint"], row["route_version"],
                    lease_id, row["attempts"] + 1))
            return claimed

    def _finish_target(self, task: TargetAlert, now: int, error_type: str | None) -> None:
        with transaction(self.connection):
            if error_type:
                delay = min(300, 2 ** min(task.attempts, 9))
                args = ('pending', now + delay, error_type, task.task_id, task.lease_id, now)
            else:
                args = ('delivered', now, None, task.task_id, task.lease_id, now)
            result = self.connection.execute("""UPDATE outbox_targets SET status=?,available_at=?,
                last_error=?,lease_id=NULL,lease_until=NULL
                WHERE task_id=? AND status='leased' AND lease_id=? AND lease_until>?""", args)
            if result.rowcount != 1:
                raise LeaseLost("target lease expired or was reclaimed")

    def acknowledge_target(self, task: TargetAlert, now: int) -> None:
        self._finish_target(task, now, None)

    def retry_target(self, task: TargetAlert, now: int, error_type: str) -> None:
        self._finish_target(task, now, error_type)

    def pause_target(self, task_id: int, now: int, reason: str) -> None:
        """Freeze a queued task against an explicit handling decision.

        Pausing is an explicit status transition, not a silent reroute: the
        task keeps its original recipient, endpoint snapshot and route version,
        leaves the claim/lease state machine, and is never reclaimed until an
        operator resumes it.
        """
        with transaction(self.connection):
            result = self.connection.execute("""UPDATE outbox_targets
                SET status='paused', available_at=?, last_error=?, lease_id=NULL, lease_until=NULL
                WHERE task_id=? AND status IN ('pending','leased')""", (now, reason, task_id))
            if result.rowcount != 1:
                raise LeaseLost("task already settled or paused")
            self.connection.execute("""INSERT INTO target_task_events(task_id,event,detail,created_at)
                VALUES(?,?,?,?)""", (task_id, "paused", reason, now))

    def resume_target(self, task_id: int, now: int) -> None:
        with transaction(self.connection):
            result = self.connection.execute("""UPDATE outbox_targets
                SET status='pending', available_at=?, lease_id=NULL, lease_until=NULL
                WHERE task_id=? AND status='paused'""", (now, task_id))
            if result.rowcount != 1:
                raise LeaseLost("task is not paused")
            self.connection.execute("""INSERT INTO target_task_events(task_id,event,detail,created_at)
                VALUES(?,?,?,?)""", (task_id, "resumed", "operator resume", now))

    def repoint_target(self, task_id: int, new_endpoint: str, route_version: str, now: int, reason: str) -> None:
        """Explicitly move one queued task to a changed endpoint.

        This never happens implicitly when routing configuration changes; it is
        an operator decision recorded as an audit event. The recipient identity
        and the receiver-side idempotency key stay unchanged, so the recipient
        still dedupes redeliveries of the same case revision.
        """
        if not isinstance(new_endpoint, str) or not new_endpoint.strip():
            raise ValueError("new endpoint must be nonempty text")
        with transaction(self.connection):
            result = self.connection.execute("""UPDATE outbox_targets
                SET endpoint=?, route_version=?, status='pending', available_at=?,
                    lease_id=NULL, lease_until=NULL
                WHERE task_id=? AND status IN ('pending','paused','leased')""",
                (new_endpoint.strip(), route_version, now, task_id))
            if result.rowcount != 1:
                raise LeaseLost("task cannot be repointed in its current state")
            self.connection.execute("""INSERT INTO target_task_events(task_id,event,detail,created_at)
                VALUES(?,?,?,?)""",
                (task_id, "repointed", f"{reason} -> {new_endpoint.strip()} @ {route_version}", now))

    def task_events(self, task_id: int) -> list[sqlite3.Row]:
        return self.connection.execute("""SELECT * FROM target_task_events
            WHERE task_id=? ORDER BY event_id""", (task_id,)).fetchall()

    def target_states(self, alert_id: int) -> list[sqlite3.Row]:
        return self.connection.execute("""SELECT * FROM outbox_targets
            WHERE alert_id=? ORDER BY task_id""", (alert_id,)).fetchall()

    def find_target_task(self, alert_id: int, recipient_id: str) -> sqlite3.Row | None:
        return self.connection.execute("""SELECT * FROM outbox_targets
            WHERE alert_id=? AND recipient_id=?""", (alert_id, recipient_id)).fetchone()
