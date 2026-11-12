from __future__ import annotations

from dataclasses import asdict
import json
import sqlite3
from pathlib import Path
import hashlib

from .models import BatchResult, SourceDocument, SourceState
from .database import transaction
from .outbox import Outbox
from .normalize import NormalizationError, evidence_bytes, normalize
from .reconcile import case_key, case_fingerprint, summarize
from .risk import needs_alert


class StaleCursor(RuntimeError):
    pass


class RecallStore:
    """Persistent collection state. One connection per worker; transactions serialize writers."""
    def __init__(self, database: str | Path):
        self.connection = sqlite3.connect(database, isolation_level=None, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS source_state (
            source TEXT PRIMARY KEY, cursor TEXT, failures INTEGER NOT NULL DEFAULT 0,
            not_before INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS raw_evidence (
            evidence_hash TEXT PRIMARY KEY, source TEXT NOT NULL, source_id TEXT NOT NULL,
            published_at TEXT NOT NULL, body BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS rejected_records (
            source TEXT NOT NULL, source_id TEXT NOT NULL, evidence_hash TEXT NOT NULL
            REFERENCES raw_evidence(evidence_hash), reason TEXT NOT NULL,
            PRIMARY KEY(source, source_id, evidence_hash));
        CREATE TABLE IF NOT EXISTS cases (
            case_key TEXT PRIMARY KEY, revision INTEGER NOT NULL,
            fingerprint TEXT NOT NULL, summary TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS source_signals (
            source TEXT NOT NULL, source_id TEXT NOT NULL, case_key TEXT NOT NULL,
            evidence_hash TEXT NOT NULL REFERENCES raw_evidence(evidence_hash),
            published_at TEXT NOT NULL, brand TEXT NOT NULL, product TEXT NOT NULL,
            market TEXT NOT NULL, hazard TEXT NOT NULL, severity TEXT NOT NULL,
            PRIMARY KEY(source, source_id));
        CREATE INDEX IF NOT EXISTS signals_by_case ON source_signals(case_key);
        CREATE TABLE IF NOT EXISTS case_revisions (
            case_key TEXT NOT NULL REFERENCES cases(case_key), revision INTEGER NOT NULL,
            summary TEXT NOT NULL, PRIMARY KEY(case_key, revision));
        """)
        self.outbox = Outbox(self.connection)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def source_state(self, source: str) -> SourceState:
        row = self.connection.execute("SELECT * FROM source_state WHERE source=?", (source,)).fetchone()
        return SourceState(source, row["cursor"], row["failures"], row["not_before"]) if row else SourceState(source, None, 0, 0)

    def record_failure(self, source: str, now: int) -> None:
        with transaction(self.connection):
            old = self.source_state(source)
            failures = old.consecutive_failures + 1
            delay = min(300, 2 ** min(failures, 9))
            self.connection.execute("""INSERT INTO source_state(source,cursor,failures,not_before)
              VALUES(?,?,?,?) ON CONFLICT(source) DO UPDATE SET failures=excluded.failures,
              not_before=excluded.not_before""", (source, old.cursor, failures, now + delay))

    def commit_batch(self, source: str, expected_cursor: str | None, next_cursor: str | None,
                     documents: list[SourceDocument], now: int) -> BatchResult:
        accepted = rejected = changed = alerts = 0
        if int(next_cursor or "0") < int(expected_cursor or "0"):
            raise ValueError("cursor regression")
        if any(d.source != source or not (int(expected_cursor or "0") < int(d.cursor) <= int(next_cursor or "0")) for d in documents):
            raise ValueError("record does not belong to the claimed source cursor interval")
        with transaction(self.connection):
            if self.source_state(source).cursor != expected_cursor:
                raise StaleCursor("another worker advanced the source")
            affected = set()
            for document in documents:
                raw = evidence_bytes(document)
                digest = hashlib.sha256(raw).hexdigest()
                self.connection.execute("INSERT OR IGNORE INTO raw_evidence VALUES(?,?,?,?,?)",
                    (digest, source, document.source_id, document.published_at, raw))
                try:
                    signal = normalize(document)
                except NormalizationError as exc:
                    self.connection.execute("INSERT OR IGNORE INTO rejected_records VALUES(?,?,?,?)",
                        (source, document.source_id, digest, str(exc)))
                    rejected += 1
                    continue
                key = case_key(signal)
                old = self.connection.execute("SELECT case_key FROM source_signals WHERE source=? AND source_id=?",
                    (source, document.source_id)).fetchone()
                if old:
                    affected.add(old["case_key"])
                fields = asdict(signal)
                self.connection.execute("""INSERT INTO source_signals VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source,source_id) DO UPDATE SET case_key=excluded.case_key,
                    evidence_hash=excluded.evidence_hash,published_at=excluded.published_at,
                    brand=excluded.brand,product=excluded.product,market=excluded.market,
                    hazard=excluded.hazard,severity=excluded.severity""",
                    (source, document.source_id, key, digest, signal.published_at,
                     fields["brand"], fields["product"], fields["market"], fields["hazard"], fields["severity"]))
                affected.add(key)
                accepted += 1
            # Reduce the full page as a unit: no transient per-record risk changes.
            for key in sorted(affected):
                rows = self.connection.execute("SELECT * FROM source_signals WHERE case_key=?", (key,)).fetchall()
                prior = self.connection.execute("SELECT * FROM cases WHERE case_key=?", (key,)).fetchone()
                if rows:
                    summary = summarize(rows)
                elif prior:
                    summary = {**json.loads(prior["summary"]), "active": False}
                else:
                    continue
                fingerprint = case_fingerprint(summary)
                if prior and prior["fingerprint"] == fingerprint:
                    continue
                revision = prior["revision"] + 1 if prior else 1
                payload = json.dumps(summary, sort_keys=True, ensure_ascii=False)
                self.connection.execute("""INSERT INTO cases VALUES(?,?,?,?) ON CONFLICT(case_key)
                   DO UPDATE SET revision=excluded.revision,fingerprint=excluded.fingerprint,summary=excluded.summary""",
                   (key, revision, fingerprint, payload))
                self.connection.execute("INSERT INTO case_revisions VALUES(?,?,?)", (key, revision, payload))
                changed += 1
                previous_summary = json.loads(prior["summary"]) if prior else None
                if rows and needs_alert(previous_summary, summary):
                    self.outbox.enqueue(key, revision, summary["severity"], payload, now)
                    alerts += 1
            self.connection.execute("""INSERT INTO source_state(source,cursor) VALUES(?,?)
                ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor, failures=0, not_before=0""", (source, next_cursor))
        return BatchResult(accepted, rejected, changed, alerts, next_cursor)

    def list_cases(self) -> list[dict]:
        rows = self.connection.execute("SELECT * FROM cases ORDER BY case_key").fetchall()
        return [{"case_key":r["case_key"], "revision":r["revision"], **json.loads(r["summary"])} for r in rows]

    def counts(self) -> dict[str, int]:
        return {table:self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("raw_evidence", "rejected_records", "cases", "source_signals", "case_revisions", "outbox")}
