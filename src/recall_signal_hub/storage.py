from __future__ import annotations

from dataclasses import asdict
import json
import sqlite3
from pathlib import Path
import hashlib

from .models import BatchResult, ReceptionState, SourceDocument, SourceState, SnapshotPage
from .database import transaction
from .outbox import Outbox
from .normalize import NormalizationError, evidence_bytes, normalize
from .reconcile import case_key, case_fingerprint, summarize
from .risk import needs_alert
from .sources import GenerationAdvanced, SnapshotConflict


# Staged pages are streamed back one page at a time; whole generations never enter memory.
# This matches the 2000-record wire cap divided by the largest standard HTTP page (2 MB / ~2 KB rows),
# and is deliberately no larger than one received page so residency stays page-bounded.
APPLY_CHUNK_SIZE = 200


class StaleCursor(RuntimeError):
    pass


class StaleGeneration(RuntimeError):
    """A late or competing worker tried to put an older generation into force."""


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
        CREATE TABLE IF NOT EXISTS snapshot_receptions (
            source TEXT PRIMARY KEY, generation INTEGER NOT NULL, complete INTEGER NOT NULL DEFAULT 0,
            next_token TEXT, received_pages INTEGER NOT NULL DEFAULT 0,
            received_records INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS snapshot_pages (
            source TEXT NOT NULL, request_token TEXT NOT NULL, generation INTEGER NOT NULL,
            content_hash TEXT NOT NULL, next_token TEXT,
            PRIMARY KEY(source, request_token));
        CREATE TABLE IF NOT EXISTS snapshot_records (
            source TEXT NOT NULL, generation INTEGER NOT NULL, sequence INTEGER NOT NULL,
            source_id TEXT NOT NULL, published_at TEXT NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(source, generation, sequence));
        CREATE INDEX IF NOT EXISTS snapshot_records_order
            ON snapshot_records(source, generation, sequence);
        """)
        # Databases created before snapshot sources keep their integer cursors and evidence verbatim.
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(source_state)")}
        if "applied_generation" not in columns:
            self.connection.execute("ALTER TABLE source_state ADD COLUMN applied_generation INTEGER")
        self.outbox = Outbox(self.connection)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def source_state(self, source: str) -> SourceState:
        row = self.connection.execute("SELECT * FROM source_state WHERE source=?", (source,)).fetchone()
        if not row:
            return SourceState(source, None, 0, 0)
        keys = row.keys()
        applied = row["applied_generation"] if "applied_generation" in keys else None
        return SourceState(source, row["cursor"], row["failures"], row["not_before"], applied)

    def reception_state(self, source: str) -> ReceptionState | None:
        """Recoverable receive progress; it never describes evidence already in force."""
        row = self.connection.execute("SELECT * FROM snapshot_receptions WHERE source=?", (source,)).fetchone()
        if not row:
            return None
        return ReceptionState(source, row["generation"], row["received_pages"], row["received_records"],
                              bool(row["complete"]), row["next_token"])

    def record_failure(self, source: str, now: int) -> None:
        with transaction(self.connection):
            old = self.source_state(source)
            failures = old.consecutive_failures + 1
            delay = min(300, 2 ** min(failures, 9))
            self.connection.execute("""INSERT INTO source_state(source,cursor,failures,not_before)
              VALUES(?,?,?,?) ON CONFLICT(source) DO UPDATE SET failures=excluded.failures,
              not_before=excluded.not_before""", (source, old.cursor, failures, now + delay))

    def _process_documents(self, source: str, documents: list[SourceDocument]) -> tuple[int, int, set[str]]:
        """Persist evidence, isolate bad records, and upsert source claims for one page or chunk."""
        accepted = rejected = 0
        affected: set[str] = set()
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
        return accepted, rejected, affected

    def _reconcile_cases(self, affected: set[str], now: int) -> tuple[int, int]:
        """Reduce all touched cases as a unit: no transient per-record risk changes or alerts."""
        changed = alerts = 0
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
        return changed, alerts

    def commit_batch(self, source: str, expected_cursor: str | None, next_cursor: str | None,
                     documents: list[SourceDocument], now: int) -> BatchResult:
        """Incremental sources: evidence, case revisions, alerts and cursor move in one transaction."""
        accepted = rejected = changed = alerts = 0
        if int(next_cursor or "0") < int(expected_cursor or "0"):
            raise ValueError("cursor regression")
        if any(d.source != source or not (int(expected_cursor or "0") < int(d.cursor) <= int(next_cursor or "0")) for d in documents):
            raise ValueError("record does not belong to the claimed source cursor interval")
        with transaction(self.connection):
            if self.source_state(source).cursor != expected_cursor:
                raise StaleCursor("another worker advanced the source")
            accepted, rejected, affected = self._process_documents(source, documents)
            changed, alerts = self._reconcile_cases(affected, now)
            self.connection.execute("""INSERT INTO source_state(source,cursor) VALUES(?,?)
                ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor, failures=0, not_before=0""", (source, next_cursor))
        return BatchResult(accepted, rejected, changed, alerts, next_cursor)

    # -- Snapshot sources: recoverable reception is kept apart from evidence in force ----------

    def _discard_reception(self, source: str) -> None:
        self.connection.execute("DELETE FROM snapshot_records WHERE source=?", (source,))
        self.connection.execute("DELETE FROM snapshot_pages WHERE source=?", (source,))
        self.connection.execute("DELETE FROM snapshot_receptions WHERE source=?", (source,))

    def start_snapshot_reception(self, source: str, generation: int) -> None:
        """Begin a generation. Any superseded partial reception is discarded inside one transaction."""
        with transaction(self.connection):
            existing = self.connection.execute(
                "SELECT generation FROM snapshot_receptions WHERE source=?", (source,)).fetchone()
            if existing:
                raise ValueError("a snapshot reception for this source is already in progress")
            self._discard_reception(source)
            self.connection.execute("""INSERT INTO snapshot_receptions(source,generation) VALUES(?,?)""",
                (source, generation))

    def abandon_snapshot_reception(self, source: str, generation: int) -> None:
        """Drop an unfinishable generation (expired token or a higher generation appearing)."""
        with transaction(self.connection):
            existing = self.connection.execute(
                "SELECT generation FROM snapshot_receptions WHERE source=?", (source,)).fetchone()
            if existing and existing["generation"] != generation:
                raise ValueError("cannot abandon a different generation than the one in progress")
            self._discard_reception(source)

    def stage_snapshot_page(self, source: str, generation: int, request_token: str | None,
                            page: SnapshotPage) -> None:
        """Durably receive one page without touching formal evidence, cases or alerts."""
        token_key = request_token or ""
        with transaction(self.connection):
            reception = self.connection.execute(
                "SELECT * FROM snapshot_receptions WHERE source=?", (source,)).fetchone()
            if not reception:
                raise ValueError("no snapshot reception in progress")
            if reception["generation"] != generation or page.generation != generation:
                raise GenerationAdvanced("page generation does not match the generation being received")
            prior_page = self.connection.execute(
                "SELECT content_hash FROM snapshot_pages WHERE source=? AND request_token=?",
                (source, token_key)).fetchone()
            if prior_page:
                # Replaying the identical page is how a retry resumes; different contents are rejected.
                if prior_page["content_hash"] != page.content_hash:
                    raise SnapshotConflict("the same resume token returned different page contents")
                return
            max_sequence = self.connection.execute(
                "SELECT MAX(sequence) FROM snapshot_records WHERE source=? AND generation=?",
                (source, generation)).fetchone()[0] or 0
            sequences = [int(d.cursor) for d in page.documents]
            if any(sequence <= max_sequence for sequence in sequences):
                raise ValueError("snapshot page sequences must continue strictly after prior pages")
            self.connection.execute("INSERT INTO snapshot_pages VALUES(?,?,?,?,?)",
                (source, token_key, generation, page.content_hash, page.next_token))
            for document in page.documents:
                self.connection.execute(
                    "INSERT INTO snapshot_records VALUES(?,?,?,?,?,?)",
                    (source, generation, int(document.cursor), document.source_id,
                     document.published_at, json.dumps(dict(document.payload), sort_keys=True, ensure_ascii=False)))
            self.connection.execute("""UPDATE snapshot_receptions SET complete=?, next_token=?,
                received_pages=received_pages+1,
                received_records=received_records+? WHERE source=?""",
                (1 if page.next_token is None else 0, page.next_token, len(page.documents), source))

    def apply_snapshot(self, source: str, generation: int, now: int) -> BatchResult:
        """Move one fully received generation into force; compare-and-swap on the applied generation."""
        accepted = rejected = changed = alerts = 0
        with transaction(self.connection):
            state = self.source_state(source)
            if state.applied_generation is not None and generation <= state.applied_generation:
                raise StaleGeneration("an equal or newer generation is already in force")
            reception = self.connection.execute(
                "SELECT * FROM snapshot_receptions WHERE source=?", (source,)).fetchone()
            if not reception or reception["generation"] != generation or not reception["complete"]:
                raise ValueError("the claimed generation is not fully received")
            affected: set[str] = set()
            last_sequence = 0
            while True:
                rows = self.connection.execute("""SELECT sequence, source_id, published_at, payload
                    FROM snapshot_records WHERE source=? AND generation=? AND sequence>?
                    ORDER BY sequence LIMIT ?""",
                    (source, generation, last_sequence, APPLY_CHUNK_SIZE)).fetchall()
                if not rows:
                    break
                documents = [SourceDocument(source, row["source_id"], str(row["sequence"]),
                                            row["published_at"], json.loads(row["payload"])) for row in rows]
                chunk_accepted, chunk_rejected, chunk_affected = self._process_documents(source, documents)
                accepted += chunk_accepted
                rejected += chunk_rejected
                affected |= chunk_affected
                last_sequence = rows[-1]["sequence"]
                del documents, rows  # release each page-sized chunk before loading the next one
            changed, alerts = self._reconcile_cases(affected, now)
            self.connection.execute("""INSERT INTO source_state(source,cursor,failures,not_before,applied_generation)
                VALUES(?,NULL,0,0,?) ON CONFLICT(source) DO UPDATE SET failures=0, not_before=0,
                applied_generation=excluded.applied_generation""", (source, generation))
            # The generation is in force; its recoverable staging area no longer represents pending work.
            self._discard_reception(source)
        return BatchResult(accepted, rejected, changed, alerts, None, generation)

    def list_cases(self) -> list[dict]:
        rows = self.connection.execute("SELECT * FROM cases ORDER BY case_key").fetchall()
        return [{"case_key":r["case_key"], "revision":r["revision"], **json.loads(r["summary"])} for r in rows]

    def counts(self) -> dict[str, int]:
        return {table:self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("raw_evidence", "rejected_records", "cases", "source_signals", "case_revisions", "outbox")}
