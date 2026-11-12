from __future__ import annotations

from dataclasses import asdict
import json
import os
import sqlite3
from pathlib import Path
import hashlib

from .models import BatchResult, SourceDocument, SourceState
from .database import transaction
from .evidence import (build_manifest, verify_evidence_package, write_evidence_package)
from .normalize import NormalizationError, evidence_bytes, normalize_record
from .outbox import Outbox
from .reconcile import case_key, case_fingerprint, summarize
from .risk import needs_alert, policy_rules


class StaleCursor(RuntimeError):
    pass


class IncompleteEvidenceChain(RuntimeError):
    """A revision predates closure snapshots (or lost one); no plausible-complete package may be built."""


class EvidenceExportFailed(RuntimeError):
    """Package assembly or self-verification failed; no accepted artifact is left behind."""


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
            policy_version TEXT NOT NULL, decision_reason TEXT NOT NULL,
            supplied_severity TEXT,
            PRIMARY KEY(source, source_id));
        CREATE INDEX IF NOT EXISTS signals_by_case ON source_signals(case_key);
        CREATE TABLE IF NOT EXISTS case_revisions (
            case_key TEXT NOT NULL REFERENCES cases(case_key), revision INTEGER NOT NULL,
            summary TEXT NOT NULL, PRIMARY KEY(case_key, revision));
        CREATE TABLE IF NOT EXISTS revision_evidence (
            case_key TEXT NOT NULL, revision INTEGER NOT NULL, claim_id TEXT NOT NULL,
            source TEXT NOT NULL, source_id TEXT NOT NULL, evidence_hash TEXT NOT NULL,
            published_at TEXT NOT NULL, brand TEXT NOT NULL, product TEXT NOT NULL,
            market TEXT NOT NULL, hazard TEXT NOT NULL, severity TEXT NOT NULL,
            policy_version TEXT NOT NULL, decision_reason TEXT NOT NULL,
            supplied_severity TEXT,
            PRIMARY KEY(case_key, revision, claim_id),
            FOREIGN KEY(case_key, revision) REFERENCES case_revisions(case_key, revision));
        """)
        self._migrate_evidence_columns()
        self.outbox = Outbox(self.connection)

    def _migrate_evidence_columns(self) -> None:
        """Additive migration for databases created before evidence closures existed.

        Old rows get NULL rule-identity columns; they keep working for collection and
        alerts but their revisions cannot be exported as complete evidence packages.
        """
        columns = {r["name"] for r in self.connection.execute("PRAGMA table_info(source_signals)")}
        if "policy_version" not in columns:
            self.connection.execute("ALTER TABLE source_signals ADD COLUMN policy_version TEXT")
        if "decision_reason" not in columns:
            self.connection.execute("ALTER TABLE source_signals ADD COLUMN decision_reason TEXT")
        if "supplied_severity" not in columns:
            self.connection.execute("ALTER TABLE source_signals ADD COLUMN supplied_severity TEXT")

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
                    record = normalize_record(document)
                    signal = record.signal
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
                self.connection.execute("""INSERT INTO source_signals
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source,source_id) DO UPDATE SET case_key=excluded.case_key,
                    evidence_hash=excluded.evidence_hash,published_at=excluded.published_at,
                    brand=excluded.brand,product=excluded.product,market=excluded.market,
                    hazard=excluded.hazard,severity=excluded.severity,
                    policy_version=excluded.policy_version,decision_reason=excluded.decision_reason,
                    supplied_severity=excluded.supplied_severity""",
                    (source, document.source_id, key, digest, signal.published_at,
                     fields["brand"], fields["product"], fields["market"], fields["hazard"], fields["severity"],
                     record.policy_version, record.decision_reason, record.supplied_severity))
                affected.add(key)
                accepted += 1
            # Reduce the full page as a unit: no transient per-record risk changes.
            for key in sorted(affected):
                rows = self.connection.execute("SELECT * FROM source_signals WHERE case_key=?", (key,)).fetchall()
                prior = self.connection.execute("SELECT * FROM cases WHERE case_key=?", (key,)).fetchone()
                if rows:
                    summary = summarize(rows)
                    derivation = "merged-claims"
                elif prior:
                    summary = {**json.loads(prior["summary"]), "active": False}
                    derivation = "deactivated-no-claims"
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
                # Freeze the exact evidence closure this revision was derived from, inside the
                # same transaction. Later overwrites of source_signals cannot rewrite history.
                # A retirement revision carries an empty closure: its summary already preserves
                # the last merged business state and no claim participates in this derivation.
                for order, row in enumerate(sorted(rows, key=lambda r: (r["source"], r["source_id"]))):
                    claim_id = f"{revision}:{order:04d}:{row['source']}:{row['source_id']}"
                    self.connection.execute("""INSERT INTO revision_evidence
                        (case_key,revision,claim_id,source,source_id,evidence_hash,published_at,
                         brand,product,market,hazard,severity,policy_version,decision_reason,supplied_severity)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (key, revision, claim_id, row["source"], row["source_id"], row["evidence_hash"],
                         row["published_at"], row["brand"], row["product"], row["market"], row["hazard"],
                         row["severity"], row["policy_version"], row["decision_reason"],
                         row["supplied_severity"]))
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

    def list_revisions(self, case_key_value: str) -> list[dict]:
        rows = self.connection.execute(
            "SELECT case_key,revision,summary FROM case_revisions WHERE case_key=? ORDER BY revision",
            (case_key_value,)).fetchall()
        return [{"case_key":r["case_key"], "revision":r["revision"], **json.loads(r["summary"])} for r in rows]

    def export_evidence_package(self, case_key_value: str, revision: int, target: str | Path) -> dict:
        """Freeze an export on the requested revision; concurrent collection cannot move it.

        Reads run on a separate read-only connection inside one snapshot transaction.
        The package is built in a sibling temp file and self-verified before it replaces
        the destination, so a failed export never leaves an acceptable-looking artifact.
        """
        target = Path(target)
        read_only = sqlite3.connect(f"file:{self.connection.execute('PRAGMA database_list').fetchone()[2]}?mode=ro",
                                    uri=True, isolation_level=None)
        read_only.row_factory = sqlite3.Row
        temp_target = target.with_name(f".{target.name}.tmp-{os.getpid()}-{hashlib.sha1(os.urandom(16)).hexdigest()[:12]}")
        try:
            read_only.execute("BEGIN")
            revision_row = read_only.execute(
                "SELECT * FROM case_revisions WHERE case_key=? AND revision=?",
                (case_key_value, revision)).fetchone()
            if revision_row is None:
                raise EvidenceExportFailed(f"revision {revision} of case {case_key_value} does not exist")
            closure = read_only.execute(
                "SELECT * FROM revision_evidence WHERE case_key=? AND revision=? ORDER BY claim_id",
                (case_key_value, revision)).fetchall()
            summary = json.loads(revision_row["summary"])
            derivation = "deactivated-no-claims" if summary.get("active") is False else "merged-claims"
            # Revisions written before closure snapshots existed cannot be reconstructed honestly.
            # An empty closure is only legitimate for a retirement (deactivated) revision.
            if not closure and derivation != "deactivated-no-claims":
                raise IncompleteEvidenceChain(
                    f"case {case_key_value} revision {revision} has no frozen evidence closure; "
                    "the evidence chain is incomplete and no package was produced")
            claims = [{k: row[k] for k in
                       ("claim_id", "source", "source_id", "evidence_hash", "published_at",
                        "brand", "product", "market", "hazard", "severity",
                        "policy_version", "decision_reason", "supplied_severity")}
                      | {"case_key": case_key_value} for row in closure]
            digests = sorted({row["evidence_hash"] for row in closure})
            evidence_rows = read_only.execute(
                f"SELECT evidence_hash,source,source_id,published_at,body FROM raw_evidence "
                f"WHERE evidence_hash IN ({','.join('?' for _ in digests)})", digests).fetchall() if digests else []
            present = {row["evidence_hash"] for row in evidence_rows}
            missing = sorted(set(digests) - present)
            if missing:
                raise IncompleteEvidenceChain(
                    f"case {case_key_value} revision {revision} references missing raw evidence {missing}; "
                    "the evidence chain is incomplete and no package was produced")
            evidence = [{"evidence_hash": r["evidence_hash"], "source": r["source"],
                         "source_id": r["source_id"], "published_at": r["published_at"]}
                        for r in evidence_rows]
            bodies = {r["evidence_hash"]: r["body"] for r in evidence_rows}
            versions = sorted({row["policy_version"] for row in closure})
            policies = []
            for version in versions:
                rules = policy_rules(version)
                if rules is None:
                    # Refuse rather than ship a package whose risk decisions cannot be replayed.
                    raise IncompleteEvidenceChain(
                        f"case {case_key_value} revision {revision} depends on risk policy {version!r} "
                        "whose rules are not retained by this deployment; the evidence chain is incomplete")
                policies.append({"version": version,
                                 "rules": [{"rule_id": r.rule_id, "severity": r.severity, "phrase": r.phrase}
                                           for r in rules]})
            # The package is bound to the requested revision's fingerprint, not the current
            # case row, so later concurrent collection cannot move what this package means.
            revision_fingerprint = _revision_fingerprint(summary, derivation)
            manifest = build_manifest(case_key_value=case_key_value, revision=revision, summary=summary,
                                      fingerprint=revision_fingerprint,
                                      claims=claims, evidence=evidence, policies=policies,
                                      derivation=derivation)
            read_only.execute("COMMIT")
            write_evidence_package(temp_target, manifest, bodies)
            report = verify_evidence_package(temp_target)
            if not report.ok:
                raise EvidenceExportFailed(
                    "self-verification rejected the assembled package: "
                    + "; ".join(i.code for i in report.issues))
            os.replace(temp_target, target)
            return {"path": str(target), "package_digest": manifest["package_digest"],
                    "claims": len(claims), "evidence": len(bodies)}
        except (IncompleteEvidenceChain, EvidenceExportFailed):
            raise
        except Exception as exc:
            raise EvidenceExportFailed(f"export failed: {type(exc).__name__}") from None
        finally:
            try:
                read_only.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            read_only.close()
            if temp_target.exists():
                temp_target.unlink()

    def counts(self) -> dict[str, int]:
        return {table:self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("raw_evidence", "rejected_records", "cases", "source_signals",
                              "case_revisions", "revision_evidence", "outbox")}


def _revision_fingerprint(summary: dict, derivation: str) -> str:
    # Mirrors the fingerprint used when the revision row was written: the active
    # flag is excluded so retirement never changes the underlying business fingerprint.
    base = {k: v for k, v in summary.items() if k != "active"} if derivation == "deactivated-no-claims" else summary
    return case_fingerprint(base)
