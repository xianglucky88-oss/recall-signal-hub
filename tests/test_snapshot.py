from __future__ import annotations

import gc
import json
import weakref

import pytest

from recall_signal_hub.pipeline import CollectionPipeline
from recall_signal_hub.sources import (
    ContinuationExpired,
    SnapshotConflict,
    SnapshotHttpSource,
    decode_snapshot_page,
    SourceError,
)
from recall_signal_hub.storage import RecallStore, StaleGeneration
from recall_signal_hub.transport import TransportError


# ---------------------------------------------------------------------------
# Test doubles: an in-process snapshot publisher with 410/conflict injection,
# and an HTTP transport double that maps the same conditions onto status codes.
# ---------------------------------------------------------------------------


class TokenExpired(Exception):
    pass


class ScriptedSnapshotPublisher:
    """Pages are immutable per token; a 410 or a content swap can be injected."""

    def __init__(self, name: str = "snap"):
        self.name = name
        self.generations: dict[int, list[dict]] = {}
        self.page_size = 2
        self.expired_tokens: set[str] = set()
        self.conflict_token: str | None = None
        self.requests: list[str | None] = []

    def set_generation(self, generation: int, records: list[dict]):
        self.generations[generation] = records

    @staticmethod
    def _token(generation: int, index: int) -> str:
        # Opaque in shape even though the test publisher happens to know its own layout.
        return f"g{generation}-page{index}-opaque"

    def _wire(self, generation: int, index: int):
        records = self.generations[generation]
        start = index * self.page_size
        page_records = records[start:start + self.page_size]
        end = start + self.page_size
        next_token = None if end >= len(records) else self._token(generation, index + 1)
        return {"generation": generation, "records": page_records, "next_token": next_token}

    def serve(self, token: str | None):
        self.requests.append(token)
        if token is None:
            generation = max(self.generations)
            return self._wire(generation, 0)
        if token in self.expired_tokens:
            raise TokenExpired(token)
        _, index_text, _ = token.split("-", 2)
        index = int(index_text.removeprefix("page"))
        generation = int(token[1:token.index("-")])
        page = self._wire(generation, index)
        if token == self.conflict_token:
            page = {**page, "records": [{**page["records"][0], "product": {**page["records"][0]["product"], "hazard": "tampered"}}]}
        return page


class ScriptedSnapshotSource:
    def __init__(self, publisher: ScriptedSnapshotPublisher):
        self.name = publisher.name
        self.publisher = publisher

    def start_generation(self):
        return decode_snapshot_page(self.name, json.dumps(self.publisher.serve(None)).encode())

    def continue_page(self, token: str):
        try:
            wire = self.publisher.serve(token)
        except TokenExpired:
            raise ContinuationExpired("resume token is no longer valid") from None
        return decode_snapshot_page(self.name, json.dumps(wire).encode())


class FakeHttpTransport:
    def __init__(self, publisher: ScriptedSnapshotPublisher):
        self.publisher = publisher

    def request(self, url: str, *, data=None, headers=None):
        from urllib.parse import parse_qs, urlsplit
        query = parse_qs(urlsplit(url).query)
        token = query["token"][0] if "token" in query else None
        try:
            wire = self.publisher.serve(token)
        except TokenExpired:
            raise TransportError("HTTP status 410", status_code=410) from None
        return json.dumps(wire).encode()


def make_record(record_id: str, *, sequence: int, hazard: str = "fire", severity: str = "high",
                product: str = "Kettle K100", market: str = "EU") -> dict:
    return {"id": record_id, "sequence": sequence,
            "published_at": f"2026-11-01T00:00:0{sequence % 10}Z",
            "product": {"brand": "Northline", "product": product, "market": market,
                        "hazard": hazard, "severity": severity}}


def drain(pipeline: CollectionPipeline, source, cap: int = 50, now: int = 100):
    """Drive repeated collect_snapshot calls like a scheduler restarting the process."""
    results = []
    for _ in range(cap):
        result = pipeline.collect_snapshot(source, now=now)
        results.append(result.status)
        if result.status in {"completed", "superseded", "failed", "deferred"}:
            return results
    raise AssertionError(f"pipeline did not settle: {results}")


def gen_records(count: int, **overrides) -> list[dict]:
    return [make_record(f"R{i:03d}", sequence=i, **{k: v for k, v in overrides.items()}) for i in range(1, count + 1)]


# ---------------------------------------------------------------------------
# Decoder contract
# ---------------------------------------------------------------------------


def test_decode_snapshot_page_keeps_opaque_token_and_generation_scoped_sequence():
    page = {"generation": 7, "records": [make_record("R1", sequence=1)], "next_token": "abc-不可解析"}
    decoded = decode_snapshot_page("snap", json.dumps(page).encode())
    assert decoded.generation == 7
    assert decoded.next_token == "abc-不可解析"
    assert decoded.documents[0].source_id == "R1" and decoded.documents[0].cursor == "1"
    assert decoded.content_hash == decode_snapshot_page("snap", json.dumps(page).encode()).content_hash
    # Whitespace and key ordering do not change page identity; changed contents do.
    reshuffled = {"next_token": "abc-不可解析", "generation": 7, "records": page["records"]}
    assert decode_snapshot_page("snap", json.dumps(reshuffled, indent=3).encode()).content_hash == decoded.content_hash
    tampered = {**page, "records": [make_record("R1", sequence=1, hazard="burn")]}
    assert decode_snapshot_page("snap", json.dumps(tampered).encode()).content_hash != decoded.content_hash


@pytest.mark.parametrize("wire", [
    {"generation": 0, "records": [], "next_token": None},
    {"generation": 1.5, "records": [], "next_token": None},
    {"generation": True, "records": [], "next_token": None},
    {"generation": 2, "records": [make_record("R1", sequence=0)], "next_token": None},
    {"generation": 2, "records": [make_record("R1", sequence=2), make_record("R2", sequence=2)], "next_token": None},
    {"generation": 2, "records": [make_record("R2", sequence=2), make_record("R1", sequence=1)], "next_token": None},
    {"generation": 2, "records": [], "next_token": ""},
    {"records": [], "next_token": None},
])
def test_decode_snapshot_page_rejects_invalid_envelopes(wire):
    with pytest.raises(SourceError):
        decode_snapshot_page("snap", json.dumps(wire).encode())


def test_http_410_is_classified_as_continuation_expired():
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(3))
    publisher.expired_tokens.add("g1-page1-opaque")
    source = SnapshotHttpSource("snap", "http://publisher.invalid/feed", transport=FakeHttpTransport(publisher))
    first = source.start_generation()
    assert first.generation == 1 and first.next_token == "g1-page1-opaque"
    with pytest.raises(ContinuationExpired):
        source.continue_page("g1-page1-opaque")


# ---------------------------------------------------------------------------
# Two tiers of progress: formal evidence never sees a half generation
# ---------------------------------------------------------------------------


def test_partial_reception_changes_no_formal_state_and_resumes_across_reopen(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(5))  # three pages: 2, 2, 1
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        source = ScriptedSnapshotSource(publisher)
        pipeline = CollectionPipeline(store)
        assert pipeline.collect_snapshot(source, now=100).status == "receiving"
        assert pipeline.collect_snapshot(source, now=100).status == "receiving"
        reception = store.reception_state("snap")
        assert reception.generation == 1 and reception.received_pages == 2
        assert reception.received_records == 4 and not reception.complete
        # Formal tables are untouched while the generation is still open.
        assert all(count == 0 for table, count in store.counts().items())
        assert store.source_state("snap").applied_generation is None
    # Process restart: a brand new connection must know exactly what was received.
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        result = pipeline.collect_snapshot(source, now=101)
        assert result.status == "completed"
        assert result.batch.generation == 1
        assert result.batch.accepted == 5 and result.batch.alerts_created == 1
        reception = store.reception_state("snap")
        assert reception is None  # staging area is discarded once the generation is in force
        state = store.source_state("snap")
        assert state.applied_generation == 1 and state.cursor is None
        assert store.counts()["cases"] == 1 and store.counts()["outbox"] == 1
        assert publisher.requests[0] is None and publisher.requests[-1] == "g1-page2-opaque"


def test_crash_between_final_page_and_apply_still_switches_once(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(3))
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        source = ScriptedSnapshotSource(publisher)
        pipeline = CollectionPipeline(store)
        pipeline.collect_snapshot(source, now=100)
        # Simulate process loss after the last page was durably received but before apply ran:
        # fetch and stage the final page directly, bypassing the apply step.
        reception = store.reception_state("snap")
        final_page = source.continue_page(reception.next_token)
        store.stage_snapshot_page("snap", 1, reception.next_token, final_page)
        reception = store.reception_state("snap")
        assert reception.complete
        assert store.counts()["cases"] == 0
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        result = pipeline.collect_snapshot(ScriptedSnapshotSource(publisher), now=101)
        assert result.status == "completed" and result.batch.generation == 1
        assert store.counts()["case_revisions"] == 1
    # A further run must not fabricate another revision or alert.
    with RecallStore(database) as store:
        result = CollectionPipeline(store).collect_snapshot(ScriptedSnapshotSource(publisher), now=102)
        assert result.status == "completed" and result.batch is None
        counts = store.counts()
        assert counts["case_revisions"] == 1 and counts["outbox"] == 1


def test_re_receiving_an_applied_generation_adds_no_revisions_or_alerts(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(3))
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
        counts = store.counts()
    # Same generation offered again from the start (publisher retained it; replay from page one).
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        result = pipeline.collect_snapshot(ScriptedSnapshotSource(publisher), now=200)
        assert result.status == "completed" and result.batch is None
        assert store.reception_state("snap") is None
        assert store.counts() == counts
        assert store.source_state("snap").applied_generation == 1


def test_expired_token_abandons_the_generation_and_the_next_run_starts_afresh(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(5))
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        pipeline.collect_snapshot(source, now=100)  # page 1 received
        publisher.expired_tokens.add("g1-page1-opaque")
        result = pipeline.collect_snapshot(source, now=101)
        assert result.status == "restarted" and result.error_type == "ContinuationExpired"
        assert store.reception_state("snap") is None  # partial old generation is gone
        assert all(count == 0 for count in store.counts().values())
        # Repeated failure does not burn the backoff budget; the source remains immediately retryable.
        assert store.source_state("snap").not_before == 0
    # Publisher now serves a fresh generation with reset sequence numbers.
    publisher.set_generation(2, gen_records(2))
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        results = drain(pipeline, ScriptedSnapshotSource(publisher))
        assert results[-1] == "completed"
        assert store.source_state("snap").applied_generation == 2
        assert store.counts()["source_signals"] == 2


def test_pagination_crossing_into_a_higher_generation_is_refused_not_mixed(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(5))
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        pipeline.collect_snapshot(source, now=100)
        # While generation 1 is incomplete, the publisher rotates to generation 2 under the same token.
        publisher.set_generation(2, gen_records(4))
        publisher.expired_tokens.add("g1-page1-opaque")
        result = pipeline.collect_snapshot(source, now=101)
        assert result.status == "restarted"
        assert result.error_type == "ContinuationExpired"
        assert store.reception_state("snap") is None
    # New run begins generation 2 cleanly; no generation-1 rows are mixed in.
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
        rows = store.connection.execute("SELECT DISTINCT source_id FROM source_signals").fetchall()
        assert {r[0] for r in rows} == {"R001", "R002", "R003", "R004"}
        assert store.source_state("snap").applied_generation == 2


def test_same_token_replayed_with_different_contents_is_rejected(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(6))  # three pages; page two is a real retry point
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        pipeline.collect_snapshot(source, now=100)  # page one stored under the empty token
        pipeline.collect_snapshot(source, now=100)  # page two stored under g1-page1-opaque
        reception = store.reception_state("snap")
        assert reception.received_pages == 2
        # The process loses the in-memory page; on retry the same token returns different contents.
        tampered = decode_snapshot_page("snap", json.dumps(publisher._wire(1, 1)).encode())
        tampered = type(tampered)(tampered.generation,
                                  tuple([type(d)(d.source, d.source_id, d.cursor, d.published_at,
                                                 {**dict(d.payload), "hazard": "tampered"}) for d in tampered.documents]),
                                  tampered.next_token, "other-content-hash")
        with pytest.raises(SnapshotConflict):
            store.stage_snapshot_page("snap", 1, "g1-page1-opaque", tampered)
        # The stored reception stays intact at two pages; nothing formal was touched.
        assert store.reception_state("snap").received_pages == 2
        assert store.counts()["cases"] == 0
        # The pipeline maps the same refusal onto a failed run without corrupting reception.
        original = source.continue_page("g1-page1-opaque")
        real_stage = store.stage_snapshot_page
        def flaky_stage(s, g, token, page):
            if token == "g1-page2-opaque":
                raise SnapshotConflict("simulated divergent replay")
            return real_stage(s, g, token, page)
        store.stage_snapshot_page = flaky_stage  # type: ignore[method-assign]
        result = pipeline.collect_snapshot(source, now=101)
        assert result.status == "failed" and result.error_type == "SnapshotConflict"
        assert store.reception_state("snap").received_pages == 2
        del store.stage_snapshot_page
        # Once the publisher replays genuine pages, reception completes normally.
        # now=1000 is past the two-second failure backoff recorded above.
        assert drain(CollectionPipeline(store), source, now=1000)[-1] == "completed"
        assert store.counts()["cases"] == 1
        assert store.source_state("snap").applied_generation == 1
        assert original is not None


def test_identical_replay_of_the_same_page_is_idempotent(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(8))  # four pages, so reception stays open for the test
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        pipeline.collect_snapshot(source, now=100)
        pipeline.collect_snapshot(source, now=100)
        reception_before = store.reception_state("snap")
        assert reception_before.received_pages == 2
        # A retried request carrying the same stored token returns identical semantics.
        page_two = source.continue_page("g1-page1-opaque")
        store.stage_snapshot_page("snap", 1, "g1-page1-opaque", page_two)
        reception_after = store.reception_state("snap")
        assert reception_after == reception_before
        # The pipeline restart path fetches the genuinely next page and keeps making progress.
        assert pipeline.collect_snapshot(source, now=101).status == "receiving"
        assert store.reception_state("snap").received_pages == 3


def test_competing_collectors_older_and_late_generations_never_override(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(6))  # three pages
    database = tmp_path / "hub.sqlite3"

    # Scenario 1: an older generation is fully received but not applied before a newer one lands.
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        pipeline.collect_snapshot(source, now=100)  # page 1 of 3
        pipeline.collect_snapshot(source, now=100)  # page 2 of 3: reception stays open
        reception = store.reception_state("snap")
        assert not reception.complete and reception.generation == 1 and reception.received_pages == 2
        # A competing collector (separate process, same database) finishes a whole newer generation
        # and applies it while the late worker still holds its partial generation-1 reception.
        newer = ScriptedSnapshotPublisher()
        newer.set_generation(2, gen_records(3, hazard="burn"))
        competitor = ScriptedSnapshotSource(newer)
        first = competitor.start_generation()
        store.abandon_snapshot_reception("snap", 1)
        store.start_snapshot_reception("snap", 2)
        store.stage_snapshot_page("snap", 2, None, first)
        assert drain(CollectionPipeline(store), competitor)[-1] == "completed"
        assert store.source_state("snap").applied_generation == 2
    # The late worker A restarts and tries to resume its old generation-1 token.
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        source = ScriptedSnapshotSource(publisher)
        # Reconstruct A's partial reception as it existed before the competitor's discard.
        first = source.start_generation()
        store.start_snapshot_reception("snap", 1)
        store.stage_snapshot_page("snap", 1, None, first)
        second = source.continue_page(first.next_token)
        store.stage_snapshot_page("snap", 1, first.next_token, second)
        assert store.reception_state("snap").received_pages == 2
        # The publisher rotated: the token now answers 410, so the stale reception is abandoned.
        publisher.expired_tokens.add("g1-page2-opaque")
        result = pipeline.collect_snapshot(source, now=101)
        assert result.status == "restarted" and result.error_type == "ContinuationExpired"
        assert store.reception_state("snap") is None
        assert store.source_state("snap").applied_generation == 2
        # Even if the old generation had been fully received, compare-and-swap refuses to apply it.
        with pytest.raises(StaleGeneration):
            store.apply_snapshot("snap", 1, now=102)
        cases = store.list_cases()
        assert cases and all("burn" in c["hazards"] for c in cases)

    # Scenario 2: a generation already in force cannot be rolled back by an even later stale apply.
    with RecallStore(database) as store:
        with pytest.raises(StaleGeneration):
            store.apply_snapshot("snap", 1, now=200)
        assert store.source_state("snap").applied_generation == 2
        assert store.counts()["cases"] == 1  # only the generation-2 product identity is in force


def test_sequence_numbers_reset_between_generations_without_collision(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, [make_record("G1-R1", sequence=1), make_record("G1-R2", sequence=2)])
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
    publisher.set_generation(2, [make_record("G2-R1", sequence=1), make_record("G2-R2", sequence=2)])
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
        ids = {r[0] for r in store.connection.execute("SELECT source_id FROM source_signals")}
        assert ids == {"G1-R1", "G1-R2", "G2-R1", "G2-R2"}
        assert store.source_state("snap").applied_generation == 2


def test_snapshot_records_absent_from_a_generation_are_not_treated_as_withdrawals(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, [make_record("KEEP", sequence=1), make_record("DROP", sequence=2)])
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        drain(pipeline, ScriptedSnapshotSource(publisher))
        assert store.counts()["source_signals"] == 2
    publisher.set_generation(2, [make_record("KEEP", sequence=1)])
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        drain(pipeline, ScriptedSnapshotSource(publisher))
        ids = {r[0] for r in store.connection.execute("SELECT source_id FROM source_signals")}
        assert ids == {"KEEP", "DROP"}  # snapshot omission is not a retraction
        assert store.counts()["rejected_records"] == 0


def test_bad_snapshot_records_are_isolated_when_the_generation_is_applied(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    good = make_record("GOOD", sequence=1)
    bad = make_record("BAD", sequence=2)
    bad["product"] = {**bad["product"], "product": ""}
    publisher.set_generation(1, [good, bad])
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        result = drain(pipeline, ScriptedSnapshotSource(publisher))
        assert result[-1] == "completed"
        counts = store.counts()
        assert counts["source_signals"] == 1 and counts["rejected_records"] == 1
        assert counts["raw_evidence"] == 2


def test_incremental_and_snapshot_sources_coexist_with_distinct_progress(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, [
        make_record("S1", sequence=1, market="US", product="Toaster T50"),
        make_record("S2", sequence=2, market="US", product="Blender B9"),
    ])
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        store.commit_batch("agency", None, "1", [document()], 0)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
        incremental = store.source_state("agency")
        snapshot = store.source_state("snap")
        assert incremental.cursor == "1" and incremental.applied_generation is None
        assert snapshot.cursor is None and snapshot.applied_generation == 1
        assert store.counts()["cases"] == 3
        assert store.counts()["source_signals"] == 3


def test_legacy_database_keeps_integer_cursor_and_evidence_semantics(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    # A database created by the pre-snapshot schema: no applied_generation column.
    import sqlite3
    legacy = sqlite3.connect(database)
    legacy.executescript("""
    CREATE TABLE source_state (source TEXT PRIMARY KEY, cursor TEXT, failures INTEGER NOT NULL DEFAULT 0,
        not_before INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE raw_evidence (evidence_hash TEXT PRIMARY KEY, source TEXT NOT NULL, source_id TEXT NOT NULL,
        published_at TEXT NOT NULL, body BLOB NOT NULL);
    CREATE TABLE rejected_records (source TEXT NOT NULL, source_id TEXT NOT NULL, evidence_hash TEXT NOT NULL
        REFERENCES raw_evidence(evidence_hash), reason TEXT NOT NULL,
        PRIMARY KEY(source, source_id, evidence_hash));
    CREATE TABLE cases (case_key TEXT PRIMARY KEY, revision INTEGER NOT NULL, fingerprint TEXT NOT NULL, summary TEXT NOT NULL);
    CREATE TABLE source_signals (source TEXT NOT NULL, source_id TEXT NOT NULL, case_key TEXT NOT NULL,
        evidence_hash TEXT NOT NULL REFERENCES raw_evidence(evidence_hash), published_at TEXT NOT NULL,
        brand TEXT NOT NULL, product TEXT NOT NULL, market TEXT NOT NULL, hazard TEXT NOT NULL, severity TEXT NOT NULL,
        PRIMARY KEY(source, source_id));
    CREATE TABLE case_revisions (case_key TEXT NOT NULL REFERENCES cases(case_key), revision INTEGER NOT NULL,
        summary TEXT NOT NULL, PRIMARY KEY(case_key, revision));
    """)
    legacy.commit()
    legacy.close()
    with RecallStore(database) as store:
        # Migration must not reinterpret the integer cursor or touch existing evidence.
        result = store.commit_batch("agency", None, "1", [document()], 0)
        assert result.cursor == "1"
        state = store.source_state("agency")
        assert state.cursor == "1" and state.applied_generation is None
        assert store.counts()["raw_evidence"] == 1
        # The snapshot machinery is available without rebuilding or clearing the database.
        publisher = ScriptedSnapshotPublisher()
        publisher.set_generation(1, [
            make_record("S1", sequence=1, market="US", product="Toaster T50"),
            make_record("S2", sequence=2, market="US", product="Blender B9"),
        ])
        assert drain(CollectionPipeline(store), ScriptedSnapshotSource(publisher))[-1] == "completed"
        assert store.source_state("snap").applied_generation == 1
        assert store.source_state("agency").cursor == "1"


def test_publisher_serving_an_older_generation_fails_without_touching_state(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(2, gen_records(2))
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        assert drain(pipeline, ScriptedSnapshotSource(publisher))[-1] == "completed"
        assert store.source_state("snap").applied_generation == 2
    # A different publisher endpoint (or rolled-back state) only knows generation 1.
    stale_publisher = ScriptedSnapshotPublisher()
    stale_publisher.set_generation(1, gen_records(2))
    with RecallStore(database) as store:
        result = CollectionPipeline(store).collect_snapshot(ScriptedSnapshotSource(stale_publisher), now=500)
        assert result.status == "failed" and result.error_type == "GenerationAdvanced"
        # Backoff is recorded for a publisher inconsistency, and formal state is untouched.
        assert store.source_state("snap").not_before > 500
        assert store.source_state("snap").applied_generation == 2
        assert store.reception_state("snap") is None


def test_snapshot_http_source_sends_token_only_for_resume_requests():
    from urllib.parse import urlsplit
    captured = []

    class CapturingTransport:
        def request(self, url, *, data=None, headers=None):
            captured.append(urlsplit(url).query)
            return json.dumps({"generation": 1, "records": [], "next_token": None}).encode()

    source = SnapshotHttpSource("snap", "http://publisher.invalid/feed?channel=appliances",
                                transport=CapturingTransport())
    source.start_generation()
    assert captured[0] == "channel=appliances"  # no token parameter on the generation-start request
    source.continue_page("opaque-标记")
    assert "channel=appliances" in captured[1] and "token=opaque" in captured[1]


def test_non_410_http_errors_keep_resume_progress_and_use_backoff(tmp_path):
    publisher = ScriptedSnapshotPublisher()
    publisher.set_generation(1, gen_records(5))
    database = tmp_path / "hub.sqlite3"

    class IntermittentTransport(FakeHttpTransport):
        def __init__(self, publisher, fail_once_token):
            super().__init__(publisher)
            self.fail_once_token = fail_once_token
            self.failed = False

        def request(self, url, *, data=None, headers=None):
            from urllib.parse import parse_qs, urlsplit
            token = parse_qs(urlsplit(url).query).get("token", [None])[0]
            if not self.failed and token == self.fail_once_token:
                self.failed = True
                raise TransportError("HTTP status 503", status_code=503)
            return super().request(url, data=data, headers=headers)

    with RecallStore(database) as store:
        source = SnapshotHttpSource("snap", "http://publisher.invalid/feed",
                                    transport=IntermittentTransport(publisher, "g1-page1-opaque"))
        pipeline = CollectionPipeline(store)
        pipeline.collect_snapshot(source, now=100)
        failed = pipeline.collect_snapshot(source, now=100)
        assert failed.status == "failed" and failed.error_type == "TransportError"
        reception = store.reception_state("snap")
        assert reception.received_pages == 1  # progress survived the 503
        assert store.source_state("snap").not_before == 102
        deferred = pipeline.collect_snapshot(source, now=101)
        assert deferred.status == "deferred"
        assert drain(pipeline, source, now=1000)[-1] == "completed"
        assert store.source_state("snap").applied_generation == 1


def test_apply_streaming_keeps_memory_bounded_by_page_size(tmp_path):
    record_count = 600  # three pages of two hundred; generation far larger than one page
    publisher = ScriptedSnapshotPublisher()
    publisher.page_size = 200
    publisher.set_generation(1, gen_records(record_count))
    database = tmp_path / "hub.sqlite3"
    document_refs: list[weakref.ReferenceType] = []
    page_refs: list[weakref.ReferenceType] = []

    class TrackingSource(ScriptedSnapshotSource):
        def start_generation(self):
            page = super().start_generation()
            page_refs.append(weakref.ref(page))
            document_refs.extend(weakref.ref(d) for d in page.documents)
            return page

        def continue_page(self, token):
            page = super().continue_page(token)
            page_refs.append(weakref.ref(page))
            document_refs.extend(weakref.ref(d) for d in page.documents)
            return page

    with RecallStore(database) as store:
        pipeline = CollectionPipeline(store)
        results = drain(pipeline, TrackingSource(publisher), cap=10)
        assert results[-1] == "completed"
        gc.collect()
        # Every page and every decoded document has been released: peak residency is page-sized.
        assert all(ref() is None for ref in page_refs)
        assert all(ref() is None for ref in document_refs)
        assert store.counts()["source_signals"] == record_count


def test_record_working_set_stays_page_sized_as_generation_grows(tmp_path):
    """The number of decoded records live at any apply boundary must not scale with generation size."""
    import gc as gc_module
    from recall_signal_hub import models as models_mod
    import recall_signal_hub.storage as storage_mod

    def distinct_records(count):
        # Distinct identities so each record maps to its own case; no other collection can hide growth.
        return [make_record(f"P{i:05d}", sequence=i, product=f"Product P{i}") for i in range(1, count + 1)]

    def concurrent_document_counts(record_count):
        publisher = ScriptedSnapshotPublisher()
        publisher.page_size = 200
        publisher.set_generation(1, distinct_records(record_count))
        database = tmp_path / f"hub-{record_count}.sqlite3"
        counts: list[int] = []
        original = storage_mod.RecallStore._process_documents

        def probe(self, source, documents):
            counts.append(sum(1 for obj in gc_module.get_objects()
                              if type(obj) is models_mod.SourceDocument))
            return original(self, source, documents)

        storage_mod.RecallStore._process_documents = probe
        try:
            with RecallStore(database) as store:
                source = ScriptedSnapshotSource(publisher)
                page = source.start_generation()
                store.start_snapshot_reception("snap", 1)
                token = None
                while True:
                    store.stage_snapshot_page("snap", 1, token, page)
                    if page.next_token is None:
                        break
                    token = page.next_token
                    page = source.continue_page(token)
                gc_module.collect()
                store.apply_snapshot("snap", 1, 1)
        finally:
            storage_mod.RecallStore._process_documents = original
        return counts

    small = concurrent_document_counts(200)
    large = concurrent_document_counts(2000)
    assert len(small) == 1 and len(large) == 10  # one page per streamed chunk
    # Ten times the generation, yet the decoded-record working set never exceeds one page (~200)
    # plus the chunk currently being normalized, and is identical across both runs.
    assert max(large) == max(small) <= 400
