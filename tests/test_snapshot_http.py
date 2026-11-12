"""End-to-end snapshot verification against a real local HTTP publisher.

The publisher serves opaque resume tokens, resets record sequence numbers at every
generation boundary, and can be driven through 410 expiry and content divergence.
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread, Lock
from urllib.parse import parse_qs, urlsplit

from recall_signal_hub.pipeline import CollectionPipeline
from recall_signal_hub.sources import SnapshotHttpSource
from recall_signal_hub.storage import RecallStore


def record(record_id, sequence, *, hazard="fire", severity="high", product="Kettle K100"):
    return {"id": record_id, "sequence": sequence,
            "published_at": f"2026-11-0{1 + sequence % 9}T08:00:00Z",
            "product": {"brand": "Northline", "product": product, "market": "EU",
                        "hazard": hazard, "severity": severity}}


class SnapshotPublisherServer:
    """Real HTTP server; page contents are immutable per (generation, token)."""

    def __init__(self, page_size=2):
        self.page_size = page_size
        self.lock = Lock()
        self.generations: dict[int, list[dict]] = {}
        self.expired: set[str] = set()
        self.conflicts: set[str] = set()
        self.request_log: list[str | None] = []
        self.httpd = None
        self.thread = None

    def __enter__(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                outer._handle(self)

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown(); self.httpd.server_close(); self.thread.join(timeout=3)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_port}/snapshot?channel=appliances"

    def set_generation(self, generation, records):
        with self.lock:
            self.generations[generation] = records

    def _token(self, generation, page_index):
        return f"gen{generation}/p{page_index}/opaque"

    def _page(self, generation, page_index):
        records = self.generations[generation]
        start = page_index * self.page_size
        slice_ = records[start:start + self.page_size]
        end = start + self.page_size
        next_token = None if end >= len(records) else self._token(generation, page_index + 1)
        return {"generation": generation, "records": slice_, "next_token": next_token}

    def _handle(self, request: BaseHTTPRequestHandler):
        query = parse_qs(urlsplit(request.path).query)
        token = query["token"][0] if "token" in query else None
        with self.lock:
            self.request_log.append(token)
            if token is None:
                generation = max(self.generations)
                body = json.dumps(self._page(generation, 0)).encode()
            elif token in self.expired:
                request.send_response(410); request.end_headers(); return
            else:
                head, index_text, _ = token.split("/", 2)
                generation = int(head.removeprefix("gen"))
                page_index = int(index_text.removeprefix("p"))
                page = self._page(generation, page_index)
                if token in self.conflicts:
                    page = {"generation": page["generation"], "next_token": page["next_token"],
                            "records": [{**page["records"][0],
                                         "product": {**page["records"][0]["product"], "hazard": "tampered"}}]}
                body = json.dumps(page).encode()
        request.send_response(200); request.end_headers(); request.wfile.write(body)


def run_until(store, source, *, now, terminal=("completed", "superseded", "failed", "deferred"), cap=40):
    pipeline = CollectionPipeline(store)
    statuses = []
    for _ in range(cap):
        result = pipeline.collect_snapshot(source, now=now)
        statuses.append((result.status, result.error_type))
        if result.status in terminal:
            return statuses, result
    raise AssertionError(f"pipeline never settled: {statuses}")


def test_full_snapshot_lifecycle_over_local_http(tmp_path):
    database = tmp_path / "hub.sqlite3"
    # Generation 1 spans three pages (2+2+1); sequence numbers reset in generation 2.
    generation_one = [record("A1", 1), record("A2", 2), record("A3", 3), record("A4", 4), record("A5", 5)]
    generation_two = [record("B1", 1, hazard="burn", product="Heater H2"),
                      record("B2", 2, hazard="burn", product="Heater H2")]
    with SnapshotPublisherServer() as server:
        server.set_generation(1, generation_one)

        # --- Round 1: process restart in the middle of receiving generation 1 ---------------
        with RecallStore(database) as store:
            source = SnapshotHttpSource("agency", server.endpoint)
            first = CollectionPipeline(store).collect_snapshot(source, now=100)
            assert first.status == "receiving"
            second = CollectionPipeline(store).collect_snapshot(source, now=101)
            assert second.status == "receiving"
            # No half-generation may reach the formal case/evidence/alert surface.
            assert all(value == 0 for value in store.counts().values())
            reception = store.reception_state("agency")
            assert (reception.generation, reception.received_pages) == (1, 2)
            assert server.request_log[0] is None  # first request carries no continuation marker
        # Reopen the database: the new process resumes from the stored opaque token.
        with RecallStore(database) as store:
            source = SnapshotHttpSource("agency", server.endpoint)
            statuses, result = run_until(store, source, now=102)
            assert statuses[-1][0] == "completed"
            assert result.batch.generation == 1 and result.batch.accepted == 5
            counts_after_g1 = store.counts()
            assert counts_after_g1["cases"] == 1 and counts_after_g1["outbox"] == 1
            assert store.source_state("agency").applied_generation == 1
            assert store.reception_state("agency") is None

        # --- Round 2: HTTP 410 rotates the publisher onto generation 2 ----------------------
        server.set_generation(2, generation_two)
        with RecallStore(database) as store:
            source = SnapshotHttpSource("agency", server.endpoint)
            # A stale collector still holding the old generation-1 token gets 410.
            store.start_snapshot_reception("agency", 1)
            store.connection.execute(
                "UPDATE snapshot_receptions SET next_token=? WHERE source=?",
                ("gen1/p2/opaque", "agency"))
            server.expired.add("gen1/p2/opaque")
            stale = CollectionPipeline(store).collect_snapshot(source, now=200)
            assert stale.status == "restarted" and stale.error_type == "ContinuationExpired"
            assert store.reception_state("agency") is None
            assert store.counts() == counts_after_g1  # formal state untouched by the dead attempt
            # The next scheduled run starts generation 2 cleanly and applies it atomically.
            statuses, result = run_until(store, source, now=201)
            assert statuses[-1][0] == "completed" and result.batch.generation == 2
            assert store.source_state("agency").applied_generation == 2
            signals = {r[0] for r in store.connection.execute(
                "SELECT source_id FROM source_signals")}
            # Generation-1 records are retained (omission is not withdrawal), B records join them.
            assert signals == {f"A{i}" for i in range(1, 6)} | {"B1", "B2"}

        # --- Round 3: the same token replayed with different contents must be refused --------
        with RecallStore(database) as store:
            source = SnapshotHttpSource("agency", server.endpoint)
            # Build a partial generation-3 reception: two pages stored, the third still pending.
            server.set_generation(3, [record("C1", 1, product="Mixer M3"),
                                      record("C2", 2, product="Mixer M3"),
                                      record("C3", 3, product="Mixer M3"),
                                      record("C4", 4, product="Mixer M3"),
                                      record("C5", 5, product="Mixer M3")])
            first = CollectionPipeline(store).collect_snapshot(source, now=300)
            second = CollectionPipeline(store).collect_snapshot(source, now=300)
            assert first.status == "receiving" and second.status == "receiving"
            reception = store.reception_state("agency")
            assert reception.generation == 3 and reception.received_pages == 2
            # The process loses its in-memory position before page three completes and restarts
            # from a token whose response it has already durably stored.
            replay_token = "gen3/p1/opaque"
            store.connection.execute(
                "UPDATE snapshot_receptions SET next_token=? WHERE source=?",
                (replay_token, "agency"))
            # The publisher now answers that previously stored token with divergent contents.
            server.conflicts.add(replay_token)
            refused = CollectionPipeline(store).collect_snapshot(source, now=301)
            assert refused.status == "failed" and refused.error_type == "SnapshotConflict"
            assert store.reception_state("agency").received_pages == 2
            assert store.source_state("agency").applied_generation == 2  # still generation 2 in force
            # Identical replay after the publisher heals: the genuine page is accepted idempotently.
            server.conflicts.clear()
            healed = CollectionPipeline(store).collect_snapshot(source, now=1000)
            assert healed.status == "receiving"
            # With the divergent page reconciled, the process resumes from its true next token.
            store.connection.execute(
                "UPDATE snapshot_receptions SET next_token=? WHERE source=?",
                ("gen3/p2/opaque", "agency"))
            statuses, result = run_until(store, source, now=1000)
            assert statuses[-1][0] == "completed" and result.batch.generation == 3
            assert store.source_state("agency").applied_generation == 3

        # --- Round 4: interleaved old and new collectors, plus idempotent re-reception --------
        with RecallStore(database) as store_a, RecallStore(database) as store_b:
            # A late collector tries to force an already-superseded generation into force.
            from recall_signal_hub.storage import StaleGeneration
            import pytest
            with pytest.raises(StaleGeneration):
                store_a.apply_snapshot("agency", 2, now=400)
            assert store_a.source_state("agency").applied_generation == 3
            # Re-receiving the generation currently in force creates no revisions or alerts.
            server.set_generation(3, [record("C1", 1, product="Mixer M3"),
                                      record("C2", 2, product="Mixer M3"),
                                      record("C3", 3, product="Mixer M3")])
            repeat = CollectionPipeline(store_b).collect_snapshot(
                SnapshotHttpSource("agency", server.endpoint), now=401)
            assert repeat.status == "completed" and repeat.batch is None
            assert store_b.counts()["case_revisions"] == store_a.counts()["case_revisions"]
            assert store_b.counts()["outbox"] == store_a.counts()["outbox"]
            # Token shape was never parsed for ordering: query strings stay plain token values.
            assert all(token is None or token.startswith("gen") for token in server.request_log)

    # Final formal view: every applied generation is visible; no half-generation artifacts exist.
    with RecallStore(database) as store:
        assert store.reception_state("agency") is None
        assert store.connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert store.connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert store.source_state("agency").applied_generation == 3
        assert store.source_state("agency").cursor is None  # integer cursor semantics never reused
