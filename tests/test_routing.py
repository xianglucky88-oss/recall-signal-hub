"""Per-target routing, frozen expansion and independent delivery state.

Every recipient here is a local in-process double; no message leaves the
process and no real receiver is contacted.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from recall_signal_hub.alerts import (RecordingTargetSink, RoutedWebhookSink,
                                      TargetDispatcher)
from recall_signal_hub.models import TargetAlert
from recall_signal_hub.outbox import LeaseLost
from recall_signal_hub.route_admin import RouteAdministrator
from recall_signal_hub.routing import RouteConfig, RoutingError, Subscription
from recall_signal_hub.storage import RecallStore


# --------------------------------------------------------------------- helpers
def two_recipient_config(*, endpoint_a="http://127.0.0.1/a", endpoint_b="http://127.0.0.1/b"):
    return RouteConfig.from_subscriptions((
        Subscription.create("recipient-a", ["EU"], "high", endpoint_a),
        Subscription.create("recipient-b", ["EU", "US"], "medium", endpoint_b),
    ))


def seed_routed_alert(tmp_path, config=None, *, document_kwargs=None):
    config = config or two_recipient_config()
    database = tmp_path / "routed.sqlite3"
    store = RecallStore(database, route_config=config)
    payload = {"brand": "Northline", "product": "Kettle K100", "market": "EU",
               "hazard": "fire", "severity": "high"}
    if document_kwargs:
        payload.update(document_kwargs)
    from recall_signal_hub.models import SourceDocument
    doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z", payload)
    result = store.commit_batch("agency", None, "1", [doc], 100)
    assert result.alerts_created == 1
    assert store.counts()["outbox_alerts"] == 1
    return store, config


def task_rows(store, alert_id=1):
    return store.outbox.target_states(alert_id)


# ---------------------------------------------------------------- configuration
def test_config_version_is_content_addressed_and_stable():
    first = two_recipient_config()
    second = two_recipient_config()
    assert first.version == second.version
    assert len(first.version) == 16
    # Declaration order does not change the version.
    reordered = RouteConfig.from_subscriptions(tuple(reversed(first.subscriptions)))
    assert reordered.version == first.version


def test_config_rejects_duplicate_recipients_and_bad_declarations():
    with pytest.raises(RoutingError):
        RouteConfig.from_subscriptions((
            Subscription.create("r1", ["EU"], "high", "http://x"),
            Subscription.create("r1", ["US"], "low", "http://y"),
        ))
    with pytest.raises(RoutingError):
        Subscription.create("", ["EU"], "high", "http://x")
    with pytest.raises(RoutingError):
        Subscription.create("r1", ["EU"], "bogus", "http://x")
    with pytest.raises(RoutingError):
        Subscription.create("r1", [], "low", "http://x")


def test_market_and_minimum_severity_filtering():
    config = two_recipient_config()
    # Case summaries carry canonical (casefolded) market and severity values.
    assert {s.recipient_id for s in config.subscriptions
            if s.matches({"market": "eu", "severity": "high"})} == {"recipient-a", "recipient-b"}
    # Below recipient-a's minimum severity: only recipient-b matches.
    assert {s.recipient_id for s in config.subscriptions
            if s.matches({"market": "eu", "severity": "medium"})} == {"recipient-b"}
    # Unknown market: no recipient matches.
    assert config.expand({"market": "jp", "severity": "critical"})[1] == ()


def test_routing_config_loads_from_local_json(tmp_path):
    path = tmp_path / "routes.json"
    path.write_text(json.dumps({"subscriptions": [
        {"recipient_id": "r1", "markets": ["EU"], "min_severity": "high",
         "endpoint": "http://127.0.0.1/r1"}]}), encoding="utf-8")
    config = RouteConfig.load(str(path))
    assert config.subscriptions[0].recipient_id == "r1"


# ------------------------------------------------------------- frozen expansion
def test_alert_expands_to_one_frozen_task_per_recipient(tmp_path):
    store, config = seed_routed_alert(tmp_path)
    rows = task_rows(store)
    assert [r["recipient_id"] for r in rows] == ["recipient-a", "recipient-b"]
    assert {r["route_version"] for r in rows} == {config.version}
    assert {r["status"] for r in rows} == {"pending"}
    assert {r["endpoint"] for r in rows} == {"http://127.0.0.1/a", "http://127.0.0.1/b"}
    alert = store.connection.execute("SELECT * FROM outbox_alerts").fetchone()
    assert alert["target_count"] == 2 and alert["expanded_at"] == 100
    assert alert["route_version"] == config.version
    # The legacy single-target queue stays empty in routed mode.
    assert store.counts()["outbox"] == 0


def test_no_matching_recipient_is_an_explained_empty_target_set(tmp_path):
    config = two_recipient_config()
    store, _ = seed_routed_alert(tmp_path, config,
                                 document_kwargs={"market": "JP", "product": "Other"})
    alert = store.connection.execute("SELECT * FROM outbox_alerts").fetchone()
    assert alert["target_count"] == 0 and alert["expanded_at"] == 100
    assert task_rows(store, alert["alert_id"]) == []
    # Nothing waits forever: the dispatcher has no claimable work.
    assert store.outbox.claim_targets(100) == []
    states = store.connection.execute(
        "SELECT recipient_id, COUNT(*) FROM outbox_targets GROUP BY recipient_id").fetchall()
    assert states == []


def test_enqueue_boundary_rejects_repeated_recipient_tuple_atomically(tmp_path):
    # The route config itself forbids duplicate recipient ids; demonstrate the
    # persistence boundary also rejects a repeated recipient tuple atomically.
    config = two_recipient_config()
    store = RecallStore(tmp_path / "dedup.sqlite3", route_config=config)
    from recall_signal_hub.models import SourceDocument
    doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                         {"brand": "B", "product": "P", "market": "EU",
                          "hazard": "fire", "severity": "high"})
    store.commit_batch("agency", None, "1", [doc], 100)
    from recall_signal_hub.database import transaction
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(store.connection):
            store.outbox.enqueue_routed("k", 9, "high", "{}", config.version,
                                        ("recipient-a", "recipient-a"),
                                        {"recipient-a": "http://x"}, 100)
    rows = store.connection.execute("SELECT * FROM outbox_targets").fetchall()
    # Only the legitimate expansion exists; the duplicate insert failed atomically.
    assert [r["recipient_id"] for r in rows].count("recipient-a") == 1


# --------------------------------------------------------- independent delivery
def test_one_target_failure_does_not_block_or_complete_the_other(tmp_path):
    store, _ = seed_routed_alert(tmp_path)

    class Flaky:
        def __init__(self): self.failed = set()
        def deliver(self, task: TargetAlert):
            if task.recipient_id == "recipient-a" and task.recipient_id not in self.failed:
                self.failed.add(task.recipient_id)
                raise OSError("recipient-a endpoint down")

    sink = Flaky()
    dispatcher = TargetDispatcher(store.outbox, sink, clock=lambda: 100)
    result = dispatcher.dispatch()
    assert result == {"delivered": 1, "retry": 1, "lease_lost": 0}
    by_id = {r["recipient_id"]: r for r in task_rows(store)}
    assert by_id["recipient-b"]["status"] == "delivered"
    assert by_id["recipient-a"]["status"] == "pending"
    assert by_id["recipient-a"]["attempts"] == 1
    assert by_id["recipient-a"]["last_error"] == "OSError"
    # Backoff: not immediately claimable, but still independently retryable.
    assert store.outbox.claim_targets(100) == []
    recovered = RecordingTargetSink()
    assert TargetDispatcher(store.outbox, recovered, clock=lambda: 102).dispatch()["delivered"] == 1
    assert {r["status"] for r in task_rows(store)} == {"delivered"}


def test_idempotency_keys_distinguish_revision_and_recipient(tmp_path):
    store, _ = seed_routed_alert(tmp_path)
    tasks = store.outbox.claim_targets(100)
    keys = {t.recipient_id: t.idempotency_key for t in tasks}
    assert keys["recipient-a"] != keys["recipient-b"]
    assert keys["recipient-a"] == "recall:" + tasks[0].case_key + ":1:recipient-a"
    # Receiver-side emulation keeps one entry per (revision, recipient) key.
    sink = RecordingTargetSink()
    for task in tasks:
        sink.deliver(task)
    assert len(sink.received) == 2
    # Redelivery after a crash reuses the same key and does not duplicate.
    again = store.outbox.claim_targets(1_000)  # leases expired far in the future
    for task in again:
        sink.deliver(task)
    assert len(sink.received) == 2


def test_each_target_keeps_its_own_lease_attempts_and_ack_state(tmp_path):
    store, _ = seed_routed_alert(tmp_path)
    first = store.outbox.claim_targets(100, lease_seconds=10)
    assert len(first) == 2 and len({t.lease_id for t in first}) == 2
    # Expire only by waiting past the lease; one stale ack must not touch the other.
    with pytest.raises(LeaseLost):
        store.outbox.acknowledge_target(first[0], 111)
    reclaimed = store.outbox.claim_targets(111)
    assert len(reclaimed) == 2
    by_id = {t.recipient_id: t for t in reclaimed}
    assert by_id["recipient-a"].attempts == 2 and by_id["recipient-b"].attempts == 2
    # Acknowledging one leaves the other leased.
    store.outbox.acknowledge_target(by_id["recipient-a"], 111)
    remaining = store.outbox.claim_targets(200)
    assert [t.recipient_id for t in remaining] == ["recipient-b"]


# -------------------------------- subscription changes cannot rewrite old targets
def test_subscription_change_leaves_queued_tasks_on_frozen_snapshot(tmp_path):
    store, old_config = seed_routed_alert(tmp_path)
    # Before anything is deliverable, change the configuration: recipient-a moves
    # endpoint and recipient-b disappears; a brand-new recipient appears.
    new_config = RouteConfig.from_subscriptions((
        Subscription.create("recipient-a", ["EU"], "high", "http://127.0.0.1/a-v2"),
        Subscription.create("recipient-c", ["EU"], "high", "http://127.0.0.1/c"),
    ))
    assert new_config.version != old_config.version

    # Reopen with the new configuration: queued rows are untouched by loading it.
    store.close()
    with RecallStore(tmp_path / "routed.sqlite3", route_config=new_config) as reopened:
        rows = task_rows(reopened)
        assert sorted(r["recipient_id"] for r in rows) == ["recipient-a", "recipient-b"]
        frozen = {r["recipient_id"]: r for r in rows}
        assert frozen["recipient-a"]["endpoint"] == "http://127.0.0.1/a"
        assert frozen["recipient-a"]["route_version"] == old_config.version
        assert frozen["recipient-b"]["status"] == "pending"

        drift = RouteAdministrator(reopened.outbox).detect_drift(new_config)
        kinds = {(d.recipient_id, d.kind) for d in drift}
        assert kinds == {("recipient-a", "endpoint-changed"),
                         ("recipient-b", "recipient-removed")}


def test_late_ack_from_old_claimant_cannot_steal_reclaimed_task(tmp_path):
    store, _ = seed_routed_alert(tmp_path)
    old_claims = {t.recipient_id: t for t in store.outbox.claim_targets(100, lease_seconds=10)}
    old_a = old_claims["recipient-a"]
    # Lease expires and a later worker reclaims recipient-a.
    new_claims = {t.recipient_id: t for t in store.outbox.claim_targets(111, lease_seconds=10)}
    new_a = new_claims["recipient-a"]
    assert new_a.lease_id != old_a.lease_id and new_a.attempts == old_a.attempts + 1
    # The old claimant finally confirms: it must lose, and must not complete the task.
    with pytest.raises(LeaseLost):
        store.outbox.acknowledge_target(old_a, 112)
    row = store.outbox.find_target_task(old_a.alert_id, "recipient-a")
    assert row["status"] == "leased" and row["lease_id"] == new_a.lease_id
    # The new claimant's confirmation still succeeds.
    store.outbox.acknowledge_target(new_a, 112)
    assert store.outbox.find_target_task(new_a.alert_id, "recipient-a")["status"] == "delivered"


def test_endpoint_change_defaults_to_frozen_endpoint_until_explicit_action(tmp_path):
    store, old_config = seed_routed_alert(tmp_path)
    new_config = RouteConfig.from_subscriptions((
        Subscription.create("recipient-a", ["EU"], "high", "http://127.0.0.1/a-v2"),
        Subscription.create("recipient-b", ["EU", "US"], "medium", "http://127.0.0.1/b"),
    ))
    store.close()
    database = tmp_path / "routed.sqlite3"
    with RecallStore(database, route_config=new_config) as reopened:
        # Default semantics: delivery still targets the frozen original endpoint.
        claimed = {t.recipient_id: t for t in reopened.outbox.claim_targets(100)}
        assert claimed["recipient-a"].endpoint == "http://127.0.0.1/a"
        assert claimed["recipient-a"].route_version == old_config.version

        # Explicit repoint is an audited decision; the idempotency key is unchanged.
        admin = RouteAdministrator(reopened.outbox)
        repointed = admin.repoint_changed_endpoints(new_config, 200)
        assert repointed == [claimed["recipient-a"].task_id]
        reopened.outbox.acknowledge_target(claimed["recipient-b"], 100)
        row = reopened.outbox.find_target_task(1, "recipient-a")
        assert row["endpoint"] == "http://127.0.0.1/a-v2"
        assert row["route_version"] == new_config.version
        events = reopened.outbox.task_events(row["task_id"])
        assert [e["event"] for e in events] == ["repointed"]
        task = reopened.outbox.claim_targets(201)[0]
        assert task.idempotency_key == claimed["recipient-a"].idempotency_key


def test_removed_recipient_can_be_explicitly_paused_and_resumed(tmp_path):
    store, _ = seed_routed_alert(tmp_path)
    new_config = RouteConfig.from_subscriptions((
        Subscription.create("recipient-a", ["EU"], "high", "http://127.0.0.1/a"),
    ))
    admin = RouteAdministrator(store.outbox)
    paused = admin.pause_removed_recipients(new_config, 200)
    row_b = store.outbox.find_target_task(1, "recipient-b")
    assert paused == [row_b["task_id"]] and row_b["status"] == "paused"
    # The paused task never enters the claim set; the other recipient still can.
    claimable = {t.recipient_id: t for t in store.outbox.claim_targets(201)}
    assert set(claimable) == {"recipient-a"}
    store.outbox.acknowledge_target(claimable["recipient-a"], 201)
    # After the other task settles, nothing of recipient-b is claimable.
    assert store.outbox.claim_targets(202) == []
    store.outbox.resume_target(row_b["task_id"], 203)
    assert store.outbox.find_target_task(1, "recipient-b")["status"] == "pending"
    assert [e["event"] for e in store.outbox.task_events(row_b["task_id"])] == ["paused", "resumed"]


# ------------------------------------------------------------- full scenario test
def test_two_targets_failure_success_then_subscription_change_and_late_ack(tmp_path):
    """The end-to-end acceptance scenario from the routing requirement."""
    store, old_config = seed_routed_alert(tmp_path)
    sink = RecordingTargetSink()

    class PartialFailure:
        def __init__(self): self.attempt = 0
        def deliver(self, task: TargetAlert):
            self.attempt += 1
            if task.recipient_id == "recipient-a" and self.attempt == 1:
                raise ConnectionError("a temporarily unavailable")
            sink.deliver(task)

    first_run = TargetDispatcher(store.outbox, PartialFailure(), clock=lambda: 100).dispatch()
    assert first_run == {"delivered": 1, "retry": 1, "lease_lost": 0}
    rows = {r["recipient_id"]: r for r in task_rows(store)}
    assert rows["recipient-b"]["status"] == "delivered"
    assert rows["recipient-a"]["status"] == "pending" and rows["recipient-a"]["attempts"] == 1
    # Exactly one task per recipient and distinct idempotency keys.
    assert len(task_rows(store)) == 2
    case_key = store.connection.execute(
        "SELECT case_key FROM outbox_alerts WHERE alert_id=1").fetchone()["case_key"]
    keys = {r["recipient_id"]: f"recall:{case_key}:1:{r['recipient_id']}"
            for r in task_rows(store)}
    assert keys["recipient-a"] != keys["recipient-b"]

    # Recipient-a's retry is claimed by worker 1, then its lease expires.
    stale = store.outbox.claim_targets(102, lease_seconds=10)
    assert [t.recipient_id for t in stale] == ["recipient-a"]
    stale_task = stale[0]

    # Meanwhile the subscription configuration changes: a's endpoint moves and b is removed.
    new_config = RouteConfig.from_subscriptions((
        Subscription.create("recipient-a", ["EU"], "high", "http://127.0.0.1/a-v2"),
        Subscription.create("recipient-c", ["EU"], "high", "http://127.0.0.1/c"),
    ))
    store.route_config = new_config  # loading new config must not rewrite rows
    rows_after = {r["recipient_id"]: r for r in task_rows(store)}
    assert rows_after["recipient-a"]["endpoint"] == "http://127.0.0.1/a"
    assert rows_after["recipient-a"]["route_version"] == old_config.version
    assert "recipient-c" not in rows_after  # new subscriptions never backfill old alerts

    # Worker 2 reclaims the expired task; worker 1's late acknowledgement loses.
    reclaimed = store.outbox.claim_targets(113)[0]
    assert reclaimed.recipient_id == "recipient-a" and reclaimed.attempts == 3
    with pytest.raises(LeaseLost):
        store.outbox.acknowledge_target(stale_task, 114)
    assert store.outbox.find_target_task(1, "recipient-a")["lease_id"] == reclaimed.lease_id

    # The queued task still uses the frozen endpoint; worker 2 delivers and confirms.
    assert reclaimed.endpoint == "http://127.0.0.1/a"
    fresh_sink = RecordingTargetSink()
    fresh_sink.deliver(reclaimed)
    store.outbox.acknowledge_target(reclaimed, 114)
    final = {r["recipient_id"]: r for r in task_rows(store)}
    assert final["recipient-a"]["status"] == "delivered"
    assert final["recipient-b"]["status"] == "delivered"
    # Recipient b got exactly one receiver-side delivery despite a's retries.
    assert len(sink.deliveries_for("recipient-b")) == 1
    assert len(fresh_sink.deliveries_for("recipient-a")) == 1
    # Drift is still explainable against the new configuration; delivered rows
    # stay visible when settled history is requested.
    drift = {(d.recipient_id, d.kind)
             for d in RouteAdministrator(store.outbox).detect_drift(
                 new_config, include_settled=True)}
    assert ("recipient-b", "recipient-removed") in drift


# ------------------------------------------------------------ local HTTP receiver
def test_routed_webhook_uses_per_task_endpoint_and_key(tmp_path, http_server):
    received = {"a": [], "b": []}

    def handler(request):
        body = request.rfile.read(int(request.headers["Content-Length"]))
        bucket = "a" if request.path.endswith("/a") else "b"
        received[bucket].append((request.headers["Idempotency-Key"],
                                 request.headers["X-Route-Version"],
                                 request.headers["X-Recipient"], body))
        request.send_response(200)
        request.end_headers()
        request.wfile.write(b"ok")

    with http_server(handler) as endpoint:
        config = two_recipient_config(endpoint_a=endpoint + "/a",
                                      endpoint_b=endpoint + "/b")
        database = tmp_path / "routed-http.sqlite3"
        with RecallStore(database, route_config=config) as http_store:
            from recall_signal_hub.models import SourceDocument
            doc = SourceDocument("agency", "X1", "1", "2026-10-01T12:00:00Z",
                                 {"brand": "B", "product": "P", "market": "EU",
                                  "hazard": "fire", "severity": "high"})
            http_store.commit_batch("agency", None, "1", [doc], 100)
            result = TargetDispatcher(http_store.outbox, RoutedWebhookSink(),
                                      clock=lambda: 100).dispatch()
            assert result == {"delivered": 2, "retry": 0, "lease_lost": 0}
    assert len(received["a"]) == 1 and len(received["b"]) == 1
    assert received["a"][0][0].endswith(":recipient-a")
    assert received["b"][0][0].endswith(":recipient-b")
    assert received["a"][0][1] == config.version
    assert [item[2] for item in received["a"]] == ["recipient-a"]


# ------------------------------------------------------------- legacy mode stays
def test_without_routing_config_the_single_target_path_is_unchanged(store, document):
    store.commit_batch("agency", None, "1", [document()], 100)
    from recall_signal_hub.alerts import AlertDispatcher, RecordingSink
    sink = RecordingSink()
    result = AlertDispatcher(store.outbox, sink, clock=lambda: 100).dispatch()
    assert result == {"delivered": 1, "retry": 0, "lease_lost": 0}
    assert store.counts()["outbox_alerts"] == 0 and store.counts()["outbox_targets"] == 0


def test_routed_enqueue_failure_rolls_back_alert_and_all_targets(tmp_path):
    config = two_recipient_config()
    store = RecallStore(tmp_path / "rollback.sqlite3", route_config=config)
    store.connection.execute("""CREATE TRIGGER reject_target BEFORE INSERT ON outbox_targets
                                BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END""")
    from recall_signal_hub.models import SourceDocument
    doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                         {"brand": "B", "product": "P", "market": "EU",
                          "hazard": "fire", "severity": "high"})
    with pytest.raises(sqlite3.IntegrityError):
        store.commit_batch("agency", None, "1", [doc], 100)
    assert store.counts()["outbox_alerts"] == 0
    assert store.counts()["outbox_targets"] == 0
    assert store.source_state("agency").cursor is None


def test_two_connections_claim_disjoint_target_tasks(tmp_path):
    database = tmp_path / "parallel-routed.sqlite3"
    config = two_recipient_config()
    with RecallStore(database, route_config=config) as first:
        from recall_signal_hub.models import SourceDocument
        doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                             {"brand": "B", "product": "P", "market": "EU",
                              "hazard": "fire", "severity": "high"})
        first.commit_batch("agency", None, "1", [doc], 100)
    with RecallStore(database, route_config=config) as a, \
         RecallStore(database, route_config=config) as b:
        left = {t.recipient_id for t in a.outbox.claim_targets(100)}
        right = {t.recipient_id for t in b.outbox.claim_targets(100)}
        assert left == {"recipient-a", "recipient-b"} and right == set()
        assert left.isdisjoint(right)


def test_routed_state_survives_reopen_and_keeps_frozen_snapshot(tmp_path):
    database = tmp_path / "reopen.sqlite3"
    config = two_recipient_config()
    with RecallStore(database, route_config=config) as store:
        from recall_signal_hub.models import SourceDocument
        doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                             {"brand": "B", "product": "P", "market": "EU",
                              "hazard": "fire", "severity": "high"})
        store.commit_batch("agency", None, "1", [doc], 100)
    with RecallStore(database, route_config=config) as reopened:
        assert reopened.connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert reopened.connection.execute("PRAGMA foreign_key_check").fetchall() == []
        tasks = {t.recipient_id: t for t in reopened.outbox.claim_targets(100)}
        assert set(tasks) == {"recipient-a", "recipient-b"}
        assert all(t.route_version == config.version for t in tasks.values())
        RecordingTargetSink()  # local double only; nothing leaves the process


def test_case_revisions_get_distinct_tasks_and_idempotency_keys(tmp_path):
    from dataclasses import replace
    from recall_signal_hub.models import SourceDocument
    config = two_recipient_config()
    store = RecallStore(tmp_path / "revisions.sqlite3", route_config=config)
    first = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                           {"brand": "B", "product": "P", "market": "EU",
                            "hazard": "fire", "severity": "high"})
    store.commit_batch("agency", None, "1", [first], 100)
    second = replace(first, cursor="2", payload={**first.payload, "hazard": "burn"})
    store.commit_batch("agency", "1", "2", [second], 200)
    assert store.counts()["outbox_alerts"] == 2
    assert store.counts()["outbox_targets"] == 4
    keys = {t.idempotency_key for t in store.outbox.claim_targets(200)}
    assert len(keys) == 4  # revision 1 and revision 2 never share a receiver-side key
    assert sum(1 for key in keys if ":1:recipient-a" in key) == 1
    assert sum(1 for key in keys if ":2:recipient-a" in key) == 1
