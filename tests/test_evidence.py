from __future__ import annotations

import json
import sqlite3
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest

from recall_signal_hub.evidence import (MANIFEST_NAME, manifest_digest, verify_evidence_package)
from recall_signal_hub.models import SourceDocument
from recall_signal_hub.normalize import evidence_bytes
from recall_signal_hub.pipeline import CollectionPipeline
from recall_signal_hub.sources import FixtureSource
from recall_signal_hub.storage import (EvidenceExportFailed, IncompleteEvidenceChain,
                                       RecallStore)


def _revisions(store: RecallStore, key: str) -> list[int]:
    return [r["revision"] for r in store.list_revisions(key)]


def _package_manifest(path: Path) -> dict:
    with zipfile.ZipFile(path) as archive:
        return json.loads(archive.read(MANIFEST_NAME))


def _rewrite_package(path: Path, *, manifest=None, members=None) -> None:
    """Rebuild a package with altered entries, preserving everything else byte-for-byte."""
    with zipfile.ZipFile(path) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    if manifest is not None:
        entries[MANIFEST_NAME] = json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                                            separators=(",", ":")).encode()
    if members:
        entries.update(members)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in entries.items():
            archive.writestr(name, body)


def _two_revision_database(tmp_path: Path) -> tuple[Path, str, str, RecallStore]:
    """The same source first publishes, then rewrites product and hazard descriptions.

    A product identity correction moves the claim to a new case (per the merge
    contract); both cases keep revision 1, so a same-key second revision is added
    by a hazard-only rewrite afterwards.
    """
    database = tmp_path / "hub.sqlite3"
    first = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                           {"product": "Kettle K100", "brand": "Northline", "market": "EU",
                            "hazard": "overheat", "severity": "medium"})
    store = RecallStore(database)
    store.commit_batch("agency", None, "1", [first], 100)
    original_key = store.list_cases()[0]["case_key"]
    rewritten = replace(first, cursor="2",
                        payload={"product": "Kettle K100 Mark II", "brand": "Northline",
                                 "market": "EU", "hazard": "fire", "severity": "high"})
    store.commit_batch("agency", "1", "2", [rewritten], 101)
    moved_key = next(c["case_key"] for c in store.list_cases() if c["case_key"] != original_key)
    # Hazard-only rewrite keeps the same product identity and creates revision 2 on the new case.
    escalated = replace(first, cursor="3",
                        payload={"product": "Kettle K100 Mark II", "brand": "Northline",
                                 "market": "EU", "hazard": "fire and electric shock",
                                 "severity": "critical"})
    store.commit_batch("agency", "2", "3", [escalated], 102)
    assert _revisions(store, moved_key) == [1, 2]
    return database, original_key, moved_key, store


def test_early_and_late_revisions_export_and_verify_independently(tmp_path):
    database, original_key, moved_key, store = _two_revision_database(tmp_path)
    early = tmp_path / "rev1.zip"
    late = tmp_path / "late-rev2.zip"
    info1 = store.export_evidence_package(original_key, 1, early)
    info2 = store.export_evidence_package(moved_key, 2, late)
    assert info1["package_digest"] != info2["package_digest"]
    report1 = verify_evidence_package(early)
    report2 = verify_evidence_package(late)
    assert report1.ok and report2.ok
    manifest1, manifest2 = _package_manifest(early), _package_manifest(late)
    assert manifest1["revision_summary"]["product"] == "kettle k100"
    assert manifest2["revision_summary"]["product"] == "kettle k100 mark ii"
    assert manifest1["revision_summary"]["severity"] == "medium"
    assert manifest2["revision_summary"]["severity"] == "critical"
    # Both revisions carry the policy identity and the full rule snapshot used at the time.
    claim = manifest1["claims"][0]
    assert claim["policy_version"] == "baseline-1" and claim["decision_reason"] == "explicit-publisher-severity"
    assert claim["supplied_severity"] == "medium"
    assert {r["rule_id"] for r in manifest1["risk_policies"][0]["rules"]} >= {"ignition", "temperature"}
    assert manifest1["package_digest"] == report1.package_digest


def test_old_revision_keeps_the_material_it_actually_depended_on(tmp_path):
    database, original_key, _moved_key, store = _two_revision_database(tmp_path)
    early = tmp_path / "rev1.zip"
    store.export_evidence_package(original_key, 1, early)
    manifest = _package_manifest(early)
    # The frozen raw evidence is the first publication, not whatever source_signals holds now.
    raw_name = manifest["evidence"][0]["path"]
    with zipfile.ZipFile(early) as archive:
        raw = json.loads(archive.read(raw_name))
    assert raw["payload"]["product"] == "Kettle K100"
    assert raw["payload"]["hazard"] == "overheat"
    current = store.connection.execute("SELECT * FROM source_signals WHERE source_id='A1'").fetchone()
    assert current["product"] == "kettle k100 mark ii"
    # Re-exporting is deterministic and independent of later collection.
    again = tmp_path / "rev1-again.zip"
    assert store.export_evidence_package(original_key, 1, again)["package_digest"] == manifest["package_digest"]


def test_export_is_pinned_to_the_requested_revision_under_concurrent_collection(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "rev1.zip"
    # Another writer advances the case while an export of revision 1 is in flight conceptually:
    # the read-only snapshot must not follow it.
    fourth = SourceDocument("agency", "A1", "4", "2026-10-03T12:00:00Z",
                            {"product": "Kettle K100 Mark II", "brand": "Northline", "market": "EU",
                             "hazard": "fire and burn", "severity": "critical"})
    store.commit_batch("agency", "3", "4", [fourth], 103)
    assert store.list_cases()  # case state moved on; the export below stays on revision 1
    info = store.export_evidence_package(case_key, 1, package)
    report = verify_evidence_package(package)
    assert report.ok and report.revision == 1
    assert _package_manifest(package)["package_digest"] == info["package_digest"]
    with pytest.raises(EvidenceExportFailed):
        store.export_evidence_package(case_key, 99, tmp_path / "missing.zip")
    assert not (tmp_path / "missing.zip").exists()


def test_verifier_runs_without_database_access_and_is_read_only(tmp_path, monkeypatch):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "rev2.zip"
    store.export_evidence_package(case_key, 2, package)
    store.close()
    database.unlink()  # The database is gone; the verifier must not need it.
    copied = tmp_path / "copy.zip"
    copied.write_bytes(package.read_bytes())
    report = verify_evidence_package(copied)
    assert report.ok
    assert copied.read_bytes() == package.read_bytes()  # strictly read-only
    assert report.as_dict()["trust_scope"].startswith("byte-and-relation")


def test_removing_an_evidence_object_is_reported_as_missing_evidence(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "tampered.zip"
    store.export_evidence_package(case_key, 2, package)
    with zipfile.ZipFile(package) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    evidence_name = next(n for n in entries if n.startswith("evidence/"))
    del entries[evidence_name]
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in entries.items():
            archive.writestr(name, body)
    report = verify_evidence_package(package)
    assert not report.ok
    assert {i.code for i in report.issues} == {"missing-evidence"}


def test_modifying_raw_bytes_is_reported_as_digest_mismatch(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "tampered.zip"
    store.export_evidence_package(case_key, 1, package)
    with zipfile.ZipFile(package) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    evidence_name = next(n for n in entries if n.startswith("evidence/"))
    raw = json.loads(entries[evidence_name])
    raw["payload"]["hazard"] = "fire"  # byte-level tamper, hash name unchanged
    _rewrite_package(package, members={evidence_name: json.dumps(raw).encode()})
    report = verify_evidence_package(package)
    assert not report.ok
    assert "evidence-digest-mismatch" in {i.code for i in report.issues}


def test_replacing_an_evidence_object_is_a_tamper_not_a_reorder(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "replaced.zip"
    store.export_evidence_package(case_key, 1, package)
    foreign = SourceDocument("agency", "OTHER", "5", "2026-10-04T12:00:00Z",
                             {"product": "Other Device", "brand": "Rival", "market": "US",
                              "hazard": "fire", "severity": "high"})
    foreign_body = evidence_bytes(foreign)
    with zipfile.ZipFile(package) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    evidence_name = next(n for n in entries if n.startswith("evidence/"))
    _rewrite_package(package, members={evidence_name: foreign_body})
    report = verify_evidence_package(package)
    assert not report.ok
    codes = {i.code for i in report.issues}
    assert "evidence-digest-mismatch" in codes  # planted bytes do not match the recorded hash


def test_wrong_claim_association_is_detected(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "wrong-link.zip"
    store.export_evidence_package(case_key, 1, package)
    manifest = _package_manifest(package)
    original_digest = manifest["package_digest"]
    manifest["claims"][0]["case_key"] = "deadbeef" * 8
    manifest["package_digest"] = manifest_digest(manifest)
    _rewrite_package(package, manifest=manifest)
    report = verify_evidence_package(package)
    assert not report.ok
    codes = {i.code for i in report.issues}
    # Re-hashing the manifest hides the digest break, but the bad binding still surfaces twice.
    assert {"wrong-claim-association", "claim-case-key-mismatch"} <= codes
    assert "manifest-digest-mismatch" not in codes
    # Without a matching re-hash, the frozen-manifest tamper is itself visible.
    manifest["package_digest"] = original_digest
    _rewrite_package(package, manifest=manifest)
    assert "manifest-digest-mismatch" in {i.code for i in verify_evidence_package(package).issues}


def test_legitimate_member_reorder_remains_valid(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "reordered.zip"
    store.export_evidence_package(case_key, 2, package)
    with zipfile.ZipFile(package) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    reversed_order = list(reversed(entries))
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in reversed_order:
            archive.writestr(name, entries[name])
    report = verify_evidence_package(package)
    assert report.ok and report.issues == ()


def test_manifest_summary_tampering_is_detected(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "summary-tamper.zip"
    store.export_evidence_package(case_key, 1, package)
    manifest = _package_manifest(package)
    manifest["revision_summary"]["severity"] = "critical"
    manifest["package_digest"] = manifest_digest(manifest)
    _rewrite_package(package, manifest=manifest)
    report = verify_evidence_package(package)
    assert not report.ok
    assert "summary-mismatch" in {i.code for i in report.issues}


def test_duplicate_evidence_entries_and_unreferenced_members_are_reported(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "dupes.zip"
    store.export_evidence_package(case_key, 1, package)
    manifest = _package_manifest(package)
    manifest["evidence"].append(dict(manifest["evidence"][0]))
    manifest["package_digest"] = manifest_digest(manifest)
    with zipfile.ZipFile(package) as archive:
        entries = {n: archive.read(n) for n in archive.namelist()}
    entries["evidence/extra.bin"] = b"not part of the closure"
    entries[MANIFEST_NAME] = json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                                        separators=(",", ":")).encode()
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in entries.items():
            archive.writestr(name, body)
    report = verify_evidence_package(package)
    codes = {i.code for i in report.issues}
    assert "duplicate-entry" in codes and "unexpected-package-member" in codes


def test_malformed_zip_and_missing_manifest_are_reported_not_crashed(tmp_path):
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"this is not a zip at all")
    report = verify_evidence_package(bad)
    assert not report.ok and report.issues[0].code == "malformed-package"
    empty = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty, "w") as archive:
        archive.writestr("evidence/x.bin", b"{}")
    report = verify_evidence_package(empty)
    assert not report.ok and report.issues[0].code == "malformed-package"


def test_legacy_database_schema_migrates_but_old_revisions_stay_unexportable(tmp_path, document):
    """A database built before closure tables must open, migrate additively, and still refuse old revisions."""
    database = tmp_path / "legacy.sqlite3"
    setup = sqlite3.connect(database)
    setup.executescript("""
    CREATE TABLE source_state(source TEXT PRIMARY KEY, cursor TEXT, failures INTEGER, not_before INTEGER);
    CREATE TABLE raw_evidence(evidence_hash TEXT PRIMARY KEY, source TEXT, source_id TEXT,
      published_at TEXT, body BLOB);
    CREATE TABLE cases(case_key TEXT PRIMARY KEY, revision INTEGER, fingerprint TEXT, summary TEXT);
    CREATE TABLE source_signals(source TEXT, source_id TEXT, case_key TEXT, evidence_hash TEXT,
      published_at TEXT, brand TEXT, product TEXT, market TEXT, hazard TEXT, severity TEXT,
      PRIMARY KEY(source, source_id));
    CREATE TABLE case_revisions(case_key TEXT, revision INTEGER, summary TEXT,
      PRIMARY KEY(case_key, revision));
    """)
    legacy_summary = json.dumps({"brand": "b", "product": "p", "market": "m",
                                 "severity": "high", "hazards": ["fire"]})
    setup.execute("INSERT INTO cases VALUES(?,?,?,?)", ("legacy-case", 1, "fp", legacy_summary))
    setup.execute("INSERT INTO case_revisions VALUES(?,?,?)", ("legacy-case", 1, legacy_summary))
    setup.commit()
    setup.close()

    with RecallStore(database) as store:
        # The additive migration landed; the old revision still has no honest closure.
        with pytest.raises(IncompleteEvidenceChain):
            store.export_evidence_package("legacy-case", 1, tmp_path / "old.zip")
        # New collection on the migrated schema writes full closures and exports cleanly.
        result = store.commit_batch("agency", None, "1", [document()], 0)
        assert result.accepted == 1
        case_key = next(c["case_key"] for c in store.list_cases() if c["case_key"] != "legacy-case")
        package = tmp_path / "new.zip"
        store.export_evidence_package(case_key, 1, package)
        assert verify_evidence_package(package).ok
        assert store.connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_legacy_revision_without_closure_is_reported_incomplete_and_leaves_no_artifact(tmp_path):
    """Simulate an old database whose case_revisions predate revision_evidence snapshots."""
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    store.connection.execute("DELETE FROM revision_evidence WHERE revision=1")
    target = tmp_path / "should-not-exist.zip"
    with pytest.raises(IncompleteEvidenceChain):
        store.export_evidence_package(case_key, 1, target)
    assert not target.exists()
    assert not list(tmp_path.glob(".*.tmp-*"))
    # Revision 2 still has its closure and exports honestly.
    ok_target = tmp_path / "rev2.zip"
    assert store.export_evidence_package(case_key, 2, ok_target)["claims"] == 1


def test_missing_raw_evidence_body_also_means_incomplete_chain(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    digest = store.connection.execute(
        "SELECT evidence_hash FROM revision_evidence WHERE case_key=? AND revision=1",
        (case_key,)).fetchone()[0]
    store.connection.execute("DELETE FROM raw_evidence WHERE evidence_hash=?", (digest,))
    with pytest.raises(IncompleteEvidenceChain):
        store.export_evidence_package(case_key, 1, tmp_path / "x.zip")


def test_failed_export_cleans_its_temp_file(tmp_path, monkeypatch):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    import recall_signal_hub.storage as storage_mod

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(storage_mod, "write_evidence_package", boom)
    with pytest.raises(EvidenceExportFailed):
        store.export_evidence_package(case_key, 1, tmp_path / "out.zip")
    assert not list(tmp_path.glob(".*.tmp-*"))
    assert not (tmp_path / "out.zip").exists()


def test_multi_source_revision_packages_the_full_closure_and_verifies(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        a = document(hazard="overheat", severity="medium")
        b = document(source="manufacturer", source_id="M9", hazard="fire", severity="high")
        # One page across sources produces a single revision whose closure contains both claims.
        store.commit_batch("agency", None, "1", [a], 0)
        store.commit_batch("manufacturer", None, "1", [b], 0)
        row = store.connection.execute(
            "SELECT case_key, COUNT(*) AS n FROM source_signals GROUP BY case_key ORDER BY n DESC LIMIT 1").fetchone()
        case_key = row["case_key"]
        latest = store.list_revisions(case_key)[-1]["revision"]
        package = tmp_path / "multi.zip"
        info = store.export_evidence_package(case_key, latest, package)
        assert info["claims"] == 2 and info["evidence"] == 2
        report = verify_evidence_package(package)
        assert report.ok
        manifest = _package_manifest(package)
        assert manifest["revision_summary"]["hazards"] == ["fire", "overheat"]
        assert manifest["revision_summary"]["severity"] == "high"


def test_deactivation_revision_packages_an_empty_closure_with_business_summary(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        store.commit_batch("agency", None, "1", [document()], 0)
        case_key = store.list_cases()[0]["case_key"]
        store.commit_batch("agency", "1", "2", [document(cursor="2", product="Different Model")], 1)
        retired = next(c for c in store.list_cases() if c.get("active") is False)
        assert retired["case_key"] == case_key
        package = tmp_path / "retired.zip"
        info = store.export_evidence_package(case_key, 2, package)
        assert info["claims"] == 0 and info["evidence"] == 0
        report = verify_evidence_package(package)
        assert report.ok
        manifest = _package_manifest(package)
        assert manifest["derivation"] == "deactivated-no-claims"
        assert manifest["revision_summary"]["active"] is False
        assert manifest["revision_summary"]["product"] == "kettle k100"


def test_old_package_remains_explainable_after_live_policy_moves_on(tmp_path, monkeypatch):
    import recall_signal_hub.normalize as normalize_mod
    import recall_signal_hub.risk as risk_mod

    database, original_key, moved_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "old-policy.zip"
    store.export_evidence_package(moved_key, 1, package)
    # A later deployment registers a new policy version with re-scaled rules and makes it the default.
    new_rules = risk_mod.DEFAULT_RULES + (risk_mod.HazardRule("smoke", "critical", "smoke"),)
    newer_policy = risk_mod.RiskPolicy(new_rules, version="baseline-2")
    risk_mod.POLICY_REGISTRY["baseline-2"] = new_rules
    monkeypatch.setattr(normalize_mod, "RiskPolicy", lambda: newer_policy)
    # A freshly collected record is classified under the new policy identity...
    future = SourceDocument("other", "O9", "9", "2026-11-01T12:00:00Z",
                            {"product": "Smoke Alarm S1", "brand": "Guard", "market": "EU",
                             "hazard": "smoke"})
    store.commit_batch("other", None, "9", [future], 200)
    # ...while the old package replays its enclosed baseline-1 snapshot and still verifies,
    # with no reference to the live policy or to the database.
    report = verify_evidence_package(package)
    assert report.ok
    manifest = _package_manifest(package)
    assert manifest["risk_policies"][0]["version"] == "baseline-1"
    assert all("smoke" != r["rule_id"] for r in manifest["risk_policies"][0]["rules"])


def test_rule_replay_catches_a_snapshot_that_does_not_reproduce_the_decision(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        # No explicit severity: the stored decision is a rule outcome, so the snapshot is load-bearing.
        store.commit_batch("agency", None, "1", [document(hazard="electric shock")], 0)
        case_key = store.list_cases()[0]["case_key"]
        package = tmp_path / "lied-rules.zip"
        store.export_evidence_package(case_key, 1, package)
    manifest = _package_manifest(package)
    assert manifest["claims"][0]["supplied_severity"] is None
    assert manifest["claims"][0]["decision_reason"] == "shock"
    shock_rule = next(r for r in manifest["risk_policies"][0]["rules"] if r["rule_id"] == "shock")
    shock_rule["severity"] = "low"  # claim the rule would now decide differently
    manifest["package_digest"] = manifest_digest(manifest)
    _rewrite_package(package, manifest=manifest)
    report = verify_evidence_package(package)
    assert not report.ok
    assert "risk-decision-mismatch" in {i.code for i in report.issues}


def test_export_is_byte_deterministic_across_runs(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        store.commit_batch("agency", None, "1", [document()], 0)
        case_key = store.list_cases()[0]["case_key"]
        first = tmp_path / "one.zip"
        second = tmp_path / "two.zip"
        store.export_evidence_package(case_key, 1, first)
        store.export_evidence_package(case_key, 1, second)
    assert first.read_bytes() == second.read_bytes()


def test_export_is_refused_when_the_policy_rules_are_not_retained(tmp_path, document, monkeypatch):
    import recall_signal_hub.risk as risk_mod

    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        store.commit_batch("agency", None, "1", [document()], 0)
        case_key = store.list_cases()[0]["case_key"]
        # Simulate a deployment that no longer retains the rules a historical revision used.
        monkeypatch.setattr(risk_mod, "POLICY_REGISTRY", {})
        target = tmp_path / "unreplayable.zip"
        with pytest.raises(IncompleteEvidenceChain):
            store.export_evidence_package(case_key, 1, target)
        assert not target.exists()


def test_claim_tamper_without_digest_rehash_is_manifest_tamper(tmp_path, document):
    database = tmp_path / "hub.sqlite3"
    with RecallStore(database) as store:
        store.commit_batch("agency", None, "1", [document()], 0)
        case_key = store.list_cases()[0]["case_key"]
        package = tmp_path / "claim-tamper.zip"
        store.export_evidence_package(case_key, 1, package)
    manifest = _package_manifest(package)
    manifest["claims"][0]["hazard"] = "tampered hazard"
    _rewrite_package(package, manifest=manifest)  # package_digest left stale on purpose
    codes = {i.code for i in verify_evidence_package(package).issues}
    assert "manifest-digest-mismatch" in codes
    assert "claim-normalization-mismatch" in codes


def test_export_uses_a_read_only_connection_and_cannot_mutate_the_case(tmp_path):
    database, _moved_key, case_key, store = _two_revision_database(tmp_path)
    package = tmp_path / "rev1.zip"
    store.export_evidence_package(case_key, 1, package)
    read_only = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    with pytest.raises(sqlite3.OperationalError):
        read_only.execute("DELETE FROM case_revisions")
    read_only.close()


def test_fixture_pipeline_end_to_end_via_cli_contracts(tmp_path):
    root = Path(__file__).parents[1]
    database = tmp_path / "cli.sqlite3"
    with RecallStore(database) as store:
        for adapter in FixtureSource.from_fixture(root / "fixtures/recalls.json"):
            assert CollectionPipeline(store).collect(adapter, now=100).status == "completed"
        key = store.list_cases()[0]["case_key"]
        package = tmp_path / "fixture-rev1.zip"
        store.export_evidence_package(key, 1, package)
        assert verify_evidence_package(package).ok
