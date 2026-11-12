"""End-to-end evidence-package demonstration (not part of the shipped package).

Scenario:
  1. A source publishes a recall; a revision is produced.
  2. The same source rewrites the product identity and hazard description;
     subsequent revisions follow the merge contract.
  3. The early and the later revisions are exported independently.
  4. Each package is then damaged one object at a time (remove / replace /
     alter) and reordered, proving the verifier distinguishes missing parts,
     tampering, wrong associations and legitimate reordering.

Run with:  PYTHONPATH=src python3 demo/evidence_demo.py
"""
from __future__ import annotations

import json
import shutil
import sys
import zipfile
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from recall_signal_hub.evidence import MANIFEST_NAME, manifest_digest, verify_evidence_package
from recall_signal_hub.models import SourceDocument
from recall_signal_hub.normalize import evidence_bytes
from recall_signal_hub.storage import IncompleteEvidenceChain, RecallStore

WORK = ROOT / "demo" / "workdir"


def reset_workdir() -> None:
    shutil.rmtree(WORK, ignore_errors=True)
    WORK.mkdir(parents=True)


def build_database() -> tuple[RecallStore, str, str]:
    database = WORK / "hub.sqlite3"
    store = RecallStore(database)
    first = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                           {"product": "Travel Kettle K100", "brand": "Northline",
                            "market": "EU", "hazard": "overheat", "severity": "medium"})
    store.commit_batch("agency", None, "1", [first], 100)
    original_key = store.list_cases()[0]["case_key"]

    # Same publisher record id, rewritten product identity and hazard -> claim moves case.
    rewritten = replace(first, cursor="2",
                        payload={"product": "Travel Kettle K100 Mark II", "brand": "Northline",
                                 "market": "EU", "hazard": "fire", "severity": "high"})
    store.commit_batch("agency", "1", "2", [rewritten], 101)
    moved_key = next(c["case_key"] for c in store.list_cases() if c["case_key"] != original_key)

    # Hazard-only rewrite on the new identity creates a genuine revision 2 on the same case.
    escalated = replace(first, cursor="3",
                        payload={"product": "Travel Kettle K100 Mark II", "brand": "Northline",
                                 "market": "EU", "hazard": "fire and electric shock",
                                 "severity": "critical"})
    store.commit_batch("agency", "2", "3", [escalated], 102)
    return store, original_key, moved_key


def read_manifest(path: Path) -> dict:
    with zipfile.ZipFile(path) as archive:
        return json.loads(archive.read(MANIFEST_NAME))


def repack(path: Path, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in entries.items():
            archive.writestr(name, body)


def entries_of(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        return {n: archive.read(n) for n in archive.namelist()}


def show(title: str, path: Path) -> None:
    report = verify_evidence_package(path)
    status = "ACCEPTED (ok)" if report.ok else "REJECTED"
    print(f"  [{status}] {title}")
    for issue in report.issues:
        print(f"      - {issue.code}: {issue.message}" + (f" (subject={issue.subject})" if issue.subject else ""))


def main() -> None:
    reset_workdir()
    store, original_key, moved_key = build_database()
    print("== cases and revisions produced ==")
    for case in store.list_cases():
        print(f"  case {case['case_key'][:12]}... revision={case['revision']} "
              f"product={case['product']!r} severity={case['severity']} active={case.get('active', True)}")

    early = WORK / "early-rev1.zip"
    late = WORK / "late-rev2.zip"
    store.export_evidence_package(original_key, 1, early)
    store.export_evidence_package(moved_key, 2, late)
    print("\n== exports ==")
    for path in (early, late):
        manifest = read_manifest(path)
        print(f"  {path.name}: revision={manifest['revision']} "
              f"product={manifest['revision_summary']['product']!r} "
              f"severity={manifest['revision_summary']['severity']} "
              f"claims={len(manifest['claims'])} evidence={len(manifest['evidence'])} "
              f"digest={manifest['package_digest'][:16]}...")

    print("\n== baseline verification (database still present) ==")
    show("early package, untouched", early)
    show("late package, untouched", late)

    # The verifier must work with the database deleted entirely.
    store.close()
    (WORK / "hub.sqlite3").unlink()
    for suffix in ("-wal", "-shm"):
        p = WORK / f"hub.sqlite3{suffix}"
        if p.exists():
            p.unlink()
    print("\n== offline verification (database removed) ==")
    show("early package with no database", early)
    show("late package with no database", late)

    print("\n== tamper matrix on the late package ==")
    target = WORK / "tamper.zip"

    # 1. Remove one evidence object -> missing-evidence only.
    shutil.copy(late, target)
    entries = entries_of(target)
    victim = next(n for n in entries if n.startswith("evidence/"))
    del entries[victim]
    repack(target, entries)
    show(f"remove evidence object {victim}", target)

    # 2. Alter the raw bytes (same member name, changed payload) -> digest mismatch.
    shutil.copy(late, target)
    entries = entries_of(target)
    victim = next(n for n in entries if n.startswith("evidence/"))
    raw = json.loads(entries[victim])
    raw["payload"]["hazard"] = "totally different hazard"
    entries[victim] = json.dumps(raw).encode()
    repack(target, entries)
    show("alter raw bytes inside an evidence object", target)

    # 3. Replace an evidence object with a foreign, internally valid document -> digest mismatch.
    shutil.copy(late, target)
    entries = entries_of(target)
    victim = next(n for n in entries if n.startswith("evidence/"))
    foreign = SourceDocument("agency", "FOREIGN", "9", "2026-12-01T00:00:00Z",
                             {"product": "Other Device X", "brand": "Rival Co", "market": "US",
                              "hazard": "fire", "severity": "high"})
    entries[victim] = evidence_bytes(foreign)
    repack(target, entries)
    show("replace evidence object with a foreign document", target)

    # 4. Wrong association: bind a claim to a different case and re-hash the manifest.
    shutil.copy(late, target)
    manifest = read_manifest(target)
    manifest["claims"][0]["case_key"] = "0" * 64
    manifest["package_digest"] = manifest_digest(manifest)
    entries = entries_of(target)
    entries[MANIFEST_NAME] = json.dumps(manifest, sort_keys=True,
                                        ensure_ascii=False, separators=(",", ":")).encode()
    repack(target, entries)
    show("rewrite a claim's case_key (wrong association)", target)

    # 5. Legitimate reorder: reverse zip member order, identical bytes per member.
    shutil.copy(late, target)
    entries = entries_of(target)
    repack(target, {n: entries[n] for n in reversed(list(entries))})
    show("reverse package member order (legitimate reorder)", target)

    # 6. Old revision still points at its own material, even though the source moved on.
    print("\n== historical fidelity ==")
    early_manifest = read_manifest(early)
    with zipfile.ZipFile(early) as archive:
        raw = json.loads(archive.read(early_manifest["evidence"][0]["path"]))
    print(f"  early package raw product={raw['payload']['product']!r} hazard={raw['payload']['hazard']!r}")
    print(f"  early package summary product={early_manifest['revision_summary']['product']!r}")
    show("early package remains valid and pinned to revision 1 material", early)

    print("\n== incomplete evidence chain is refused, no artifact left ==")
    store2 = RecallStore(WORK / "legacy.sqlite3")
    doc = SourceDocument("agency", "A1", "1", "2026-10-01T12:00:00Z",
                         {"product": "Kettle", "brand": "B", "market": "EU", "hazard": "fire"})
    store2.commit_batch("agency", None, "1", [doc], 0)
    key = store2.list_cases()[0]["case_key"]
    store2.connection.execute("DELETE FROM revision_evidence")
    refused = WORK / "refused.zip"
    try:
        store2.export_evidence_package(key, 1, refused)
    except IncompleteEvidenceChain as exc:
        print(f"  IncompleteEvidenceChain raised: {exc}")
    print(f"  artifact exists after refusal? {refused.exists()} (expected False)")
    leftovers = list(WORK.glob(".*.tmp-*"))
    print(f"  temp files left behind? {len(leftovers)} (expected 0)")


if __name__ == "__main__":
    main()
