"""Self-contained recall evidence packages and their offline verifier.

A package freezes *one* case revision together with the minimal evidence
closure the revision actually depended on: the merged source claims, the raw
publisher payloads behind each claim, and the risk/merge rules in force at the
time. The verifier needs no database and no network: every check re-derives
the recorded facts from the enclosed bytes and reports mismatches read-only.

Package self-consistency means bytes, references and derivations agree. It does
not authenticate the publisher and there is deliberately no signature service.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import io
import json
import zipfile
from pathlib import Path

from .models import SourceDocument
from .normalize import canonical_text, evidence_bytes
from .reconcile import case_fingerprint, case_key, summarize
from .risk import HazardRule, RiskError, RiskPolicy as _RiskPolicy


def _derive_signal_fields(raw: dict) -> dict:
    """Deterministic, policy-independent normalization re-derived from raw evidence."""
    payload = raw["payload"]
    return {
        "source": raw["source"], "source_id": raw["source_id"],
        "published_at": raw["published_at"],
        "brand": canonical_text(payload["brand"]), "product": canonical_text(payload["product"]),
        "market": canonical_text(payload["market"]), "hazard": canonical_text(payload["hazard"]),
        "supplied_severity": canonical_text(payload["severity"]) if payload.get("severity") is not None else None}


@dataclass(frozen=True)
class _Identity:
    brand: str
    product: str
    market: str


class _SnapshotRiskPolicy:
    """A policy reconstructed purely from the bytes enclosed in the package.

    It deliberately bypasses the live rule registry so later deployments cannot
    change how an old package is replayed.
    """

    def __init__(self, rules: tuple[HazardRule, ...], version: str):
        self._policy = _RiskPolicy(rules, version)

    def assess(self, hazard: str, supplied: str | None = None):
        return self._policy.assess(hazard, supplied)

PACKAGE_FORMAT = "recall-evidence-package/1"
MANIFEST_NAME = "manifest.json"
EVIDENCE_DIR = "evidence"
# Package self-consistency never claims publisher authentication.
TRUST_SCOPE = "byte-and-relation-consistency-only; publisher identity is not authenticated"
# Fixed zip metadata so re-exporting the same revision yields identical bytes.
_ZIP_DATE = (1980, 1, 1, 0, 0, 0)


class EvidencePackageError(RuntimeError):
    """The package cannot even be parsed well enough to be verified."""


@dataclass(frozen=True)
class VerificationIssue:
    code: str
    message: str
    subject: str | None = None


@dataclass(frozen=True)
class VerificationReport:
    path: str
    ok: bool
    package_digest: str | None
    case_key: str | None
    revision: int | None
    issues: tuple[VerificationIssue, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict:
        return {"path": self.path, "ok": self.ok, "package_digest": self.package_digest,
                "case_key": self.case_key, "revision": self.revision, "trust_scope": TRUST_SCOPE,
                "issues": [{"code": i.code, "message": i.message, "subject": i.subject} for i in self.issues]}


def canonical_manifest(manifest: dict) -> bytes:
    """Deterministic byte form of the manifest, excluding its own digest."""
    body = {k: v for k, v in manifest.items() if k != "package_digest"}
    return json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def manifest_digest(manifest: dict) -> str:
    return hashlib.sha256(canonical_manifest(manifest)).hexdigest()


def build_manifest(*, case_key_value: str, revision: int, summary: dict, fingerprint: str,
                   claims: list[dict], evidence: list[dict], policies: list[dict],
                   derivation: str) -> dict:
    """Assemble the closed, order-independent manifest for one revision."""
    if derivation not in {"merged-claims", "deactivated-no-claims"}:
        raise ValueError("unknown revision derivation")
    manifest = {
        "format": PACKAGE_FORMAT,
        "case_key": case_key_value,
        "revision": revision,
        "revision_summary": dict(summary),
        "summary_fingerprint": fingerprint,
        "derivation": derivation,
        "merge_contract": {
            "identity": ["brand", "product", "market"],
            "severity": "maximum-by-severity-rank",
            "hazards": "sorted-union"},
        "claims": sorted(claims, key=lambda c: c["claim_id"]),
        "evidence": sorted((dict(e, path=f"{EVIDENCE_DIR}/{e['evidence_hash']}.bin") for e in evidence),
                           key=lambda e: e["evidence_hash"]),
        "risk_policies": sorted(policies, key=lambda p: p["version"]),
    }
    manifest["package_digest"] = manifest_digest(manifest)
    return manifest


def write_evidence_package(target: str | Path, manifest: dict, bodies: dict[str, bytes]) -> str:
    """Write the package deterministically. Caller is responsible for atomic placement."""
    if manifest_digest(manifest) != manifest.get("package_digest"):
        raise EvidencePackageError("manifest digest is stale")
    expected = {e["evidence_hash"] for e in manifest["evidence"]}
    if set(bodies) != expected:
        raise EvidencePackageError("evidence bodies do not match the manifest closure")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        first = zipfile.ZipInfo(MANIFEST_NAME, _ZIP_DATE)
        first.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(first, json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                                           separators=(",", ":")).encode())
        for digest in sorted(bodies):
            info = zipfile.ZipInfo(f"{EVIDENCE_DIR}/{digest}.bin", _ZIP_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, bodies[digest])
    Path(target).write_bytes(buffer.getvalue())
    return manifest["package_digest"]


def _load(path: str | Path) -> tuple[dict, dict[str, bytes], list[VerificationIssue]]:
    issues: list[VerificationIssue] = []
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if MANIFEST_NAME not in names:
                raise EvidencePackageError("manifest is missing from the package")
            manifest = json.loads(archive.read(MANIFEST_NAME).decode())
            if not isinstance(manifest, dict):
                raise EvidencePackageError("manifest must be a JSON object")
            members = {n: archive.read(n) for n in names if not n.endswith("/") and n != MANIFEST_NAME}
    except (zipfile.BadZipFile, KeyError, UnicodeError, json.JSONDecodeError, EvidencePackageError) as exc:
        raise EvidencePackageError(f"unreadable evidence package: {type(exc).__name__}") from None
    return manifest, members, issues


def verify_evidence_package(path: str | Path) -> VerificationReport:
    """Offline, read-only verification. Never writes, notifies or opens a database."""
    path = str(path)
    issues: list[VerificationIssue] = []
    try:
        manifest, members, load_issues = _load(path)
        issues.extend(load_issues)
    except EvidencePackageError as exc:
        return VerificationReport(path, False, None, None, None,
                                  (VerificationIssue("malformed-package", str(exc)),))

    case_key_value = manifest.get("case_key")
    revision = manifest.get("revision")

    if manifest.get("format") != PACKAGE_FORMAT:
        issues.append(VerificationIssue("unsupported-format",
                                        f"format must be {PACKAGE_FORMAT}", manifest.get("format")))
    recorded_digest = manifest.get("package_digest")
    actual_digest = manifest_digest(manifest)
    if recorded_digest != actual_digest:
        issues.append(VerificationIssue("manifest-digest-mismatch",
                                        "manifest content was altered or its digest was recomputed",
                                        recorded_digest))

    claims = manifest.get("claims")
    evidence = manifest.get("evidence")
    if not isinstance(claims, list) or not isinstance(evidence, list) or not isinstance(case_key_value, str) \
            or not isinstance(revision, int) or revision < 1 or not isinstance(manifest.get("revision_summary"), dict):
        issues.append(VerificationIssue("malformed-manifest", "required manifest fields are missing or mistyped"))
        return _finalize(path, issues, actual_digest, case_key_value, revision)

    # --- Reference closure: every claim resolves, every evidence object is used. ---
    evidence_index: dict[str, dict] = {}
    for entry in evidence:
        digest = entry.get("evidence_hash")
        if digest in evidence_index:
            issues.append(VerificationIssue("duplicate-entry", "evidence hash listed twice", digest))
            continue
        evidence_index[digest] = entry
        expected_path = f"{EVIDENCE_DIR}/{digest}.bin"
        if entry.get("path") != expected_path:
            issues.append(VerificationIssue("evidence-path-mismatch", "evidence path disagrees with its hash",
                                            entry.get("path")))
        if expected_path not in members:
            issues.append(VerificationIssue("missing-evidence", "evidence object referenced by manifest is absent",
                                            digest))
    claim_ids: set[str] = set()
    referenced: set[str] = set()
    for claim in claims:
        claim_id = claim.get("claim_id")
        if claim_id in claim_ids:
            issues.append(VerificationIssue("duplicate-entry", "claim id listed twice", claim_id))
        claim_ids.add(claim_id)
        digest = claim.get("evidence_hash")
        referenced.add(digest)
        if digest not in evidence_index:
            issues.append(VerificationIssue("dangling-claim-reference",
                                            "claim points at an evidence object outside the package", claim_id))
        if claim.get("case_key") != case_key_value:
            issues.append(VerificationIssue("wrong-claim-association",
                                            "claim is bound to a different case than the package revision", claim_id))
    for digest in sorted(set(evidence_index) - referenced):
        issues.append(VerificationIssue("unreferenced-evidence",
                                        "evidence object is not part of the minimal closure of any claim", digest))
    allowed_members = {entry["path"] for entry in evidence_index.values()}
    for name in sorted(set(members) - allowed_members):
        issues.append(VerificationIssue("unexpected-package-member", "package contains a file not declared in the manifest", name))

    # --- Byte-level evidence hashes and raw-document re-derivation. ---
    rebuilt: dict[str, dict] = {}
    for digest, entry in evidence_index.items():
        body = members.get(entry.get("path", ""))
        if body is None:
            continue
        if hashlib.sha256(body).hexdigest() != digest:
            issues.append(VerificationIssue("evidence-digest-mismatch",
                                            "raw evidence bytes do not hash to the recorded digest", digest))
            continue
        try:
            raw = json.loads(body.decode())
            document = SourceDocument(raw["source"], raw["source_id"], "0",
                                      raw["published_at"], raw["payload"])
            derived = _derive_signal_fields(raw)
        except (KeyError, UnicodeError, json.JSONDecodeError, TypeError):
            issues.append(VerificationIssue("malformed-evidence", "raw evidence is not a canonical source document", digest))
            continue
        if (entry.get("source"), entry.get("source_id"), entry.get("published_at")) != \
                (document.source, document.source_id, document.published_at):
            issues.append(VerificationIssue("evidence-metadata-mismatch",
                                            "evidence metadata disagrees with the enclosed raw document", digest))
        if hashlib.sha256(evidence_bytes(document)).hexdigest() != digest:
            issues.append(VerificationIssue("evidence-digest-mismatch",
                                            "canonical re-serialization disagrees with the recorded digest", digest))
            continue
        rebuilt[digest] = derived

    # --- Claim fields must equal what the raw evidence derives to. ---
    policy_snapshots = {p.get("version"): p for p in manifest.get("risk_policies", []) if isinstance(p, dict)}
    for claim in claims:
        derived = rebuilt.get(claim.get("evidence_hash"))
        if derived is None:
            continue
        # Text/identity fields are deterministic normalizations of the raw bytes.
        expected_fields = {**derived, "evidence_hash": claim.get("evidence_hash")}
        for field_name, expected in expected_fields.items():
            if claim.get(field_name) != expected:
                issues.append(VerificationIssue("claim-normalization-mismatch",
                                                f"claim field {field_name} is not derivable from the raw evidence",
                                                claim.get("claim_id")))
                break
        identity_signal = _Identity(derived["brand"], derived["product"], derived["market"])
        if case_key(identity_signal) != claim.get("case_key"):
            issues.append(VerificationIssue("claim-case-key-mismatch",
                                            "recomputed product identity disagrees with the bound case_key",
                                            claim.get("claim_id")))
        snapshot = policy_snapshots.get(claim.get("policy_version"))
        if snapshot is None:
            issues.append(VerificationIssue("policy-snapshot-missing",
                                            "the rules that classified this claim are not enclosed",
                                            claim.get("policy_version")))
            continue
        try:
            policy = _SnapshotRiskPolicy(tuple(HazardRule(r["rule_id"], r["severity"], r["phrase"])
                                               for r in snapshot["rules"]), snapshot["version"])
            replayed = policy.assess(derived["hazard"], claim.get("supplied_severity"))
        except (RiskError, KeyError, TypeError):
            issues.append(VerificationIssue("policy-snapshot-malformed",
                                            "enclosed rule snapshot cannot reconstruct the policy",
                                            snapshot.get("version")))
            continue
        # The risk decision is replayed solely from the enclosed rule snapshot, never live rules.
        if (replayed.severity, replayed.policy_version, replayed.reason) != \
                (claim.get("severity"), claim.get("policy_version"), claim.get("decision_reason")):
            issues.append(VerificationIssue("risk-decision-mismatch",
                                            "replaying the enclosed rules does not reproduce the stored decision",
                                            claim.get("claim_id")))

    # --- Revision summary must be reproducible from the frozen claims. ---
    summary = manifest["revision_summary"]
    derivation = manifest.get("derivation")
    try:
        if derivation == "merged-claims":
            if not claims:
                issues.append(VerificationIssue("summary-mismatch",
                                                "an active revision requires a nonempty claim closure"))
            elif summarize(claims) != summary:
                issues.append(VerificationIssue("summary-mismatch",
                                                "revision summary is not the order-independent merge of the claims"))
            base = summary
        elif derivation == "deactivated-no-claims":
            if summary.get("active") is not False:
                issues.append(VerificationIssue("summary-mismatch",
                                                "deactivated revision must carry active=false"))
            base = {k: v for k, v in summary.items() if k != "active"}
            if claims:
                # Retirement freezes no claims; the last business state is carried by the summary.
                if summarize(claims) != base:
                    issues.append(VerificationIssue("summary-mismatch",
                                                    "deactivated summary does not preserve the last merged claims"))
            else:
                required = {"brand", "product", "market", "severity", "hazards"}
                if not required <= set(summary):
                    issues.append(VerificationIssue("summary-mismatch",
                                                    "empty-closure retirement must still carry the full business summary"))
        else:
            issues.append(VerificationIssue("malformed-manifest", "unknown revision derivation", derivation))
            base = summary
        if case_fingerprint(base if derivation == "deactivated-no-claims" else summary) != \
                manifest.get("summary_fingerprint"):
            issues.append(VerificationIssue("fingerprint-mismatch",
                                            "recorded summary fingerprint does not match the summary bytes"))
    except (KeyError, TypeError, ValueError) as exc:
        issues.append(VerificationIssue("summary-mismatch", f"summary cannot be re-derived: {exc}"))

    return _finalize(path, issues, actual_digest, case_key_value, revision)


def _finalize(path: str, issues: list[VerificationIssue], digest: str | None,
              case_key_value: str | None, revision: int | None) -> VerificationReport:
    return VerificationReport(path, not issues, digest, case_key_value, revision, tuple(issues))
