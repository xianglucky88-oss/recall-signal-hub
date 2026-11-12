from __future__ import annotations

import hashlib
import json
import unicodedata
from datetime import datetime

from .models import RecallSignal, SourceDocument
from .risk import RiskError, RiskPolicy


class NormalizationError(ValueError):
    pass


def canonical_text(value: str) -> str:
    """Normalize spacing and punctuation while preserving non-Latin identifiers."""
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join("".join(c if c.isalnum() else " " for c in normalized).split())


def evidence_bytes(document: SourceDocument) -> bytes:
    # Cursor identifies transport position, not a change to the publisher evidence.
    return json.dumps({"source": document.source, "source_id": document.source_id,
                       "published_at": document.published_at, "payload": dict(document.payload)},
                      ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def classify_severity(hazard: str, supplied: str | None) -> str:
    try:
        return RiskPolicy().assess(canonical_text(hazard), canonical_text(supplied or "")).severity
    except RiskError as exc:
        raise NormalizationError(str(exc)) from exc


def normalize(document: SourceDocument) -> RecallSignal:
    if not document.source.strip() or not document.source_id.strip():
        raise NormalizationError("source and source_id are required")
    if not isinstance(document.published_at, str):
        raise NormalizationError("published_at must be text")
    try:
        published = datetime.fromisoformat(document.published_at.replace("Z", "+00:00"))
        if published.tzinfo is None:
            raise ValueError("timestamp requires an offset")
    except (ValueError, TypeError) as exc:
        raise NormalizationError("published_at must be an offset-aware ISO timestamp") from exc
    payload = document.payload
    required = ("product", "brand", "market", "hazard")
    if any(not isinstance(payload.get(k), str) or not canonical_text(payload[k]) for k in required):
        raise NormalizationError("product, brand, market and hazard require nonempty text")
    supplied = payload.get("severity")
    if supplied is not None and not isinstance(supplied, str):
        raise NormalizationError("severity must be text")
    return RecallSignal(
        source=document.source, source_id=document.source_id, published_at=document.published_at,
        product=canonical_text(payload["product"]), brand=canonical_text(payload["brand"]),
        market=canonical_text(payload["market"]), hazard=canonical_text(payload["hazard"]),
        severity=classify_severity(payload["hazard"], supplied),
        evidence_hash=hashlib.sha256(evidence_bytes(document)).hexdigest())
