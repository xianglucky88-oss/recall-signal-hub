from __future__ import annotations

import hashlib
import json
from typing import Iterable, Mapping

from .models import RecallSignal
from .risk import SEVERITY_RANK


def case_key(signal: RecallSignal) -> str:
    # A tuple serialization prevents delimiter collisions. No fuzzy identity guessing.
    identity = json.dumps([signal.brand, signal.product, signal.market], ensure_ascii=False)
    return hashlib.sha256(identity.encode()).hexdigest()


def summarize(signals: Iterable[Mapping[str, str]]) -> dict:
    """Reduce latest publisher claims, independent of arrival order."""
    rows = list(signals)
    if not rows:
        raise ValueError("a case requires at least one source signal")
    identity = {(r["brand"], r["product"], r["market"]) for r in rows}
    if len(identity) != 1:
        raise ValueError("cannot reconcile different product identities")
    brand, product, market = next(iter(identity))
    return {"brand": brand, "product": product, "market": market,
            "severity": max((r["severity"] for r in rows), key=SEVERITY_RANK.__getitem__),
            "hazards": sorted({r["hazard"] for r in rows})}


def case_fingerprint(summary: Mapping) -> str:
    value = json.dumps(dict(summary), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()
