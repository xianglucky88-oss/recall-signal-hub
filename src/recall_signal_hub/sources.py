from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .models import SourceDocument, SnapshotPage
from .transport import HttpTransport, TransportError


class SourceError(RuntimeError):
    pass


class ContinuationExpired(SourceError):
    """The publisher rejected a resume token with HTTP 410."""


class GenerationAdvanced(SourceError):
    """Pagination crossed into a different (higher) generation mid-reception."""


class SnapshotConflict(SourceError):
    """The same token was replayed with page contents different from the first reception."""


class SourceAdapter(Protocol):
    name: str
    def pull(self, cursor: str | None) -> tuple[list[SourceDocument], str | None]: ...


class SnapshotAdapter(Protocol):
    name: str
    def start_generation(self) -> SnapshotPage: ...
    def continue_page(self, token: str) -> SnapshotPage: ...


INCREMENTAL_FORMATS = {"product-feed", "recall-bulletin"}
SNAPSHOT_FORMAT = "snapshot-feed"
SUPPORTED_FORMATS = INCREMENTAL_FORMATS | {SNAPSHOT_FORMAT}


def decode_page(name: str, body: bytes, format: str, cursor: str | None) -> tuple[list[SourceDocument], str | None]:
    """Translate two independently versioned publisher wire formats."""
    if format not in INCREMENTAL_FORMATS:
        raise SourceError("unknown publisher format")
    try:
        page = json.loads(body)
        entries = page["records"] if format == "product-feed" else page["bulletins"]
        if not isinstance(entries, list) or len(entries) > 2000:
            raise ValueError("invalid page size")
        documents = []
        for entry in entries:
            if format == "product-feed":
                doc = SourceDocument(name, entry["id"], str(entry["sequence"]), entry["published_at"], entry["product"])
            elif format == "recall-bulletin":
                doc = SourceDocument(name, entry["reference"], str(entry["position"]), entry["issued_at"], entry["details"])
            else:
                raise ValueError("unknown adapter format")
            if not isinstance(doc.source_id, str) or not isinstance(doc.payload, dict):
                raise ValueError("invalid record envelope")
            if int(doc.cursor) <= int(cursor or "0"):
                continue  # Overlapping pages are permitted; no cursor regression.
            documents.append(doc)
        if [int(d.cursor) for d in documents] != sorted({int(d.cursor) for d in documents}):
            raise ValueError("page sequence must be strictly increasing")
        next_cursor = str(page["next_cursor"]) if page.get("next_cursor") is not None else cursor
        if int(next_cursor or "0") < max([int(cursor or "0"), *(int(d.cursor) for d in documents)]):
            raise ValueError("cursor regresses or precedes page contents")
        return documents, next_cursor
    except (KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise SourceError(f"invalid publisher page ({type(exc).__name__})") from None


def _snapshot_content_hash(generation: int, documents: list[SourceDocument], next_token: str | None) -> str:
    """Identity of a page's semantic contents; independent of JSON whitespace or key order."""
    payload = [generation, [[d.source_id, d.cursor, d.published_at, dict(d.payload)] for d in documents], next_token]
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def decode_snapshot_page(name: str, body: bytes) -> SnapshotPage:
    """Decode a full-snapshot page. Tokens are opaque strings, never parsed or compared by value."""
    try:
        page = json.loads(body)
        generation = page["generation"]
        if not isinstance(generation, int) or isinstance(generation, bool) or generation <= 0:
            raise ValueError("generation must be a positive integer")
        entries = page["records"]
        if not isinstance(entries, list) or len(entries) > 2000:
            raise ValueError("invalid page size")
        if "next_token" not in page:
            raise ValueError("next_token is required")
        next_token = page["next_token"]
        if next_token is not None and (not isinstance(next_token, str) or not next_token):
            raise ValueError("next_token must be a nonempty string or null")
        documents: list[SourceDocument] = []
        for entry in entries:
            doc = SourceDocument(name, entry["id"], str(entry["sequence"]), entry["published_at"], entry["product"])
            if not isinstance(doc.source_id, str) or not isinstance(doc.payload, dict):
                raise ValueError("invalid record envelope")
            sequence = int(doc.cursor)  # sequence only has to be an integer within the generation
            if sequence <= 0:
                raise ValueError("sequence must be a positive integer")
            documents.append(doc)
        sequences = [int(d.cursor) for d in documents]
        if sequences != sorted(set(sequences)):
            raise ValueError("within-generation sequence must be strictly increasing")
        return SnapshotPage(generation, tuple(documents), next_token,
                            _snapshot_content_hash(generation, documents, next_token))
    except (KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise SourceError(f"invalid publisher page ({type(exc).__name__})") from None


class HttpSource:
    def __init__(self, name: str, endpoint: str, *, format: str = "product-feed",
                 transport: HttpTransport | None = None):
        if format not in SUPPORTED_FORMATS:
            raise ValueError("unsupported publisher format")
        self.name, self.endpoint, self.format = name, endpoint, format
        self.transport = transport or HttpTransport()

    def pull(self, cursor: str | None) -> tuple[list[SourceDocument], str | None]:
        parts = urlsplit(self.endpoint)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "after"]
        query.append(("after", cursor or "0"))
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
        return decode_page(self.name, self.transport.request(url), self.format, cursor)


class SnapshotHttpSource:
    """Publisher delivering full per-generation snapshots behind opaque resume tokens."""

    def __init__(self, name: str, endpoint: str, *, transport: HttpTransport | None = None):
        self.name, self.endpoint = name, endpoint
        self.transport = transport or HttpTransport()

    def _url(self, token: str | None) -> str:
        parts = urlsplit(self.endpoint)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "token"]
        if token is not None:
            query.append(("token", token))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))

    def _fetch(self, token: str | None) -> SnapshotPage:
        try:
            body = self.transport.request(self._url(token))
        except TransportError as exc:
            if exc.status_code == 410:
                raise ContinuationExpired("resume token is no longer valid") from None
            raise
        return decode_snapshot_page(self.name, body)

    def start_generation(self) -> SnapshotPage:
        return self._fetch(None)

    def continue_page(self, token: str) -> SnapshotPage:
        return self._fetch(token)


class FixtureSource:
    def __init__(self, name: str, documents: list[SourceDocument]) -> None:
        self.name = name
        self._documents = sorted(documents, key=lambda d: int(d.cursor))
        if any(d.source != name for d in documents):
            raise ValueError("fixture source identity mismatch")

    def pull(self, cursor: str | None) -> tuple[list[SourceDocument], str | None]:
        pending = [d for d in self._documents if int(d.cursor) > int(cursor or "0")]
        return pending, pending[-1].cursor if pending else cursor

    @classmethod
    def from_fixture(cls, path: str | Path) -> list["FixtureSource"]:
        sources = json.loads(Path(path).read_text())["sources"]
        return [cls(name, [SourceDocument(name, e["source_id"], str(e["cursor"]), e["published_at"], e["payload"])
                           for e in entries]) for name, entries in sources.items()]
