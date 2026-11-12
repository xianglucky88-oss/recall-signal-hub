from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .models import SourceDocument
from .transport import HttpTransport


class SourceError(RuntimeError):
    pass


class SourceAdapter(Protocol):
    name: str
    def pull(self, cursor: str | None) -> tuple[list[SourceDocument], str | None]: ...


def decode_page(name: str, body: bytes, format: str, cursor: str | None) -> tuple[list[SourceDocument], str | None]:
    """Translate two independently versioned publisher wire formats."""
    if format not in {"product-feed", "recall-bulletin"}:
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


class HttpSource:
    def __init__(self, name: str, endpoint: str, *, format: str = "product-feed",
                 transport: HttpTransport | None = None):
        if format not in {"product-feed", "recall-bulletin"}:
            raise ValueError("unsupported publisher format")
        self.name, self.endpoint, self.format = name, endpoint, format
        self.transport = transport or HttpTransport()

    def pull(self, cursor: str | None) -> tuple[list[SourceDocument], str | None]:
        parts = urlsplit(self.endpoint)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "after"]
        query.append(("after", cursor or "0"))
        url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))
        return decode_page(self.name, self.transport.request(url), self.format, cursor)


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
