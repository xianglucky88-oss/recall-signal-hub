from __future__ import annotations

from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


class TransportError(RuntimeError):
    """A sanitized failure: never echo endpoint query strings or credentials."""


@dataclass
class HttpTransport:
    timeout_seconds: float = 10.0
    max_bytes: int = 2_000_000

    def request(self, url: str, *, data: bytes | None = None,
                headers: dict[str, str] | None = None) -> bytes:
        parts = urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
            raise TransportError("HTTP endpoint must use http(s) without embedded credentials")
        request_headers = {"User-Agent": "recall-signal-hub/0.1", **(headers or {})}
        try:
            with urlopen(Request(url, data=data, headers=request_headers), timeout=self.timeout_seconds) as response:
                body = response.read(self.max_bytes + 1)
                if len(body) > self.max_bytes:
                    raise TransportError("HTTP body exceeds configured size limit")
                return body
        except HTTPError as exc:
            raise TransportError(f"HTTP status {exc.code}") from None
        except (URLError, OSError, ValueError) as exc:
            raise TransportError(f"HTTP request failed ({type(exc).__name__})") from None
