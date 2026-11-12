from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest

from recall_signal_hub.models import SourceDocument
from recall_signal_hub.storage import RecallStore


@pytest.fixture
def store(tmp_path):
    with RecallStore(tmp_path / "recalls.sqlite3") as store:
        yield store


@pytest.fixture
def document():
    def make(*, source="agency", source_id="A1", cursor="1", product="Kettle K100", brand="Northline",
             market="EU", hazard="fire", severity=None, published="2026-10-01T12:00:00Z"):
        payload={"product":product,"brand":brand,"market":market,"hazard":hazard}
        if severity is not None:
            payload["severity"]=severity
        return SourceDocument(source,source_id,cursor,published,payload)
    return make


@pytest.fixture
def http_server():
    @contextmanager
    def start(handler):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self): handler(self)
            def do_POST(self): handler(self)
            def log_message(self,*args): pass
        server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown();server.server_close();thread.join(timeout=3)
    return start
