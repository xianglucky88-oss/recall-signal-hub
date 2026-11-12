import json
from urllib.parse import parse_qs, urlsplit

import pytest

from recall_signal_hub.alerts import AlertDispatcher, WebhookSink
from recall_signal_hub.pipeline import CollectionPipeline
from recall_signal_hub.sources import HttpSource, SourceError, decode_page
from recall_signal_hub.transport import HttpTransport, TransportError


def wire_page():
    return {"records":[{"id":"R1","sequence":1,"published_at":"2026-10-01T12:00:00Z",
            "product":{"brand":"Northline","product":"K100","market":"EU","hazard":"fire"}}],"next_cursor":"1"}


def test_real_http_collection_and_webhook_delivery(store,http_server):
    received={};queries=[]
    def handler(request):
        if request.command=="GET":
            queries.append(parse_qs(urlsplit(request.path).query));body=json.dumps(wire_page()).encode()
        else:
            key=request.headers["Idempotency-Key"]
            received[key]=request.rfile.read(int(request.headers["Content-Length"]))
            body=b"ok"
        request.send_response(200);request.end_headers();request.wfile.write(body)
    with http_server(handler) as endpoint:
        source=HttpSource("agency",endpoint+"/feed?category=appliances")
        assert CollectionPipeline(store).collect(source,now=100).status=="completed"
        assert CollectionPipeline(store).collect(source,now=101).batch.accepted==0
        assert queries[0]["after"]==["0"] and queries[1]["after"]==["1"]
        assert AlertDispatcher(store.outbox,WebhookSink(endpoint+"/alerts"),clock=lambda:101).dispatch()["delivered"]==1
    assert len(received)==1
    assert json.loads(next(iter(received.values())))["severity"]=="high"


def test_second_publisher_wire_format():
    page={"bulletins":[{"reference":"B1","position":1,"issued_at":"2026-10-01T00:00:00Z","details":{"product":"K100"}}],"next_cursor":1}
    docs,cursor=decode_page("manufacturer",json.dumps(page).encode(),"recall-bulletin",None)
    assert docs[0].source_id=="B1" and cursor=="1"


@pytest.mark.parametrize("mutation",[lambda p:p.update(next_cursor="0"),lambda p:p.update(records="bad"),lambda p:p.update(next_cursor="invalid")])
def test_invalid_pages_do_not_advance_cursor(mutation):
    page=wire_page();mutation(page)
    with pytest.raises(SourceError):decode_page("agency",json.dumps(page).encode(),"product-feed",None)


def test_nonmonotonic_page_is_rejected():
    page=wire_page();page["records"].append({**page["records"][0],"id":"R2"})
    with pytest.raises(SourceError):decode_page("agency",json.dumps(page).encode(),"product-feed",None)


def test_http_status_is_sanitized(http_server):
    def handler(request):request.send_response(503);request.end_headers()
    with http_server(handler) as endpoint:
        with pytest.raises(TransportError) as exc:HttpTransport().request(endpoint+"/?private=value")
    assert str(exc.value)=="HTTP status 503"


def test_response_limit(http_server):
    def handler(request):request.send_response(200);request.end_headers();request.wfile.write(b"x"*100)
    with http_server(handler) as endpoint:
        with pytest.raises(TransportError,match="size limit"):HttpTransport(max_bytes=10).request(endpoint)


@pytest.mark.parametrize("endpoint",["file:///private/tmp/source","ftp://example.invalid/feed","https://user:password@example.invalid/feed"])
def test_unsafe_endpoint_forms_are_rejected(endpoint):
    with pytest.raises(TransportError):HttpTransport().request(endpoint)
