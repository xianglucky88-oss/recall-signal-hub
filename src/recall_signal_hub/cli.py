from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import time

from .alerts import (AlertDispatcher, RecordingSink, RecordingTargetSink,
                     RoutedWebhookSink, TargetDispatcher, WebhookSink)
from .pipeline import CollectionPipeline
from .routing import RouteConfig, RoutingError
from .sources import FixtureSource, HttpSource
from .storage import RecallStore


def load_route_config(path: str | None) -> RouteConfig | None:
    if not path:
        return None
    return RouteConfig.load(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect recall signals and deliver risk alerts")
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect")
    collect.add_argument("--database", required=True)
    sources = collect.add_mutually_exclusive_group(required=True)
    sources.add_argument("--fixture")
    sources.add_argument("--http", action="store_true", help="read publisher endpoint from RECALL_SOURCE_URL")
    collect.add_argument("--source", default="publisher")
    collect.add_argument("--format", choices=["product-feed","recall-bulletin"], default="product-feed")
    collect.add_argument("--routes", help="local routing configuration JSON; without it single-target mode is used")
    dispatch = commands.add_parser("dispatch")
    dispatch.add_argument("--database", required=True)
    dispatch.add_argument("--webhook", action="store_true", help="read receiver endpoint from RECALL_WEBHOOK_URL")
    dispatch.add_argument("--routes", help="local routing configuration JSON used to identify routed stores")
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--database", required=True)
    inspect.add_argument("--routes", help="local routing configuration used to identify routed stores")
    args = parser.parse_args()
    try:
        route_config = load_route_config(getattr(args, "routes", None))
    except (OSError, RoutingError, json.JSONDecodeError) as exc:
        parser.error(f"invalid routing configuration: {type(exc).__name__}")
    with RecallStore(args.database, route_config=route_config) as store:
        if args.command == "collect":
            if args.fixture:
                adapters = FixtureSource.from_fixture(args.fixture)
            else:
                endpoint = os.environ.get("RECALL_SOURCE_URL")
                if not endpoint:
                    parser.error("RECALL_SOURCE_URL is required in HTTP mode")
                adapters = [HttpSource(args.source, endpoint, format=args.format)]
            pipeline = CollectionPipeline(store)
            results = [asdict(pipeline.collect(source, now=int(time.time()))) for source in adapters]
            print(json.dumps({"runs":results,"counts":store.counts()}, ensure_ascii=False))
            if any(r["status"] == "failed" for r in results):
                raise SystemExit(1)
        elif args.command == "dispatch":
            if route_config is not None:
                # Routed mode: each task carries its own frozen endpoint; in
                # webhook mode no single ambient endpoint is required.
                sink = RoutedWebhookSink() if args.webhook else RecordingTargetSink()
                result = TargetDispatcher(store.outbox, sink).dispatch()
            else:
                endpoint = os.environ.get("RECALL_WEBHOOK_URL") if args.webhook else None
                if args.webhook and not endpoint:
                    parser.error("RECALL_WEBHOOK_URL is required in webhook mode")
                sink = WebhookSink(endpoint) if endpoint else RecordingSink()
                result = AlertDispatcher(store.outbox, sink).dispatch()
            print(json.dumps(result))
        else:
            print(json.dumps({"cases":store.list_cases(),"counts":store.counts()},ensure_ascii=False))


if __name__ == "__main__":
    main()
