from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import time

from .alerts import AlertDispatcher, RecordingSink, WebhookSink
from .pipeline import CollectionPipeline
from .sources import FixtureSource, HttpSource, SnapshotHttpSource, SNAPSHOT_FORMAT
from .storage import RecallStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect recall signals and deliver risk alerts")
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect")
    collect.add_argument("--database", required=True)
    sources = collect.add_mutually_exclusive_group(required=True)
    sources.add_argument("--fixture")
    sources.add_argument("--http", action="store_true", help="read publisher endpoint from RECALL_SOURCE_URL")
    collect.add_argument("--source", default="publisher")
    collect.add_argument("--format", choices=["product-feed","recall-bulletin","snapshot-feed"], default="product-feed")
    dispatch = commands.add_parser("dispatch")
    dispatch.add_argument("--database", required=True)
    dispatch.add_argument("--webhook", action="store_true", help="read receiver endpoint from RECALL_WEBHOOK_URL")
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--database", required=True)
    args = parser.parse_args()
    with RecallStore(args.database) as store:
        if args.command == "collect":
            if args.fixture and args.format == SNAPSHOT_FORMAT:
                parser.error("snapshot-feed is an HTTP publisher format and cannot be read from a fixture")
            if args.fixture:
                adapters = FixtureSource.from_fixture(args.fixture)
            else:
                endpoint = os.environ.get("RECALL_SOURCE_URL")
                if not endpoint:
                    parser.error("RECALL_SOURCE_URL is required in HTTP mode")
                if args.format == SNAPSHOT_FORMAT:
                    adapters = [SnapshotHttpSource(args.source, endpoint)]
                else:
                    adapters = [HttpSource(args.source, endpoint, format=args.format)]
            pipeline = CollectionPipeline(store)
            if args.format == SNAPSHOT_FORMAT:
                results = [asdict(pipeline.collect_snapshot(adapters[0], now=int(time.time())))]
            else:
                results = [asdict(pipeline.collect(source, now=int(time.time()))) for source in adapters]
            print(json.dumps({"runs":results,"counts":store.counts()}, ensure_ascii=False))
            if any(r["status"] == "failed" for r in results):
                raise SystemExit(1)
        elif args.command == "dispatch":
            endpoint = os.environ.get("RECALL_WEBHOOK_URL") if args.webhook else None
            if args.webhook and not endpoint:
                parser.error("RECALL_WEBHOOK_URL is required in webhook mode")
            sink = WebhookSink(endpoint) if endpoint else RecordingSink()
            result = AlertDispatcher(store.outbox,sink).dispatch()
            print(json.dumps(result))
        else:
            print(json.dumps({"cases":store.list_cases(),"counts":store.counts()},ensure_ascii=False))


if __name__ == "__main__":
    main()
