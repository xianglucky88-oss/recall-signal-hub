from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import sys
import time

from .alerts import AlertDispatcher, RecordingSink, WebhookSink
from .evidence import verify_evidence_package
from .pipeline import CollectionPipeline
from .sources import FixtureSource, HttpSource
from .storage import EvidenceExportFailed, IncompleteEvidenceChain, RecallStore


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
    dispatch = commands.add_parser("dispatch")
    dispatch.add_argument("--database", required=True)
    dispatch.add_argument("--webhook", action="store_true", help="read receiver endpoint from RECALL_WEBHOOK_URL")
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--database", required=True)
    export = commands.add_parser("export", help="export a frozen, self-verifying evidence package for one revision")
    export.add_argument("--database", required=True)
    export.add_argument("--case-key", required=True)
    export.add_argument("--revision", required=True, type=int)
    export.add_argument("--output", required=True)
    verify = commands.add_parser("verify", help="offline, read-only verification of an evidence package (no database)")
    verify.add_argument("package")
    args = parser.parse_args()
    if args.command == "verify":
        report = verify_evidence_package(args.package)
        print(json.dumps(report.as_dict(), ensure_ascii=False))
        raise SystemExit(0 if report.ok else 2)
    with RecallStore(args.database) as store:
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
            endpoint = os.environ.get("RECALL_WEBHOOK_URL") if args.webhook else None
            if args.webhook and not endpoint:
                parser.error("RECALL_WEBHOOK_URL is required in webhook mode")
            sink = WebhookSink(endpoint) if endpoint else RecordingSink()
            result = AlertDispatcher(store.outbox,sink).dispatch()
            print(json.dumps(result))
        elif args.command == "export":
            try:
                result = store.export_evidence_package(args.case_key, args.revision, args.output)
            except IncompleteEvidenceChain as exc:
                print(json.dumps({"status":"incomplete-evidence-chain","error":str(exc)}, ensure_ascii=False),
                      file=sys.stderr)
                raise SystemExit(3)
            except EvidenceExportFailed as exc:
                print(json.dumps({"status":"export-failed","error":str(exc)}, ensure_ascii=False), file=sys.stderr)
                raise SystemExit(4)
            print(json.dumps({"status":"exported", **result}, ensure_ascii=False))
        else:
            print(json.dumps({"cases":store.list_cases(),"counts":store.counts()},ensure_ascii=False))


if __name__ == "__main__":
    main()
