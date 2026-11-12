from __future__ import annotations

from .models import RunResult
from .sources import SourceAdapter
from .storage import RecallStore, StaleCursor


class CollectionPipeline:
    def __init__(self, store: RecallStore):
        self.store = store

    def collect(self, source: SourceAdapter, *, now: int) -> RunResult:
        state = self.store.source_state(source.name)
        if state.not_before > now:
            return RunResult(source.name, "deferred")
        try:
            documents, next_cursor = source.pull(state.cursor)
            batch = self.store.commit_batch(source.name, state.cursor, next_cursor, documents, now)
            return RunResult(source.name, "completed", batch)
        except StaleCursor:
            return RunResult(source.name, "superseded")
        except Exception as exc:
            self.store.record_failure(source.name, now)
            # Failure classifications are safe to expose; raw exception text may contain credentials.
            return RunResult(source.name, "failed", error_type=type(exc).__name__)
