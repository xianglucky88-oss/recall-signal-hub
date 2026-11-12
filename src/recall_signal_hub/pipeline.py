from __future__ import annotations

from .models import RunResult
from .sources import (
    ContinuationExpired,
    GenerationAdvanced,
    SnapshotAdapter,
    SourceAdapter,
)
from .storage import RecallStore, StaleCursor, StaleGeneration


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

    def collect_snapshot(self, source: SnapshotAdapter, *, now: int) -> RunResult:
        """Collect a full-snapshot source. Reception is recoverable; nothing changes until a
        complete generation is applied in a single transaction."""
        name = source.name
        state = self.store.source_state(name)
        if state.not_before > now:
            return RunResult(name, "deferred")
        reception = self.store.reception_state(name)
        try:
            if reception is not None and reception.complete:
                # The process died after the last page landed but before/during apply: finish the switch.
                batch = self.store.apply_snapshot(name, reception.generation, now)
                return RunResult(name, "completed", batch)
            if reception is None:
                page = source.start_generation()
                if state.applied_generation is not None and page.generation < state.applied_generation:
                    raise GenerationAdvanced("publisher serves an older generation than the one in force")
                if state.applied_generation == page.generation:
                    # The generation already in force was re-received; nothing must be revised or alerted.
                    return RunResult(name, "completed")
                self.store.start_snapshot_reception(name, page.generation)
                request_token = None
            else:
                token = reception.next_token
                if token is None:
                    raise GenerationAdvanced("reception marker is inconsistent with its stored token")
                page = source.continue_page(token)
                if page.generation != reception.generation:
                    raise GenerationAdvanced("pagination crossed into a different generation")
                request_token = token
            self.store.stage_snapshot_page(name, page.generation, request_token, page)
            reception = self.store.reception_state(name)
            if reception is not None and reception.complete:
                batch = self.store.apply_snapshot(name, page.generation, now)
                return RunResult(name, "completed", batch)
            return RunResult(name, "receiving")
        except (ContinuationExpired, GenerationAdvanced) as exc:
            if reception is None:
                # A tokenless start returned a generation older than the one in force: the
                # publisher is inconsistent, nothing was received, and backoff must apply.
                self.store.record_failure(name, now)
                return RunResult(name, "failed", error_type=type(exc).__name__)
            # The unfinished generation can never be completed or mixed with a newer one.
            self.store.abandon_snapshot_reception(name, reception.generation)
            return RunResult(name, "restarted", error_type=type(exc).__name__)
        except StaleGeneration:
            return RunResult(name, "superseded")
        except Exception as exc:
            self.store.record_failure(name, now)
            # Failure classifications are safe to expose; raw exception text may contain credentials.
            return RunResult(name, "failed", error_type=type(exc).__name__)
