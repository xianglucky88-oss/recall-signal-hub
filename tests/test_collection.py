from dataclasses import replace
from pathlib import Path
import sqlite3

import pytest

from recall_signal_hub.pipeline import CollectionPipeline
from recall_signal_hub.sources import FixtureSource, SourceError
from recall_signal_hub.storage import RecallStore, StaleCursor


def test_fixture_flow_and_restart_are_idempotent(tmp_path):
    database=tmp_path/"hub.sqlite3"
    adapters=FixtureSource.from_fixture(Path(__file__).parents[1]/"fixtures/recalls.json")
    with RecallStore(database) as store:
        pipeline=CollectionPipeline(store)
        for adapter in adapters:assert pipeline.collect(adapter,now=100).status=="completed"
        counts=store.counts()
        assert counts==dict(raw_evidence=4,rejected_records=1,cases=2,source_signals=3,
                            case_revisions=3,revision_evidence=4,outbox=2)
    with RecallStore(database) as store:
        for adapter in adapters:
            result=CollectionPipeline(store).collect(adapter,now=101)
            assert result.batch.accepted==0
        assert store.counts()==counts
        assert all(c["severity"]=="high" for c in store.list_cases())


def test_conflicting_sources_do_not_oscillate_or_make_duplicate_alerts(store,document):
    a=document(hazard="overheat",severity="medium")
    b=document(source="manufacturer",hazard="fire",severity="high")
    store.commit_batch("agency",None,"1",[a],0)
    store.commit_batch("manufacturer",None,"1",[b],0)
    store.commit_batch("agency","1","2",[replace(a,cursor="2")],0)
    assert store.list_cases()[0]["severity"]=="high"
    assert store.counts()["case_revisions"]==2
    assert store.counts()["outbox"]==1


def test_page_aggregation_has_no_transient_revisions(store,document):
    low=document(hazard="label defect",severity="low")
    high=document(cursor="2",severity="high")
    result=store.commit_batch("agency",None,"2",[low,high],10)
    assert result.cases_changed==result.alerts_created==1
    assert store.counts()["raw_evidence"]==2


def test_poison_record_is_durably_isolated_and_cursor_advances(store,document):
    invalid=document(source_id="broken",cursor="2",product="")
    result=store.commit_batch("agency",None,"2",[document(),invalid],10)
    assert (result.accepted,result.rejected)==(1,1)
    assert store.source_state("agency").cursor=="2"
    assert store.counts()["raw_evidence"]==2


def test_failed_transaction_rolls_back_evidence_case_outbox_and_cursor(store,document):
    store.connection.execute("""CREATE TRIGGER reject_alert BEFORE INSERT ON outbox
                                BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):store.commit_batch("agency",None,"1",[document()],0)
    assert not any(store.counts().values())
    assert store.source_state("agency").cursor is None
    store.connection.execute("DROP TRIGGER reject_alert")
    assert store.commit_batch("agency",None,"1",[document()],0).alerts_created==1


def test_cursor_compare_and_swap_blocks_stale_workers(store,document):
    store.commit_batch("agency",None,"1",[document()],0)
    with pytest.raises(StaleCursor):store.commit_batch("agency",None,"2",[document(cursor="2",hazard="burn")],1)
    assert store.counts()["raw_evidence"]==1


@pytest.mark.parametrize("source,start,end",[("other",None,"1"),("agency","2","1")])
def test_invalid_intervals_leave_no_state(store,document,source,start,end):
    with pytest.raises(ValueError):store.commit_batch(source,start,end,[document()],0)
    assert not any(store.counts().values())


def test_source_failure_backoff_is_persistent_and_isolated(tmp_path,document):
    class Broken:
        name="broken"
        def pull(self,cursor):raise SourceError("unsafe detail must not be recorded")
    database=tmp_path/"hub.sqlite3"
    with RecallStore(database) as store:
        result=CollectionPipeline(store).collect(Broken(),now=100)
        assert result.status=="failed" and result.error_type=="SourceError"
        assert store.source_state("broken").not_before==102
        assert CollectionPipeline(store).collect(FixtureSource("agency",[document()]),now=100).status=="completed"
    with RecallStore(database) as store:
        assert CollectionPipeline(store).collect(Broken(),now=101).status=="deferred"
        assert CollectionPipeline(store).collect(Broken(),now=102).status=="failed"
        assert store.source_state("broken").not_before==106


def test_source_identity_correction_retires_orphan_case(store,document):
    store.commit_batch("agency",None,"1",[document()],0)
    store.commit_batch("agency","1","2",[document(cursor="2",product="Different Model")],1)
    cases=store.list_cases()
    assert len(cases)==2
    assert len([c for c in cases if c.get("active") is False])==1
    assert store.connection.execute("PRAGMA foreign_key_check").fetchall()==[]


def test_database_integrity_after_reopen(tmp_path,document):
    db=tmp_path/"hub.sqlite3"
    with RecallStore(db) as store:store.commit_batch("agency",None,"1",[document()],0)
    with RecallStore(db) as store:
        assert store.connection.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        assert store.counts()["outbox"]==1


def test_reactivated_identity_emits_an_alert(store,document):
    store.commit_batch("agency",None,"1",[document()],0)
    store.commit_batch("agency","1","2",[document(cursor="2",product="Different Model")],1)
    result=store.commit_batch("agency","2","3",[document(cursor="3")],2)
    assert result.alerts_created==1
    assert store.counts()["outbox"]==3


def test_nontext_timestamp_is_isolated_as_business_error(store,document):
    result=store.commit_batch("agency",None,"1",[document(published=123)],0)
    assert result.rejected==1
    assert store.source_state("agency").cursor=="1"
