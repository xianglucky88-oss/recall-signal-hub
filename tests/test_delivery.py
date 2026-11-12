import pytest

from recall_signal_hub.alerts import AlertDispatcher, RecordingSink
from recall_signal_hub.storage import RecallStore
from recall_signal_hub.outbox import LeaseLost


def seed(store,document):store.commit_batch("agency",None,"1",[document()],100)


def test_dispatch_ack_and_no_second_delivery(store,document):
    seed(store,document);sink=RecordingSink();dispatcher=AlertDispatcher(store.outbox,sink,clock=lambda:100)
    assert dispatcher.dispatch()==dict(delivered=1,retry=0,lease_lost=0)
    assert dispatcher.dispatch()["delivered"]==0
    assert len(sink.received)==1


def test_sink_failure_is_delayed_and_retryable(store,document):
    seed(store,document)
    class Failing:
        def deliver(self,alert):raise OSError("sink failed")
    assert AlertDispatcher(store.outbox,Failing(),clock=lambda:100).dispatch()["retry"]==1
    assert store.outbox.claim(101)==[]
    sink=RecordingSink()
    assert AlertDispatcher(store.outbox,sink,clock=lambda:102).dispatch()["delivered"]==1


def test_worker_crash_reclaims_lease_with_same_receiver_key(tmp_path,document):
    database=tmp_path/"hub.sqlite3";sink=RecordingSink()
    with RecallStore(database) as store:
        seed(store,document);first=store.outbox.claim(100,lease_seconds=10)[0];sink.deliver(first)
        # Simulate process loss after receiver delivery and before durable acknowledgement.
    with RecallStore(database) as store:
        assert store.outbox.claim(109)==[]
        reclaimed=store.outbox.claim(110,lease_seconds=10)[0]
        assert reclaimed.idempotency_key==first.idempotency_key
        assert reclaimed.lease_id!=first.lease_id and reclaimed.attempts==2
        sink.deliver(reclaimed);store.outbox.acknowledge(reclaimed,111)
        assert len(sink.received)==1
        with pytest.raises(LeaseLost):store.outbox.acknowledge(first,111)


def test_two_store_connections_claim_disjoint_work(tmp_path,document):
    database=tmp_path/"hub.sqlite3"
    with RecallStore(database) as a, RecallStore(database) as b:
        seed(a,document)
        assert len(a.outbox.claim(100))==1
        assert b.outbox.claim(100)==[]


def test_expired_lease_cannot_acknowledge_or_retry(store,document):
    seed(store,document);alert=store.outbox.claim(100,lease_seconds=1)[0]
    with pytest.raises(LeaseLost):store.outbox.acknowledge(alert,101)
    with pytest.raises(LeaseLost):store.outbox.retry(alert,101,"OSError")
    assert len(store.outbox.claim(101))==1


def test_slow_recipient_uses_actual_completion_time(store,document):
    seed(store,document);now=[100]
    class Slow:
        def deliver(self,alert):now[0]=140
    result=AlertDispatcher(store.outbox,Slow(),clock=lambda:now[0]).dispatch()
    assert result==dict(delivered=0,retry=0,lease_lost=1)
    assert len(store.outbox.claim(140))==1


def test_outbox_cannot_enqueue_without_the_domain_transaction(store):
    with pytest.raises(RuntimeError,match="transaction"):
        store.outbox.enqueue("unknown-case",1,"high","{}",100)


def test_simultaneous_workers_claim_only_once(tmp_path,document):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    database=tmp_path/"parallel.sqlite3"
    with RecallStore(database) as store:seed(store,document)
    gate=Barrier(2)
    def worker():
        with RecallStore(database) as store:
            gate.wait(timeout=5)
            return store.outbox.claim(100)
    with ThreadPoolExecutor(max_workers=2) as workers:
        futures=[workers.submit(worker) for _ in range(2)]
        claims=[f.result(timeout=10) for f in futures]
    assert sum(map(len,claims))==1
