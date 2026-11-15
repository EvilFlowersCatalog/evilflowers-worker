"""The runtime against a real Redis and a real prefork worker (``probe_app``).

- Scale-in (analyzer#2): what a SIGTERMed worker does with the job in hand.
- The delivery cap (analyzer#3): a job that kills its worker every time is given up.
- Wire facts other services rely on: a command sent from inside a task carries its ``parent_id`` (the catalog's job
  ledger), and a leaf job reads its run, sends ``?attempt=`` and reports, or reports an invalid message failed.

Needs ``TEST_REDIS_URL`` (default ``redis://localhost:6379/15``); skipped when no Redis answers, except in CI (``CI``
set), where a missing Redis is a failure: these tests are the only guard of the private Celery and kombu APIs the
shutdown uses.
"""

import os
import signal
import time
from pathlib import Path

import pytest

from evilflowers_worker.deliveries import DeliveryCounter
from evilflowers_worker.runtime import create_app
from evilflowers_worker.settings import WorkerSettings
from evilflowers_worker.testing import StubCatalogServer, WorkerProcess, redis_client, wait_for

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
HERE = Path(__file__).parent
BOX = {"x0": 0.1, "y0": 0.1, "x1": 0.5, "y1": 0.5}


@pytest.fixture
def broker():
    if redis_client(REDIS_URL) is None:
        if os.environ.get("CI"):
            pytest.fail(f"no Redis at {REDIS_URL}: the real-broker tests must run in CI")
        pytest.skip(f"no Redis at {REDIS_URL}")
    return REDIS_URL


@pytest.fixture
def probe(broker, tmp_path):
    """``probe(**env)``: a running probe worker; files it writes are in ``tmp_path``."""
    started: list[WorkerProcess] = []

    def make(**env: str) -> WorkerProcess:
        files = {
            "PROBE_STARTED": str(tmp_path / "started"),
            "PROBE_FINISHED": str(tmp_path / "finished"),
            "PROBE_DELIVERIES": str(tmp_path / "deliveries"),
            "WORKER_CANCEL_TIMEOUT": "3",
        }
        worker = WorkerProcess("probe_app", redis_url=broker, pythonpath=[str(HERE)], env={**files, **env})
        started.append(worker.__enter__())
        return worker

    yield make
    for worker in started:
        worker.__exit__(None, None, None)


def start_book(worker: WorkerProcess, tmp_path: Path) -> tuple[str, int]:
    """Send ``probe.book`` and wait until it runs; its task id and delivery number."""
    worker.send("probe.book")
    assert wait_for((tmp_path / "started").exists, 60), "the book never started"
    assert worker.redis.llen(worker.queue) == 0
    task_id, delivery = (tmp_path / "started").read_text().split()
    return task_id, int(delivery)


# -- Configuration ----------------------------------------------------------------------------------------------------


def test_celery_never_blocks_on_its_own_soft_shutdown(settings):
    app = create_app("probe", settings.model_copy(update={"worker_soft_shutdown_timeout": 7.5}), include=[])
    assert app.conf.worker_soft_shutdown_timeout == 0
    assert app.conf.worker_prefetch_multiplier == 1
    assert app.conf.broker_transport_options["visibility_timeout"] == 12 * 3600


def test_queues_are_direct_exchanges_named_after_them():
    app = create_app("probe", WorkerSettings(_env_file=None, queue="evilflowers.probe.a"), include=[])
    (queue,) = app.conf.task_queues
    assert (queue.name, queue.exchange.name, queue.exchange.type, queue.routing_key) == (
        "evilflowers.probe.a",
        "evilflowers.probe.a",
        "direct",
        "evilflowers.probe.a",
    )


# -- Scale-in ---------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("kill_denied", [False, True], ids=["kill-allowed", "kill-denied-as-on-dev"])
def test_a_job_still_running_at_the_deadline_is_requeued_at_once(probe, tmp_path, kill_denied):
    worker = probe(WORKER_SOFT_SHUTDOWN_TIMEOUT="2", **({"PROBE_KILL_DENIED": "1"} if kill_denied else {}))
    task_id, delivery = start_book(worker, tmp_path)
    assert delivery == 1

    sent = time.monotonic()
    worker.signal(signal.SIGTERM)

    assert wait_for(lambda: worker.redis.llen(worker.queue) == 1, 30), "the job was not requeued"
    assert time.monotonic() - sent < 30
    assert worker.wait(timeout=30) is not None
    assert not (tmp_path / "finished").exists()
    (requeued,) = worker.queued()
    assert requeued["properties"]["delivery_info"]["redelivered"] is True
    # A deliberate requeue gives its delivery back: stopping a pod never counts towards the cap.
    assert not worker.redis.exists(DeliveryCounter(None, prefix="probe").key(task_id))


@pytest.mark.parametrize("remap_sigterm", [False, True], ids=["image", "manifest-sets-REMAP_SIGTERM"])
def test_a_job_that_ends_within_the_grace_finishes_and_is_not_redone(probe, tmp_path, remap_sigterm):
    env = {"WORKER_SOFT_SHUTDOWN_TIMEOUT": "20", "PROBE_SECONDS": "4"}
    worker = probe(**env, **({"REMAP_SIGTERM": "SIGQUIT"} if remap_sigterm else {}))
    before = worker.unacked()
    start_book(worker, tmp_path)

    worker.signal(signal.SIGTERM)

    # The worker exits as soon as the job is done, well before the grace runs out.
    assert worker.wait(timeout=20) == 0
    assert (tmp_path / "finished").exists()
    # Acknowledged: neither back on the queue nor left for Redis to restore later.
    assert worker.redis.llen(worker.queue) == 0
    assert worker.unacked() == before


def test_an_idle_worker_exits_at_once(probe):
    worker = probe()
    worker.wait_ready()

    sent = time.monotonic()
    worker.signal(signal.SIGTERM)

    assert worker.wait(timeout=20) == 0
    assert time.monotonic() - sent < 20


# -- Delivery cap -----------------------------------------------------------------------------------------------------


def test_a_job_that_kills_its_worker_every_time_is_given_up_after_the_cap(probe, tmp_path):
    worker = probe(MAX_DELIVERIES="2")
    worker.wait_ready()
    # The unacked hash is one per db, shared by every queue: compare, never count.
    before = worker.unacked()
    worker.send("probe.doomed")

    deliveries = tmp_path / "deliveries"
    assert wait_for(lambda: deliveries.exists() and deliveries.read_text().split() == ["1", "2", "3"], 90), (
        deliveries.read_text() if deliveries.exists() else "never delivered"
    )
    # The third delivery is past the cap: it returns instead of dying, and the message is acknowledged.
    assert wait_for(lambda: worker.redis.llen(worker.queue) == 0 and worker.unacked() == before, 30)


# -- Wire facts -------------------------------------------------------------------------------------------------------


def test_a_command_sent_from_a_task_carries_its_parent(probe):
    """The catalog's job ledger hangs child tasks under their run by ``parent_id``."""
    worker = probe()
    worker.wait_ready()
    downstream = f"{worker.queue}-downstream"
    try:
        parent = worker.send("probe.coordinator", downstream=downstream)
        assert wait_for(lambda: worker.redis.llen(downstream) == 1, 30), "nothing was sent downstream"
        (sent,) = worker.queued(downstream)
        headers = sent["headers"]
        assert headers["task"] == "probe.figures"
        assert headers["parent_id"] == parent and headers["root_id"] == parent
        assert WorkerProcess.kwargs(sent) == {
            "run_id": "run-1",
            "source": "a.pdf",
            "output_dir": "out",
            "attempt": 3,
            "regions": [],
        }
    finally:
        worker.redis.delete(downstream)


def test_a_leaf_job_runs_end_to_end_against_the_catalog(probe):
    run = {"status": "running", "jobs": {"figures": "pending"}, "attempt": 3}
    with StubCatalogServer(run=run) as catalog:
        worker = probe(CATALOG_API_URL=catalog.url)
        worker.wait_ready()
        regions = [{"page": 2, "seq": 0, "bbox": BOX}]
        worker.send("probe.figures", run_id="run-1", source="a.pdf", output_dir="out", attempt=3, regions=regions)
        assert wait_for(lambda: any("/jobs/" in path for _m, path in catalog.paths()), 30), catalog.calls

    assert catalog.paths() == [
        ("GET", "/extraction/v1/runs/run-1"),
        ("POST", "/extraction/v1/runs/run-1/regions?attempt=3"),
        ("POST", "/extraction/v1/runs/run-1/jobs/figures?attempt=3"),
    ]
    assert catalog.calls[2][2]["status"] == "done"


def test_an_invalid_message_is_reported_failed_and_dropped(probe):
    with StubCatalogServer() as catalog:
        worker = probe(CATALOG_API_URL=catalog.url)
        worker.wait_ready()
        before = worker.unacked()
        # A region on page 0: the analyzer never sends one, but a consumer must not choke on it.
        bad = [{"page": 0, "seq": 0, "bbox": BOX}]
        worker.send("probe.figures", run_id="run-1", source="a.pdf", output_dir="out", attempt=3, regions=bad)
        assert wait_for(lambda: catalog.calls, 30), "nothing was reported"
        assert wait_for(lambda: worker.redis.llen(worker.queue) == 0 and worker.unacked() == before, 30)

    ((method, path, body),) = catalog.calls
    assert (method, path) == ("POST", "/extraction/v1/runs/run-1/jobs/figures?attempt=3")
    assert body["status"] == "failed" and body["error"].startswith("Invalid message")
