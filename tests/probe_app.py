"""A worker built from the kit alone, run as a real prefork worker by ``test_runtime``.

- ``probe.book``: counts its delivery, writes ``<task id> <delivery>`` to ``PROBE_STARTED``, sleeps ``PROBE_SECONDS``
  (600 by default), touches ``PROBE_FINISHED``;
- ``probe.doomed``: kills its own process (an OOM, as far as the parent can tell) until its delivery is past
  ``MAX_DELIVERIES``; appends each delivery number to ``PROBE_DELIVERIES``;
- ``probe.coordinator``: sends a ``RegionsJob`` to ``probe.figures`` on the ``downstream`` queue (``CeleryPublisher``);
- ``probe.figures``: the example leaf job (``example.EchoFigures``) against ``CATALOG_API_URL``, through ``run_job``;
- ``probe.convert``: the example catalog job (``example.convert``) through ``run_task``: writes its output and an
  outcome file under ``STORAGE_PATH``, or kills its own process while ``die`` is set, until the cap writes a ``failed``
  outcome.
"""

import os
import signal
import time
from pathlib import Path
from typing import Any

from evilflowers_worker.contract import RegionsJob, Route
from evilflowers_worker.deliveries import capped
from evilflowers_worker.runtime import JOB_TASK_OPTIONS, CeleryPublisher, create_app, run_job, run_task, runtime
from evilflowers_worker.settings import WorkerSettings
from example import ConvertRequest, EchoFigures, convert, give_up

if os.environ.get("PROBE_KILL_DENIED"):
    # As on dev (2026-11-14): the parent may not signal its prefork child (PermissionError, errno 13).
    import billiard.pool

    _kill = os.kill

    def _denied(pid: int, sig: int) -> None:
        if pid != os.getpid():
            raise PermissionError(13, "Permission denied")
        _kill(pid, sig)

    os.kill = _denied  # billiard.popen_fork (terminating the pool)
    billiard.pool._kill = _denied  # Pool.terminate_job (cancelling the running task), bound at import

settings = WorkerSettings()
app = create_app("probe", settings, include=[], queues=[settings.queue])


@app.task(name="probe.book", bind=True, **JOB_TASK_OPTIONS)
def book(self: Any) -> None:
    delivery = runtime().delivery(self)
    Path(os.environ["PROBE_STARTED"]).write_text(f"{self.request.id} {delivery}")
    time.sleep(float(os.environ.get("PROBE_SECONDS", "600")))
    Path(os.environ["PROBE_FINISHED"]).touch()


@app.task(name="probe.doomed", bind=True, **JOB_TASK_OPTIONS)
def doomed(self: Any) -> str:
    delivery = runtime().delivery(self)
    with Path(os.environ["PROBE_DELIVERIES"]).open("a") as deliveries:
        deliveries.write(f"{delivery}\n")
    if capped(delivery, settings.max_deliveries):
        return "capped"
    os.kill(os.getpid(), signal.SIGKILL)
    return "unreachable"


@app.task(name="probe.coordinator", bind=True, **JOB_TASK_OPTIONS)
def coordinator(self: Any, downstream: str) -> None:
    message = RegionsJob(run_id="run-1", source="a.pdf", output_dir="out", attempt=3)
    CeleryPublisher(self.app).publish(Route("probe.figures", downstream), message)


@app.task(name="probe.figures", bind=True, **JOB_TASK_OPTIONS)
def figures(self: Any, **kwargs: Any) -> str:
    return run_job(self, EchoFigures, RegionsJob, kwargs)


@app.task(name="probe.convert", bind=True, **JOB_TASK_OPTIONS)
def convert_task(self: Any, **kwargs: Any) -> dict[str, Any]:
    return run_task(self, convert, ConvertRequest, kwargs, on_capped=give_up)
