"""Telemetry and JSON logs wired into the Celery worker's signals.

``setup()`` runs on ``worker_process_init``, which fires in the process that runs the tasks: under prefork in each
child after the fork, so exporter and profiler threads are started where they survive. The parent only consumes
messages and never starts them.
"""

import time
from typing import Any

from celery import signals

from evilflowers_worker.observability import metrics
from evilflowers_worker.observability.logs import use_json_logs
from evilflowers_worker.observability.setup import setup, shutdown

_task_started: dict[str, float] = {}
_connected = False


def _task_prerun(task_id: str | None = None, **_kwargs: Any) -> None:
    if task_id:
        _task_started[task_id] = time.perf_counter()


def _task_postrun(task_id: str | None = None, task: Any = None, state: str | None = None, **_kwargs: Any) -> None:
    started = _task_started.pop(task_id, None) if task_id else None
    if started is not None and task is not None:
        metrics.TASK_DURATION.record(
            time.perf_counter() - started, {"celery.task.name": task.name, "celery.task.state": state or "UNKNOWN"}
        )


def connect() -> None:
    """Hook telemetry and JSON logs into the worker. Idempotent; ``runtime.create_app`` calls it."""
    global _connected
    if _connected:
        return
    _connected = True
    signals.worker_process_init.connect(lambda **_: setup(), weak=False)
    signals.worker_process_shutdown.connect(shutdown, weak=False)
    signals.worker_shutdown.connect(shutdown, weak=False)
    signals.after_setup_logger.connect(use_json_logs, weak=False)
    signals.after_setup_task_logger.connect(use_json_logs, weak=False)
    signals.task_prerun.connect(_task_prerun, weak=False)
    signals.task_postrun.connect(_task_postrun, weak=False)
