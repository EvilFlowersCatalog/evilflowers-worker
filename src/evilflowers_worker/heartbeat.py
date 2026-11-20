"""The heartbeat file behind the k8s exec liveness probe (evilflowers-worker#11).

The workers have no port, so the probe tests a file instead: the worker touches ``WORKER_HEARTBEAT_FILE`` (default
``/tmp/evilflowers-worker.heartbeat``) every ``WORKER_HEARTBEAT_INTERVAL`` seconds (default 30) while it is healthy,
and ``evilflowers-worker-alive --max-age 90`` (``alive``) fails once the file is older than the threshold. **The
recommended threshold is 3 x the interval** (90 s by default).

The touch is a consumer bootstep on the worker's own timer, the one Celery's event heartbeat runs on, not a thread of
its own. The timer fires from the consumer's event loop in the main process, so:

- a main process that hangs, deadlocks, blocks its event loop or is stopped (``kill -STOP``) stops touching;
- a long task does not: prefork (and threads) run the task off the event loop, so a 2 h book keeps the file fresh.
  The ``solo`` pool runs the task *on* the event loop, and would stop touching for its duration: do not combine it
  with the probe;
- a worker that has lost its broker stops too (the consumer restarts and blocks reconnecting), so a Redis outage
  longer than the threshold restarts the pods. They retry the connection on start
  (``broker_connection_retry_on_startup``), so nothing is lost.

A file that cannot be written (no emptyDir at that path) is logged once and never fails the worker; the probe then
fails, which shows the manifest is wrong.
"""

import logging
from pathlib import Path
from typing import Any

from celery import bootsteps
from celery.worker.consumer.tasks import Tasks

logger = logging.getLogger(__name__)


class Heartbeat(bootsteps.StartStopStep):
    """Touch ``path`` every ``interval`` seconds from the consumer's timer. Made by ``heartbeat_step``."""

    requires = (Tasks,)
    path: Path
    interval: float

    def __init__(self, c: Any, **kwargs: Any):
        super().__init__(c, **kwargs)
        self._entry: Any = None
        self._failing = False

    def start(self, c: Any) -> None:
        self.touch()
        self._entry = c.timer.call_repeatedly(self.interval, self.touch)
        logger.info("Heartbeat: touching %s every %.0f s", self.path, self.interval)

    def stop(self, c: Any) -> None:
        if self._entry is not None:
            self._entry.cancel()
            self._entry = None

    shutdown = stop

    def touch(self) -> None:
        try:
            self.path.touch()
        except OSError as error:
            if not self._failing:
                logger.error("Heartbeat: cannot touch %s (%s); the liveness probe will fail", self.path, error)
            self._failing = True
        else:
            self._failing = False


def heartbeat_step(path: Path, interval: float) -> type[Heartbeat]:
    """A ``Heartbeat`` bound to ``path`` and ``interval``, for ``app.steps["consumer"].add(...)``."""
    return type("Heartbeat", (Heartbeat,), {"path": path, "interval": interval})
