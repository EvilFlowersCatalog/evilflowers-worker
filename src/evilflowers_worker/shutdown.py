"""SIGTERM (scale-in, rollout) without losing or redoing a job (analyzer#2).

- **SIGTERM is Celery's warm shutdown.** The worker takes no new message, and the job in hand runs on with the event
  loop alive, so a job that finishes is acknowledged and the worker exits at once. An idle worker exits at once, and
  any prefetched message goes back on its queue.
- **After ``WORKER_SOFT_SHUTDOWN_TIMEOUT``** (``cancel_after_grace``) the worker sends itself SIGQUIT: Celery's cold
  shutdown cancels the job, and Redis has the message back at once instead of after the visibility timeout.
- **If even that cannot stop the job within ``WORKER_CANCEL_TIMEOUT``** (on dev the parent's kill of its prefork
  child was denied, ``PermissionError`` errno 13, 2026-11-14), the worker restores the message itself and exits. It
  is the container's PID 1, so the child it could not kill goes with it.

A deliberate requeue gives its delivery back (``on_requeue``), so stopping a pod never counts towards the cap.

Celery's own soft shutdown (``REMAP_SIGTERM=SIGQUIT`` + ``worker_soft_shutdown_timeout``) is not used. It sleeps the
whole timeout inside the signal handler, even after the job is done, and then has the pool kill the child before the
job's acknowledgement is processed. On dev that kill was denied, the shutdown aborted, and a book that had succeeded
was restored and redone on another pod. ``keep_sigterm_warm`` holds even if a manifest sets ``REMAP_SIGTERM``.

``tests/test_shutdown.py`` checks every case against a real Redis, the denied kill included.
"""

import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any

from celery import signals

logger = logging.getLogger(__name__)

#: Called with the task ids of the jobs that are being requeued on purpose.
OnRequeue = Callable[[Iterable[str]], None]


class Shutdown:
    def __init__(self, *, grace: float, cancel_timeout: float, redis_url: str, on_requeue: OnRequeue | None = None):
        self.grace = grace
        self.cancel_timeout = cancel_timeout
        self.redis_url = redis_url
        self.on_requeue = on_requeue
        self._timer: threading.Timer | None = None

    def install(self) -> None:
        """Make this the process's shutdown protocol. The receivers are connected once; the last install wins."""
        global _active
        first = _active is None
        _active = self
        if first:
            signals.worker_ready.connect(_on_worker_ready, weak=False)
            signals.worker_shutting_down.connect(_on_worker_shutting_down, weak=False)

    # -- Signal receivers -------------------------------------------------------------------------------------------

    def keep_sigterm_warm(self, sender: Any = None, **_: Any) -> None:
        """Make SIGTERM a warm shutdown even if a manifest sets ``REMAP_SIGTERM=SIGQUIT``.

        With the remap and Celery's soft shutdown off (``worker_soft_shutdown_timeout = 0``), SIGTERM would cancel the
        job in hand at once instead of giving it its grace.
        """
        from celery.apps.worker import _shutdown_handler

        _shutdown_handler(sender.controller, sig="SIGTERM", how="Warm")

    def cancel_after_grace(self, sender: Any = None, how: str | None = None, **_: Any) -> None:
        """On a warm shutdown, switch to a cold one once the grace has passed."""
        if how != "Warm" or self._timer is not None:
            return
        logger.warning(
            "Warm shutdown: a running job has %.0f s to finish before it is cancelled and requeued", self.grace
        )
        self._timer = threading.Timer(self.grace, self._cancel, (os.getpid(),))
        self._timer.daemon = True
        self._timer.start()

    # -- After the grace ---------------------------------------------------------------------------------------------

    def _cancel(self, pid: int) -> None:
        """The grace is over: cold shutdown, and if even that cannot stop the job, requeue it and exit."""
        logger.warning("Grace over: cancelling the running job and requeueing it")
        self._give_back(_active_task_ids())
        os.kill(pid, signal.SIGQUIT)
        time.sleep(self.cancel_timeout)
        self.requeue_and_exit()

    def requeue_and_exit(self) -> None:
        """Put the running jobs back on their queues (flagged redelivered) and end the process at once."""
        import redis
        from celery.worker import state

        for request in list(state.active_requests):
            try:
                # kombu's own restore (unacked hash → head of the queue, flagged redelivered), on a client of its
                # own: the channel's client belongs to the event loop, and from this thread
                # `request.reject(requeue=True)` leaves the message in the unacked hash.
                with redis.Redis.from_url(self.redis_url, socket_timeout=5) as client:
                    request.message.channel.qos.restore_by_tag(
                        request.message.delivery_tag, client=client, leftmost=True
                    )
                logger.error("Cold shutdown could not cancel task %s; requeued it and exiting", request.id)
            except Exception:
                logger.exception("Cannot requeue task %s; it returns after the visibility timeout", request.id)
        logging.shutdown()
        os._exit(1)

    def _give_back(self, task_ids: list[str]) -> None:
        if self.on_requeue is None or not task_ids:
            return
        try:
            self.on_requeue(task_ids)
        except Exception:
            logger.warning("Cannot give back the deliveries of %s", task_ids, exc_info=True)


_active: Shutdown | None = None


def _on_worker_ready(sender: Any = None, **kwargs: Any) -> None:
    if _active is not None:
        _active.keep_sigterm_warm(sender, **kwargs)


def _on_worker_shutting_down(sender: Any = None, **kwargs: Any) -> None:
    if _active is not None:
        _active.cancel_after_grace(sender, **kwargs)


def _active_task_ids() -> list[str]:
    from celery.worker import state

    return [request.id for request in list(state.active_requests)]
