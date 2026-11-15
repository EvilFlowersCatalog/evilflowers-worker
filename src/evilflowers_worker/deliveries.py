"""How many times a message has been delivered: the cap on a job that kills its worker every time.

A worker that dies mid-job (an OOM in the child, or of the whole container) leaves the message unacknowledged, and
Redis delivers it again; nothing caps that loop on its own (analyzer#3: one book, 24 passes). The count lives in the
broker's Redis, keyed by task id:

- a fresh delivery (not ``redelivered``) starts at 1. The catalog sends a re-run under the same task id
  (``uuid5(extract:<acq>:v<version>)``), so counting fresh deliveries would cap a book after five deliberate re-runs;
- a redelivery adds one;
- a deliberate requeue (scale-in, ``shutdown``) gives its count back (``forget``), so stopping a pod never uses up a
  job's deliveries.

Counting is best effort: if Redis cannot be reached, the delivery counts as the first, so a broken counter never
stops a job.
"""

import logging
from collections.abc import Iterable
from typing import Any

from evilflowers_worker.observability import metrics

logger = logging.getLogger(__name__)

#: Longer than any job takes, short enough that keys of abandoned tasks go away.
TTL_SECONDS = 7 * 24 * 3600


def capped(delivery: int, cap: int) -> bool:
    """Whether this delivery is past the cap (``MAX_DELIVERIES``; 0: no cap); counted as ``deliveries.capped``."""
    if cap and delivery > cap:
        metrics.DELIVERIES_CAPPED.add(1)
        return True
    return False


class DeliveryCounter:
    def __init__(self, client: Any, *, prefix: str):
        """``client``: a redis-py client (or ``None`` when Redis is not reachable); ``prefix``: e.g. ``analyzer``."""
        self.client = client
        self.prefix = prefix

    @classmethod
    def from_url(cls, url: str, *, prefix: str) -> "DeliveryCounter":
        try:
            import redis

            client = redis.Redis.from_url(url, socket_timeout=5, socket_connect_timeout=5)
        except Exception:
            logger.warning("Cannot create the Redis client for delivery counts; every delivery counts as the first")
            client = None
        return cls(client, prefix=prefix)

    def key(self, task_id: str) -> str:
        return f"evilflowers:deliveries:{self.prefix}:{task_id}"

    def record(self, task_id: str, *, redelivered: bool) -> int:
        """This delivery's number, from 1."""
        if self.client is None:
            return 1
        key = self.key(task_id)
        try:
            if not redelivered:
                self.client.set(key, 1, ex=TTL_SECONDS)
                return 1
            with self.client.pipeline() as pipe:
                count, _ = pipe.incr(key).expire(key, TTL_SECONDS).execute()
            return int(count)
        except Exception:
            logger.warning("Cannot count deliveries of task %s; this one counts as the first", task_id, exc_info=True)
            return 1

    def forget(self, task_ids: Iterable[str]) -> None:
        """Give back one delivery of each task: it is being requeued on purpose, not lost."""
        if self.client is None:
            return
        for task_id in task_ids:
            key = self.key(task_id)
            try:
                if int(self.client.decr(key)) <= 0:
                    self.client.delete(key)
            except Exception:
                logger.warning("Cannot give back the delivery of task %s", task_id, exc_info=True)
