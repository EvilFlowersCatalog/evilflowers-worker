"""The Celery side of a worker: one app factory, one set of task options, one way to send a command.

``create_app`` gives every service the same delivery semantics (docs/architecture.md §3.3):

- late acknowledgement and requeue when the worker process dies (``JOB_TASK_OPTIONS``);
- one message at a time per process (prefetch 1);
- the db-wide visibility timeout;
- its queues declared as direct exchanges of the same name (what Celery declares for a queue a producer names, so
  ``send_task(..., queue=...)`` from anywhere reaches it);
- the scale-in protocol of ``shutdown``;
- telemetry and JSON logs (``observability``).

Nobody waits for a task's result (every job reports to the catalog), so there is no result backend.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from celery import Celery, Task
from kombu import Exchange, Queue
from pydantic import ValidationError

from evilflowers_worker import observability
from evilflowers_worker.contract import JobMessage, Message, Route
from evilflowers_worker.deliveries import DeliveryCounter
from evilflowers_worker.jobs import LeafJob, report_invalid_message
from evilflowers_worker.observability.signals import connect as connect_telemetry
from evilflowers_worker.settings import WorkerSettings
from evilflowers_worker.shutdown import Shutdown

logger = logging.getLogger(__name__)

#: Options of every task that does a job: acknowledged after the job, requeued if the worker process dies (and capped
#: by ``deliveries``).
JOB_TASK_OPTIONS: Mapping[str, Any] = {"acks_late": True, "reject_on_worker_lost": True}


class CeleryConfig:
    """The configuration shared by every extraction service."""

    def __init__(self, settings: WorkerSettings, queues: Sequence[str], routes: Mapping[str, str]):
        self.broker_url = settings.redis_url
        self.broker_connection_retry_on_startup = True
        self.broker_transport_options = {"visibility_timeout": settings.broker_visibility_timeout}
        # Scale-in has its own grace (`shutdown`); Celery's would block the event loop while it waits.
        self.worker_soft_shutdown_timeout = 0

        self.task_serializer = "json"
        self.result_serializer = "json"
        self.accept_content = ["json"]
        self.timezone = "Europe/Bratislava"
        self.enable_utc = True

        self.task_queues = [Queue(name, Exchange(name, type="direct"), routing_key=name) for name in queues]
        self.task_default_queue = queues[0]
        self.task_routes = {pattern: {"queue": queue} for pattern, queue in routes.items()}
        # A job takes minutes: one at a time per worker process.
        self.worker_prefetch_multiplier = 1
        # Monitoring (Flower, `celery events`, the catalog's job ledger).
        self.worker_send_task_events = True
        self.task_send_sent_event = True


@dataclass(frozen=True)
class Runtime:
    """What a running service is: its name, its settings and its delivery counter."""

    service: str
    settings: WorkerSettings
    deliveries: DeliveryCounter

    def delivery(self, task: Task) -> int:
        """The number of this delivery of the task's message, from 1 (a redelivery after a lost worker adds one)."""
        request = task.request
        redelivered = bool((request.delivery_info or {}).get("redelivered"))
        return self.deliveries.record(request.id, redelivered=redelivered)


_runtime: Runtime | None = None


def create_app(
    service: str,
    settings: WorkerSettings,
    *,
    include: Sequence[str],
    queues: Sequence[str] | None = None,
    routes: Mapping[str, str] | None = None,
) -> Celery:
    """The service's Celery app. ``service`` is its short name (``analyzer``): metric prefix, scope, Pyroscope app.

    ``queues``: what it consumes (default: ``settings.queue``); ``routes``: task-name pattern → queue for tasks it
    sends without naming a queue (default: ``evilflowers_<service>.*`` → the first queue).
    """
    global _runtime
    observability.configure(service, version=settings.image_tag, log_format=settings.log_format)
    connect_telemetry()

    queues = list(queues or [settings.queue])
    routes = dict(routes or {f"evilflowers_{service}.*": queues[0]})
    deliveries = DeliveryCounter.from_url(settings.redis_url, prefix=service)
    _runtime = Runtime(service=service, settings=settings, deliveries=deliveries)

    app = Celery(f"evilflowers_{service}", include=list(include))
    app.config_from_object(CeleryConfig(settings, queues, routes))
    Shutdown(
        grace=settings.worker_soft_shutdown_timeout,
        cancel_timeout=settings.worker_cancel_timeout,
        redis_url=settings.redis_url,
        on_requeue=deliveries.forget,
    ).install()
    return app


def runtime() -> Runtime:
    """The runtime ``create_app`` set up in this process."""
    if _runtime is None:
        raise RuntimeError(
            "create_app() has not been called in this process: the module that defines the tasks must import the "
            "module that creates the app (a pool child started by spawn imports only the tasks module)"
        )
    return _runtime


def run_job[M: JobMessage](
    task: Task, job_type: type[LeafJob[M, Any]], message_type: type[M], kwargs: dict[str, Any]
) -> str:
    """The body of a leaf job's task: validate the message, count the delivery, run the job.

    An invalid message is reported failed and dropped (``jobs.report_invalid_message``), never retried::

        @shared_task(name="evilflowers_image.figures", bind=True, **JOB_TASK_OPTIONS)
        def figures(self, **kwargs) -> str:
            return run_job(self, FiguresJob, RegionsJob, kwargs)
    """
    current = runtime()
    try:
        message = message_type.model_validate(kwargs)
    except ValidationError as error:
        return report_invalid_message(kwargs, error, job=job_type.kind, settings=current.settings)
    job = job_type(message, settings=current.settings, task_id=task.request.id, delivery=current.delivery(task))
    return job.run()


class Publisher(Protocol):
    """Sends a command. Celery today; a Kafka transport would send ``message.kwargs()`` to topic ``route.queue``."""

    def publish(self, route: Route, message: Message) -> None: ...


class CeleryPublisher:
    """Sends from inside the running task, so Celery stamps ``parent_id``/``root_id`` (the catalog's job ledger
    attributes child tasks by them) and the OTel instrumentation injects ``traceparent``."""

    def __init__(self, app: Celery):
        self.app = app

    def publish(self, route: Route, message: Message) -> None:
        self.app.send_task(route.task, kwargs=message.kwargs(), queue=route.queue)
