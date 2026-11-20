"""The Celery side of a worker: one app factory, one set of task options, one way to send a command.

``create_app`` gives every service the same delivery semantics (docs/architecture.md §3.3):

- late acknowledgement and requeue when the worker process dies (``JOB_TASK_OPTIONS``);
- one message at a time per process (prefetch 1);
- the db-wide visibility timeout;
- its queues declared as direct exchanges of the same name (what Celery declares for a queue a producer names, so
  ``send_task(..., queue=...)`` from anywhere reaches it);
- the scale-in protocol of ``shutdown``;
- the liveness heartbeat file of ``heartbeat`` (the k8s exec probe ``evilflowers-worker-alive``);
- telemetry and JSON logs (``observability``).

Nobody waits for a task's result (every job reports to the catalog, or leaves an outcome file), so there is no result
backend. A standalone job's orchestrator learns of the end through ``link`` / ``link_error`` callbacks
(``standalone_callbacks``), which need none.

A task body is one of two calls (docs/architecture.md §2):

- ``run_job``: an extraction leaf job (``LeafJob``), which reads its run and reports over the ingestion API;
- ``run_task``: anything else, e.g. a catalog job that writes an outcome file. The service says what happens past the
  delivery cap and with an invalid message.

What ``create_app`` does not set is per task, on the task: ``**JOB_TASK_OPTIONS`` always, and limits such as
``time_limit=`` where the service has one. A service never needs a Celery configuration of its own.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from celery import Celery, Signature, Task
from kombu import Exchange, Queue
from pydantic import BaseModel, ValidationError

from evilflowers_worker import observability
from evilflowers_worker.contract import JobMessage, Message, Route
from evilflowers_worker.deliveries import DeliveryCounter, capped
from evilflowers_worker.heartbeat import heartbeat_step
from evilflowers_worker.jobs import LeafJob, report_invalid_message
from evilflowers_worker.observability.signals import connect as connect_telemetry
from evilflowers_worker.settings import RuntimeSettings, WorkerSettings
from evilflowers_worker.shutdown import Shutdown

logger = logging.getLogger(__name__)

#: Options of every task that does a job: acknowledged after the job, requeued if the worker process dies (and capped
#: by ``deliveries``).
JOB_TASK_OPTIONS: Mapping[str, Any] = {"acks_late": True, "reject_on_worker_lost": True}


class CeleryConfig:
    """The configuration shared by every service on the db."""

    def __init__(self, settings: RuntimeSettings, queues: Sequence[str], routes: Mapping[str, str]):
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
    settings: RuntimeSettings
    deliveries: DeliveryCounter

    def delivery(self, task: Task) -> int:
        """The number of this delivery of the task's message, from 1 (a redelivery after a lost worker adds one)."""
        request = task.request
        redelivered = bool((request.delivery_info or {}).get("redelivered"))
        return self.deliveries.record(request.id, redelivered=redelivered)


_runtime: Runtime | None = None


def create_app(
    service: str,
    settings: RuntimeSettings,
    *,
    include: Sequence[str],
    queues: Sequence[str] | None = None,
    routes: Mapping[str, str] | None = None,
    attributes: Mapping[str, str] | None = None,
) -> Celery:
    """The service's Celery app. ``service`` is its short name (``analyzer``): metric prefix, scope, Pyroscope app.

    - ``queues``: what it consumes (default: ``settings.queue``);
    - ``routes``: task-name pattern → queue for tasks it sends without naming a queue (default:
      ``evilflowers_<service>.*`` → the first queue). A service whose task names follow another prefix passes its own
      (``{"evilflowers_ocr_worker.*": settings.queue}``): the task names are a contract with the catalog;
    - ``attributes``: span names of the fields it binds beyond the canonical set
      (``observability.CATALOG_JOB_ATTRIBUTES`` for a catalog job).
    """
    global _runtime
    observability.configure(service, version=settings.image_tag, log_format=settings.log_format, attributes=attributes)
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
    app.steps["consumer"].add(heartbeat_step(settings.worker_heartbeat_file, settings.worker_heartbeat_interval))
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
    settings = current.settings
    if not isinstance(settings, WorkerSettings):
        raise TypeError(
            "run_job needs WorkerSettings (a leaf job reports over the catalog ingestion API); "
            "a worker without the catalog client uses run_task"
        )
    try:
        message = message_type.model_validate(kwargs)
    except ValidationError as error:
        return report_invalid_message(kwargs, error, job=job_type.kind, settings=settings)
    job = job_type(message, settings=settings, task_id=task.request.id, delivery=current.delivery(task))
    return job.run()


def run_task[M: BaseModel, R](
    task: Task,
    handler: Callable[[M, int], R],
    message_type: type[M],
    kwargs: dict[str, Any],
    *,
    on_capped: Callable[[M, int], R],
    on_invalid: Callable[[dict[str, Any], ValidationError], R] | None = None,
) -> R:
    """The body of a task that is not an extraction leaf job: validate the message, count the delivery, run it.

    - ``handler(message, delivery)``: the work. ``delivery`` counts from 1;
    - ``on_capped(message, delivery)``: called *instead of* the handler once the delivery is past ``MAX_DELIVERIES``
      (the job killed its worker every time). It records the job failed the service's way (a catalog job writes a
      ``failed`` outcome file and raises); afterwards the message is acknowledged and never delivered again;
    - ``on_invalid(kwargs, error)``: a message that does not validate. Default: raise the ``ValidationError``, so the
      task fails at once and is not retried (the same message fails the same way).

    ``task_id`` and ``delivery`` are bound for the whole call (log lines, the task's span)::

        @shared_task(name="evilflowers_ocr_worker.ocr", bind=True, time_limit=..., **JOB_TASK_OPTIONS)
        def ocr(self, **kwargs) -> dict:
            return run_task(self, jobs.run, OcrRequest, kwargs, on_capped=jobs.give_up)
    """
    current = runtime()
    try:
        message = message_type.model_validate(kwargs)
    except ValidationError as error:
        logger.error("Invalid message for task %s: %s", task.name, error)
        if on_invalid is None:
            raise
        return on_invalid(kwargs, error)
    delivery = current.delivery(task)
    with observability.bind(task_id=task.request.id, delivery=delivery):
        if capped(delivery, current.settings.max_deliveries):
            logger.error(
                "Task %s gave up after %d deliveries: the worker died or was stopped during each of them",
                task.request.id,
                delivery - 1,
            )
            return on_capped(message, delivery)
        return handler(message, delivery)


def standalone_callbacks(
    app: Celery, *, done: str, failed: str, queue: str, job_id: str, **bound: Any
) -> dict[str, Signature]:
    """``link`` and ``link_error`` for a standalone job, sent by the orchestrator (docs/architecture.md §2)::

        app.send_task(route.task, kwargs=message.kwargs(), queue=route.queue,
                      **standalone_callbacks(app, done="digitalizer.job_done", failed="digitalizer.job_failed",
                                             queue="digitalizer.portal", job_id=job.id))

    No result backend is needed: Celery sends each callback as an ordinary message to ``queue``, which the
    orchestrator consumes. ``tests/test_runtime.py`` shows, against Redis: ``link`` on success; ``link_error`` on a
    raise, on an invalid message (no outcome file then) and past the delivery cap; neither on a deliberate requeue
    (SIGTERM). The probe on evilflowers-worker#5 adds: neither on a ``retry``; a worker killed mid-task fires one on
    the redelivery's outcome.

    - The worker sends the callback on its own broker: ``queue`` must be on the same Redis db as the job's queue.
    - At least once: Celery sends ``link`` from the task, before the late acknowledgement, so a worker that dies in
      between sends it again on the redelivery. ``done`` and ``failed`` must be idempotent (check the job's state).

    - ``done(result, job_id=…)``: applied with the task's **return value** as its first argument, so the return value
      stays compact (the outcome file holds the rest).
    - ``failed(task_id, job_id=…)``: the single-argument errback form, the one the Celery docs prescribe for tasks
      sent by name. **Declare it with** ``bind=True`` (``def job_failed(self, task_id, job_id)``): an errback with
      more than one positional parameter that is registered in the failing app is called directly as
      ``(request, exc, traceback)`` instead, and fails with ``TypeError``.

    ``bound`` adds further keyword arguments to both callbacks.
    """
    kwargs = {"job_id": job_id, **bound}
    return {
        "link": app.signature(done, kwargs=kwargs, queue=queue),
        "link_error": app.signature(failed, kwargs=kwargs, queue=queue),
    }


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
