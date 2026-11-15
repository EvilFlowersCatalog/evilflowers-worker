"""Metrics, declared once; each is created on first use and is a no-op without OpenTelemetry.

A name may hold ``{service}``: ``"{service}.task.duration"`` is ``analyzer.task.duration`` in the analyzer, which
Mimir exposes as ``analyzer_task_duration_seconds_bucket`` (it adds the unit and type suffix).

Attributes stay low-cardinality: never a run id, a document id or a batch job name (those go on spans and log lines).
"""

import threading
from dataclasses import dataclass, field
from typing import Any, Literal

from evilflowers_worker.observability.identity import identity
from evilflowers_worker.observability.otel import metrics

#: Seconds: a catalog call up to the layout pass of a 1000-page book.
DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200)
COUNT_BUCKETS = (0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)

_lock = threading.Lock()


@dataclass(eq=False)
class Instrument:
    kind: Literal["counter", "histogram"]
    name: str
    unit: str
    description: str
    buckets: tuple[float, ...] | None = None
    _created: Any = field(default=None, init=False, repr=False)

    def _get(self) -> Any:
        if metrics is None:
            return None
        if self._created is None:
            with _lock:
                if self._created is None:
                    service = identity()
                    # A proxy meter: its instruments become real once `setup()` sets the provider.
                    meter = metrics.get_meter(service.scope)
                    name = self.name.format(service=service.service)
                    if self.kind == "counter":
                        self._created = meter.create_counter(name, self.unit, self.description)
                    else:
                        self._created = meter.create_histogram(
                            name,
                            self.unit,
                            self.description,
                            explicit_bucket_boundaries_advisory=list(self.buckets or DURATION_BUCKETS),
                        )
        return self._created

    def add(self, value: float = 1, attributes: dict[str, Any] | None = None) -> None:
        if (instrument := self._get()) is not None:
            instrument.add(value, attributes or {})

    def record(self, value: float, attributes: dict[str, Any] | None = None) -> None:
        if (instrument := self._get()) is not None:
            instrument.record(value, attributes or {})


# -- The standard set every service has (docs/architecture.md §4) -------------------------------------------------
TASK_DURATION = Instrument("histogram", "{service}.task.duration", "s", "Celery task run time")
JOB_OUTCOME = Instrument("counter", "{service}.job.outcome", "{job}", "Jobs handled, by job kind and outcome")
CATALOG_RETRIES = Instrument(
    "counter", "{service}.catalog.retries", "{retry}", "Retried calls to the catalog ingestion API"
)
DELIVERIES_CAPPED = Instrument(
    "counter", "{service}.deliveries.capped", "{delivery}", "Messages given up after too many deliveries"
)


def record_job(kind: str, outcome: str) -> None:
    """``kind``: the job's kind (``figures``), never a batch name; ``outcome``: see ``jobs.Outcome``."""
    JOB_OUTCOME.add(1, {"job": kind, "outcome": outcome})
