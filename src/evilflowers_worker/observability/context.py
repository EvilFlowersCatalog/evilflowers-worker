"""What a task is working on, carried to its span and to every log line it writes.

``bind(run_id=..., job=...)`` is the one call a job makes: the values go on the current span under their canonical
attribute names, and into a context variable that ``logs.JsonFormatter`` adds to each line under the short names.
A Loki query for ``run_id`` then finds every line of a run in every service, even outside a sampled trace.
"""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType
from typing import Any

from evilflowers_worker.observability.otel import trace

#: Short (log) name → span attribute name, the same in every extraction service (docs/architecture.md §4).
#: Other keys keep their name on both.
SPAN_ATTRIBUTES: Mapping[str, str] = MappingProxyType(
    {
        "run_id": "evilflowers.extraction.run_id",
        "job": "evilflowers.extraction.job",
        "attempt": "evilflowers.extraction.attempt",
        "pipeline_version": "evilflowers.extraction.pipeline_version",
        "delivery": "evilflowers.extraction.delivery",
        "extract_pass": "evilflowers.extraction.pass",
        "acquisition_id": "evilflowers.acquisition_id",
        "entry_id": "evilflowers.entry_id",
        "source": "evilflowers.source",
        "task_id": "messaging.message.id",
    }
)

#: Never mutated in place: `bind` sets a new mapping and resets to the old one.
_fields: ContextVar[Mapping[str, Any]] = ContextVar("evilflowers_worker_fields", default=MappingProxyType({}))


def clean(attributes: Mapping[str, Any]) -> dict[str, Any]:
    """Drop the ``None`` values (OpenTelemetry rejects them)."""
    return {key: value for key, value in attributes.items() if value is not None}


def current() -> Mapping[str, Any]:
    """The fields bound in this context (empty outside a task)."""
    return _fields.get()


@contextmanager
def bind(**fields: Any) -> Iterator[None]:
    """Attach ``fields`` to the current span and to every log line written inside the block."""
    fields = clean(fields)
    if trace is not None:
        span = trace.get_current_span()
        for key, value in fields.items():
            span.set_attribute(SPAN_ATTRIBUTES.get(key, key), value)
    token = _fields.set({**_fields.get(), **fields})
    try:
        yield
    finally:
        _fields.reset(token)
