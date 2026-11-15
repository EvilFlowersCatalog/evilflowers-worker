"""Hand-made spans where the time goes; no-ops without OpenTelemetry."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from evilflowers_worker.observability.context import clean
from evilflowers_worker.observability.identity import identity
from evilflowers_worker.observability.otel import Status, StatusCode, trace


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    """A child span of the current one (``<service>.<area>[.<step>]``), or ``None``.

    An exception escaping the block marks it failed.
    """
    if trace is None:
        yield None
        return
    with trace.get_tracer(identity().scope).start_as_current_span(name, attributes=clean(attributes)) as current:
        yield current


def annotate(**attributes: Any) -> None:
    """Attributes on the current span."""
    if trace is None:
        return
    current = trace.get_current_span()
    for key, value in clean(attributes).items():
        current.set_attribute(key, value)


def fail(error: BaseException) -> None:
    """Mark the current span failed for an exception the code catches (and reports) itself.

    Jobs report their own failures to the catalog and return, so without this the Celery span would end green.
    """
    if trace is None:
        return
    current = trace.get_current_span()
    current.record_exception(error)
    current.set_status(Status(StatusCode.ERROR, str(error)[:200]))


@contextmanager
def untraced_http() -> Iterator[None]:
    """No CLIENT spans for the HTTP calls inside the block (a loop of thousands of embedding requests, text#10)."""
    try:
        from opentelemetry.instrumentation.utils import suppress_http_instrumentation
    except ImportError:
        yield
        return
    with suppress_http_instrumentation():
        yield
