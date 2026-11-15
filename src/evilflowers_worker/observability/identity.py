"""Who is reporting: the one thing a service tells the observability package (``configure``).

Everything else (scope, metric prefix, default ``service.name``, Pyroscope application) derives from the short
service name, so the services cannot drift into one convention each. The only per-service vocabulary is
``attributes``: the span names of the fields a service binds beyond the canonical set (``context.SPAN_ATTRIBUTES``).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

_NONE: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True)
class Identity:
    #: Short name: metric prefix (``analyzer_task_duration_seconds``), Pyroscope application, Loki ``service_name``.
    service: str = "worker"
    #: ``service.version``: the image tag CI builds in.
    version: str = "dev"
    #: ``json`` (one object per line, what Loki parses) or ``text`` (Celery's own format, for a terminal).
    log_format: Literal["json", "text"] = "json"
    #: Short (log) name → span attribute name, over the canonical set: a key here wins over ``SPAN_ATTRIBUTES``.
    attributes: Mapping[str, str] = field(default=_NONE)

    @property
    def scope(self) -> str:
        """Instrumentation scope of every span and instrument the service creates."""
        return f"evilflowers.{self.service}"

    @property
    def default_service_name(self) -> str:
        """``service.name`` when ``OTEL_SERVICE_NAME`` is not set (the pod sets it to the app label)."""
        return f"evilflowers-{self.service}"


_identity = Identity()


def configure(
    service: str,
    *,
    version: str = "dev",
    log_format: Literal["json", "text"] = "json",
    attributes: Mapping[str, str] | None = None,
) -> Identity:
    """Set the service identity; call once, where the Celery app is created, before any telemetry is used.

    ``attributes``: span names of the service's own bound fields, or overrides of canonical ones (a catalog job binds
    ``run_id`` as ``evilflowers.run.id``, not as an extraction run: ``context.CATALOG_JOB_ATTRIBUTES``).
    """
    global _identity
    _identity = Identity(
        service=service,
        version=version,
        log_format=log_format,
        attributes=MappingProxyType(dict(attributes or {})),
    )
    return _identity


def identity() -> Identity:
    return _identity
