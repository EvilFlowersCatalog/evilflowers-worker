"""Who is reporting: the one thing a service tells the observability package (``configure``).

Everything else (scope, metric prefix, default ``service.name``, Pyroscope application) derives from the short
service name, so four services cannot drift into four conventions.
"""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Identity:
    #: Short name: metric prefix (``analyzer_task_duration_seconds``), Pyroscope application, Loki ``service_name``.
    service: str = "worker"
    #: ``service.version``: the image tag CI builds in.
    version: str = "dev"
    #: ``json`` (one object per line, what Loki parses) or ``text`` (Celery's own format, for a terminal).
    log_format: Literal["json", "text"] = "json"

    @property
    def scope(self) -> str:
        """Instrumentation scope of every span and instrument the service creates."""
        return f"evilflowers.{self.service}"

    @property
    def default_service_name(self) -> str:
        """``service.name`` when ``OTEL_SERVICE_NAME`` is not set (the pod sets it to the app label)."""
        return f"evilflowers-{self.service}"


_identity = Identity()


def configure(service: str, *, version: str = "dev", log_format: Literal["json", "text"] = "json") -> Identity:
    """Set the service identity; call once, where the Celery app is created, before any telemetry is used."""
    global _identity
    _identity = Identity(service=service, version=version, log_format=log_format)
    return _identity


def identity() -> Identity:
    return _identity
