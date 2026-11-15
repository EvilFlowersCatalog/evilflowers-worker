"""The runtime of EvilFlowers extraction workers (docs/architecture.md in evilflowers-analyzer-service).

- ``contract``: the messages workers send each other and the payloads they report to the catalog;
- ``catalog``: the client of the catalog's ``/extraction/v1`` ingestion API;
- ``jobs``: the lifecycle of a leaf job (check → extract → publish → report);
- ``runtime``: the Celery app factory, task options and the command publisher;
- ``deliveries`` / ``shutdown``: the delivery cap and the scale-in protocol;
- ``observability``: traces, metrics, profiles and JSON logs;
- ``settings``: what every worker is configured with.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("evilflowers-worker")
except PackageNotFoundError:  # pragma: no cover - not installed (a source checkout)
    __version__ = "0.0.0"
