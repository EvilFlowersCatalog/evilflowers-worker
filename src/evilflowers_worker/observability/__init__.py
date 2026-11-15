"""Traces, metrics, profiles and JSON logs of an extraction worker (the cluster's telemetry contract, IP-019).

Off unless the pod points it at a collector; every call here is safe either way. A service calls ``configure`` once
(``runtime.create_app`` does it) and then uses ``bind``, ``span``, ``annotate``, ``fail`` and ``metrics.Instrument``.
"""

from evilflowers_worker.observability import metrics
from evilflowers_worker.observability.context import SPAN_ATTRIBUTES, bind
from evilflowers_worker.observability.identity import Identity, configure, identity
from evilflowers_worker.observability.logs import JsonFormatter
from evilflowers_worker.observability.setup import endpoint, is_enabled, setup, shutdown
from evilflowers_worker.observability.tracing import annotate, fail, span, untraced_http

__all__ = [
    "SPAN_ATTRIBUTES",
    "Identity",
    "JsonFormatter",
    "annotate",
    "bind",
    "configure",
    "endpoint",
    "fail",
    "identity",
    "is_enabled",
    "metrics",
    "setup",
    "shutdown",
    "span",
    "untraced_http",
]
