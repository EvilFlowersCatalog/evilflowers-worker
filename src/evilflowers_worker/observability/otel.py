"""The OpenTelemetry API, or ``None`` where it is not installed (the code degrades to no-ops)."""

try:
    from opentelemetry import metrics, trace
    from opentelemetry.trace import Status, StatusCode
except ImportError:  # pragma: no cover - the kit always depends on the API
    metrics = trace = Status = StatusCode = None  # type: ignore[assignment,misc]

OTEL_AVAILABLE = trace is not None
