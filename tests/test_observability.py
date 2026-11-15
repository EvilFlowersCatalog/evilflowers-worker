import json
import logging
import sys

import pytest

from evilflowers_worker import observability
from evilflowers_worker.observability import metrics, profiling
from evilflowers_worker.observability.setup import ENDPOINT_VARIABLES


@pytest.fixture
def clean_env(monkeypatch):
    for name in (*ENDPOINT_VARIABLES, "HOST_IP"):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def record(message="hello", exc_info=None):
    return logging.LogRecord("worker", logging.WARNING, __file__, 1, message, None, exc_info)


def line(log_record=None) -> dict:
    return json.loads(observability.JsonFormatter().format(log_record or record()))


# -- Identity -------------------------------------------------------------------------------------------------------


def test_the_identity_names_everything_after_the_service():
    previous = observability.identity()
    try:
        service = observability.configure("image", version="develop@abc")
        assert service.scope == "evilflowers.image"
        assert service.default_service_name == "evilflowers-image"
        assert observability.identity().version == "develop@abc"
    finally:
        observability.configure(previous.service, version=previous.version, log_format=previous.log_format)


def test_a_catalog_job_binds_its_run_under_the_job_ledger_names():
    sdk = pytest.importorskip("opentelemetry.sdk.trace")
    tracer = sdk.TracerProvider().get_tracer("test")
    previous = observability.identity()
    try:
        observability.configure("ocr", attributes={**observability.CATALOG_JOB_ATTRIBUTES, "engine": "ocr.engine"})
        with (
            tracer.start_as_current_span("task") as span,
            observability.bind(run_id="r-1", job_id="j-1", engine="t", delivery=2),
        ):
            payload = line()
        # The log line keeps the short names: a Loki query for `run_id` finds every service's lines.
        assert payload["run_id"] == "r-1" and payload["job_id"] == "j-1"
        assert span.attributes["evilflowers.run.id"] == "r-1"
        assert span.attributes["evilflowers.job.id"] == "j-1"
        assert span.attributes["ocr.engine"] == "t"
        assert span.attributes["evilflowers.job.delivery"] == 2
        assert "evilflowers.extraction.run_id" not in span.attributes
        # Canonical names the service does not override stay canonical.
        assert observability.span_attribute("source") == "evilflowers.source"
    finally:
        observability.configure(previous.service, version=previous.version, log_format=previous.log_format)
    assert observability.span_attribute("run_id") == "evilflowers.extraction.run_id"


# -- Endpoint discovery ---------------------------------------------------------------------------------------------


def test_no_endpoint_without_configuration(clean_env):
    assert observability.endpoint() is None


def test_explicit_endpoint_wins(clean_env):
    clean_env.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    clean_env.setenv("HOST_IP", "10.0.0.1")
    assert observability.endpoint() == "http://collector:4318"


def test_endpoint_from_host_ip(clean_env):
    clean_env.setenv("HOST_IP", "192.168.122.10")
    assert observability.endpoint() == "http://192.168.122.10:4318"


def test_endpoint_from_ipv6_host_ip(clean_env):
    clean_env.setenv("HOST_IP", "fd00::1")
    assert observability.endpoint() == "http://[fd00::1]:4318"


# -- Spans, metrics, logs -------------------------------------------------------------------------------------------


def test_span_and_metrics_are_safe_before_setup():
    with observability.span("test.span", **{"absent": None, "present": 1}):
        observability.annotate(**{"key": "value"})
        observability.fail(RuntimeError("boom"))
        with observability.untraced_http():
            pass
    metrics.TASK_DURATION.record(1.0, {"celery.task.name": "t"})
    metrics.record_job("figures", "done")
    metrics.CATALOG_RETRIES.add(1, {"route": "/jobs", "reason": "503"})


def test_json_log_line_outside_a_span():
    log_record = logging.LogRecord("worker", logging.WARNING, __file__, 1, "hello %s", ("world",), None)
    payload = line(log_record)
    assert payload["level"] == "warning"
    assert payload["logger"] == "worker"
    assert payload["message"] == "hello world"
    assert "trace_id" not in payload and "run_id" not in payload


def test_json_log_line_carries_the_traceback():
    try:
        raise ValueError("bad pdf")
    except ValueError:
        payload = line(record("failed", sys.exc_info()))
    assert "ValueError: bad pdf" in payload["exception"]


def test_json_log_line_inside_a_span_has_the_trace_id():
    sdk = pytest.importorskip("opentelemetry.sdk.trace")
    tracer = sdk.TracerProvider().get_tracer("test")
    with tracer.start_as_current_span("work") as span:
        payload = line()
        expected = format(span.get_span_context().trace_id, "032x")
    assert payload["trace_id"] == expected
    assert len(payload["span_id"]) == 16


def test_bound_fields_reach_the_log_line_and_the_span_under_canonical_names():
    sdk = pytest.importorskip("opentelemetry.sdk.trace")
    tracer = sdk.TracerProvider().get_tracer("test")
    with (
        tracer.start_as_current_span("task") as span,
        observability.bind(run_id="r-1", source="a.pdf", attempt=3, acquisition_id=None),
    ):
        with observability.bind(job="figures:0001-0008"):
            payload = line()
        outer = line()
    assert payload["run_id"] == "r-1" and payload["job"] == "figures:0001-0008" and payload["attempt"] == 3
    assert "job" not in outer and "acquisition_id" not in outer
    assert span.attributes["evilflowers.extraction.run_id"] == "r-1"
    assert span.attributes["evilflowers.extraction.attempt"] == 3
    assert span.attributes["evilflowers.source"] == "a.pdf"
    assert "run_id" not in line()


def test_a_worker_thread_logs_the_task_id_it_bound():
    with observability.bind(task_id="t-1"):
        assert line()["task_id"] == "t-1"


def test_bound_fields_cannot_overwrite_the_line():
    with observability.bind(message="spoofed", level="error"):
        payload = line()
    assert payload["message"] == "hello" and payload["level"] == "warning"


def test_sent_tasks_carry_the_trace_context():
    """A consumer joins the producer's trace: `send_task` inside a span injects `traceparent`.

    Needs the `otel` extra (`make install-all`); skipped with the dev tools only.
    """
    celery_instrumentation = pytest.importorskip("opentelemetry.instrumentation.celery")
    from celery import Celery, signals
    from opentelemetry.sdk.trace import TracerProvider

    provider = TracerProvider()
    instrumentor = celery_instrumentation.CeleryInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    captured: list[dict] = []

    def capture(headers=None, **_kwargs):
        captured.append(dict(headers or {}))

    signals.before_task_publish.connect(capture, weak=False)
    try:
        app = Celery("propagation", broker="memory://")
        with provider.get_tracer("test").start_as_current_span("evilflowers_analyzer.extract") as span:
            app.send_task("evilflowers_image.figures", kwargs={"run_id": "r-1"}, queue="evilflowers_image")
            trace_id = format(span.get_span_context().trace_id, "032x")
    finally:
        signals.before_task_publish.disconnect(capture)
        instrumentor.uninstrument()

    assert captured and trace_id in captured[0]["traceparent"]


# -- Profiling -------------------------------------------------------------------------------------------------------


@pytest.fixture
def fresh_profiler(monkeypatch):
    """A profiler module that has not run `setup()` yet."""
    monkeypatch.setitem(profiling._state, "done", False)
    monkeypatch.setitem(profiling._state, "enabled", False)
    for name in ("PYROSCOPE_SERVER_ADDRESS", "PYROSCOPE_APPLICATION_NAME", "PYROSCOPE_TAGS"):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_parse_tags():
    assert profiling.parse_tags("namespace=evilflowers-dev, pod=analyzer-1 ,container=analyzer") == {
        "namespace": "evilflowers-dev",
        "pod": "analyzer-1",
        "container": "analyzer",
    }
    assert profiling.parse_tags(None) == {}
    assert profiling.parse_tags("broken, =x, y=, ok=1") == {"ok": "1"}


def test_profiling_is_off_without_a_server_address(fresh_profiler):
    assert profiling.setup() is False
    assert profiling.is_enabled() is False
    assert profiling.span_processor() is None
    profiling.shutdown()


def test_configures_pyroscope_from_the_environment(fresh_profiler):
    calls = []

    class FakePyroscope:
        @staticmethod
        def configure(**kwargs):
            calls.append(kwargs)

        @staticmethod
        def shutdown():
            calls.append("shutdown")

    fresh_profiler.setitem(sys.modules, "pyroscope", FakePyroscope)
    fresh_profiler.setenv("PYROSCOPE_SERVER_ADDRESS", "http://pyroscope:4040")
    fresh_profiler.setenv("PYROSCOPE_TAGS", "namespace=evilflowers-dev")

    assert profiling.setup() is True
    assert profiling.setup() is True  # idempotent
    assert len(calls) == 1
    assert calls[0]["application_name"] == observability.identity().service
    assert calls[0]["gil_only"] is False
    assert calls[0]["tags"] == {"namespace": "evilflowers-dev"}

    profiling.shutdown()
    assert calls[-1] == "shutdown" and profiling.is_enabled() is False
