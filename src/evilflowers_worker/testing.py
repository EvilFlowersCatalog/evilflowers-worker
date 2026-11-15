"""Test doubles and harnesses for a service's tests, shared so four services stop writing four that drift apart.

- ``FakeCatalog`` / ``RefusingCatalog`` / ``RecordingPublisher``: in-process stand-ins with the real interfaces;
- ``StubCatalogServer``: the ingestion API on localhost, for a real worker;
- ``WorkerProcess`` / ``redis_client`` / ``wait_for``: a real prefork worker against a real Redis.
"""

import contextlib
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from evilflowers_worker.catalog import RunNotRunning
from evilflowers_worker.contract import (
    JobReport,
    Message,
    Paragraph,
    ParagraphLink,
    Region,
    Route,
    RunState,
    StartRun,
    dump,
)


class FakeCatalog:
    """Stands in for ``CatalogClient``.

    - ``states``: what ``run()`` answers, in turn (the last one repeats); default: running, every job pending;
    - ``attempt``: what ``start()`` answers;
    - ``start_error`` / ``fail_on``: exceptions raised by ``start`` / by the named method (``"regions"``, ``"job"``…).
    """

    def __init__(
        self,
        *,
        states: Iterable[RunState] = (),
        attempt: int | None = None,
        start_error: Exception | None = None,
        fail_on: dict[str, Exception] | None = None,
    ):
        self._states = list(states)
        self.start_attempt = attempt
        self.start_error = start_error
        self.fail_on = dict(fail_on or {})
        self.attempt: int | None = None
        self.started: list[dict[str, Any]] = []
        self.regions_posted: list[dict[str, Any]] = []
        self.paragraphs_posted: list[dict[str, Any]] = []
        self.links_posted: list[dict[str, Any]] = []
        #: Every job report: ``{name, status, failed, error, provenance, attempt}``.
        self.jobs: list[dict[str, Any]] = []
        self.reads = 0

    def _maybe_fail(self, method: str) -> None:
        if (error := self.fail_on.get(method)) is not None:
            raise error

    def run(self) -> RunState:
        self._maybe_fail("run")
        self.reads += 1
        if not self._states:
            return RunState(status="running", jobs={}, attempt=self.attempt)
        return self._states.pop(0) if len(self._states) > 1 else self._states[0]

    def start(self, start: StartRun) -> RunState:
        if self.start_error is not None:
            raise self.start_error
        self.started.append(dump(start))
        self.attempt = self.start_attempt
        return RunState(status="running", jobs=dict.fromkeys(start.jobs, "pending"), attempt=self.attempt)

    def regions(self, regions: Sequence[Region]) -> None:
        self._maybe_fail("regions")
        self.regions_posted.extend(dump(region) for region in regions)

    def paragraphs(self, paragraphs: Sequence[Paragraph], links: Sequence[ParagraphLink] = ()) -> None:
        self._maybe_fail("paragraphs")
        self.paragraphs_posted.extend(dump(paragraph) for paragraph in paragraphs)
        self.links_posted.extend(dump(link) for link in links)

    def job(self, name: str, report: JobReport) -> None:
        self._maybe_fail("job")
        self.jobs.append(
            {
                "name": name,
                "status": report.status,
                "failed": report.status == "failed",
                "error": report.error,
                "provenance": report.provenance,
                "attempt": self.attempt,
            }
        )


def running(*, jobs: dict[str, str] | None = None, attempt: int | None = None, status: str = "running") -> RunState:
    """A run state for ``FakeCatalog(states=[...])``."""
    return RunState(status=status, jobs=jobs or {}, attempt=attempt)


class RefusingCatalog(FakeCatalog):
    """A catalog that answers every write with 409 (the run was reset or finished)."""

    def __init__(self, error: RunNotRunning | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.error = error or RunNotRunning("finished")
        self.fail_on.update({"regions": self.error, "paragraphs": self.error, "job": self.error})


def wait_for(condition: Callable[[], Any], timeout: float, interval: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return bool(condition())


def redis_client(url: str) -> Any:
    """A client of the test Redis, or ``None`` when none answers (the caller skips, or fails in CI)."""
    import redis

    client = redis.Redis.from_url(url, socket_connect_timeout=1)
    try:
        client.ping()
    except redis.RedisError:
        return None
    return client


class WorkerProcess:
    """A real worker (``celery -A <app> worker --pool=prefork --concurrency=1``) on a queue of its own, as in a pod.

    ``env`` is added to the environment (settings of the app under test); telemetry is switched off. Use it as a
    context manager: on exit the whole process group is killed and the queue deleted, and ``output`` holds the log.
    """

    def __init__(
        self,
        app: str,
        *,
        redis_url: str,
        pythonpath: Sequence[str] = (),
        env: dict[str, str] | None = None,
        queue: str | None = None,
    ):
        import redis

        self.app = app
        self.redis_url = redis_url
        self.redis = redis.Redis.from_url(redis_url)
        self.queue = queue or f"test-{uuid.uuid4().hex[:8]}"
        self.output = ""
        environment = {**os.environ, "REDIS_URL": redis_url, "QUEUE": self.queue, "LOG_FORMAT": "text", **(env or {})}
        environment["PYTHONPATH"] = os.pathsep.join([*pythonpath, *filter(None, [os.environ.get("PYTHONPATH")])])
        for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "HOST_IP", "PYROSCOPE_SERVER_ADDRESS"):
            environment.pop(name, None)
        if "REMAP_SIGTERM" not in (env or {}):
            environment.pop("REMAP_SIGTERM", None)
        self._environment = environment
        self.process: subprocess.Popen[str] | None = None

    def __enter__(self) -> "WorkerProcess":
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "celery",
                "-A",
                self.app,
                "worker",
                "--pool=prefork",
                "--concurrency=1",
                f"--queues={self.queue}",
                "--loglevel=INFO",
                f"--hostname={self.queue}@%h",
            ],
            env=self._environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            # Its own group: on exit the prefork child is killed too, or it keeps the pipe open.
            start_new_session=True,
        )
        return self

    def __exit__(self, *_exc: object) -> None:
        assert self.process is not None
        # A group that already exited: ESRCH on Linux, EPERM on macOS (its leader is a zombie).
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self.process.pid, signal.SIGKILL)
        self.output = self.process.communicate(timeout=30)[0]
        self.redis.delete(self.queue)
        print(self.output)

    def send(self, task: str, queue: str | None = None, **kwargs: Any) -> str:
        from celery import Celery

        return str(Celery(broker=self.redis_url).send_task(task, kwargs=kwargs, queue=queue or self.queue).id)

    def wait_ready(self, timeout: float = 60) -> None:
        """Until the worker consumes its queue (kombu binds it on start)."""
        if not wait_for(lambda: self.redis.exists(f"_kombu.binding.{self.queue}"), timeout):
            raise TimeoutError(f"the worker of {self.queue} never started")
        time.sleep(1)

    def signal(self, number: int) -> None:
        assert self.process is not None
        self.process.send_signal(number)

    def wait(self, timeout: float) -> int:
        assert self.process is not None
        return self.process.wait(timeout=timeout)

    def queued(self, queue: str | None = None) -> list[dict[str, Any]]:
        """The messages waiting on a queue, oldest first, as kombu stores them (``headers``, ``body`` …)."""
        import json

        return [json.loads(raw) for raw in reversed(self.redis.lrange(queue or self.queue, 0, -1))]

    @staticmethod
    def kwargs(message: dict[str, Any]) -> dict[str, Any]:
        """The keyword arguments of a queued Celery message (protocol 2)."""
        import base64
        import json

        _args, kwargs, _embed = json.loads(base64.b64decode(message["body"]))
        return dict(kwargs)

    def unacked(self) -> int:
        """Messages a worker holds unacknowledged (kombu's Redis transport keeps them in this hash)."""
        return int(self.redis.hlen("unacked"))


class StubCatalogServer:
    """The catalog's ingestion API on localhost, for tests that run a real worker (``with StubCatalogServer() as c``).

    ``GET runs/{id}`` answers ``run`` (default: running, every job pending, at ``attempt``); ``/start`` answers a
    running run at ``attempt`` with the declared jobs pending; every other POST is accepted. ``calls`` records
    ``(method, path with query, JSON body)``.
    """

    def __init__(self, *, attempt: int = 3, run: dict[str, Any] | None = None):
        self.attempt = attempt
        self.run = run
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self._server: Any = None

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def paths(self) -> list[tuple[str, str]]:
        return [(method, path) for method, path, _body in self.calls]

    def __enter__(self) -> "StubCatalogServer":
        import json
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self, status: int, body: dict[str, Any]) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                stub.calls.append(("GET", self.path, {}))
                run = stub.run or {"status": "running", "jobs": {}, "attempt": stub.attempt}
                self._answer(200, {"response": run})

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                stub.calls.append(("POST", self.path, body))
                path = self.path.split("?")[0]
                if path.endswith("/start"):
                    jobs = dict.fromkeys(body.get("jobs", []), "pending")
                    self._answer(200, {"response": {"status": "running", "jobs": jobs, "attempt": stub.attempt}})
                elif path.endswith(("/regions", "/paragraphs")):
                    self._answer(201, {"response": {"created": 0}})
                else:
                    self._answer(200, {"response": {"status": "running"}})

            def log_message(self, *_args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()


class RecordingPublisher:
    """Stands in for ``CeleryPublisher``: ``sent`` holds ``(task, kwargs, queue)`` as they would go on the wire."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any], str]] = []

    def publish(self, route: Route, message: Message) -> None:
        self.sent.append((route.task, message.kwargs(), route.queue))
