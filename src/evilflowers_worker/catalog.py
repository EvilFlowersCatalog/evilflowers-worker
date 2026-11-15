"""The catalog's extraction ingestion API (``{CATALOG_API_URL}/extraction/v1/runs/{run_id}/…``), IP-018.

Every extraction service reports through it, with the API key of the catalog's service user; none of them writes to
the catalog database. One client for all of them:

- ``run()``: the run as the services see it, read before the expensive part and between batches (#140);
- ``start()``: the coordinator opens a pass and gets its ``attempt``;
- ``regions()`` / ``paragraphs()``: results, in batches;
- ``job()``: a job's terminal report.

Once the client knows its ``attempt`` (given, or learnt from ``start``), every write carries ``?attempt=``, and the
catalog refuses results of a pass a later ``/start`` superseded (409 ``superseded`` → ``RunSuperseded``).

Connection errors and 5xx are retried with exponential backoff; a 409 is never retried; any other 4xx raises
``CatalogRefused``, whose message carries the catalog's response body (its problem detail names the field that failed
validation). The request payload is never logged: paragraph text and embeddings stay out of the logs.
"""

import json
import logging
import time
from collections.abc import Iterator, Sequence
from typing import Any, TypeVar

import requests

from evilflowers_worker.contract import JobReport, Paragraph, ParagraphLink, Region, RunState, StartRun, dump
from evilflowers_worker.http import session
from evilflowers_worker.observability import metrics
from evilflowers_worker.settings import WorkerSettings

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: How much of a refusal's response body goes into the log line and the exception. Leaves room for the prefix within
#: the catalog's ``MAX_ERROR_LENGTH``, so the job's reported error keeps the whole of it.
MAX_BODY_LENGTH = 1500


class RunNotRunning(Exception):
    """The catalog no longer accepts results for this run (409): it was reset, restarted or finished."""


class RunSuperseded(RunNotRunning):
    """409 ``superseded``: a later ``/start`` began a new attempt; this pass's results are refused."""


class DuplicateResults(Exception):
    """409 ``duplicate``: the run already holds these results (a paragraph ``seq``). Retrying cannot help, and the
    run still waits for the job, so it is a failure to report, not a refusal to drop."""


class CatalogRefused(requests.HTTPError):
    """The catalog refused a request (a 4xx other than 409, or a 5xx on the last try).

    The message names the call, the run, the attempt, the batch and the catalog's response body (capped at
    ``MAX_BODY_LENGTH``), so the job's ``failed`` report and the log line both say what was refused.
    """

    # Defaults: unpickling calls the class with ``args`` only (then restores ``__dict__``), and Celery pickles the
    # exception that leaves a task.
    def __init__(self, message: str, *, status: int = 0, body: str = "", response: Any = None):
        super().__init__(message, response=response)
        self.status = status
        self.body = body


def _body(response: Any) -> str:
    """The response body as one capped line: compact JSON when it parses, the text otherwise."""
    try:
        text = json.dumps(response.json(), ensure_ascii=False, separators=(",", ":"))
    except Exception:
        text = getattr(response, "text", "") or ""
    text = " ".join(text.split())
    return text if len(text) <= MAX_BODY_LENGTH else text[: MAX_BODY_LENGTH - 1] + "…"


def batches[T](items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    for first in range(0, len(items), size):
        yield items[first : first + size]


def _route(path: str) -> str:
    """The metric label of a path: ``/jobs/equations:0001-0008`` → ``/jobs`` (bounded cardinality)."""
    return "/" + path.strip("/").split("/")[0] if path.strip("/") else "/"


def _conflict(response: Any) -> Exception:
    """A 409 as the right exception, by the problem detail's ``reason``.

    ``superseded`` → ``RunSuperseded``; ``duplicate`` → ``DuplicateResults``; ``not_running`` or none (a catalog
    that predates the reasons) → ``RunNotRunning``.
    """
    try:
        body = response.json()
    except Exception:
        body = None
    reason = None
    if isinstance(body, dict):
        reason = body.get("reason") or (body.get("additional_data") or {}).get("reason")
    message = getattr(response, "text", "") or reason or "409"
    if reason == "duplicate":
        return DuplicateResults(message)
    return (RunSuperseded if reason == "superseded" else RunNotRunning)(message)


class CatalogClient:
    def __init__(
        self,
        run_id: str,
        *,
        settings: WorkerSettings,
        attempt: int | None = None,
        http: requests.Session | None = None,
    ):
        self.settings = settings
        self.run_id = run_id
        #: The attempt this client works for; sent with every write once known.
        self.attempt = attempt
        self.base = f"{settings.catalog_api_url.rstrip('/')}/extraction/v1/runs/{run_id}"
        self.http = http or session(settings.catalog_api_key)

    # -- The API --------------------------------------------------------------------------------------------------

    def run(self) -> RunState:
        """The run's status, jobs and attempt."""
        return RunState.model_validate(self._request("GET", ""))

    def start(self, start: StartRun) -> RunState:
        """Start a pass; the returned run carries the new ``attempt``, which every later write of this client sends."""
        state = RunState.model_validate(self._request("POST", "/start", dump(start), attempt=False))
        self.attempt = state.attempt
        return state

    def regions(self, regions: Sequence[Region]) -> None:
        chunks = list(batches(regions, self.settings.catalog_batch_size))
        for index, batch in enumerate(chunks):
            self._request(
                "POST",
                "/regions",
                {"regions": [dump(region) for region in batch]},
                batch=_describe(index, len(chunks), batch),
            )

    def paragraphs(self, paragraphs: Sequence[Paragraph], links: Sequence[ParagraphLink] = ()) -> None:
        """Paragraphs in batches; the links go with the last batch (they may point back at earlier ones)."""
        chunks = list(batches(paragraphs, self.settings.catalog_batch_size))
        for index, batch in enumerate(chunks):
            payload: dict[str, Any] = {"paragraphs": [dump(paragraph) for paragraph in batch]}
            if index == len(chunks) - 1 and links:
                payload["links"] = [dump(link) for link in links]
            self._request("POST", "/paragraphs", payload, batch=_describe(index, len(chunks), batch))

    def job(self, name: str, report: JobReport) -> None:
        """A job's terminal report. The response is not read: once ``done`` is accepted, nothing may turn it into an
        error the job would then report as ``failed``."""
        self._request("POST", f"/jobs/{name}", dump(report))

    # -- Transport ------------------------------------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        attempt: bool = True,
        batch: str | None = None,
    ) -> dict[str, Any]:
        """One call with retries; returns the response's ``response`` envelope (or the body without one).

        ``batch`` (``batch 2/12, seq 100-199``) only names the call in a refusal.
        """
        params = {"attempt": self.attempt} if attempt and method != "GET" and self.attempt is not None else None
        attempts = self.settings.catalog_attempts
        for number in range(1, attempts + 1):
            outcome = self._try(method, path, payload, params, last=number == attempts, batch=batch)
            if not isinstance(outcome, str):
                return outcome
            logger.warning("Catalog %s on %s %s, retrying", outcome, method, path)
            metrics.CATALOG_RETRIES.add(1, {"route": _route(path), "reason": outcome})
            time.sleep(2**number)
        raise AssertionError("unreachable")  # pragma: no cover - the last try returns or raises

    def _try(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        params: dict[str, Any] | None,
        *,
        last: bool,
        batch: str | None = None,
    ) -> dict[str, Any] | str:
        """One try: the unwrapped body, or why it is worth another (``unreachable``, ``503``).

        Raises on a 409, on any other 4xx, and on the last try's failure.
        """
        try:
            response = self.http.request(
                method, f"{self.base}{path}", json=payload, params=params, timeout=self.settings.catalog_timeout
            )
        except requests.RequestException:
            if last:
                raise
            return "unreachable"
        if response.status_code == 409:
            raise _conflict(response)
        if response.status_code >= 500 and not last:
            return str(response.status_code)
        if response.status_code >= 400:
            raise self._refused(method, path, response, batch)
        body = response.json() if response.content else {}
        return body.get("response", body) if isinstance(body, dict) else {}

    def _refused(self, method: str, path: str, response: Any, batch: str | None) -> CatalogRefused:
        """A refusal as an exception, logged with what the catalog said (never with what was sent)."""
        status = response.status_code
        body = _body(response)
        where = [f"run {self.run_id}", f"attempt {self.attempt}"] + ([batch] if batch else [])
        message = f"Catalog refused {method} {path or '/'} ({', '.join(where)}): {status} {body}".rstrip()
        logger.error(message)
        return CatalogRefused(message, status=status, body=body, response=response)


def _describe(index: int, count: int, batch: Sequence[Region] | Sequence[Paragraph]) -> str:
    """``batch 2/12, seq 100-199``: which part of a batched call a refusal is about."""
    return f"batch {index + 1}/{count}, seq {batch[0].seq}-{batch[-1].seq}"
