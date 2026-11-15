"""The protocol of a leaf job: one job of one run, for one attempt (text, image and equation implement it).

``LeafJob.run`` is the whole lifecycle; a service writes ``extract`` (the work) and ``publish`` (its results):

1. **Delivery cap.** Past ``MAX_DELIVERIES`` the job killed its worker every time: it is reported ``failed``.
2. **Check.** ``GET runs/{id}``: a copy of a job already reported, a run no longer running, or a superseded attempt
   stops here, before the expensive part (``already_done`` / ``not_running`` / ``superseded``).
3. ``extract()``. A long job calls ``recheck()`` between batches and stops early when the run moves on.
4. ``publish(result)``, with ``?attempt=``.
5. The terminal report: ``done``, or ``failed`` with the error. A job that finds it was superseded says so
   (``superseded``), and the catalog counts it.

A failure anywhere is reported (the run would otherwise wait for the job until the stale sweep), except a 409: the
catalog no longer wants this job's results.
"""

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar, Literal

from pydantic import ValidationError

from evilflowers_worker.catalog import CatalogClient, RunNotRunning, RunSuperseded
from evilflowers_worker.contract import JobMessage, JobReport, SkipReason, is_job_name, job_kind
from evilflowers_worker.deliveries import capped
from evilflowers_worker.observability import bind, fail, metrics
from evilflowers_worker.settings import WorkerSettings

logger = logging.getLogger(__name__)

Outcome = Literal["done", "failed", "not_running", "superseded", "already_done"]


class Stop(Exception):
    """The run does not want this job's work any more."""

    def __init__(self, reason: SkipReason):
        super().__init__(reason)
        self.reason: SkipReason = reason


class LeafJob[M: JobMessage, Result](ABC):
    """``M``: the job's message model; ``Result``: what ``extract`` returns and ``publish`` sends."""

    #: The job's kind: its catalog name unless the message names a batch, and its metric label.
    kind: ClassVar[str]

    def __init__(
        self,
        message: M,
        *,
        settings: WorkerSettings,
        catalog: CatalogClient | None = None,
        task_id: str | None = None,
        delivery: int = 1,
        recheck_interval: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.message = message
        self.settings = settings
        self.name = message.job or self.kind
        self.task_id = task_id
        self.delivery = delivery
        self.catalog = catalog or CatalogClient(message.run_id, settings=settings, attempt=message.attempt)
        self._recheck_interval = recheck_interval
        self._clock = clock
        self._checked_at = clock()

    # -- What a service writes ------------------------------------------------------------------------------------

    @abstractmethod
    def extract(self) -> Result:
        """The work."""

    @abstractmethod
    def publish(self, result: Result) -> None:
        """Send the results to the catalog (``self.catalog.regions`` / ``.paragraphs``)."""

    def provenance(self) -> dict[str, Any]:
        """What produced the results; merged into the run's provenance under the job's kind."""
        return {"image": self.settings.image_tag}

    # -- The lifecycle ----------------------------------------------------------------------------------------------

    def run(self) -> Outcome:
        message = self.message
        with bind(
            run_id=message.run_id,
            job=self.name,
            attempt=message.attempt,
            source=message.source,
            task_id=self.task_id,
            delivery=self.delivery,
        ):
            if capped(self.delivery, self.settings.max_deliveries):
                return self._give_up(
                    f"Gave up after {self.delivery - 1} deliveries: the worker died or was stopped during each of them"
                )
            try:
                self.check()
                result = self.extract()
                self.publish(result)
                self.catalog.job(self.name, JobReport(status="done", provenance=self.provenance()))
            except Stop as stop:
                return self._stopped(stop.reason)
            except RunSuperseded:
                return self._stopped("superseded")
            except RunNotRunning:
                logger.warning("Run %s is not accepting results; job %s dropped", message.run_id, self.name)
                return self._outcome("not_running")
            except Exception as error:
                logger.exception("Job %s of run %s failed", self.name, message.run_id)
                fail(error)
                self._report(JobReport(status="failed", error=f"{type(error).__name__}: {error}"))
                return self._outcome("failed")
            return self._outcome("done")

    def check(self) -> None:
        """Stop (``Stop``) unless the run still waits for this job, for this attempt."""
        self._checked_at = self._clock()
        if reason := self.catalog.run().skip_reason(self.name, self.message.attempt):
            raise Stop(reason)

    def recheck(self) -> None:
        """Between batches of a long job: ``check`` again once ``recheck_interval`` has passed.

        A failure to read the run is logged and the work goes on: only an answer stops it.
        """
        if self._clock() - self._checked_at < self._recheck_interval:
            return
        try:
            self.check()
        except Stop:
            raise
        except Exception:
            logger.warning("Cannot re-read run %s; carrying on", self.message.run_id, exc_info=True)

    def report_failure(self, error: BaseException) -> Outcome:
        """Report the job failed without running it: its worker process died and it will not be retried."""
        with bind(run_id=self.message.run_id, job=self.name, attempt=self.message.attempt, task_id=self.task_id):
            return self._give_up(f"The worker died during the job: {error!r}")

    # -- Internals --------------------------------------------------------------------------------------------------

    def _give_up(self, error: str) -> Outcome:
        logger.error("Job %s of run %s: %s", self.name, self.message.run_id, error)
        self._report(JobReport(status="failed", error=error))
        return self._outcome("failed")

    def _stopped(self, reason: SkipReason) -> Outcome:
        logger.info("Job %s of run %s stopped: %s", self.name, self.message.run_id, reason)
        if reason == "superseded":
            # The catalog counts these (EvilFlowersCatalog#140); it changes nothing in the run.
            self._report(JobReport(status="superseded"))
        return self._outcome(reason)

    def _report(self, report: JobReport) -> None:
        """Best effort: the failure is already logged and on the span; a second one must not hide it."""
        try:
            self.catalog.job(self.name, report)
        except RunNotRunning:
            logger.warning("Run %s is not accepting results; job %s not reported", self.message.run_id, self.name)
        except Exception:
            logger.exception("Cannot report job %s of run %s", self.name, self.message.run_id)

    def _outcome(self, outcome: Outcome) -> Outcome:
        metrics.record_job(self.kind, outcome)
        return outcome


def report_invalid_message(
    kwargs: dict[str, Any], error: ValidationError, *, job: str, settings: WorkerSettings
) -> Literal["failed"]:
    """A message its consumer cannot read: report the job failed (when the run is known) and drop the message.

    Never retried: the same message fails the same way. Without the report the run would wait for the stale sweep.
    ``job``: the job's default name (its kind). The message's own ``job`` wins only when it follows the contract's
    rules: a malformed one may be the very thing that made the message invalid, and reporting under it would add an
    undeclared job to the run while the declared one stays pending.
    """
    run_id = kwargs.get("run_id")
    given = kwargs.get("job")
    name = given if isinstance(given, str) and is_job_name(given) else job
    logger.error("Invalid message for job %s of run %s: %s", name, run_id, error)
    metrics.record_job(job_kind(job), "failed")
    if not isinstance(run_id, str) or not run_id:
        return "failed"
    attempt = kwargs.get("attempt") if isinstance(kwargs.get("attempt"), int) else None
    try:
        CatalogClient(run_id, settings=settings, attempt=attempt).job(
            str(name), JobReport(status="failed", error=f"Invalid message: {error}")
        )
    except Exception:
        logger.exception("Cannot report the invalid message of run %s", run_id)
    return "failed"
