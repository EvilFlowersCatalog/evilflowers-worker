"""The smallest jobs of each kind: the reference for a new service.

- ``EchoFigures``: an extraction leaf job (figures) that stores the regions it is given, through ``run_job``;
- ``convert``: a catalog job (``files.ocr``-shaped) that writes an output and then its outcome file, through
  ``run_task``; ``give_up`` is its ``on_capped``;
- ``shout``: a standalone job (``ShoutJob``): reads its inputs, writes into its job-scoped output directory, then
  its outcome file, and returns a compact summary for the orchestrator's ``link``; ``shout_give_up`` is its
  ``on_capped``.
"""

import os
import signal
import time
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from evilflowers_worker.contract import Region, RegionsJob, StandaloneJob
from evilflowers_worker.jobs import LeafJob
from evilflowers_worker.observability import bind, metrics
from evilflowers_worker.outcome import OutcomeFile, atomic_write, write_outcome
from evilflowers_worker.runtime import runtime


class EchoFigures(LeafJob[RegionsJob, list[Region]]):
    kind = "figures"

    def extract(self) -> list[Region]:
        found = []
        for number, region in enumerate(self.message.regions):
            if number and number % 100 == 0:
                self.recheck()  # a long job asks between batches whether the run still wants it
            found.append(Region(kind="figure", page=region.page, seq=region.seq, bbox=region.bbox))
        return found

    def publish(self, result: list[Region]) -> None:
        self.catalog.regions(result)

    def provenance(self) -> dict:
        return {**super().provenance(), "extractor": "echo"}


# -- A catalog job ------------------------------------------------------------------------------------------------


class ConvertRequest(BaseModel):
    """What the catalog sends: paths relative to the storage root, and the run's capability token."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    run_id: str
    token: str
    source: str
    destination: str
    outcome: str
    #: Test only: the job kills its worker (an OOM, as far as the parent can tell).
    die: bool = False
    #: Test only: seconds the work takes before anything is written (a long book).
    seconds: float = 0


class ConvertOutcome(OutcomeFile):
    status: Literal["done", "failed"]
    output: str | None = None
    error: str | None = None


def convert(message: ConvertRequest, delivery: int) -> dict[str, Any]:
    settings = runtime().settings
    with bind(run_id=message.run_id):
        if message.die:
            os.kill(os.getpid(), signal.SIGKILL)
        time.sleep(message.seconds)
        text = settings.storage_file(message.source).read_text()
        destination = settings.storage_file(message.destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(destination, text.upper().encode())  # the output first …
        outcome = ConvertOutcome(run_id=message.run_id, token=message.token, status="done", output=message.destination)
        write_outcome(settings, message.outcome, outcome)  # … then the outcome, the commit marker
        metrics.record_job("convert", "done")
        return {"status": "done", "delivery": delivery}  # compact: the event's `result` is capped at 1024 characters


def give_up(message: ConvertRequest, delivery: int) -> dict[str, Any]:
    """Past the delivery cap: a ``failed`` outcome, then raise, so the job keeps both the error and the traceback."""
    error = f"gave up after {delivery - 1} deliveries: the worker died during each of them"
    failed = ConvertOutcome(run_id=message.run_id, token=message.token, status="failed", error=error)
    write_outcome(runtime().settings, message.outcome, failed)
    metrics.record_job("convert", "failed")
    raise RuntimeError(error)


# -- A standalone job ---------------------------------------------------------------------------------------------


class ShoutJob(StandaloneJob):
    """A standalone job's own options next to the shared fields (``run_id``, ``job``, ``token``, ``inputs`` …)."""

    #: Test only: the job raises after writing a ``failed`` outcome (a domain error).
    fail: bool = False
    #: Test only: the job kills its worker.
    die: bool = False
    #: Test only: seconds before anything is written.
    seconds: float = 0


class ShoutOutcome(OutcomeFile):
    status: Literal["done", "failed"]
    job: str
    outputs: list[str] = Field(default_factory=list)
    error: str | None = None


def shout(message: ShoutJob, delivery: int) -> dict[str, Any]:
    """Each input upper-cased into the job's output directory, then the outcome; a compact summary for ``link``."""
    settings = runtime().settings
    with bind(run_id=message.run_id, job_id=message.job):
        if message.die:
            os.kill(os.getpid(), signal.SIGKILL)
        time.sleep(message.seconds)
        if message.fail:
            return _shout_failed(message, "the page is unreadable")
        message.prepare_output(settings)
        outputs = []
        for source in message.input_paths(settings):
            atomic_write(message.output_file(settings, source.name), source.read_text().upper().encode())
            outputs.append(source.name)
        outcome = ShoutOutcome(run_id=message.run_id, token=message.token, status="done", job=message.job,
                               outputs=outputs)  # fmt: skip
        write_outcome(settings, message.outcome, outcome)
        return {"status": "done", "outputs": len(outputs)}  # the link's first argument: compact


def shout_give_up(message: ShoutJob, delivery: int) -> dict[str, Any]:
    return _shout_failed(message, f"gave up after {delivery - 1} deliveries")


def _shout_failed(message: ShoutJob, error: str) -> dict[str, Any]:
    failed = ShoutOutcome(run_id=message.run_id, token=message.token, status="failed", job=message.job, error=error)
    write_outcome(runtime().settings, message.outcome, failed)
    raise RuntimeError(error)  # the task fails: link_error fires
