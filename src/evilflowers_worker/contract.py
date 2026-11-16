"""The extraction contract: the commands workers send each other and the facts they report to the catalog.

Two halves (docs/architecture.md §3):

- **Messages** (``Message`` subclasses) are Celery tasks with keyword arguments only. ``Route`` addresses them (task
  name + queue). Consumers ignore fields they do not know, so a field can be added without coordinating a release;
  removing or retyping one is a new task name.
- **Catalog payloads** mirror the catalog's ``/extraction/v1`` forms (``StartRunForm``, ``RegionsForm``,
  ``ParagraphsForm``, the job report) and its run serializer (``RunState``), as published in its OpenAPI spec
  (evilflowers-protocol).

Every model validates at the edge: a consumer builds its message with ``model_validate(kwargs)`` before any work.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from evilflowers_worker.settings import RuntimeSettings

#: The catalog stores at most this much of a job's error message.
MAX_ERROR_LENGTH = 2000
#: The catalog's limit on a job name in ``/start`` (``CharField(max_length=30)``).
MAX_JOB_NAME_LENGTH = 30
#: Dimensions of the paragraph embeddings the catalog stores (``halfvec(1024)``).
EMBEDDING_DIMENSIONS = 1024

#: ``paragraphs``, ``figures``, or a batch of a kind: ``equations:0001-0008``.
JOB_NAME_PATTERN = r"^[a-z][a-z_]*(:[0-9]+-[0-9]+)?$"
JobName = Annotated[
    str, StringConstraints(min_length=1, max_length=MAX_JOB_NAME_LENGTH, pattern=JOB_NAME_PATTERN, strict=True)
]
_JOB_NAME = re.compile(JOB_NAME_PATTERN)


def is_job_name(name: str) -> bool:
    """Whether the catalog would take ``name`` as a job: ``figures``, ``equations:0001-0008``; ≤ 30 characters."""
    return len(name) <= MAX_JOB_NAME_LENGTH and _JOB_NAME.match(name) is not None


def batch_job(kind: str, first: int, last: int) -> str:
    """The job of one batch of pages: ``equations:0001-0008``."""
    name = f"{kind}:{first:04d}-{last:04d}"
    if not is_job_name(name):
        raise ValueError(f"Job name {name!r} breaks the catalog's rules (≤ {MAX_JOB_NAME_LENGTH} chars)")
    return name


def job_kind(name: str) -> str:
    """The kind of a job: ``equations:0001-0008`` → ``equations``. Metrics label by kind, never by batch."""
    return name.split(":", 1)[0]


# -- Geometry -----------------------------------------------------------------------------------------------------


class Box(BaseModel):
    """Page fractions, origin top-left, on the original file's 1-based page."""

    model_config = ConfigDict(frozen=True)

    x0: float
    y0: float
    x1: float
    y1: float


class LayoutRegion(BaseModel):
    """A region the analyzer's layout pass found, as the region services receive it."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    page: int = Field(ge=1)
    seq: int = Field(ge=0)
    bbox: Box
    caption: str | None = None


#: A step the catalog can re-run on its own (EvilFlowersCatalog#183): one job kind of the extraction pipeline.
Step = Literal["equations", "figures", "paragraphs", "tables"]

#: ``[level, title, page]``: PyMuPDF's ``get_toc(simple=True)`` entry, 1-based page.
OutlineEntry = tuple[int, str, int]


# -- Messages (commands) ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Route:
    """Where a message goes: the task that handles it and the queue its consumers listen on."""

    task: str
    queue: str


class Message(BaseModel):
    """A command: the keyword arguments of one Celery task."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    def kwargs(self) -> dict[str, Any]:
        """The wire form: JSON values, top-level ``None`` left out (absent and ``None`` mean the same to consumers)."""
        return {key: value for key, value in self.model_dump(mode="json").items() if value is not None}


class ExtractRequest(Message):
    """``evilflowers_analyzer.extract``: the catalog asks for a run once a PDF is stored."""

    TASK: ClassVar[str] = "evilflowers_analyzer.extract"

    run_id: str
    #: Relative to the catalog's storage root.
    source: str
    #: Where crops go, relative to the storage root (``catalogs/<catalog>/<entry>/extraction/<acq>/v<version>``).
    output_dir: str
    acquisition_id: str | None = None
    pipeline_version: int | None = None
    #: A step re-run (EvilFlowersCatalog#183, analyzer#7): the sorted subset of steps to redo. Absent or empty: a full
    #: pass.
    steps: list[Step] = Field(default_factory=list)


class JobMessage(Message):
    """What every job of a run receives."""

    run_id: str
    source: str
    output_dir: str
    #: The run's attempt from ``/start``; the job sends it back (``?attempt=``) and stops when it is superseded.
    attempt: int | None = None
    #: The catalog job to report under when it is not the task's default kind (a batch: ``equations:0001-0008``).
    job: JobName | None = None


class ParagraphsJob(JobMessage):
    """``evilflowers_text_worker.paragraphs``: the paragraph index of the whole document."""

    #: The headings the paragraphs fall under (#5). Present (even ``[]``): authoritative. Absent: read the bookmarks.
    outline: list[OutlineEntry] | None = None


class RegionsJob(JobMessage):
    """``figures`` / ``tables`` / ``equations``: the regions of one kind the layout pass found.

    Sent even when empty, so every declared job reports and the run can finish.
    """

    regions: list[LayoutRegion] = Field(default_factory=list)


class StandaloneJob(Message):
    """A catalog-free job: one job of any orchestrator's run (docs/architecture.md §2, evilflowers-worker#5).

    The orchestrator (the books digitalizer, or any other) is the system of record. The worker reads ``inputs``,
    writes into its job-scoped ``output_dir``, writes the outcome file last (``outcome.write_outcome``) and returns
    a compact summary. It holds no database or catalog credential. The orchestrator learns that the job ended from
    the ``link`` / ``link_error`` callbacks it attached (``runtime.standalone_callbacks``), reads the outcome, checks
    the token and promotes the output.

    A service subclasses it for its own options. Run it through ``runtime.run_task``.
    """

    #: The orchestrator's run.
    run_id: str = Field(min_length=1)
    #: The orchestrator's job (its ledger id); names the job-scoped output directory.
    job: str = Field(min_length=1)
    #: The per-job capability token, echoed back untouched in the outcome file.
    token: str = Field(min_length=1)
    #: Leaf image paths, or one PDF, relative to ``STORAGE_PATH``.
    inputs: list[str] = Field(min_length=1)
    #: Job-scoped, relative to ``STORAGE_PATH``: ``<item>/_studio/jobs/<job>/``. The orchestrator promotes it.
    output_dir: str = Field(min_length=1)
    #: Where the outcome file goes, relative to ``STORAGE_PATH``.
    outcome: str = Field(min_length=1)

    def input_paths(self, settings: RuntimeSettings) -> list[Path]:
        """The inputs as absolute paths, each checked to stay under ``STORAGE_PATH`` (a message is input)."""
        return [settings.storage_file(relative) for relative in self.inputs]

    def prepare_output(self, settings: RuntimeSettings) -> Path:
        """The job-scoped output directory, created and checked to stay under ``STORAGE_PATH``."""
        directory = settings.storage_file(self.output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def output_file(self, settings: RuntimeSettings, name: str) -> Path:
        """A file in the output directory (``page-0001.hocr``, ``pages/0001.png``); refuses one that escapes it.

        Its parent directories are created, so the path can be written at once (``outcome.atomic_write``).
        """
        directory = settings.storage_file(self.output_dir)
        path = settings.storage_file(f"{self.output_dir.rstrip('/')}/{name}")
        if not path.is_relative_to(directory) or path == directory:
            raise ValueError(f"path escapes the job's output directory: {name!r}")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


# -- Catalog payloads (facts) -------------------------------------------------------------------------------------


class Region(BaseModel):
    """A region a service stores (``RegionsForm``).

    The decision fields (EvilFlowersCatalog#189, evilflowers-worker#7) are optional; ``dump`` leaves out the ones that
    are ``None``, and the catalog stores such a region with no band, as before. ``Verdict.region_fields()`` (the
    ``judges`` extra) fills ``band``, ``p``, ``decision`` and ``decider_model``.
    """

    kind: Literal["figure", "table", "equation"]
    page: int = Field(ge=1)
    seq: int = Field(ge=0)
    bbox: Box
    image_path: str | None = Field(None, max_length=255)
    caption: str | None = None
    printed_caption: str | None = None
    description: str | None = None
    category: str | None = Field(None, max_length=50)
    markdown: str | None = None
    latex: str | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)
    # -- Decision (EvilFlowersCatalog#189) ----------------------------------------------------------------------------
    #: ``hide`` is kept in the run and hidden from readers.
    band: Literal["keep", "review", "hide"] | None = None
    #: Probability of ``decision``; ``0.0`` is a value, not "unknown".
    p: float | None = Field(None, ge=0.0, le=1.0)
    #: A deterministic quality score (a table's grounded share combined with its other signals).
    score: float | None = Field(None, ge=0.0, le=1.0)
    #: The answer itself (``stamp``, ``chart``, ``repeated``). ``repeated`` with no ``p`` should come with ``band``.
    decision: str | None = Field(None, max_length=100)
    #: ``model@revision`` of the decider; collected in the run's provenance too.
    decider_model: str | None = Field(None, max_length=200)
    #: What a review did to the region (a table review: ``kept``, ``rewritten``, ``transcribed``, ``failed``).
    review_outcome: str | None = Field(None, max_length=50)
    #: Share of the region's values found in the PDF's own text inside its box.
    grounded_share: float | None = Field(None, ge=0.0, le=1.0)
    #: The cheap signals behind the decision (sizes, ratios, hashes); free-form.
    signals: dict[str, Any] | None = None


class Sentence(BaseModel):
    seq: int
    start: int
    end: int
    page: int
    quads: list[Box] = Field(default_factory=list)


class Paragraph(BaseModel):
    """A paragraph with its sentences and embedding (``ParagraphsForm``)."""

    model_config = ConfigDict(validate_assignment=True)

    page: int
    seq: int
    section: str | None = None
    bbox: Box
    text: str
    sentences: list[Sentence] = Field(default_factory=list)
    embedding: list[float] | None = None

    @field_validator("embedding")
    @classmethod
    def _dimensions(cls, value: list[float] | None) -> list[float] | None:
        if value is not None and len(value) != EMBEDDING_DIMENSIONS:
            raise ValueError(f"embedding has {len(value)} dimensions, the catalog stores {EMBEDDING_DIMENSIONS}")
        return value


class ParagraphLink(BaseModel):
    source: int
    target: int
    kind: Literal["next", "similar"]
    score: float | None = None


class StartRun(BaseModel):
    """``POST /start`` (``StartRunForm``): what the coordinator found and the jobs it is about to send."""

    page_count: int = Field(ge=0)
    page_sizes: list[list[float]]
    jobs: list[JobName]
    provenance: dict[str, Any] = Field(default_factory=dict)
    #: The steps of a step re-run, echoed from ``ExtractRequest.steps``. ``None`` on a full pass, so ``dump`` sends no
    #: ``steps`` field (``[]`` would put ``"steps": []`` on every full ``/start``). The catalog refuses a subset it did
    #: not ask for with a 409 ``steps_refused`` (→ ``RunNotRunning``).
    steps: list[Step] | None = None


class JobReport(BaseModel):
    """``POST /jobs/{job}``: a job's terminal report."""

    status: Literal["done", "failed", "superseded"]
    error: str | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)

    @field_validator("error")
    @classmethod
    def _truncate(cls, value: str | None) -> str | None:
        return value[:MAX_ERROR_LENGTH] if value else None


SkipReason = Literal["not_running", "superseded", "already_done"]


class RunState(BaseModel):
    """The run as the services see it (``GET runs/{id}``, also returned by ``/start`` and job reports)."""

    model_config = ConfigDict(extra="ignore")

    status: str
    jobs: dict[str, str] = Field(default_factory=dict)
    #: ``None`` from a catalog that predates EvilFlowersCatalog#140.
    attempt: int | None = None

    def skip_reason(self, job: str, attempt: int | None = None) -> SkipReason | None:
        """Why a job for ``attempt`` must not do its work now, or ``None`` when it should."""
        if attempt is not None and self.attempt is not None and self.attempt != attempt:
            return "superseded"
        if self.status != "running":
            return "not_running"
        if self.jobs.get(job) != "pending":
            return "already_done"
        return None


def dump(model: BaseModel) -> dict[str, Any]:
    """A catalog payload: JSON values, ``None`` fields left out."""
    return model.model_dump(mode="json", exclude_none=True)
