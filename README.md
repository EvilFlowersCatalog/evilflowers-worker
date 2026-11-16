# evilflowers-worker

The runtime of the EvilFlowers workers: the message contract, the catalog client, outcome files, the Celery delivery
semantics and the observability, in one package. The design is in
[`docs/architecture.md`](docs/architecture.md).

It serves three kinds of worker (§2 of the design):
- **extraction jobs** (analyzer, text, image, equation), which report to the catalog's ingestion API;
- **catalog jobs** (ocr, lcpencrypt), which leave one outcome file for the catalog and hold no catalog credential;
- **standalone jobs**, which run without the catalog for any orchestrator (the books digitalizer). They leave an
  outcome file in a job-scoped directory, and the orchestrator learns the end from Celery `link` / `link_error`
  callbacks.

It has no model, layout or PDF dependency and runs on Python 3.12 to 3.14, so every service can depend on it:

```toml
# pyproject.toml of a service
dependencies = ["evilflowers-worker"]

[tool.uv.sources]
evilflowers-worker = { git = "https://github.com/EvilFlowersCatalog/evilflowers-worker", rev = "<sha>" }
```

Extras:
- `otel`: the OpenTelemetry SDK and instrumentations;
- `profiling`: the Pyroscope SDK;
- `judges`: the judges library (`evilflowers_worker.judges`). It adds no package: it is HTTP only, on the core's
  `requests`.

The image installs `otel` and `profiling`. Without them, telemetry and profiling are a no-op.

## Modules

| Module | What it gives a service |
|---|---|
| `contract` | `ExtractRequest` (`steps`: a step re-run), `ParagraphsJob`, `RegionsJob`, `StandaloneJob` (`input_paths()`, `prepare_output()`, `output_file()`) (commands); `Region` (with the optional decision fields `band`, `p`, `score`, `decision`, `decider_model`, …), `Paragraph` (with the optional `decision`, `p`, `decider_model`), `JobReport`, `RunState`, `StartRun` (catalog payloads); job-name rules |
| `catalog` | `CatalogClient`: `run()`, `start()`, `regions()`, `paragraphs()`, `job()`; `?attempt=`; `RunNotRunning` / `RunSuperseded` / `DuplicateResults` / `CatalogRefused` (a refusal with the catalog's body) |
| `jobs` | `LeafJob`: delivery cap, check, `extract`, `publish`, terminal report, `recheck()` for long jobs |
| `outcome` | `OutcomeFile`, `atomic_write`, `write_outcome`: a catalog job's outcome file, last and atomic |
| `runtime` | `create_app()`, `JOB_TASK_OPTIONS`, `run_job()` (leaf jobs), `run_task()` (anything else), `standalone_callbacks()` (an orchestrator's `link` / `link_error`), `Publisher` / `CeleryPublisher`, `runtime().delivery(task)` |
| `deliveries` | `DeliveryCounter` and `capped()`: the Redis count behind the delivery cap |
| `shutdown` | SIGTERM: warm shutdown, grace timer, cold cancel, fallback requeue |
| `observability` | `configure`, `bind`, `span`, `annotate`, `fail`, `untraced_http`, `metrics.Instrument`, JSON logs, Pyroscope; `CATALOG_JOB_ATTRIBUTES` |
| `settings` | `RuntimeSettings`: broker, storage (`storage_file`), shutdown, delivery cap, logs; `WorkerSettings`: plus the catalog ingestion API |
| `judges` | `Judges.from_settings()`, `Panel.ask()`, `Panel.ask_batch()` (one question about many states, `decider` only, 64 per request): narrow checks by `decider`, `decider-vision`, Ollama and tei-rerank (`Choice` / `Binary` → `Verdict` with a band); budget per job, circuit breaker per client, never fails the job; `JudgeSettings` mixin (docs/architecture.md §5.1) |
| `testing` | `FakeCatalog`, `RefusingCatalog`, `RecordingPublisher`, `running()`; `StubCatalogServer`, `assert_outcome`, `WorkerProcess` (a real worker against Redis) |

## A leaf service in five files

```python
# settings.py
class Settings(WorkerSettings):
    queue: str = Field("evilflowers_image", validation_alias=AliasChoices("IMAGE_QUEUE", "QUEUE"))
    crop_dpi: int = 144


# jobs.py
class FiguresJob(LeafJob[RegionsJob, list[Region]]):
    kind = "figures"

    def extract(self) -> list[Region]: ...  # call self.recheck() between batches of a long job

    def publish(self, result: list[Region]) -> None:
        self.catalog.regions(result)


# worker.py
app = runtime.create_app("image", settings, include=["evilflowers_image.tasks"])


# tasks.py
from evilflowers_image import worker  # noqa: F401 - create_app must run in a spawned pool child too


@shared_task(name="evilflowers_image.figures", bind=True, **runtime.JOB_TASK_OPTIONS)
def figures(self, **kwargs) -> str:
    # Validates (an invalid message is reported failed, never retried), counts the delivery, runs the job.
    return runtime.run_job(self, FiguresJob, RegionsJob, kwargs)
```

## A catalog job

```python
# settings.py: no catalog URL, no API key
class Settings(RuntimeSettings):
    queue: str = Field("evilflowers_ocr_worker", validation_alias=AliasChoices("OCR_QUEUE", "QUEUE"))
    task_time_limit: int = 14_100


# jobs.py
class OcrOutcome(OutcomeFile):  # run_id, token, status + the kind's fields
    status: Literal["done", "skipped", "failed"]
    output: str | None = None


def run(message: OcrRequest, delivery: int) -> dict:
    ...  # write the output (atomic_write) first
    write_outcome(settings, message.outcome, OcrOutcome(run_id=..., token=message.token, status="done", output=...))
    return {"status": "done"}  # compact: the Celery event's `result` is capped at 1024 characters


def give_up(message: OcrRequest, delivery: int) -> dict:  # past MAX_DELIVERIES
    write_outcome(settings, message.outcome, OcrOutcome(run_id=..., token=message.token, status="failed"))
    raise RuntimeError(f"gave up after {delivery - 1} deliveries")


# worker.py: the task names are a contract with the catalog, so the routes follow them
app = runtime.create_app(
    "ocr",
    settings,
    include=["evilflowers_ocr_worker.tasks"],
    queues=[settings.queue],
    routes={"evilflowers_ocr_worker.*": settings.queue},
    attributes=observability.CATALOG_JOB_ATTRIBUTES,
)


# tasks.py
@shared_task(
    name="evilflowers_ocr_worker.ocr", bind=True, time_limit=settings.task_time_limit, **runtime.JOB_TASK_OPTIONS
)
def ocr(self, **kwargs) -> dict:
    # Validates (an invalid message raises), counts the delivery, calls give_up past the cap instead of run.
    return runtime.run_task(self, jobs.run, OcrRequest, kwargs, on_capped=jobs.give_up)
```

Never write the outcome when the worker is stopped (SIGTERM, the cold cancel): the redelivery carries the same token.

## A standalone job

```python
# jobs.py: the shared fields (run_id, job, token, inputs, output_dir, outcome) + the service's own
class HocrJob(StandaloneJob):
    language: str = "slk"


def run(message: HocrJob, delivery: int) -> dict:
    message.prepare_output(settings)                       # <item>/_studio/jobs/<job>/, path-checked
    for leaf in message.input_paths(settings):
        atomic_write(message.output_file(settings, f"{leaf.stem}.hocr"), ocr(leaf, message.language))
    write_outcome(settings, message.outcome, HocrOutcome(run_id=..., token=message.token, status="done", ...))
    return {"status": "done", "pages": n}                  # the orchestrator's `link` receives this: keep it compact


# tasks.py: the same run_task as a catalog job (on_capped writes a failed outcome and raises → link_error)
@shared_task(name="evilflowers_ocr_worker.standalone.hocr", bind=True, **runtime.JOB_TASK_OPTIONS)
def hocr(self, **kwargs) -> dict:
    return runtime.run_task(self, jobs.run, HocrJob, kwargs, on_capped=jobs.give_up)


# the orchestrator's side: no result backend; the callbacks arrive on its own queue
app.send_task("evilflowers_ocr_worker.standalone.hocr", kwargs=message.kwargs(), queue="evilflowers_ocr.standalone",
              **runtime.standalone_callbacks(app, done="digitalizer.job_done", failed="digitalizer.job_failed",
                                             queue="digitalizer.portal", job_id=job.id))


@app.task(name="digitalizer.job_failed", bind=True)  # bind=True: always applied as a task with the task id
def job_failed(self, task_id: str, job_id: str): ...
```

Never set `priority` on a message for a kit queue: a kit worker does not poll kombu's priority lists, so the message
would be stranded (docs/architecture.md §3.3).

## Tests

For the tests:
- run the job in-process against `testing.FakeCatalog`, or check a catalog job's file with `testing.assert_outcome`;
- run the real worker with `testing.WorkerProcess` against Redis and `testing.StubCatalogServer`, as
  `tests/test_runtime.py` does here and the analyzer's `tests/test_end_to_end.py` does for a whole run.

`tests/example.py` is a complete leaf job, a complete catalog job and a complete standalone job, and
`tests/probe_app.py` is a complete worker. Both use nothing outside this package.

**macOS, Python 3.14:** the prefork pool starts its children by spawn, and billiard can fail every task with
`ValueError: not enough values to unpack (expected 3, got 0)` before the task body runs. Celery then fires
`link_error`, which looks like a failing job. Run a local worker with `--pool=solo`. The Linux images fork and are not
affected.
