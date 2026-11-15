# evilflowers-worker

The runtime of the EvilFlowers extraction workers: the message contract, the catalog client, the Celery delivery
semantics and the observability, in one package. The design is in
[`docs/architecture.md`](docs/architecture.md).

It has no model, layout or PDF dependency, so every service can depend on it:

```toml
# pyproject.toml of a service
dependencies = ["evilflowers-worker"]

[tool.uv.sources]
evilflowers-worker = { git = "https://github.com/EvilFlowersCatalog/evilflowers-worker", rev = "<sha>" }
```

Extras:
- `otel`: the OpenTelemetry SDK and instrumentations;
- `profiling`: the Pyroscope SDK.

The image installs both. Without them, everything is a no-op.

## Modules

| Module | What it gives a service |
|---|---|
| `contract` | `ExtractRequest`, `ParagraphsJob`, `RegionsJob` (commands); `Region`, `Paragraph`, `JobReport`, `RunState`, `StartRun` (catalog payloads); job-name rules |
| `catalog` | `CatalogClient`: `run()`, `start()`, `regions()`, `paragraphs()`, `job()`; `?attempt=`; `RunNotRunning` / `RunSuperseded` / `DuplicateResults` |
| `jobs` | `LeafJob`: delivery cap, check, `extract`, `publish`, terminal report, `recheck()` for long jobs |
| `runtime` | `create_app()`, `JOB_TASK_OPTIONS`, `run_job()`, `Publisher` / `CeleryPublisher`, `runtime().delivery(task)` |
| `deliveries` | `DeliveryCounter` and `capped()`: the Redis count behind the delivery cap |
| `shutdown` | SIGTERM: warm shutdown, grace timer, cold cancel, fallback requeue |
| `observability` | `configure`, `bind`, `span`, `annotate`, `fail`, `untraced_http`, `metrics.Instrument`, JSON logs, Pyroscope |
| `settings` | `WorkerSettings`: broker, catalog, storage, shutdown, delivery cap, logs |
| `testing` | `FakeCatalog`, `RefusingCatalog`, `RecordingPublisher`, `running()`; `StubCatalogServer`, `WorkerProcess` (a real worker against Redis) |

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

For the tests:
- run the job in-process against `testing.FakeCatalog`;
- run the real worker with `testing.WorkerProcess` against Redis and `testing.StubCatalogServer`, as
  `tests/test_runtime.py` does here and the analyzer's `tests/test_end_to_end.py` does for a whole run.

`tests/example.py` is a complete leaf job, and `tests/probe_app.py` is a complete worker. Both use nothing outside this
package.
