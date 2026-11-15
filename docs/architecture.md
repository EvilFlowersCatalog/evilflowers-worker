# Extraction workers: architecture and the reference service

Status: **proposed** (2026-11-15). Implemented first by this repository (the kit) and by
[evilflowers-analyzer-service](https://github.com/EvilFlowersCatalog/evilflowers-analyzer-service) (the first service
built on it). The text, image and equation services move to it through the issues listed at the end.

## 1. Why

The four extraction workers were written from one template and then fixed one by one. On `develop` (2026-11-15) they
shared the same concerns and solved each of them differently:

| Concern | analyzer | text | image | equation |
|---|---|---|---|---|
| Reads the run before the work | — (coordinator) | yes | **no** | yes, and every 5 min |
| Sends `?attempt=` | n/a (gets it) | **no** | **no** | yes |
| Reports under a batch `job` name | sends them | **no** | **no** | yes |
| A worker that dies on every try | pass cap (Redis) | `retry_on_worker_lost` + signal | **loops forever** | delivery cap (Redis) |
| SIGTERM during a job | warm + own grace timer | Celery soft shutdown | `REMAP_SIGTERM` | Celery soft shutdown |
| Default visibility timeout | 7200 | 43200 | 7200 | 7200 |
| Catalog client | POST only | GET + POST | POST only | GET + POST + attempt |
| Job protocol | — | ABC | copy, no base | copy, no base |
| Span attribute keys | 6 | 4 | 4 | 5 |

Each of these differences has caused a production incident or would cause one:
- analyzer#3: 24 passes of one book.
- The rollout of 2026-11-14 redid a finished book.
- EvilFlowersCatalog#120: books processed twice.
- EvilFlowersCatalog#140: work done for a superseded attempt.

The observability package was about 600 lines copied four times, and it was already drifting.

Goals:
- **One contract**, written down and validated at both ends.
- **One runtime** with the delivery semantics proven once against a real Redis.
- **One observability package.**
- **A service is its domain code plus about 50 lines of wiring.**

## 2. The model

```
                 ┌───────────── catalog (Django) ─────────────┐
 upload / job ──▶│ ExtractionRun: status, attempt, jobs{…}    │◀── facts: POST /extraction/v1/runs/{id}/…
                 │ system of record, finishes the run          │     (start, regions, paragraphs, jobs/{job})
                 └──────┬──────────────────────────────────────┘
                        │ command: evilflowers_analyzer.extract (Celery, Redis)
                        ▼
                 analyzer  (coordinator)  ── opens the PDF, POST /start → attempt, lays out
                        │ commands, one per job (Celery, Redis), each carrying run_id + attempt (+ job)
          ┌─────────────┼───────────────┬─────────────────┐
          ▼             ▼               ▼                 ▼
   text.paragraphs  text.tables   image.figures   equation.equations   (leaf jobs)
```

**Roles**
- **Catalog: system of record.** It owns the run's state machine (`queued → running → done | failed`) and its
  `attempt`, and it finishes a run when every declared job has reported. Workers never write to its database and
  hold no state of their own beyond a delivery counter.
- **Coordinator (the analyzer).** It turns one `extract` command into a declared set of jobs: `/start` with their
  names returns the attempt, and then it sends one command per job. It reports only its own failure, as job
  `analyzer`.
- **Leaf job (text, image, equation).** It does one job for one attempt. Its results and its terminal report go to
  the catalog over HTTP.

**Two directions**
- **Commands flow over the broker.** Celery messages are *work orders* (`extract`, `figures`, `paragraphs`, …)
  addressed to one queue, delivered at least once.
- **Facts flow back over HTTP to the catalog,** the event sink: results and a terminal report per job. There is no
  reply queue, and no service listens to another service.

This is choreography around a shared state store. It keeps the catalog the only place that decides whether a run is
done, and it lets every worker scale on its own queue (KEDA, knowledge#43).

**Why not Kafka (now)**
- Nothing runs it (knowledge: the `kafka` profile is off, and the analyzer left Kafka on 2026-11-03).
- Every message here is a command with exactly one consumer.
- Replay and fan-in, Kafka's strengths, are covered by the catalog's run state and the stale sweep.

The seam is kept anyway: messages are pydantic models with a stable task name, and `Publisher` is a protocol. A Kafka
transport would be:
- a `Publisher` that sends `message.kwargs()` to topic = queue, key = `run_id`, with `traceparent` as a header;
- a consumer loop that validates the same model and calls the same job.

## 3. The contract

### 3.1 Messages (commands)

All messages are Celery tasks with **keyword arguments only** and a JSON body.

| Task | Queue | Producer → consumer | Model (`evilflowers_worker.contract`) |
|---|---|---|---|
| `evilflowers_analyzer.extract` | `evilflowers_analyzer` | catalog → analyzer | `ExtractRequest` |
| `evilflowers_text_worker.paragraphs` | `evilflowers.text.paragraphs` | analyzer → text | `ParagraphsJob` |
| `evilflowers_text_worker.tables` | `evilflowers.text.tables` | analyzer → text | `RegionsJob` |
| `evilflowers_image.figures` | `evilflowers_image` | analyzer → image | `RegionsJob` |
| `evilflowers_equation.equations` | `evilflowers_equation` | analyzer → equation | `RegionsJob` |

Rules:
1. **Additive only.** A new field is optional, and consumers ignore what they do not know (`extra="ignore"` /
   `**_`). Removing or retyping a field is a new task name.
2. **`None` is not sent.** Empty lists are sent: `regions: []` means "no regions, report anyway".
3. **Every job message carries `run_id`, `source`, `output_dir`, and `attempt` once the run has one.**
4. **A batch job carries `job`:** `<kind>:<first>-<last>`, zero-padded, at most 30 characters (the catalog's limit).
5. **Commands are sent from inside the consuming task**, so Celery stamps `parent_id`/`root_id` (the catalog's job
   ledger attributes child tasks by `parent_id`) and the OTel instrumentation injects `traceparent`.
6. **Validation happens at the edge.** A consumer validates its message first. An invalid message is reported as a
   failed job (when `run_id` can be read) and acknowledged; it is never retried.

### 3.2 Facts (catalog ingestion API)

This is the catalog's `/extraction/v1` API, documented in its OpenAPI spec (`evilflowers-protocol`).
`evilflowers_worker.catalog.CatalogClient` is the one client:

| Call | When |
|---|---|
| `GET runs/{id}` → `RunState` | a leaf job, before the expensive part, and between batches |
| `POST runs/{id}/start` → `RunState` (new `attempt`) | the coordinator, once per pass |
| `POST runs/{id}/regions?attempt=` | figures / tables / equations, in batches of ≤ 100 (catalog cap 5000) |
| `POST runs/{id}/paragraphs?attempt=` | paragraphs, batches of ≤ 100 (cap 1000), links on the last batch |
| `POST runs/{id}/jobs/{job}?attempt=` | the terminal report: `done`, `failed` or `superseded` |

Behaviour:
- **Errors.** A 409 is read by the problem's `reason`:
  - `superseded` → `RunSuperseded`: stop, report `superseded`;
  - `not_running` or none → `RunNotRunning`: drop quietly;
  - `duplicate` → `DuplicateResults`: report the job `failed`, because the run still waits for it.

  Other 4xx raise. Connection errors and 5xx are retried with exponential backoff and counted in
  `<service>_catalog_retries_total{route,reason}`.

### 3.3 Delivery semantics

At-least-once, made safe by idempotency on the catalog's side:

| Situation | What happens | Mechanism |
|---|---|---|
| Worker dies mid-job (OOM) | message redelivered | `acks_late` + `reject_on_worker_lost` |
| Dies on every try | reported `failed` after `MAX_DELIVERIES` | `DeliveryCounter` (Redis, per task id; a fresh delivery resets it) |
| Scale-in mid-job | finishes within `WORKER_SOFT_SHUTDOWN_TIMEOUT`, else requeued at once | `shutdown` (warm + grace timer + fallback requeue) |
| Run restarted meanwhile | leftover work refused, job stops early | `attempt` + `GET runs/{id}` (`superseded`) |
| Copy of a job already reported | does nothing | `RunState.skip_reason` (`already_done`) |
| Message stuck unacked | restored after `BROKER_VISIBILITY_TIMEOUT` | **one value for every consumer of a Redis db: 12 h** |

The last row is a rule, not a default: kombu restores *every* unacked message older than the restoring consumer's own
timeout, in any queue (EvilFlowersCatalog#120, text#9, lcpencrypt#6).

## 4. Observability contract

All of it lives in `evilflowers_worker.observability`, configured by service name. It follows the cluster contract in
knowledge `Cookbooks/Deploy-a-Service` §3.

- **Resource:** `service.name` (`OTEL_SERVICE_NAME`), `service.namespace=elvira`, `service.version=IMAGE_TAG`,
  `service.instance.id`. k8s attributes come from Alloy.
- **Traces:**
  - a CONSUMER span per task, continuing the producer's `traceparent`;
  - PRODUCER spans per command sent;
  - CLIENT spans per catalog call;
  - hand-made spans `<service>.<area>[.<step>]`.
- **Canonical attributes:** they are set by `bind()`, on the span and on every log line.

  | Log field | Span attribute |
  |---|---|
  | `run_id` | `evilflowers.extraction.run_id` |
  | `job` | `evilflowers.extraction.job` |
  | `attempt` | `evilflowers.extraction.attempt` |
  | `pipeline_version` | `evilflowers.extraction.pipeline_version` |
  | `delivery` | `evilflowers.extraction.delivery` |
  | `extract_pass` | `evilflowers.extraction.pass` (analyzer, kept for its dashboards) |
  | `acquisition_id` | `evilflowers.acquisition_id` |
  | `entry_id` | `evilflowers.entry_id` |
  | `source` | `evilflowers.source` |
  | `task_id` | `messaging.message.id` |
- **Standard metrics:** every service gets these from the kit; service-specific ones sit next to them under the same
  prefix.
  - `<service>_task_duration_seconds{celery_task_name,celery_task_state}`
  - `<service>_job_outcome_total{job,outcome}`. Here `job` is the kind, never a batch name.
  - `<service>_catalog_retries_total{route,reason}`
  - `<service>_deliveries_capped_total`
- **Logs:** one JSON object per line on stdout, carrying `time`, `level`, `logger`, `message`, `trace_id`, `span_id`,
  `task_name`, `task_id`, the bound fields and `exception`.
- **Profiles:** the Pyroscope SDK when `PYROSCOPE_SERVER_ADDRESS` is set, with `gil_only=False`, and spans linked to
  profiles.
- **Everything is off** without `OTEL_EXPORTER_OTLP_ENDPOINT` / `HOST_IP`, so Compose and tests need nothing.

## 5. The kit

```
src/evilflowers_worker/
├── settings.py        WorkerSettings: broker, catalog, storage, shutdown, delivery cap, logs (services subclass it)
├── contract.py        message models + catalog payload models (Box, LayoutRegion, Region, JobReport, RunState, …)
├── catalog.py         CatalogClient for /extraction/v1, RunNotRunning / RunSuperseded / DuplicateResults
├── http.py            requests session (User-Agent, bearer key)
├── deliveries.py      DeliveryCounter (Redis): count, cap, forget on a deliberate requeue
├── jobs.py            LeafJob: check → extract → publish → report, outcomes, invalid-message report
├── runtime.py         create_app(), run_job(): Celery config, queues, telemetry, shutdown; Publisher
├── shutdown.py        SIGTERM: warm shutdown, grace timer, cold cancel, fallback requeue
├── testing.py         FakeCatalog, RecordingPublisher, StubCatalogServer, WorkerProcess
└── observability/     otel setup, spans, metrics (Instrument), JSON logs, bind(), Pyroscope, Celery signals
```

It has no docling, torch or model dependency. A service pins it to a commit:

```toml
[tool.uv.sources]
evilflowers-worker = { git = "https://github.com/EvilFlowersCatalog/evilflowers-worker", rev = "<sha>" }
```

A fix here reaches a service when it bumps `rev` (and runs `uv lock`); nothing changes under a service that does not.

### A new service, step by step

1. `settings.py`: `class Settings(WorkerSettings)` with the domain's own fields.
2. `jobs.py`: `class FiguresJob(LeafJob[RegionsJob, list[Region]])` with `kind = "figures"`, `extract()` and
   `publish()`.
3. `worker.py`: `app = runtime.create_app("image", settings, include=[...], queues=[settings.queue])`.
4. `tasks.py`: `@shared_task(name=..., bind=True, **runtime.JOB_TASK_OPTIONS)` whose body is
   `return runtime.run_job(self, FiguresJob, RegionsJob, kwargs)`. That validates the message (an invalid one is
   reported failed by `jobs.report_invalid_message`), counts the delivery and runs the job. The module imports the one
   that calls `create_app`: a pool child started by spawn imports only the tasks module.
5. Tests: the job against `testing.FakeCatalog`; the real worker through `testing.WorkerProcess` against Redis and
   `testing.StubCatalogServer` (`tests/test_runtime.py` here, the analyzer's `tests/test_end_to_end.py` for a whole
   run).

## 6. Roadmap

| Step | Where |
|---|---|
| Kit | this repository (`master` @ `ae8e238`) |
| Analyzer on the kit | evilflowers-analyzer-service (pinned to `ae8e238`) |
| text, image, equation adopt the kit | text-service#16, #17 (+ #15 `outline`), image-service#5, equation-service#7 |
| Housekeeping: README, tooling, CI with Redis | evilflowers-worker#1 |
| `reason` on every 409 (`duplicate`); refuse undeclared job names; `//` in the OpenAPI paths | EvilFlowersCatalog#180 |
| Task messages as AsyncAPI next to the OpenAPI spec | evilflowers-protocol#1 |
| Telemetry, settings and manifest changes; worker liveness without a port; the denied kill on `control` | knowledge#56 |
| Analyzer OOM at 6Gi | knowledge#55 |
