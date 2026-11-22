# Changelog

All notable changes to the kit. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
versions follow the release rule in the README (*Releases*): while the kit is `0.x`, a breaking change to a public API
bumps the minor version and anything else is a patch. Each entry names the issue it comes from.

## [Unreleased]

## [0.1.0] - 2026-11-22

The first release. Everything the extraction services, the catalog jobs and the standalone jobs use today.

### Added

- The message contract (`contract`): `ExtractRequest`, `ParagraphsJob`, `RegionsJob`, the catalog payloads `Region`,
  `Paragraph`, `JobReport`, `RunState`, `StartRun`, and the job-name rules. The design is in `docs/architecture.md`,
  moved here from evilflowers-analyzer-service (#2).
- `CatalogClient` for the catalog's ingestion API, the `LeafJob` base with the delivery cap and the terminal report,
  `runtime.create_app()` / `run_job()`, the Redis `DeliveryCounter`, SIGTERM handling (warm shutdown, grace timer,
  cold cancel, fallback requeue) and the observability setup (OTLP, JSON logs, Pyroscope), extracted from
  evilflowers-analyzer-service. CI runs lint, mypy and the tests against Redis on Python 3.12 and 3.14 (#1).
- Catalog jobs (`files.ocr`, `readium.encrypt`): `OutcomeFile`, `atomic_write`, `write_outcome`, `RuntimeSettings`
  without the catalog client, `runtime.run_task()` with `on_capped`, `CATALOG_JOB_ATTRIBUTES`. Python 3.12 to 3.14
  (#3).
- `CatalogRefused`: a 4xx from the catalog carries the response body (#4).
- Standalone jobs: `StandaloneJob` (`input_paths()`, `prepare_output()`, `output_file()`) and
  `runtime.standalone_callbacks()` for an orchestrator's Celery `link` / `link_error` (#5).
- The `judges` extra (`evilflowers_worker.judges`): clients of decider, decider-vision, Ollama and tei-rerank,
  `Choice` / `Binary` questions, verdicts with a band, a budget per job and a circuit breaker per client;
  `JudgeSettings` (#6).
- Decision fields on `contract.Region`: `band`, `p`, `score`, `decision`, `decider_model`, `signals` (#7).
- Step re-run: `ExtractRequest.steps` and `StartRun.steps` (#9).
- `Panel.ask_batch()`: one question about up to 64 states in one request to decider, and the decision fields
  `decision`, `p`, `decider_model` on `contract.Paragraph` (#10).
- The heartbeat file for the k8s exec liveness probe (`WORKER_HEARTBEAT_FILE`, `WORKER_HEARTBEAT_INTERVAL`) and the
  `evilflowers-worker-alive --max-age` script (#11).
- The release rule (README, *Releases*) and this changelog (#13).

### Fixed

- `JudgeSettings`: an empty `JUDGE_THRESHOLDS=` no longer stops the worker at startup (#8).

[Unreleased]: https://github.com/EvilFlowersCatalog/evilflowers-worker/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/EvilFlowersCatalog/evilflowers-worker/releases/tag/v0.1.0
