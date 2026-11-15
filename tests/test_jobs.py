"""The leaf job lifecycle: check → extract → publish → report (evilflowers_worker.jobs)."""

import pytest

from evilflowers_worker.catalog import RunNotRunning, RunSuperseded
from evilflowers_worker.contract import RegionsJob
from evilflowers_worker.testing import FakeCatalog, RefusingCatalog, running

from .example import EchoFigures

BOX = {"x0": 0.1, "y0": 0.2, "x1": 0.3, "y1": 0.4}
PENDING = {"figures": "pending", "figures:0001-0008": "pending"}


def message(**overrides) -> RegionsJob:
    fields = {
        "run_id": "run-1",
        "source": "a.pdf",
        "output_dir": "out",
        "attempt": 2,
        "regions": [{"page": 1, "seq": 0, "bbox": BOX}],
    }
    return RegionsJob(**{**fields, **overrides})


def job(settings, catalog, **kwargs) -> EchoFigures:
    return EchoFigures(kwargs.pop("message", message()), settings=settings, catalog=catalog, **kwargs)


def test_a_job_does_its_work_and_reports_done(settings):
    catalog = FakeCatalog(states=[running(jobs=PENDING, attempt=2)])
    assert job(settings, catalog).run() == "done"
    assert catalog.regions_posted == [{"kind": "figure", "page": 1, "seq": 0, "bbox": BOX, "provenance": {}}]
    assert [(r["name"], r["status"]) for r in catalog.jobs] == [("figures", "done")]
    assert catalog.jobs[0]["provenance"] == {"image": "test", "extractor": "echo"}


def test_a_batch_job_reports_under_its_own_name(settings):
    catalog = FakeCatalog(states=[running(jobs=PENDING, attempt=2)])
    assert job(settings, catalog, message=message(job="figures:0001-0008")).run() == "done"
    assert catalog.jobs[0]["name"] == "figures:0001-0008"


@pytest.mark.parametrize(
    ("state", "outcome"),
    [
        (running(jobs={"figures": "done"}, attempt=2), "already_done"),
        (running(jobs=PENDING, attempt=2, status="done"), "not_running"),
        (running(jobs=PENDING, attempt=3), "superseded"),
    ],
)
def test_a_copy_or_a_stale_job_stops_before_the_work(settings, state, outcome):
    catalog = FakeCatalog(states=[state])
    assert job(settings, catalog).run() == outcome
    assert catalog.regions_posted == []
    # Only a superseded job says so (the catalog counts them); the others report nothing.
    assert [r["status"] for r in catalog.jobs] == (["superseded"] if outcome == "superseded" else [])


def test_a_long_job_stops_when_the_run_moves_on(settings):
    now = [0.0]
    catalog = FakeCatalog(states=[running(jobs=PENDING, attempt=2), running(jobs=PENDING, attempt=3)])
    regions = [{"page": 1, "seq": n, "bbox": BOX} for n in range(250)]

    def clock() -> float:
        now[0] += 100
        return now[0]

    leaf = job(settings, catalog, message=message(regions=regions), recheck_interval=60, clock=clock)
    assert leaf.run() == "superseded"
    assert catalog.reads == 2 and catalog.regions_posted == []


def test_a_failure_is_reported(settings):
    catalog = FakeCatalog(states=[running(jobs=PENDING, attempt=2)], fail_on={"regions": ValueError("bad box")})
    assert job(settings, catalog).run() == "failed"
    assert catalog.jobs[0]["status"] == "failed"
    assert catalog.jobs[0]["error"] == "ValueError: bad box"


def test_a_refused_write_drops_the_job_quietly(settings):
    catalog = RefusingCatalog(states=[running(jobs=PENDING, attempt=2)])
    assert job(settings, catalog).run() == "not_running"


def test_a_superseded_write_stops_the_job(settings):
    catalog = RefusingCatalog(RunSuperseded("attempt 3"), states=[running(jobs=PENDING, attempt=2)])
    assert job(settings, catalog).run() == "superseded"


def test_past_the_delivery_cap_the_job_is_failed_without_running(settings):
    catalog = FakeCatalog(states=[running(jobs=PENDING, attempt=2)])
    outcome = job(settings, catalog, delivery=settings.max_deliveries + 1).run()
    assert outcome == "failed"
    assert catalog.reads == 0 and catalog.regions_posted == []
    assert catalog.jobs[0]["error"].startswith(f"Gave up after {settings.max_deliveries} deliveries")


def test_a_lost_worker_is_reported_without_running(settings):
    catalog = FakeCatalog()
    assert job(settings, catalog).report_failure(RuntimeError("SIGKILL")) == "failed"
    assert catalog.reads == 0 and catalog.jobs[0]["status"] == "failed"


@pytest.mark.parametrize(
    ("job_name", "reported_as"),
    [
        ("figures:0001-0008", "figures:0001-0008"),  # a valid batch: its own name
        ("figures:0001-0008-and-much-more-than-30", "figures"),  # the invalid part: never a new, undeclared job
        ("Figures", "figures"),
        (42, "figures"),
    ],
)
def test_an_invalid_message_is_reported_under_a_name_the_run_declared(settings, monkeypatch, job_name, reported_as):
    from pydantic import ValidationError

    from evilflowers_worker import jobs

    catalog = FakeCatalog()
    monkeypatch.setattr(jobs, "CatalogClient", lambda run_id, settings, attempt: catalog)
    kwargs = {"run_id": "run-1", "attempt": 2, "job": job_name, "regions": [{"page": 0}]}
    with pytest.raises(ValidationError) as invalid:
        RegionsJob.model_validate(kwargs)

    assert jobs.report_invalid_message(kwargs, invalid.value, job="figures", settings=settings) == "failed"
    assert [(r["name"], r["status"]) for r in catalog.jobs] == [(reported_as, "failed")]


def test_an_invalid_message_without_a_run_is_only_logged(settings, monkeypatch):
    from pydantic import ValidationError

    from evilflowers_worker import jobs

    monkeypatch.setattr(jobs, "CatalogClient", lambda *a, **k: pytest.fail("nothing to report to"))
    with pytest.raises(ValidationError) as invalid:
        RegionsJob.model_validate({})
    assert jobs.report_invalid_message({}, invalid.value, job="figures", settings=settings) == "failed"


def test_a_report_the_catalog_refuses_never_raises(settings):
    catalog = FakeCatalog(
        states=[running(jobs=PENDING, attempt=2)],
        fail_on={"regions": ValueError("bad"), "job": RunNotRunning("finished")},
    )
    assert job(settings, catalog).run() == "failed"
