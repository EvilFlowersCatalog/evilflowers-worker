import json
import pickle

import pytest
import requests
from pydantic import SecretStr

from evilflowers_worker import catalog as catalog_module
from evilflowers_worker.catalog import CatalogClient, CatalogRefused, RunNotRunning, RunSuperseded
from evilflowers_worker.contract import MAX_ERROR_LENGTH, JobReport, Paragraph, ParagraphLink, Region, StartRun
from evilflowers_worker.http import session, user_agent

BOX = {"x0": 0.1, "y0": 0.2, "x1": 0.3, "y1": 0.4}


class Response:
    def __init__(self, status_code: int, body: bytes | dict = b"{}"):
        self.status_code = status_code
        self.content = json.dumps(body).encode() if isinstance(body, dict) else body
        self.text = self.content.decode()

    def json(self):
        return json.loads(self.content or b"{}")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class FakeHttp:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []

    def request(self, method, url, json=None, params=None, timeout=None):
        self.calls.append((method, url, json, params))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(catalog_module.time, "sleep", lambda seconds: None)


def client(settings, *answers, attempt=None) -> tuple[CatalogClient, FakeHttp]:
    http = FakeHttp(*answers)
    return CatalogClient("run-1", settings=settings, attempt=attempt, http=http), http


def test_start_posts_to_the_run_and_learns_the_attempt(settings):
    catalog, http = client(settings, Response(200, {"response": {"status": "running", "attempt": 4}}), Response(200))
    state = catalog.start(StartRun(page_count=1, page_sizes=[[595.0, 842.0]], jobs=["paragraphs"]))
    method, url, payload, params = http.calls[0]
    assert (method, url, params) == ("POST", "http://catalog:8000/extraction/v1/runs/run-1/start", None)
    assert payload == {"page_count": 1, "page_sizes": [[595.0, 842.0]], "jobs": ["paragraphs"], "provenance": {}}
    assert state.attempt == 4

    # Every write after it carries the attempt.
    catalog.job("figures", JobReport(status="done"))
    assert http.calls[1][3] == {"attempt": 4}


def test_a_catalog_without_attempts(settings):
    catalog, http = client(settings, Response(200, {"response": {"status": "running"}}), Response(200, b""))
    assert catalog.start(StartRun(page_count=1, page_sizes=[[1, 1]], jobs=[])).attempt is None
    catalog.job("analyzer", JobReport(status="failed"))
    assert http.calls[1][3] is None


def test_reads_the_run_without_an_attempt_parameter(settings):
    body = {"response": {"status": "running", "jobs": {"figures": "pending"}, "attempt": 2, "storage_dir": "x"}}
    catalog, http = client(settings, Response(200, body), attempt=2)
    state = catalog.run()
    assert (state.status, state.jobs, state.attempt) == ("running", {"figures": "pending"}, 2)
    assert http.calls[0][:2] == ("GET", "http://catalog:8000/extraction/v1/runs/run-1") and http.calls[0][3] is None


def test_results_go_in_batches_with_the_attempt(settings):
    small = settings.model_copy(update={"catalog_batch_size": 2})
    regions = [Region(kind="figure", page=1, seq=n, bbox=BOX) for n in range(3)]
    catalog, http = client(small, Response(201), Response(201), attempt=5)
    catalog.regions(regions)
    assert [len(call[2]["regions"]) for call in http.calls] == [2, 1]
    assert all(call[3] == {"attempt": 5} for call in http.calls)
    assert http.calls[0][2]["regions"][0] == {"kind": "figure", "page": 1, "seq": 0, "bbox": BOX, "provenance": {}}


def test_paragraph_links_go_with_the_last_batch(settings):
    small = settings.model_copy(update={"catalog_batch_size": 2})
    paragraphs = [Paragraph(page=1, seq=n, bbox=BOX, text="t") for n in range(3)]
    links = [ParagraphLink(source=0, target=1, kind="next")]
    catalog, http = client(small, Response(201), Response(201))
    catalog.paragraphs(paragraphs, links)
    assert ["links" in call[2] for call in http.calls] == [False, True]


def test_a_failed_job_carries_a_truncated_error(settings):
    catalog, http = client(settings, Response(200, b""))
    catalog.job("analyzer", JobReport(status="failed", error="x" * 5000))
    _method, url, payload, _params = http.calls[0]
    assert url.endswith("/jobs/analyzer")
    assert payload["status"] == "failed" and len(payload["error"]) == 2000


def test_5xx_and_connection_errors_are_retried(settings):
    catalog, http = client(settings, Response(503), Response(200))
    catalog.job("analyzer", JobReport(status="failed"))
    assert len(http.calls) == 2

    catalog, http = client(settings, requests.ConnectionError("down"), Response(200))
    catalog.job("analyzer", JobReport(status="failed"))
    assert len(http.calls) == 2


def test_the_last_attempt_raises(settings):
    catalog, _http = client(settings, Response(503), Response(503))
    with pytest.raises(requests.HTTPError):
        catalog.job("analyzer", JobReport(status="failed"))

    catalog, _http = client(settings, requests.ConnectionError("down"), requests.ConnectionError("down"))
    with pytest.raises(requests.ConnectionError):
        catalog.job("analyzer", JobReport(status="failed"))


@pytest.mark.parametrize(
    ("body", "error"),
    [
        (b"finished", RunNotRunning),
        ({"title": "not running", "reason": "not_running"}, RunNotRunning),
        # A step re-run's /start refused (EvilFlowersCatalog#183): the analyzer relies on RunNotRunning here.
        ({"title": "steps refused", "reason": "steps_refused"}, RunNotRunning),
        ({"title": "started again", "reason": "superseded", "attempt": 4}, RunSuperseded),
        ({"title": "started again", "additional_data": {"reason": "superseded"}}, RunSuperseded),
    ],
)
def test_409_is_never_retried_and_tells_a_superseded_attempt_apart(settings, body, error):
    catalog, http = client(settings, Response(409, body))
    with pytest.raises(error) as raised:
        catalog.job("figures", JobReport(status="done"))
    assert len(http.calls) == 1
    if error is RunNotRunning:
        assert not isinstance(raised.value, RunSuperseded)


def test_a_duplicate_is_a_failure_to_report_not_a_refusal(settings):
    from evilflowers_worker.catalog import DuplicateResults

    catalog, http = client(settings, Response(409, {"title": "already holds", "reason": "duplicate"}))
    with pytest.raises(DuplicateResults):
        catalog.paragraphs([Paragraph(page=1, seq=0, bbox=BOX, text="t")])
    assert len(http.calls) == 1
    assert not issubclass(DuplicateResults, RunNotRunning)


def test_4xx_is_not_retried(settings):
    catalog, http = client(settings, Response(400))
    with pytest.raises(requests.HTTPError):
        catalog.start(StartRun(page_count=1, page_sizes=[[1, 1]], jobs=[]))
    assert len(http.calls) == 1


PROBLEM = {
    "type": "/errors/validation",
    "title": "Invalid ParagraphsForm",
    "status": 422,
    "errors": {"paragraphs.37.embedding": ["expected 1024 dimensions, got 768"]},
}


def test_a_refusal_carries_the_catalogs_body_into_the_exception_and_the_log(settings, caplog):
    small = settings.model_copy(update={"catalog_batch_size": 100})
    paragraphs = [Paragraph(page=1, seq=n, bbox=BOX, text=f"secret paragraph {n}") for n in range(250)]
    catalog, http = client(small, Response(201), Response(422, PROBLEM), attempt=4)

    with caplog.at_level("ERROR", logger="evilflowers_worker.catalog"), pytest.raises(CatalogRefused) as raised:
        catalog.paragraphs(paragraphs)

    error = raised.value
    assert len(http.calls) == 2  # the second batch, not retried
    assert isinstance(error, requests.HTTPError) and error.status == 422
    assert str(error).startswith(
        "Catalog refused POST /paragraphs (run run-1, attempt 4, batch 2/3, seq 100-199): 422 "
    )
    assert "expected 1024 dimensions, got 768" in str(error) and "paragraphs.37.embedding" in str(error)

    [record] = [record for record in caplog.records if record.levelname == "ERROR"]
    assert record.getMessage() == str(error)
    # What was sent never reaches the log.
    assert "secret paragraph" not in caplog.text


def test_a_refusal_fits_the_reported_error_whole(settings):
    catalog, _http = client(settings, Response(400, {"detail": "x" * 10_000}))
    with pytest.raises(CatalogRefused) as raised:
        catalog.regions([Region(kind="figure", page=1, seq=7, bbox=BOX)])
    error = raised.value
    assert len(error.body) == catalog_module.MAX_BODY_LENGTH and error.body.endswith("…")
    assert "(run run-1, attempt None, batch 1/1, seq 7-7): 400 " in str(error)
    # What `LeafJob` reports as the job's error, within the catalog's limit.
    assert len(f"{type(error).__name__}: {error}") <= MAX_ERROR_LENGTH


def test_a_refusal_without_json_keeps_the_text(settings):
    catalog, _http = client(settings, Response(403, b"<html>\n  Forbidden\n</html>"))
    with pytest.raises(
        CatalogRefused,
        match=r"^Catalog refused POST /jobs/figures \(run run-1, attempt None\): 403 <html> Forbidden </html>$",
    ):
        catalog.job("figures", JobReport(status="done"))


def test_a_refusal_survives_pickling():
    error = pickle.loads(pickle.dumps(CatalogRefused("Catalog refused POST /regions: 422 {}", status=422, body="{}")))
    assert (str(error), error.status, error.body) == ("Catalog refused POST /regions: 422 {}", 422, "{}")


def test_the_last_5xx_carries_its_body_too(settings):
    catalog, _http = client(settings, Response(503), Response(503, {"title": "maintenance"}))
    with pytest.raises(CatalogRefused, match=r"503 .*maintenance"):
        catalog.run()


def test_retry_metric_routes_have_bounded_cardinality():
    assert catalog_module._route("/jobs/equations:0001-0008") == "/jobs"
    assert catalog_module._route("") == "/"
    assert catalog_module._route("/start") == "/start"


def test_session_sends_the_key_only_when_set():
    assert session(SecretStr("token")).headers["Authorization"] == "Bearer token"
    assert "Authorization" not in session(SecretStr("")).headers
    assert "Authorization" not in session(None).headers
    assert session().headers["User-Agent"] == user_agent()
