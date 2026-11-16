import base64
import json
from typing import Any

import pytest
import requests
from pydantic import ValidationError

from evilflowers_worker.contract import Paragraph, Region
from evilflowers_worker.judges import (
    Binary,
    Budget,
    Choice,
    CircuitBreaker,
    DeciderClient,
    JudgeInvalid,
    JudgeRefused,
    Judges,
    JudgeSettings,
    JudgeUnavailable,
    OllamaClient,
    Panel,
    RerankClient,
    Thresholds,
    Verdict,
    panel,
)
from evilflowers_worker.settings import RuntimeSettings, WorkerSettings

FIGURE_KIND = Choice(
    id="figure_kind",
    instructions="What is this image?",
    criteria={"chart": "chart", "photo": "photo", "stamp": "stamp, signature or logo", "blank": "blank"},
    reject={"stamp", "blank"},
)
IS_FIGURE = Binary(id="is_figure", instructions="Is this a real figure?")


def response(status: int = 200, body: Any = None, *, text: str | None = None) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = (text if text is not None else json.dumps(body)).encode()
    return r


class FakeHttp:
    """A requests.Session stand-in: records each POST and returns (or raises) the next queued answer."""

    def __init__(self, *answers: requests.Response | Exception):
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, *, json: Any, timeout: float) -> requests.Response:
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def decider(*answers: requests.Response | Exception, vision: bool = True, **kwargs: Any) -> DeciderClient:
    return DeciderClient(
        "decider-vision" if vision else "decider",
        "http://decider-vision.shared.svc.cluster.local",
        vision=vision,
        timeout=60.0,
        http=FakeHttp(*answers),  # type: ignore[arg-type]
        **kwargs,
    )


def choice_answer(choice: str = "stamp", p: float = 0.93) -> requests.Response:
    probabilities = {"chart": 0.02, "photo": 0.03, "stamp": 0.02, "blank": 0.02} | {choice: p}
    return response(
        body={"answers": {"figure_kind": {"choice": choice, "probabilities": probabilities, "confidence": p}},
              "model": "strands-decider-2B-qwen3.5-v1-2610"}
    )  # fmt: skip


@pytest.fixture
def counted(monkeypatch):
    """Every `<service>_judge_total` increment, as (attributes)."""
    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(panel.JUDGE_CALLS, "add", lambda value=1, attributes=None: seen.append(attributes or {}))
    return seen


# -- Questions and verdicts ----------------------------------------------------------------------------------------


def test_a_choice_refuses_a_reject_that_is_not_a_criterion():
    with pytest.raises(ValueError, match="not criteria"):
        Choice(id="kind", instructions="?", criteria={"a": "a", "b": "b"}, reject={"c"})


@pytest.mark.parametrize(
    ("verdict", "p", "band"),
    [("chart", 0.85, "keep"), ("chart", 0.6, "review"), ("stamp", 0.95, "hide"), ("stamp", 0.7, "review")],
)
def test_bands_follow_the_thresholds_and_the_rejecting_answers(verdict, p, band):
    thresholds = Thresholds(keep=0.8, hide=0.9)
    answer = Verdict(question="figure_kind", verdict=verdict, p=p, model="m")
    assert thresholds.band(FIGURE_KIND, answer) == band


def test_a_verdict_is_stored_as_provenance_keyed_by_its_question():
    verdict = Verdict(question="figure_kind", verdict="stamp", p=0.9, model="strands", revision="216d1ccb",
                      band="hide")  # fmt: skip
    assert verdict.model_ref == "strands@216d1ccb"
    region = Region(kind="figure", page=1, seq=0, bbox={"x0": 0, "y0": 0, "x1": 1, "y1": 1})
    region = Region.model_validate(region.model_dump() | verdict.region_fields())
    assert (region.band, region.p, region.decision, region.decider_model) == ("hide", 0.9, "stamp", "strands@216d1ccb")
    assert verdict.provenance() == {
        "figure_kind": {"question": "figure_kind", "verdict": "stamp", "p": 0.9, "model": "strands",
                        "revision": "216d1ccb", "band": "hide"}
    }  # fmt: skip


# -- decider / decider-vision ---------------------------------------------------------------------------------------


def test_decider_vision_sends_keyed_questions_and_base64_images():
    client = decider(choice_answer(), revision="216d1ccb")
    verdict = client.ask(FIGURE_KIND, state="A region cropped from a page.", images=[b"\x89PNG"], timeout=5.0)

    call = client.http.calls[0]  # type: ignore[attr-defined]
    assert call["url"] == "http://decider-vision.shared.svc.cluster.local/v1/systemone"
    assert call["timeout"] == 5.0
    assert call["json"]["images"] == [base64.b64encode(b"\x89PNG").decode()]
    assert call["json"]["questions"]["figure_kind"] == {
        "type": "choice", "instructions": "What is this image?", "criteria": dict(FIGURE_KIND.criteria)
    }  # fmt: skip
    model = "strands-decider-2B-qwen3.5-v1-2610"
    assert verdict == Verdict(question="figure_kind", verdict="stamp", p=0.93, model=model, revision="216d1ccb")


def test_the_text_decider_pins_lays_checkpoint_and_asks_binary_as_noul():
    client = decider(response(body={"answers": {"is_figure": {"noul": 0.2}}, "model": "laya"}), vision=False,
                     model="multilingual")  # fmt: skip
    verdict = client.ask(IS_FIGURE, state={"body": "Obr. 3.2"})
    sent = client.http.calls[0]["json"]  # type: ignore[attr-defined]
    assert sent["model"] == "multilingual" and sent["state"] == {"body": "Obr. 3.2"}
    assert sent["questions"]["is_figure"]["type"] == "noul"
    assert (verdict.verdict, verdict.p) == ("no", pytest.approx(0.8))


def test_the_text_decider_refuses_images_before_any_request():
    client = decider(vision=False)
    with pytest.raises(ValueError, match="text only"):
        client.ask(FIGURE_KIND, images=[b"png"])
    assert client.http.calls == []  # type: ignore[attr-defined]


def test_decider_vision_takes_at_most_four_images():
    with pytest.raises(ValueError, match="at most 4"):
        decider().ask(FIGURE_KIND, images=[b"x"] * 5)


@pytest.mark.parametrize("url", ["http://d", "http://d/", "http://d/v1/systemone"])
def test_the_url_may_be_the_address_or_the_endpoint(url):
    assert DeciderClient("d", url, vision=False, timeout=1).url == "http://d"


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (requests.ConnectTimeout("slow"), JudgeUnavailable),
        (requests.ConnectionError("refused"), JudgeUnavailable),
        (response(503, {}), JudgeUnavailable),
        (response(429, {}), JudgeUnavailable),
        (response(422, {"detail": "image state"}), JudgeRefused),
        (response(200, text="<html>"), JudgeInvalid),
        (response(200, {"answers": {}}), JudgeInvalid),
        (response(200, {"answers": {"figure_kind": {"choice": "sticker", "confidence": 0.9}}}), JudgeInvalid),
        (response(200, {"answers": {"figure_kind": {"choice": "stamp", "confidence": 7}}}), JudgeInvalid),
    ],
)
def test_a_failure_is_one_of_three_judge_errors(answer, error):
    with pytest.raises(error):
        decider(answer).ask(FIGURE_KIND)


# -- Ollama and tei-rerank ------------------------------------------------------------------------------------------


def test_ollama_asks_for_a_structured_answer_without_thinking():
    content = json.dumps({"verdict": "yes", "p": 0.7, "reason": "a labelled chart"})
    http = FakeHttp(response(body={"model": "elvira-assistant", "message": {"content": content}}))
    client = OllamaClient("ollama", "http://ollama/api/chat", model="elvira-assistant", timeout=120, http=http)  # type: ignore[arg-type]

    verdict = client.ask(IS_FIGURE, state="crop", images=["aGk="])

    sent = http.calls[0]["json"]
    assert http.calls[0]["url"] == "http://ollama/api/chat"
    assert sent["think"] is False and sent["stream"] is False
    assert sent["format"]["properties"]["verdict"]["enum"] == ["yes", "no"]
    assert sent["messages"][1]["images"] == ["aGk="]
    assert (verdict.verdict, verdict.p, verdict.reason) == ("yes", 0.7, "a labelled chart")


def test_rerank_picks_the_best_scoring_candidate():
    captions = Choice(id="caption", instructions="Which caption?", criteria={"c1": "Obr. 1 Mapa", "c2": "Tab. 2"})
    http = FakeHttp(response(body=[{"index": 1, "score": 0.002}, {"index": 0, "score": 0.91}]))
    client = RerankClient("tei-rerank", "http://tei-rerank", timeout=10, http=http)  # type: ignore[arg-type]

    verdict = client.ask(captions, state="a map of Bratislava")

    assert http.calls[0]["json"] == {"query": "a map of Bratislava", "texts": ["Obr. 1 Mapa", "Tab. 2"],
                                     "raw_scores": False}  # fmt: skip
    assert (verdict.verdict, verdict.p, verdict.model) == ("c1", 0.91, "tei-rerank")


def test_rerank_asks_choice_questions_only():
    with pytest.raises(ValueError, match="Choice"):
        RerankClient("tei-rerank", "http://r", timeout=1).ask(IS_FIGURE, state="q")


# -- Circuit breaker ------------------------------------------------------------------------------------------------


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_the_breaker_opens_after_consecutive_failures_and_lets_one_trial_through():
    clock = Clock()
    breaker = CircuitBreaker(failures=2, reset=30, clock=clock)
    breaker.failure()
    assert breaker.allow()
    breaker.failure()
    assert breaker.state == "open" and not breaker.allow()

    clock.now = 30
    assert breaker.allow()  # the trial
    assert not breaker.allow()  # only one at a time
    breaker.failure()
    assert breaker.state == "open" and not breaker.allow()

    clock.now = 60
    assert breaker.allow()
    breaker.success()
    assert breaker.state == "closed" and breaker.allow()


# -- Panel: a judge never fails the job ------------------------------------------------------------------------------


def test_the_panel_bands_a_verdict_and_counts_it(counted):
    p = Panel(Budget(calls=10, seconds=60), {"figure_kind": Thresholds(keep=0.8, hide=0.9)})
    verdict = p.ask(decider(choice_answer("stamp", 0.95)), FIGURE_KIND, images=[b"png"])
    assert verdict is not None and verdict.band == "hide"
    assert counted == [{"question": "figure_kind", "outcome": "judged", "band": "hide"}]


def test_a_question_without_thresholds_gets_no_band(counted):
    verdict = Panel(Budget(10, 60)).ask(decider(choice_answer("chart", 0.99)), FIGURE_KIND)
    assert verdict is not None and verdict.band is None
    assert counted[0]["band"] == "none"


@pytest.mark.parametrize(
    ("answer", "outcome"),
    [
        (requests.ReadTimeout("slow"), "unreachable"),
        (response(500, {}), "unreachable"),
        (response(422, {}), "refused"),
        (response(200, {"answers": {}}), "invalid"),
    ],
)
def test_a_failed_call_passes_the_proposal_through_unjudged(counted, answer, outcome):
    assert Panel(Budget(10, 60)).ask(decider(answer), FIGURE_KIND) is None
    assert counted == [{"question": "figure_kind", "outcome": outcome, "band": "none"}]


def test_no_client_means_disabled(counted):
    assert Panel(Budget(10, 60)).ask(None, FIGURE_KIND) is None
    assert counted[0]["outcome"] == "disabled"


def test_an_open_breaker_skips_the_call(counted):
    client = decider(requests.ConnectionError("down"), requests.ConnectionError("down"))
    client.breaker = CircuitBreaker(failures=2, reset=60)
    p = Panel(Budget(10, 60))
    assert p.ask(client, FIGURE_KIND) is None
    assert p.ask(client, FIGURE_KIND) is None
    assert p.ask(client, FIGURE_KIND) is None
    assert len(client.http.calls) == 2  # type: ignore[attr-defined]
    assert [c["outcome"] for c in counted] == ["unreachable", "unreachable", "open"]


def test_a_refusal_does_not_trip_the_breaker():
    client = decider(*[response(422, {})] * 3)
    client.breaker = CircuitBreaker(failures=1, reset=60)
    p = Panel(Budget(10, 60))
    for _ in range(3):
        p.ask(client, FIGURE_KIND)
    assert client.breaker.state == "closed" and len(client.http.calls) == 3  # type: ignore[attr-defined]


def test_the_budget_caps_the_calls_of_one_job(counted):
    client = decider(choice_answer(), choice_answer(), choice_answer())
    p = Panel(Budget(calls=2, seconds=0))
    assert p.ask(client, FIGURE_KIND) is not None
    assert p.ask(client, FIGURE_KIND) is not None
    assert p.ask(client, FIGURE_KIND) is None
    assert counted[-1]["outcome"] == "budget"


def test_a_call_waits_no_longer_than_the_seconds_left_in_the_budget():
    client = decider(choice_answer())
    Panel(Budget(calls=0, seconds=60, used_seconds=55)).ask(client, FIGURE_KIND)
    assert client.http.calls[0]["timeout"] == pytest.approx(5.0)  # type: ignore[attr-defined]


def test_a_spent_seconds_budget_stops_the_calls(counted):
    assert Panel(Budget(calls=0, seconds=60, used_seconds=60)).ask(decider(), FIGURE_KIND) is None
    assert counted[0]["outcome"] == "budget"


def test_a_callers_bug_is_not_swallowed():
    with pytest.raises(ValueError):
        Panel(Budget(10, 60)).ask(decider(vision=False), FIGURE_KIND, images=[b"png"])


def test_a_callers_bug_during_a_half_open_trial_does_not_keep_the_breaker_open():
    clock = Clock()
    client = decider(vision=False)
    client.breaker = CircuitBreaker(failures=1, reset=30, clock=clock)
    client.breaker.failure()
    clock.now = 30
    with pytest.raises(ValueError):
        Panel(Budget(10, 60)).ask(client, FIGURE_KIND, images=[b"png"])  # takes the trial, then raises
    assert client.breaker.allow()


@pytest.mark.parametrize(
    "answer",
    [
        {"answers": {"figure_kind": {"choice": {"label": "stamp"}, "confidence": 0.9}}},
        {"answers": {"figure_kind": {"choice": ["stamp"], "confidence": 0.9}}},
        {"answers": {"is_figure": {"noul": {"yes": 0.9}}}},
    ],
)
def test_an_answer_of_the_wrong_shape_is_invalid_never_an_exception(counted, answer):
    question = IS_FIGURE if "is_figure" in answer["answers"] else FIGURE_KIND
    assert Panel(Budget(10, 60)).ask(decider(response(body=answer)), question) is None
    assert counted[0]["outcome"] == "invalid"


def test_ollama_answering_a_non_string_verdict_is_invalid(counted):
    content = json.dumps({"verdict": ["yes"], "p": 0.7, "reason": "?"})
    http = FakeHttp(response(body={"model": "elvira-assistant", "message": {"content": content}}))
    client = OllamaClient("ollama", "http://ollama", model="elvira-assistant", timeout=120, http=http)  # type: ignore[arg-type]
    assert Panel(Budget(10, 60)).ask(client, IS_FIGURE) is None
    assert counted[0]["outcome"] == "invalid"


# -- Batches: one question about many states (decider) ----------------------------------------------------------------


def noul_result(yes: float | None = 0.9, *, model: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"answers": {"is_figure": {"noul": yes}}}
    return result | {"model": model} if model else result


def batch_answer(*results: dict[str, Any], model: str = "laya-multilingual") -> requests.Response:
    return response(body={"results": list(results), "model": model, "total_usage": {}})


def text_decider(*answers: requests.Response | Exception, **kwargs: Any) -> DeciderClient:
    return decider(*answers, vision=False, model="multilingual", **kwargs)


@pytest.fixture
def seconds(monkeypatch):
    """Every `<service>_judge_seconds` record, as (attributes)."""
    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(panel.JUDGE_SECONDS, "record", lambda value, attributes=None: seen.append(attributes or {}))
    return seen


def test_the_text_decider_asks_a_batch_and_keeps_the_order():
    client = text_decider(batch_answer(noul_result(0.9), noul_result(0.1, model="laya@x"), noul_result(0.7)),
                          revision="80a1")  # fmt: skip
    answers = client.ask_batch(IS_FIGURE, ["a", {"body": "b"}, "c"], timeout=7.0)

    call = client.http.calls[0]  # type: ignore[attr-defined]
    assert call["url"] == "http://decider-vision.shared.svc.cluster.local/v1/systemone/batch"
    assert call["timeout"] == 7.0
    questions = {"is_figure": {"type": "noul", "instructions": IS_FIGURE.instructions}}
    assert call["json"] == {"states": ["a", {"body": "b"}, "c"], "model": "multilingual", "questions": questions}
    assert [(a.verdict, a.p, a.model) for a in answers] == [  # type: ignore[union-attr]
        ("yes", 0.9, "laya-multilingual"), ("no", pytest.approx(0.9), "laya@x"), ("yes", 0.7, "laya-multilingual")
    ]  # fmt: skip
    assert answers[0].revision == "80a1"  # type: ignore[union-attr]


def test_decider_vision_does_not_batch():
    client = decider()
    with pytest.raises(ValueError, match="one state per request"):
        client.ask_batch(IS_FIGURE, ["a"])
    with pytest.raises(ValueError, match="one state per request"):
        Panel(Budget(10, 60)).ask_batch(client, IS_FIGURE, ["a"])
    assert client.http.calls == []  # type: ignore[attr-defined]
    assert client.breaker.state == "closed"


def test_a_client_takes_at_most_64_states():
    with pytest.raises(ValueError, match="64"):
        text_decider().ask_batch(IS_FIGURE, ["a"] * 65)


def test_an_empty_batch_sends_nothing():
    client = text_decider()
    assert Panel(Budget(10, 60)).ask_batch(client, IS_FIGURE, []) == []
    assert client.http.calls == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "body",
    [{"model": "laya"}, {"results": "nope"}, {"results": [noul_result()]}],  # no results, not a list, 1 of 2 states
)
def test_a_batch_answer_without_one_result_per_state_is_invalid_as_a_whole(body):
    with pytest.raises(JudgeInvalid):
        text_decider(response(body=body)).ask_batch(IS_FIGURE, ["a", "b"])


def test_the_panel_bands_each_state_and_keeps_the_order(counted, seconds):
    client = text_decider(batch_answer(noul_result(0.95), noul_result(0.02), noul_result(0.6)))
    p = Panel(Budget(10, 60), {"is_figure": Thresholds(keep=0.9, hide=0.9)})
    verdicts = p.ask_batch(client, IS_FIGURE, ["a", "b", "c"])
    assert [(v.verdict, v.band) for v in verdicts] == [("yes", "keep"), ("no", "hide"), ("yes", "review")]  # type: ignore[union-attr]
    assert [c["band"] for c in counted] == ["keep", "hide", "review"]
    assert seconds == [{"question": "is_figure", "outcome": "judged", "batch": "true"}]
    assert p.budget.used_calls == 1


def test_an_answer_outside_the_criteria_is_invalid_for_its_state_only(counted):
    bad_kind = {"answers": {"figure_kind": {"choice": "selfie", "probabilities": {"selfie": 0.9}}}}
    good = {"answers": {"figure_kind": {"choice": "chart", "probabilities": {"chart": 0.8}}}}
    client = text_decider(batch_answer(good, bad_kind, {"answers": {}}, good))
    verdicts = Panel(Budget(10, 60)).ask_batch(client, FIGURE_KIND, ["a", "b", "c", "d"])
    assert [v and v.verdict for v in verdicts] == ["chart", None, None, "chart"]
    assert [c["outcome"] for c in counted] == ["judged", "invalid", "invalid", "judged"]
    assert client.breaker.state == "closed"


def test_more_than_64_states_go_in_requests_of_64_in_order(seconds):
    sizes = [64, 64, 2]
    client = text_decider(*[batch_answer(*[noul_result(0.9 if i % 2 else 0.1) for i in range(n)]) for n in sizes])
    verdicts = Panel(Budget(10, 0)).ask_batch(client, IS_FIGURE, [str(i) for i in range(130)])
    assert [len(c["json"]["states"]) for c in client.http.calls] == sizes  # type: ignore[attr-defined]
    assert client.http.calls[2]["json"]["states"] == ["128", "129"]  # type: ignore[attr-defined]
    assert len(verdicts) == 130 and all(v is not None for v in verdicts)
    assert len(seconds) == 3


def test_a_spent_budget_skips_the_rest_of_the_list(counted):
    client = text_decider(batch_answer(*[noul_result()] * 64), batch_answer(*[noul_result()] * 64))
    p = Panel(Budget(calls=2, seconds=0))
    verdicts = p.ask_batch(client, IS_FIGURE, ["s"] * 150)
    assert len(client.http.calls) == 2  # type: ignore[attr-defined]
    assert verdicts[:128] == [verdicts[0]] * 128 and verdicts[0] is not None
    assert verdicts[128:] == [None] * 22
    assert [c["outcome"] for c in counted[128:]] == ["budget"] * 22


def test_a_batch_waits_no_longer_than_the_seconds_left_in_the_budget():
    client = text_decider(batch_answer(noul_result()))
    Panel(Budget(calls=0, seconds=60, used_seconds=58)).ask_batch(client, IS_FIGURE, ["a"])
    assert client.http.calls[0]["timeout"] == pytest.approx(2.0)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("answer", "outcome"),
    [(requests.ReadTimeout("slow"), "unreachable"), (response(422, {}), "refused"), (response(200, {}), "invalid")],
)
def test_a_failed_request_fails_each_of_its_states(counted, seconds, answer, outcome):
    verdicts = Panel(Budget(10, 60)).ask_batch(text_decider(answer), IS_FIGURE, ["a", "b", "c"])
    assert verdicts == [None, None, None]
    assert counted == [{"question": "is_figure", "outcome": outcome, "band": "none"}] * 3
    assert seconds == [{"question": "is_figure", "outcome": outcome, "batch": "true"}]


def test_a_failed_batch_trips_the_breaker_once_and_the_rest_is_skipped(counted):
    client = text_decider(requests.ConnectionError("down"), requests.ConnectionError("down"))
    client.breaker = CircuitBreaker(failures=2, reset=60)
    verdicts = Panel(Budget(10, 60)).ask_batch(client, IS_FIGURE, ["s"] * 200)  # 4 requests' worth

    assert verdicts == [None] * 200
    assert len(client.http.calls) == 2  # type: ignore[attr-defined]
    assert client.breaker.state == "open"
    outcomes = [c["outcome"] for c in counted]
    assert outcomes == ["unreachable"] * 128 + ["open"] * 72


def test_one_failed_batch_counts_one_breaker_failure():
    client = text_decider(requests.ConnectionError("down"), batch_answer(*[noul_result()] * 64))
    client.breaker = CircuitBreaker(failures=2, reset=60)
    Panel(Budget(10, 60)).ask_batch(client, IS_FIGURE, ["s"] * 64)
    assert client.breaker.state == "closed"  # 64 states failed, but the breaker saw one failure
    Panel(Budget(10, 60)).ask_batch(client, IS_FIGURE, ["s"] * 64)
    assert client.breaker.state == "closed" and client.breaker._count == 0


def test_no_client_disables_each_state(counted):
    assert Panel(Budget(10, 60)).ask_batch(None, IS_FIGURE, ["a", "b"]) == [None, None]
    assert [c["outcome"] for c in counted] == ["disabled", "disabled"]


def test_a_verdict_fills_a_paragraphs_decision_fields():
    verdict = Verdict(question="paragraph_kind", verdict="caption", p=0.0, model="laya", revision="80a1")
    paragraph = Paragraph(page=1, seq=0, bbox={"x0": 0, "y0": 0, "x1": 1, "y1": 1}, text="Obr. 3.2")
    paragraph = Paragraph.model_validate(paragraph.model_dump() | verdict.paragraph_fields())
    assert (paragraph.decision, paragraph.p, paragraph.decider_model) == ("caption", 0.0, "laya@80a1")


# -- Settings and wiring --------------------------------------------------------------------------------------------


class ImageSettings(WorkerSettings, JudgeSettings):
    pass


def test_judge_settings_compose_with_the_worker_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("DECIDER_ENABLED", "true")
    monkeypatch.setenv("DECIDER_VISION_URL", "http://decider-vision.shared.svc.cluster.local")
    monkeypatch.setenv("DECIDER_TIMEOUT", "30")
    monkeypatch.setenv("JUDGE_THRESHOLDS", '{"figure_kind": {"keep": 0.8, "hide": 0.9}}')
    settings = ImageSettings(_env_file=None, storage_path=tmp_path)

    assert isinstance(settings, RuntimeSettings)
    assert settings.catalog_api_url == "http://catalog:8000" and settings.max_deliveries == 5
    assert settings.judges_enabled and settings.decider_timeout == 30
    assert settings.judge_thresholds["figure_kind"] == Thresholds(keep=0.8, hide=0.9)

    judges = Judges.from_settings(settings)
    assert judges.decider is None and judges.ollama is None and judges.rerank is None
    assert judges.decider_vision is not None and judges.decider_vision.vision
    assert judges.decider_vision.timeout == 30

    p = judges.panel()
    assert p.budget == Budget(500, 900.0) and "figure_kind" in p.thresholds
    assert judges.panel().budget is not p.budget  # one budget per job


def test_empty_judge_variables_mean_their_defaults(monkeypatch, tmp_path):
    """Switching judges off leaves the env set but empty (Deploy-Decision-Models, Roll back)."""
    for name in ("DECIDER_ENABLED", "DECIDER_TIMEOUT", "DECIDER_VISION_URL", "JUDGE_BUDGET_CALLS", "JUDGE_THRESHOLDS"):
        monkeypatch.setenv(name, "")
    settings = ImageSettings(_env_file=None, storage_path=tmp_path)
    assert not settings.judges_enabled and settings.decider_timeout == 60.0
    assert settings.decider_vision_url is None and settings.judge_budget_calls == 500
    assert settings.judge_thresholds == {}


@pytest.mark.parametrize("blank", ["", "  "])
def test_blank_thresholds_mean_none(monkeypatch, blank):
    """evilflowers-worker#8: the env source JSON-decoded the raw value before any validator, and "" stopped startup."""
    monkeypatch.setenv("JUDGE_THRESHOLDS", blank)
    assert JudgeSettings(_env_file=None).judge_thresholds == {}


def test_blank_thresholds_in_a_dotenv_file_mean_none(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("JUDGE_THRESHOLDS=\nDECIDER_ENABLED=\n")
    settings = JudgeSettings(_env_file=env_file)
    assert settings.judge_thresholds == {} and not settings.judges_enabled


def test_thresholds_parse_from_json_and_from_code(monkeypatch):
    monkeypatch.setenv("JUDGE_THRESHOLDS", '{"figure_kind": {"keep": 0.8, "hide": 0.9}}')
    assert JudgeSettings(_env_file=None).judge_thresholds == {"figure_kind": Thresholds(keep=0.8, hide=0.9)}
    monkeypatch.delenv("JUDGE_THRESHOLDS")
    in_code = JudgeSettings(_env_file=None, judge_thresholds={"caption": {"keep": 0.5, "hide": 0.7}})
    assert in_code.judge_thresholds == {"caption": Thresholds(keep=0.5, hide=0.7)}


@pytest.mark.parametrize("bad", ["{not json", '{"figure_kind": {"keep": 2, "hide": 0.9}}'])
def test_malformed_thresholds_still_stop_the_worker(monkeypatch, bad):
    """Blank means none; a wrong value is a configuration error, not something to ignore."""
    monkeypatch.setenv("JUDGE_THRESHOLDS", bad)
    with pytest.raises(ValidationError):
        JudgeSettings(_env_file=None)


def test_disabled_judges_build_no_client(tmp_path):
    settings = JudgeSettings(_env_file=None, decider_url="http://decider", rerank_url="http://tei-rerank")
    judges = Judges.from_settings(settings)
    assert (judges.decider, judges.decider_vision, judges.ollama, judges.rerank) == (None, None, None, None)


def test_enabled_judges_build_every_configured_client():
    settings = JudgeSettings(
        _env_file=None,
        judges_enabled=True,
        decider_url="http://decider/v1/systemone",
        ollama_url="http://ollama/api/chat",
        rerank_url="http://tei-rerank",
        rerank_api_key="secret",
        decider_revision="e333cb7c",
    )
    judges = Judges.from_settings(settings)
    assert judges.decider is not None and judges.decider.model == "multilingual"
    assert judges.decider.revision == "e333cb7c" and judges.decider.url == "http://decider"
    assert judges.ollama is not None and judges.ollama.url == "http://ollama"
    assert judges.rerank is not None and judges.rerank.http.headers["Authorization"] == "Bearer secret"


def test_the_core_does_not_import_the_judges():
    import subprocess
    import sys

    code = "import sys, evilflowers_worker.runtime, evilflowers_worker.jobs; print('judges' in str(list(sys.modules)))"
    assert subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout == "False\n"
