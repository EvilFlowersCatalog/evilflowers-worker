"""``runtime.run_task``, in process: the body of a task that is not an extraction leaf job (a catalog job)."""

from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from evilflowers_worker import runtime as runtime_module
from evilflowers_worker.runtime import Runtime, run_job, run_task
from evilflowers_worker.settings import RuntimeSettings

from .example import EchoFigures


class Message(BaseModel):
    run_id: str
    token: str


class FixedDeliveries:
    """Stands in for ``DeliveryCounter``: every delivery is number ``delivery``."""

    def __init__(self, delivery: int):
        self.delivery = delivery

    def record(self, _task_id: str, *, redelivered: bool) -> int:
        return self.delivery


def task(task_id: str = "task-1") -> Any:
    return SimpleNamespace(name="probe.convert", request=SimpleNamespace(id=task_id, delivery_info={}))


@pytest.fixture
def with_runtime(monkeypatch, tmp_path):
    def install(delivery: int = 1, max_deliveries: int = 3) -> None:
        settings = RuntimeSettings(_env_file=None, storage_path=tmp_path, max_deliveries=max_deliveries)
        monkeypatch.setattr(
            runtime_module,
            "_runtime",
            Runtime(service="probe", settings=settings, deliveries=FixedDeliveries(delivery)),
        )

    return install


def test_the_handler_gets_the_validated_message_and_its_delivery(with_runtime):
    with_runtime(delivery=2)
    calls: list[tuple[Message, int]] = []

    result = run_task(
        task(),
        lambda message, delivery: calls.append((message, delivery)) or "done",
        Message,
        {"run_id": "r", "token": "t", "unknown": "ignored"},
        on_capped=lambda *_: pytest.fail("not capped"),
    )

    assert result == "done"
    assert calls == [(Message(run_id="r", token="t"), 2)]


def test_past_the_cap_on_capped_runs_instead_of_the_handler(with_runtime):
    with_runtime(delivery=4, max_deliveries=3)
    capped: list[tuple[str, int]] = []

    result = run_task(
        task(),
        lambda *_: pytest.fail("the handler must not run past the cap"),
        Message,
        {"run_id": "r", "token": "t"},
        on_capped=lambda message, delivery: capped.append((message.token, delivery)) or "failed",
    )

    assert result == "failed"
    assert capped == [("t", 4)]


def test_at_the_cap_the_job_still_runs(with_runtime):
    with_runtime(delivery=3, max_deliveries=3)
    result = run_task(task(), lambda *_: "ran", Message, {"run_id": "r", "token": "t"}, on_capped=lambda *_: "capped")
    assert result == "ran"


def test_no_cap_when_max_deliveries_is_zero(with_runtime):
    with_runtime(delivery=99, max_deliveries=0)
    result = run_task(task(), lambda *_: "ran", Message, {"run_id": "r", "token": "t"}, on_capped=lambda *_: "capped")
    assert result == "ran"


def test_an_invalid_message_raises_by_default_and_runs_nothing(with_runtime):
    with_runtime()
    with pytest.raises(ValidationError):
        run_task(task(), lambda *_: pytest.fail("ran"), Message, {"run_id": "r"}, on_capped=lambda *_: pytest.fail())


def test_an_invalid_message_goes_to_on_invalid_when_given(with_runtime):
    with_runtime()
    seen: list[dict[str, Any]] = []
    result = run_task(
        task(),
        lambda *_: pytest.fail("ran"),
        Message,
        {"run_id": "r"},
        on_capped=lambda *_: pytest.fail("capped"),
        on_invalid=lambda kwargs, error: seen.append(kwargs) or f"invalid: {error.error_count()}",
    )
    assert result == "invalid: 1"
    assert seen == [{"run_id": "r"}]


def test_run_job_refuses_settings_without_the_catalog_client(with_runtime):
    with_runtime()
    from evilflowers_worker.contract import RegionsJob

    with pytest.raises(TypeError, match="run_task"):
        run_job(task(), EchoFigures, RegionsJob, {"run_id": "r", "source": "a.pdf", "output_dir": "o"})
