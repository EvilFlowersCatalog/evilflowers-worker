"""Standalone jobs in-process: the message, its path helpers, the callback signatures (evilflowers-worker#5).

The real-worker behaviour (link / link_error over Redis, no result backend) is in ``test_runtime.py``.
"""

import pytest
from celery import Celery
from pydantic import ValidationError

from evilflowers_worker.contract import StandaloneJob
from evilflowers_worker.outcome import atomic_write
from evilflowers_worker.runtime import standalone_callbacks
from evilflowers_worker.settings import RuntimeSettings

KWARGS = {
    "run_id": "run-1",
    "job": "job-7",
    "token": "tok-7",
    "inputs": ["items/b1/leaves/0001.png"],
    "output_dir": "items/b1/_studio/jobs/job-7/",
    "outcome": "items/b1/_studio/jobs/job-7.json",
}


@pytest.fixture
def settings(tmp_path):
    return RuntimeSettings(_env_file=None, storage_path=tmp_path)


class OcrJob(StandaloneJob):
    language: str = "slk"


def test_a_service_subclasses_the_message_and_unknown_fields_are_ignored():
    message = OcrJob.model_validate({**KWARGS, "language": "ces", "from_a_newer_orchestrator": 1})
    assert message.language == "ces" and message.inputs == ["items/b1/leaves/0001.png"]
    assert message.kwargs() == {**KWARGS, "language": "ces"}


@pytest.mark.parametrize("missing", ["run_id", "job", "token", "inputs", "output_dir", "outcome"])
def test_every_shared_field_is_required(missing):
    with pytest.raises(ValidationError):
        StandaloneJob.model_validate({key: value for key, value in KWARGS.items() if key != missing})


def test_a_job_needs_at_least_one_input():
    with pytest.raises(ValidationError):
        StandaloneJob.model_validate({**KWARGS, "inputs": []})


def test_the_output_directory_is_created_under_the_storage(settings, tmp_path):
    message = StandaloneJob.model_validate(KWARGS)
    directory = message.prepare_output(settings)
    assert directory == tmp_path.resolve() / "items/b1/_studio/jobs/job-7" and directory.is_dir()
    assert message.input_paths(settings) == [tmp_path.resolve() / "items/b1/leaves/0001.png"]


def test_a_nested_output_file_can_be_written_at_once(settings, tmp_path):
    path = StandaloneJob.model_validate(KWARGS).output_file(settings, "pages/0001.hocr")
    assert path == tmp_path.resolve() / "items/b1/_studio/jobs/job-7/pages/0001.hocr"
    atomic_write(path, b"<html/>")
    assert path.read_bytes() == b"<html/>"


@pytest.mark.parametrize("output_dir", ["/etc", "../outside", "items/../../outside", ""])
def test_an_output_directory_outside_the_storage_is_refused(settings, output_dir):
    message = StandaloneJob.model_construct(**{**KWARGS, "output_dir": output_dir})
    with pytest.raises(ValueError):
        message.prepare_output(settings)


@pytest.mark.parametrize("name", ["../job-8/x.png", "../../leaves/0001.png", "", "."])
def test_an_output_file_outside_the_job_directory_is_refused(settings, name):
    with pytest.raises(ValueError):
        StandaloneJob.model_validate(KWARGS).output_file(settings, name)


def test_an_input_outside_the_storage_is_refused(settings):
    with pytest.raises(ValueError):
        StandaloneJob.model_validate({**KWARGS, "inputs": ["../../etc/passwd"]}).input_paths(settings)


def test_the_callbacks_are_bound_signatures_on_the_orchestrators_queue():
    app = Celery(broker="memory://")
    callbacks = standalone_callbacks(
        app, done="digitalizer.job_done", failed="digitalizer.job_failed", queue="digitalizer.portal", job_id="j-1",
        attempt=2,
    )  # fmt: skip
    assert set(callbacks) == {"link", "link_error"}
    assert callbacks["link"].task == "digitalizer.job_done"
    assert callbacks["link_error"].task == "digitalizer.job_failed"
    for signature in callbacks.values():
        assert signature.kwargs == {"job_id": "j-1", "attempt": 2}
        assert signature.options["queue"] == "digitalizer.portal"
        assert signature.args == ()


def test_in_eager_mode_a_bind_true_errback_gets_the_failed_task_id():
    """The errback rule of ``standalone_callbacks``: declared with ``bind=True`` it is applied as a task with the task
    id even when registered in the failing app (a plain two-argument errback is called with (request, exc, tb))."""
    app = Celery(broker="memory://")
    app.conf.task_always_eager = True
    seen: list[tuple[str, object]] = []

    @app.task(name="job.work")
    def work(fail: bool) -> dict:
        if fail:
            raise RuntimeError("boom")
        return {"status": "done"}

    @app.task(name="orchestrator.job_done")
    def job_done(result: dict, job_id: str) -> None:
        seen.append(("done", (result, job_id)))

    @app.task(name="orchestrator.job_failed", bind=True)
    def job_failed(self: object, task_id: str, job_id: str) -> None:
        seen.append(("failed", (task_id, job_id)))

    callbacks = standalone_callbacks(
        app, done="orchestrator.job_done", failed="orchestrator.job_failed", queue="portal", job_id="j-1"
    )
    work.apply_async(kwargs={"fail": False}, **callbacks)
    failing = work.apply_async(kwargs={"fail": True}, **callbacks)

    assert seen == [("done", ({"status": "done"}, "j-1")), ("failed", (failing.id, "j-1"))]
