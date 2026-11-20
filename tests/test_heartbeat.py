"""The heartbeat bootstep and the ``evilflowers-worker-alive`` probe, without a broker (evilflowers-worker#11).

The real worker (a frozen main process, a long job) is in ``test_runtime``.
"""

import os
import time
from pathlib import Path
from typing import Any

from evilflowers_worker import alive
from evilflowers_worker.heartbeat import Heartbeat, heartbeat_step
from evilflowers_worker.runtime import create_app
from evilflowers_worker.settings import WorkerSettings


def age(path: Path, seconds: float) -> None:
    then = time.time() - seconds
    os.utime(path, (then, then))


def test_a_fresh_file_is_alive(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    assert alive.main(["--file", str(heartbeat), "--max-age", "90"]) == 0


def test_a_stale_file_is_dead(tmp_path, capsys):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    age(heartbeat, 120)
    assert alive.main(["--file", str(heartbeat), "--max-age", "90"]) == 1
    assert "last touched 120 s ago" in capsys.readouterr().err


def test_a_missing_file_is_dead(tmp_path, capsys):
    assert alive.main(["--file", str(tmp_path / "none")]) == 1
    assert "no heartbeat file" in capsys.readouterr().err


def test_the_probe_reads_the_workers_environment(tmp_path, monkeypatch):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.touch()
    age(heartbeat, 20)
    monkeypatch.setenv("WORKER_HEARTBEAT_FILE", str(heartbeat))
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL", "5")
    # Default threshold: 3 x the interval = 15 s.
    assert alive.main([]) == 1
    monkeypatch.setenv("WORKER_HEARTBEAT_INTERVAL", "10")
    assert alive.main([]) == 0


def test_settings_default_to_tmp_and_30_s():
    settings = WorkerSettings(_env_file=None)
    assert settings.worker_heartbeat_file == Path("/tmp/evilflowers-worker.heartbeat")
    assert settings.worker_heartbeat_interval == 30


def test_create_app_adds_the_heartbeat_to_the_consumer(tmp_path):
    settings = WorkerSettings(_env_file=None, worker_heartbeat_file=tmp_path / "hb", worker_heartbeat_interval=7)
    app = create_app("probe", settings, include=[])
    (step,) = [step for step in app.steps["consumer"] if issubclass(step, Heartbeat)]
    assert (step.path, step.interval) == (tmp_path / "hb", 7)


class FakeTimer:
    def __init__(self) -> None:
        self.calls: list[tuple[float, Any]] = []
        self.cancelled = False

    def call_repeatedly(self, interval: float, fun: Any) -> "FakeTimer":
        self.calls.append((interval, fun))
        return self

    def cancel(self) -> None:
        self.cancelled = True


class FakeConsumer:
    def __init__(self) -> None:
        self.timer = FakeTimer()


def test_the_step_touches_at_once_then_on_the_consumers_timer(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    consumer = FakeConsumer()
    step = heartbeat_step(heartbeat, 30)(consumer)

    step.start(consumer)
    assert heartbeat.exists()
    ((interval, fun),) = consumer.timer.calls
    assert interval == 30
    age(heartbeat, 60)
    fun()
    assert alive.heartbeat_age(heartbeat) < 5

    step.stop(consumer)
    assert consumer.timer.cancelled


def test_an_unwritable_file_never_fails_the_worker(tmp_path, caplog):
    consumer = FakeConsumer()
    step = heartbeat_step(tmp_path / "missing-dir" / "heartbeat", 30)(consumer)
    step.start(consumer)
    step.touch()
    # Logged once, not on every touch.
    assert sum("cannot touch" in record.getMessage() for record in caplog.records) == 1
