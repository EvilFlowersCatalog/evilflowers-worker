import json
import os
from typing import Literal

import pytest
from pydantic import ValidationError

from evilflowers_worker import outcome as outcome_module
from evilflowers_worker.outcome import OutcomeFile, atomic_write, write_outcome
from evilflowers_worker.settings import RuntimeSettings
from evilflowers_worker.testing import assert_outcome


class OcrOutcome(OutcomeFile):
    status: Literal["done", "skipped", "failed"]
    output: str | None = None
    pages: int | None = None
    skipped_reason: str | None = None


@pytest.fixture
def storage(tmp_path):
    return RuntimeSettings(_env_file=None, storage_path=tmp_path)


def test_the_outcome_is_written_with_every_key_and_the_token_untouched(storage, tmp_path):
    token = "v1.eyJzdWIiOiJzeXN0ZW0ifQ.sig/+=="
    path = write_outcome(
        storage, "catalogs/c/e/ocr/run-1.json", OcrOutcome(run_id="run-1", token=token, status="skipped")
    )

    assert path == tmp_path / "catalogs/c/e/ocr/run-1.json"
    written = assert_outcome(tmp_path, "catalogs/c/e/ocr/run-1.json", run_id="run-1", token=token, status="skipped")
    # The catalog reads every key: an absent value is `null`, never a missing key.
    assert written == {
        "run_id": "run-1",
        "token": token,
        "status": "skipped",
        "output": None,
        "pages": None,
        "skipped_reason": None,
    }


def test_an_outcome_field_outside_the_contract_fails_in_the_worker():
    with pytest.raises(ValidationError):
        OcrOutcome(run_id="r", token="t", status="done", pagez=3)
    with pytest.raises(ValidationError):
        OcrOutcome(run_id="r", token="t", status="lost")


def test_an_outcome_path_that_escapes_the_storage_is_refused(storage, tmp_path):
    with pytest.raises(ValueError):
        write_outcome(storage, "../../etc/outcome.json", OcrOutcome(run_id="r", token="t", status="done"))
    assert list(tmp_path.iterdir()) == []


def test_an_interrupted_write_leaves_neither_the_file_nor_a_partial(tmp_path, monkeypatch):
    target = tmp_path / "run.json"

    def crash(*_args):
        raise OSError("disk full")

    monkeypatch.setattr(outcome_module.os, "replace", crash)
    with pytest.raises(OSError, match="disk full"):
        atomic_write(target, b"{}")
    assert list(tmp_path.iterdir()) == []


def test_an_interrupted_rewrite_keeps_the_previous_file_whole(tmp_path, monkeypatch):
    target = tmp_path / "run.json"
    target.write_bytes(b'{"status": "done"}')
    monkeypatch.setattr(outcome_module.os, "replace", lambda *_a: (_ for _ in ()).throw(OSError("crash")))
    with pytest.raises(OSError):
        atomic_write(target, b'{"status": "failed"}')
    assert target.read_bytes() == b'{"status": "done"}'
    assert list(tmp_path.iterdir()) == [target]


def test_a_partial_left_by_an_earlier_crash_is_overwritten(tmp_path):
    target = tmp_path / "run.json"
    (tmp_path / "run.json.partial").write_bytes(b"half a fi")
    atomic_write(target, b'{"status": "done"}')
    assert json.loads(target.read_text()) == {"status": "done"}
    assert list(tmp_path.iterdir()) == [target]


def test_the_data_is_on_disk_before_the_rename_and_the_rename_before_returning(tmp_path, monkeypatch):
    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(descriptor):
        events.append("fsync-dir" if _is_dir(descriptor) else "fsync-file")
        real_fsync(descriptor)

    def replace(source, destination):
        events.append("rename")
        real_replace(source, destination)

    monkeypatch.setattr(outcome_module.os, "fsync", fsync)
    monkeypatch.setattr(outcome_module.os, "replace", replace)
    atomic_write(tmp_path / "run.json", b"{}")
    assert events == ["fsync-file", "rename", "fsync-dir"]


def _is_dir(descriptor: int) -> bool:
    import stat

    return stat.S_ISDIR(os.fstat(descriptor).st_mode)


def test_assert_outcome_catches_an_outcome_written_before_the_output(tmp_path):
    (tmp_path / "run.json").write_text('{"run_id": "r", "token": "t", "status": "done"}')
    output = tmp_path / "run.pdf"
    output.write_bytes(b"%PDF")
    later = (tmp_path / "run.json").stat().st_mtime_ns + 1_000_000_000
    os.utime(output, ns=(later, later))
    with pytest.raises(AssertionError, match="written before"):
        assert_outcome(tmp_path, "run.json", after="run.pdf")
