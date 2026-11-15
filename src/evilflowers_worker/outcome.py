"""The outcome file of a catalog job: how a worker without a catalog client tells the catalog what happened.

A *catalog job* (an IP-031 job-ledger kind such as ``files.ocr``) has one terminal outcome. It writes that outcome as
a JSON file on the shared storage, next to its output. The catalog reads the file when the Celery ``task-succeeded`` /
``task-failed`` event arrives, or on its reconcile tick if the event was lost (EvilFlowersCatalog#169, which owns the
contract; docs/architecture.md §3.4).

Rules a service keeps:

- **Last.** The outcome is the commit marker: write the output first, then the outcome file.
- **Atomic.** ``atomic_write``: ``<name>.partial``, fsync, rename, fsync the directory. A reader sees the whole file or
  none; a crash leaves no outcome, and the catalog ends the run ``lost``, never applied.
- **Every status,** ``failed`` included: write the file, then raise, so the job keeps both the domain error and the
  traceback. Past the delivery cap too (``runtime.run_task``'s ``on_capped``).
- **Never on a deliberate requeue** (SIGTERM, the cold cancel of ``shutdown``). The redelivery carries the same
  ``token`` and writes the outcome then; a ``failed`` file written on the way out would consume the token, and the
  catalog would refuse the redelivery's.
- **``token`` echoed back untouched.** The catalog minted it for this run; it binds the file to the run.
"""

import contextlib
import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from evilflowers_worker.settings import RuntimeSettings

PARTIAL_SUFFIX = ".partial"


class OutcomeFile(BaseModel):
    """The fields every outcome file carries; a service subclasses it with its own (``pages``, ``engine`` …).

    ``extra="forbid"``: the file is a contract, so a misspelt field fails in the worker, not in the catalog. A
    subclass narrows ``status`` to its statuses (``Literal["done", "skipped", "failed"]``).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: The run the catalog dispatched; it checks this against the run it applies the file to.
    run_id: str
    #: The per-run capability token from the task kwargs, echoed back untouched.
    token: str
    status: str


def atomic_write(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` so that a reader sees the whole file or nothing, even across a crash.

    ``<name>.partial`` in the same directory (the rename must not cross a file system), fsync, rename over ``path``,
    fsync the directory so the rename itself is durable. A leftover ``.partial`` from an earlier crash is overwritten;
    on failure the ``.partial`` is removed and ``path`` is untouched.
    """
    partial = path.with_name(path.name + PARTIAL_SUFFIX)
    try:
        with partial.open("wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(partial, path)
    except BaseException:
        with contextlib.suppress(OSError):
            partial.unlink()
        raise
    _fsync_directory(path.parent)


def write_outcome(settings: RuntimeSettings, relative: str, outcome: OutcomeFile) -> Path:
    """Write ``outcome`` at ``relative`` (the task's ``outcome`` kwarg, relative to the storage root); its path.

    The path is checked (``RuntimeSettings.storage_file``): it comes from a task message and must not escape the
    storage. Missing directories are created; ``None`` fields are written as ``null`` (the catalog reads every key).
    """
    path = settings.storage_file(relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(path, outcome.model_dump_json().encode())
    return path


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
