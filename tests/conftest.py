import pytest

from evilflowers_worker.settings import WorkerSettings


@pytest.fixture
def settings(tmp_path) -> WorkerSettings:
    """Defaults only (no `.env`), storage in a temporary directory, two catalog attempts, no Redis."""
    return WorkerSettings(_env_file=None, storage_path=tmp_path, catalog_attempts=2, image_tag="test")
