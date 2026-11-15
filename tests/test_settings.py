import pytest

from evilflowers_worker.settings import RuntimeSettings, WorkerSettings


def test_the_runtime_settings_carry_no_catalog_client():
    """A catalog job (files.ocr) holds no catalog URL and no API key."""
    assert not any(name.startswith("catalog_") for name in RuntimeSettings.model_fields)


def test_an_extraction_worker_keeps_every_field_it_had(tmp_path):
    settings = WorkerSettings(_env_file=None, storage_path=tmp_path, catalog_api_url="http://c:8000")
    assert isinstance(settings, RuntimeSettings)
    assert settings.catalog_api_url == "http://c:8000"
    assert settings.max_deliveries == 5 and settings.broker_visibility_timeout == 12 * 3600


def test_storage_file_resolves_under_the_storage_root(tmp_path):
    settings = RuntimeSettings(_env_file=None, storage_path=tmp_path)
    assert settings.storage_file("catalogs/a/b/ocr/run.json") == tmp_path.resolve() / "catalogs/a/b/ocr/run.json"
    assert settings.storage_file("catalogs/a/../a/x.pdf") == tmp_path.resolve() / "catalogs/a/x.pdf"


@pytest.mark.parametrize("relative", ["", "/etc/passwd", "../outside.json", "catalogs/../../outside.json", "."])
def test_storage_file_refuses_a_path_that_escapes_the_storage_root(tmp_path, relative):
    settings = RuntimeSettings(_env_file=None, storage_path=tmp_path / "data")
    with pytest.raises(ValueError):
        settings.storage_file(relative)


def test_storage_file_refuses_a_symlink_out_of_the_storage_root(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    (root / "link").symlink_to(tmp_path)
    with pytest.raises(ValueError):
        RuntimeSettings(_env_file=None, storage_path=root).storage_file("link/outside.json")
