"""What every extraction worker is configured with; a service subclasses ``WorkerSettings`` for its own fields.

One typed object read from the environment (and ``.env``). The variable names are the ones the k8s manifests already
set (``REDIS_URL``, ``CATALOG_API_KEY``, ``BROKER_VISIBILITY_TIMEOUT`` …).
"""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Twelve hours. Every consumer of a Redis db restores *all* unacknowledged messages older than its own timeout, in
#: any queue, so this must outlast the longest task of any service on the db (EvilFlowersCatalog#120, text#9).
VISIBILITY_TIMEOUT = 12 * 3600


class WorkerSettings(BaseSettings):
    # `populate_by_name`: a field with an env alias (``ANALYZER_QUEUE``) can still be set by its name in code.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True, populate_by_name=True)

    # -- Broker ---------------------------------------------------------------------------------------------------
    #: The catalog's Redis (db 7 on dev).
    redis_url: str = "redis://redis:6379/7"
    #: The queue this worker consumes; a service overrides the default and its env alias.
    queue: str = "evilflowers.worker"
    #: Seconds before Redis redelivers an unacknowledged message. One value for every consumer of the db.
    broker_visibility_timeout: int = Field(VISIBILITY_TIMEOUT, ge=3600)

    # -- Delivery semantics (docs/architecture.md §3.3) -------------------------------------------------------------
    #: Seconds a SIGTERMed worker lets its job run on before it cancels it and Redis requeues it.
    #: Must stay under the pod's ``terminationGracePeriodSeconds``.
    worker_soft_shutdown_timeout: float = Field(60.0, ge=0)
    #: Seconds the cancel may take before the worker requeues the job itself and exits.
    worker_cancel_timeout: float = Field(30.0, ge=0)
    #: Deliveries of one message before the job is reported failed instead of run again (it killed its worker every
    #: time). 0 turns the cap off.
    max_deliveries: int = Field(5, ge=0)

    # -- Storage and provenance -----------------------------------------------------------------------------------
    #: The catalog's storage root: originals are read here, crops written.
    storage_path: Path = Path("/mnt/data")
    #: Built into the image by CI; recorded in provenance and as ``service.version``.
    image_tag: str = "dev"

    # -- Catalog ingestion API (IP-018) -----------------------------------------------------------------------------
    catalog_api_url: str = "http://catalog:8000"
    #: API key (bearer JWT) of the catalog user holding ``extraction.ingest_extraction``.
    catalog_api_key: SecretStr | None = None
    catalog_attempts: int = Field(4, ge=1)
    catalog_timeout: float = Field(120.0, gt=0)
    #: Rows per request: keeps each body well under the catalog's upload limit (and its caps of 5000 / 1000).
    catalog_batch_size: int = Field(100, ge=1, le=1000)

    # -- Logs -------------------------------------------------------------------------------------------------------
    #: ``json`` (one object per line, what Loki parses) or ``text`` (Celery's own format, for a terminal).
    log_format: Literal["json", "text"] = "json"

    def source_path(self, source: str) -> Path:
        """Absolute path of a file the catalog names relative to its storage root."""
        return self.storage_path / source
