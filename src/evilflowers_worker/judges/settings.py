"""The judges' settings: a mixin for a service's settings class, next to ``WorkerSettings`` or ``RuntimeSettings``.

```python
class Settings(WorkerSettings, JudgeSettings): ...
```

The variable names are the ones the cluster already sets for the image and text-tables workers (``DECIDER_ENABLED``,
``DECIDER_VISION_URL``, ``DECIDER_TIMEOUT``; evilflowers-knowledge ``Cookbooks/Deploy-Decision-Models``). A URL may be
the service address or the full endpoint.
"""

from typing import Any

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from evilflowers_worker.judges.schema import Thresholds


class JudgeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True, populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _empty_is_unset(cls, data: Any) -> Any:
        """An empty judge variable means its default: switching judges off leaves the env set but empty
        (Deploy-Decision-Models, Roll back), and that must not stop the worker. Other fields keep their own rules."""
        if isinstance(data, dict):
            return {key: value for key, value in data.items() if not (value == "" and key.lower() in _JUDGE_KEYS)}
        return data

    #: Off: every question passes through unjudged (outcome ``disabled``).
    judges_enabled: bool = Field(False, validation_alias=AliasChoices("JUDGES_ENABLED", "DECIDER_ENABLED"))

    # -- decider (Laya, text) and decider-vision (Strands Decider 2B, images) ------------------------------------
    #: ``http://decider.shared.svc.cluster.local``
    decider_url: str | None = None
    #: Laya's checkpoint; without it Laya may swap in its English one.
    decider_model: str = "multilingual"
    #: The weights' snapshot (Deploy-Decision-Models § 4); stored with every verdict.
    decider_revision: str | None = None
    #: ``http://decider-vision.shared.svc.cluster.local``
    decider_vision_url: str | None = None
    decider_vision_revision: str | None = None
    #: Seconds per call; 60 until knowledge#65 measures it.
    decider_timeout: float = Field(60.0, gt=0)

    # -- Ollama ----------------------------------------------------------------------------------------------------
    #: ``http://ollama.shared.svc.cluster.local``
    ollama_url: str | None = None
    ollama_model: str = "elvira-assistant"
    ollama_revision: str | None = None
    ollama_timeout: float = Field(120.0, gt=0)

    # -- tei-rerank ------------------------------------------------------------------------------------------------
    #: ``http://tei-rerank.shared.svc.cluster.local``
    rerank_url: str | None = None
    #: The TEI bearer key (1Password ``stu.inference.tei``).
    rerank_api_key: SecretStr | None = None
    rerank_model: str = "bge-reranker-v2-m3"
    rerank_revision: str | None = None
    rerank_timeout: float = Field(10.0, gt=0)

    # -- Bands, budget, breaker ------------------------------------------------------------------------------------
    #: Per question id, from knowledge#65: ``JUDGE_THRESHOLDS='{"figure_kind": {"keep": 0.8, "hide": 0.9}}'``.
    #: A question without thresholds gets no band.
    judge_thresholds: dict[str, Thresholds] = Field(default_factory=dict)
    #: Calls one job may make (0: no limit).
    judge_budget_calls: int = Field(500, ge=0)
    #: Seconds one job may spend waiting on judges (0: no limit).
    judge_budget_seconds: float = Field(900.0, ge=0)
    #: Consecutive failures that open a client's breaker, and the seconds it stays open.
    judge_breaker_failures: int = Field(3, ge=1)
    judge_breaker_reset: float = Field(60.0, gt=0)


#: The judge fields' names and env aliases, lower-case (``JUDGES_ENABLED`` / ``DECIDER_ENABLED`` are aliases).
_JUDGE_KEYS = frozenset(JudgeSettings.model_fields) | {"decider_enabled"}
