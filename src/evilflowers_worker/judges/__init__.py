"""Judges: narrow, model-assisted checks of an extractor's proposals (the ``judges`` extra, evilflowers-worker#6).

An extractor *proposes* (a region, an OCR line, a caption); a judge answers one narrow question about an uncertain
proposal (is this real, is this an error, is this associated) and the answer is stored as provenance. A judge never
edits and never fails the job: when no model answers, the proposal passes through unjudged and a counter records it.

```python
# worker.py: clients and their breakers live as long as the process
judges = Judges.from_settings(settings)

FIGURE_KIND = Choice(
    id="figure_kind",
    instructions="What is this image?",
    criteria={"chart": "chart", "photo": "photo", "stamp": "stamp, signature or logo", "blank": "blank", ...},
    reject={"stamp", "blank"},
)

# jobs.py: one panel per job holds the job's budget
panel = judges.panel()
for crop in uncertain:
    verdict = panel.ask(judges.decider_vision, FIGURE_KIND, state="A region cropped from a page.", images=[crop.png])
    if verdict is not None:
        region.provenance.update(verdict.provenance())  # band, p, verdict, model@revision
```

HTTP only: the kit gains no model or imaging dependency; the caller passes images already encoded (PNG, JPEG, WebP).
"""

from dataclasses import dataclass

from evilflowers_worker.judges.clients import (
    CircuitBreaker,
    DeciderClient,
    Judge,
    JudgeError,
    JudgeInvalid,
    JudgeRefused,
    JudgeUnavailable,
    OllamaClient,
    RerankClient,
)
from evilflowers_worker.judges.panel import JUDGE_CALLS, JUDGE_SECONDS, Budget, Panel
from evilflowers_worker.judges.schema import Band, Binary, Choice, Question, Thresholds, Verdict
from evilflowers_worker.judges.settings import JudgeSettings

__all__ = [
    "JUDGE_CALLS",
    "JUDGE_SECONDS",
    "Band",
    "Binary",
    "Budget",
    "Choice",
    "CircuitBreaker",
    "DeciderClient",
    "Judge",
    "JudgeError",
    "JudgeInvalid",
    "JudgeRefused",
    "JudgeSettings",
    "JudgeUnavailable",
    "Judges",
    "OllamaClient",
    "Panel",
    "Question",
    "RerankClient",
    "Thresholds",
    "Verdict",
]


@dataclass(frozen=True)
class Judges:
    """The process's judge clients; ``None`` for a model that is not configured (or all of them, when disabled)."""

    decider: DeciderClient | None
    decider_vision: DeciderClient | None
    ollama: OllamaClient | None
    rerank: RerankClient | None
    settings: JudgeSettings

    @classmethod
    def from_settings(cls, settings: JudgeSettings) -> "Judges":
        if not settings.judges_enabled:
            return cls(None, None, None, None, settings)

        def breaker() -> CircuitBreaker:
            return CircuitBreaker(settings.judge_breaker_failures, settings.judge_breaker_reset)

        return cls(
            decider=DeciderClient(
                "decider",
                settings.decider_url,
                vision=False,
                model=settings.decider_model,
                timeout=settings.decider_timeout,
                revision=settings.decider_revision,
                breaker=breaker(),
            )
            if settings.decider_url
            else None,
            decider_vision=DeciderClient(
                "decider-vision",
                settings.decider_vision_url,
                vision=True,
                timeout=settings.decider_timeout,
                revision=settings.decider_vision_revision,
                breaker=breaker(),
            )
            if settings.decider_vision_url
            else None,
            ollama=OllamaClient(
                "ollama",
                settings.ollama_url,
                model=settings.ollama_model,
                timeout=settings.ollama_timeout,
                revision=settings.ollama_revision,
                breaker=breaker(),
            )
            if settings.ollama_url
            else None,
            rerank=RerankClient(
                "tei-rerank",
                settings.rerank_url,
                model=settings.rerank_model,
                timeout=settings.rerank_timeout,
                revision=settings.rerank_revision,
                api_key=settings.rerank_api_key,
                breaker=breaker(),
            )
            if settings.rerank_url
            else None,
            settings=settings,
        )

    def panel(self) -> Panel:
        """A fresh panel for one job, with the job's budget."""
        budget = Budget(self.settings.judge_budget_calls, self.settings.judge_budget_seconds)
        return Panel(budget, self.settings.judge_thresholds)
