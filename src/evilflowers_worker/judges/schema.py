"""What a judge is asked and what it answers.

Two question shapes, as the decision services take them (``POST /v1/systemone``, knowledge#64):

- ``Choice``: one category out of ``criteria`` (``{key: description}``), answered with the key and its probability;
- ``Binary``: yes or no, answered with ``yes`` / ``no`` and the probability of that answer.

``reject`` names the answers that mean "drop the proposal" (``stamp``, ``decoration`` for a figure; ``no`` for "is
this a real figure?"; ``yes`` for "is this OCR line an error?"). Bands follow from it and the calibrated thresholds:

- the answer rejects, and ``p >= hide``  →  ``hide``;
- the answer keeps, and ``p >= keep``    →  ``keep``;
- anything else                          →  ``review`` (``disputed`` in the digitalizer).

Thresholds come from settings (``JUDGE_THRESHOLDS``, measured in knowledge#65). A question without them gets no band:
the catalog then stores ``p`` and ``decision`` and bands them itself once it has thresholds.
"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Band = Literal["keep", "review", "hide"]
#: Lower-case keys, as the services use them in ``questions`` and ``criteria``.
Key = Annotated[str, Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_]+$")]


class Choice(BaseModel):
    """One category out of ``criteria``."""

    model_config = ConfigDict(frozen=True)

    id: Key
    instructions: str = Field(min_length=1)
    #: ``{key: what the key means}``; the key is the verdict.
    criteria: dict[Key, str] = Field(min_length=2)
    #: Answers that mean "drop the proposal".
    reject: frozenset[str] = frozenset()

    @model_validator(mode="after")
    def _reject_names_criteria(self) -> "Choice":
        if unknown := self.reject - self.criteria.keys():
            raise ValueError(f"reject names answers that are not criteria: {sorted(unknown)}")
        return self


class Binary(BaseModel):
    """Yes or no."""

    model_config = ConfigDict(frozen=True)

    id: Key
    instructions: str = Field(min_length=1)
    #: ``{"no"}`` for "is this real?", ``{"yes"}`` for "is this an error?".
    reject: frozenset[Literal["yes", "no"]] = frozenset({"no"})

    @property
    def criteria(self) -> dict[str, str]:
        return {"yes": "yes", "no": "no"}


Question = Choice | Binary


class Verdict(BaseModel):
    """A judge's answer, stored as provenance on the region or line."""

    model_config = ConfigDict(frozen=True)

    #: The question's ``id`` (``figure_kind``).
    question: str
    #: The chosen key (``stamp``), or ``yes`` / ``no``.
    verdict: str
    #: Probability of ``verdict``. Calibrated for the decider services; a self-report (Ollama) or a relevance score
    #: (tei-rerank) otherwise, so band those only with thresholds measured for them.
    p: float = Field(ge=0.0, le=1.0)
    reason: str | None = None
    #: What the service says it ran (the answer's ``model``).
    model: str
    #: The weights' revision, from settings (the services do not report it).
    revision: str | None = None
    #: From the thresholds; ``None`` when the question has none yet.
    band: Band | None = None

    @property
    def model_ref(self) -> str:
        """``model@revision``: what the catalog stores as ``decider_model``."""
        return f"{self.model}@{self.revision}" if self.revision else self.model

    def region_fields(self) -> dict[str, Any]:
        """The catalog's decision fields of ``contract.Region``: ``region.model_copy(update=v.region_fields())``.

        ``band`` stays ``None`` without thresholds; the catalog bands ``p`` + ``decision`` itself once it has them.
        """
        return {"band": self.band} | self.paragraph_fields()

    def paragraph_fields(self) -> dict[str, Any]:
        """The decision fields of ``contract.Paragraph`` (no ``band`` there): ``paragraph.model_copy(update=...)``."""
        return {"p": self.p, "decision": self.verdict, "decider_model": self.model_ref}

    def provenance(self) -> dict[str, Any]:
        """The verdict as one provenance entry, keyed by the question: ``region.provenance.update(v.provenance())``."""
        return {self.question: self.model_dump(exclude_none=True)}


class Thresholds(BaseModel):
    """Calibrated thresholds of one question (knowledge#65)."""

    model_config = ConfigDict(frozen=True)

    #: ``p`` at or above which a keeping answer is kept.
    keep: float = Field(ge=0.0, le=1.0)
    #: ``p`` at or above which a rejecting answer is hidden.
    hide: float = Field(ge=0.0, le=1.0)

    def band(self, question: Question, verdict: Verdict) -> Band:
        if verdict.verdict in question.reject:
            return "hide" if verdict.p >= self.hide else "review"
        return "keep" if verdict.p >= self.keep else "review"
