"""HTTP clients of the models a judge asks. HTTP only: no model, imaging or ML dependency enters the kit.

| Client | Service | Endpoint | Takes |
|---|---|---|---|
| ``DeciderClient(vision=False)`` | ``decider`` (Laya) | ``POST /v1/systemone`` | text only (an image → 422) |
| ``DeciderClient(vision=True)`` | ``decider-vision`` (Strands) | ``POST /v1/systemone`` | state + at most 4 images |
| ``OllamaClient`` | Ollama ``elvira-assistant`` | ``POST /api/chat`` | state + images, structured JSON |
| ``RerankClient`` | ``tei-rerank`` (bge-reranker-v2-m3) | ``POST /rerank`` | a query and candidate texts |

Contracts: evilflowers-knowledge ``Cookbooks/Deploy-Decision-Models`` (knowledge#64), ``Cookbooks/Shared-Inference``.

A client raises one of three errors and nothing else for a model's failure; ``Panel`` turns each into a pass-through:

- ``JudgeUnavailable``: timeout, connection error, 429 or 5xx. Trips the client's circuit breaker.
- ``JudgeRefused``: any other non-2xx (the service is up and refused the request: a bug on our side, or an image sent
  to ``decider``). Does not trip the breaker.
- ``JudgeInvalid``: a 2xx whose body is not the expected answer. Does not trip the breaker.

A wrong call (images to a text-only client, a question shape the client cannot ask) raises ``ValueError`` before any
request: that is a bug in the caller, not a model failure.

A client and its breaker live as long as the process; every job gets its own ``Panel`` and budget.
"""

import base64
import json
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import requests
from pydantic import SecretStr, ValidationError

from evilflowers_worker.http import session
from evilflowers_worker.judges.schema import Binary, Choice, Question, Verdict

#: What a caller sends as the state: text, or an object the service serialises to JSON.
State = str | Mapping[str, Any]
#: An image: encoded bytes (PNG, JPEG, WebP), or a base64 string (a ``data:image/...;base64,`` prefix is accepted).
Image = bytes | str

#: ``decider-vision`` takes at most this many images per request.
MAX_VISION_IMAGES = 4


class JudgeError(Exception):
    """A model did not answer; the proposal passes through unjudged."""


class JudgeUnavailable(JudgeError):
    """Timeout, connection error, 429 or 5xx."""


class JudgeRefused(JudgeError):
    """A non-2xx other than 429 or 5xx: the service is up and refused the request."""


class JudgeInvalid(JudgeError):
    """A 2xx whose body is not the expected answer."""


class CircuitBreaker:
    """Stops calling a model that keeps failing, for ``reset`` seconds; then lets one trial call through.

    Closed → open after ``failures`` consecutive failures. Open → half-open after ``reset`` seconds: one call goes
    through; its success closes the breaker, its failure opens it again. Thread-safe.
    """

    def __init__(self, failures: int = 3, reset: float = 60.0, *, clock: Callable[[], float] = time.monotonic):
        self.failures = failures
        self.reset = reset
        self._clock = clock
        self._lock = threading.Lock()
        self._count = 0
        self._opened_at: float | None = None
        self._trial = False

    @property
    def state(self) -> str:
        with self._lock:
            if self._opened_at is None:
                return "closed"
            return "half-open" if self._clock() - self._opened_at >= self.reset else "open"

    def allow(self) -> bool:
        with self._lock:
            if self._opened_at is None:
                return True
            if self._clock() - self._opened_at < self.reset or self._trial:
                return False
            self._trial = True
            return True

    def success(self) -> None:
        with self._lock:
            self._count = 0
            self._opened_at = None
            self._trial = False

    def failure(self) -> None:
        with self._lock:
            self._count += 1
            if self._trial or self._count >= self.failures:
                self._opened_at = self._clock()
            self._trial = False

    def release(self) -> None:
        """A call ended without telling whether the model is up (a caller's bug, a time limit): free the trial."""
        with self._lock:
            self._trial = False


def encode_image(image: Image) -> str:
    return base64.b64encode(image).decode("ascii") if isinstance(image, bytes) else image


def _base_url(url: str, path: str) -> str:
    """Accept the service address with or without the endpoint path (env values in the cluster use both)."""
    url = url.rstrip("/")
    return url.removesuffix(path) if url.endswith(path) else url


class Judge(ABC):
    """One model behind HTTP, asked one question at a time."""

    #: The endpoint path, appended to the base URL.
    path: str

    def __init__(
        self,
        name: str,
        url: str,
        *,
        timeout: float,
        revision: str | None = None,
        api_key: SecretStr | None = None,
        breaker: CircuitBreaker | None = None,
        http: requests.Session | None = None,
    ):
        self.name = name
        self.url = _base_url(url, self.path)
        self.timeout = timeout
        self.revision = revision
        self.breaker = breaker or CircuitBreaker()
        self.http = http or session(api_key)

    @abstractmethod
    def ask(
        self, question: Question, *, state: State = "", images: Sequence[Image] = (), timeout: float | None = None
    ) -> Verdict:
        """The model's answer; raises a ``JudgeError`` when there is none."""

    def _post(self, payload: dict[str, Any], timeout: float | None) -> Any:
        try:
            response = self.http.post(f"{self.url}{self.path}", json=payload, timeout=timeout or self.timeout)
        except requests.RequestException as error:
            raise JudgeUnavailable(f"{self.name}: {error}") from error
        if response.status_code == 429 or response.status_code >= 500:
            raise JudgeUnavailable(f"{self.name}: HTTP {response.status_code}")
        if not response.ok:
            raise JudgeRefused(f"{self.name}: HTTP {response.status_code}: {response.text[:500]}")
        try:
            return response.json()
        except ValueError as error:
            raise JudgeInvalid(f"{self.name}: the answer is not JSON") from error

    def _verdict(self, question: Question, verdict: Any, p: Any, *, model: Any, reason: Any = None) -> Verdict:
        if not isinstance(verdict, str) or verdict not in question.criteria:
            raise JudgeInvalid(f"{self.name}: answer {verdict!r} is not one of {sorted(question.criteria)}")
        try:
            return Verdict(
                question=question.id,
                verdict=verdict,
                p=p,
                reason=reason if isinstance(reason, str) and reason else None,
                model=model if isinstance(model, str) and model else self.name,
                revision=self.revision,
            )
        except ValidationError as error:
            raise JudgeInvalid(f"{self.name}: {error}") from error


class DeciderClient(Judge):
    """``decider`` (text) or ``decider-vision`` (images): ``POST /v1/systemone`` with keyed questions."""

    path = "/v1/systemone"

    def __init__(self, name: str, url: str, *, vision: bool, model: str | None = None, **kwargs: Any):
        super().__init__(name, url, **kwargs)
        self.vision = vision
        #: Laya's checkpoint (``multilingual``): without it Laya auto-routes and may swap in its English checkpoint.
        self.model = model

    def ask(
        self, question: Question, *, state: State = "", images: Sequence[Image] = (), timeout: float | None = None
    ) -> Verdict:
        if images and not self.vision:
            raise ValueError(f"{self.name} takes text only; ask an image question of decider-vision")
        if len(images) > MAX_VISION_IMAGES:
            raise ValueError(f"{self.name} takes at most {MAX_VISION_IMAGES} images per request")
        spec: dict[str, Any] = {"instructions": question.instructions}
        if isinstance(question, Choice):
            spec |= {"type": "choice", "criteria": dict(question.criteria)}
        else:
            spec["type"] = "noul"
        payload: dict[str, Any] = {"state": state if isinstance(state, str) else dict(state)}
        payload["questions"] = {question.id: spec}
        if self.model:
            payload["model"] = self.model
        if images:
            payload["images"] = [encode_image(image) for image in images]

        body = self._post(payload, timeout)
        try:
            answer = body["answers"][question.id]
        except (KeyError, TypeError) as error:
            raise JudgeInvalid(f"{self.name}: no answer to {question.id!r}") from error
        if not isinstance(answer, dict):
            raise JudgeInvalid(f"{self.name}: the answer to {question.id!r} is not an object")
        verdict, p = self._read(question, answer)
        return self._verdict(question, verdict, p, model=body.get("model"), reason=answer.get("reason"))

    def _read(self, question: Question, answer: dict[str, Any]) -> tuple[Any, Any]:
        probabilities = answer.get("probabilities")
        probabilities = probabilities if isinstance(probabilities, dict) else {}
        if isinstance(question, Choice):
            choice = answer.get("choice")
            if not isinstance(choice, str):
                raise JudgeInvalid(f"{self.name}: the choice in the answer to {question.id!r} is not a string")
            return choice, probabilities.get(choice, answer.get("confidence"))
        # A ``noul`` answer: the probability of "yes"; tolerate a yes/no string with a confidence too.
        noul = answer.get("noul", answer.get("score"))
        if isinstance(noul, (int, float)) and not isinstance(noul, bool):
            return ("yes", noul) if noul >= 0.5 else ("no", 1.0 - noul)
        if isinstance(noul, str) and noul.lower() in ("yes", "no"):
            verdict = noul.lower()
            return verdict, probabilities.get(verdict, answer.get("confidence"))
        if isinstance(yes := probabilities.get("yes"), (int, float)):
            return ("yes", yes) if yes >= 0.5 else ("no", 1.0 - yes)
        raise JudgeInvalid(f"{self.name}: no yes/no probability in the answer to {question.id!r}")


#: Ollama's answer schema; ``verdict`` gets the question's keys as an enum.
_OLLAMA_SYSTEM = (
    "You check one proposal made by a document extraction pipeline. Answer only the question asked, with exactly one "
    "of the allowed answers, the probability (0 to 1) that your answer is right, and a one-sentence reason."
)


class OllamaClient(Judge):
    """Ollama ``/api/chat`` with a JSON-schema ``format``. ``p`` is the model's self-report, not calibrated."""

    path = "/api/chat"

    def __init__(self, name: str, url: str, *, model: str, **kwargs: Any):
        super().__init__(name, url, **kwargs)
        self.model = model

    def ask(
        self, question: Question, *, state: State = "", images: Sequence[Image] = (), timeout: float | None = None
    ) -> Verdict:
        answers = "\n".join(f"- {key}: {meaning}" for key, meaning in question.criteria.items())
        text = state if isinstance(state, str) else json.dumps(dict(state), ensure_ascii=False)
        user: dict[str, Any] = {
            "role": "user",
            "content": f"{question.instructions}\n\nAllowed answers:\n{answers}\n\nProposal:\n{text}",
        }
        if images:
            user["images"] = [encode_image(image) for image in images]
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": _OLLAMA_SYSTEM}, user],
            "format": {
                "type": "object",
                "properties": {
                    "verdict": {"type": "string", "enum": list(question.criteria)},
                    "p": {"type": "number", "minimum": 0, "maximum": 1},
                    "reason": {"type": "string"},
                },
                "required": ["verdict", "p", "reason"],
            },
            "stream": False,
            # Qwen3.6 thinks before answering unless told not to (Shared-Inference).
            "think": False,
            "options": {"temperature": 0},
        }
        body = self._post(payload, timeout)
        try:
            answer = json.loads(body["message"]["content"])
        except (KeyError, TypeError, ValueError) as error:
            raise JudgeInvalid(f"{self.name}: the message is not the JSON answer") from error
        if not isinstance(answer, dict):
            raise JudgeInvalid(f"{self.name}: the answer is not an object")
        return self._verdict(
            question, answer.get("verdict"), answer.get("p"), model=body.get("model"), reason=answer.get("reason")
        )


class RerankClient(Judge):
    """tei-rerank ``/rerank``: which candidate belongs to the query ("is this associated?").

    Asks ``Choice`` questions only: the state is the query (a figure's description, "see Fig. 3.2"), the criteria are
    the candidates (``{key: caption text}``). The verdict is the best-scoring key and ``p`` its relevance score, which
    is not a calibrated probability and low in absolute terms on Slovak (Shared-Inference): band it only with
    thresholds measured for it.
    """

    path = "/rerank"

    def __init__(self, name: str, url: str, *, model: str = "tei-rerank", **kwargs: Any):
        super().__init__(name, url, **kwargs)
        self.model = model

    def ask(
        self, question: Question, *, state: State = "", images: Sequence[Image] = (), timeout: float | None = None
    ) -> Verdict:
        if isinstance(question, Binary):
            raise ValueError(f"{self.name} ranks candidates: ask it a Choice question")
        if images:
            raise ValueError(f"{self.name} takes text only")
        if not isinstance(state, str) or not state:
            raise ValueError(f"{self.name} needs the query as a non-empty string state")
        keys = list(question.criteria)
        body = self._post({"query": state, "texts": list(question.criteria.values()), "raw_scores": False}, timeout)
        try:
            best = max(body, key=lambda row: row["score"])
            verdict, score = keys[best["index"]], float(best["score"])
        except (KeyError, TypeError, ValueError, IndexError) as error:
            raise JudgeInvalid(f"{self.name}: the answer is not a list of {{index, score}}") from error
        return self._verdict(question, verdict, min(max(score, 0.0), 1.0), model=self.model)
