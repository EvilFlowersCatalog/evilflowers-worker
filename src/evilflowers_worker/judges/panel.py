"""One job's judges: the per-job budget, the bands, the metrics, and the rule that a judge never fails the job."""

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from evilflowers_worker.judges.clients import (
    MAX_BATCH_STATES,
    Image,
    Judge,
    JudgeInvalid,
    JudgeRefused,
    JudgeUnavailable,
    State,
)
from evilflowers_worker.judges.schema import Question, Thresholds, Verdict
from evilflowers_worker.observability.metrics import Instrument

logger = logging.getLogger(__name__)
T = TypeVar("T")

#: ``<service>_judge_total{question, outcome, band}``, one per state asked about (a batch counts each state).
#: Outcomes: ``judged``, ``disabled`` (no client configured), ``budget`` (the job's budget is spent), ``open`` (the
#: client's breaker is open), ``unreachable``, ``refused``, ``invalid``. ``band`` is ``none`` unless the outcome is
#: ``judged`` and the question has thresholds.
JUDGE_CALLS = Instrument("counter", "{service}.judge", "{question}", "Questions put to a judge, by outcome and band")
#: ``<service>_judge_seconds{question, outcome}``: the time of each HTTP call made (not of a skipped question); a
#: batch request records once, with ``batch="true"``.
JUDGE_SECONDS = Instrument("histogram", "{service}.judge.seconds", "s", "Time of one call to a judge")


@dataclass
class Budget:
    """What one job may spend on judges, so a book never monopolises app01. ``0`` turns a limit off."""

    calls: int
    seconds: float
    used_calls: int = 0
    used_seconds: float = 0.0

    def allows(self) -> bool:
        return (not self.calls or self.used_calls < self.calls) and (
            not self.seconds or self.used_seconds < self.seconds
        )

    def remaining_seconds(self) -> float | None:
        return max(self.seconds - self.used_seconds, 0.0) if self.seconds else None

    def spend(self, seconds: float) -> None:
        self.used_calls += 1
        self.used_seconds += seconds


@dataclass
class Panel:
    """Asks a judge about one proposal; ``None`` means "pass it through unjudged", never an exception.

    One per job (``Judges.panel()``): it holds the job's budget. Not thread-safe; a job asks one question at a time
    (``ask``), or one question about many states (``ask_batch``).
    """

    budget: Budget
    thresholds: Mapping[str, Thresholds] = field(default_factory=dict)

    def ask(
        self, judge: Judge | None, question: Question, *, state: State = "", images: Sequence[Image] = ()
    ) -> Verdict | None:
        """The verdict, banded when the question has thresholds; ``None`` when no judge answered."""
        if judge is None:
            return self._skip(question, "disabled")
        if (outcome := self._blocked(judge)) is not None:
            return self._skip(question, outcome)
        verdict, outcome = self._call(
            judge, question, lambda timeout: judge.ask(question, state=state, images=images, timeout=timeout),
            what=question.id, batch=False,
        )  # fmt: skip
        return self._judged(question, verdict) if verdict is not None else self._skip(question, outcome)

    def ask_batch(self, judge: Judge | None, question: Question, states: Sequence[State]) -> list[Verdict | None]:
        """One verdict or ``None`` per state, in the order of ``states``; asked ``MAX_BATCH_STATES`` at a time.

        Each request is one call of the budget and one success or failure of the breaker. A request that gets no
        answer passes all its states through unjudged; an unreadable answer passes only its own state through. Once
        the budget is spent, the remaining states are skipped. ``decider`` only: another judge raises ``ValueError``.
        """
        if judge is None:
            return [self._skip(question, "disabled") for _ in states]
        if not judge.batches:
            raise ValueError(f"{judge.name} asks one state per request; only decider serves /v1/systemone/batch")
        verdicts: list[Verdict | None] = []
        for start in range(0, len(states), MAX_BATCH_STATES):
            chunk = states[start : start + MAX_BATCH_STATES]
            if (outcome := self._blocked(judge)) is not None:
                verdicts += [self._skip(question, outcome) for _ in chunk]
                continue
            answers, outcome = self._call(
                judge, question, lambda timeout: judge.ask_batch(question, chunk, timeout=timeout),  # noqa: B023
                what=f"{question.id} ({len(chunk)} states)", batch=True,
            )  # fmt: skip
            if answers is None:
                verdicts += [self._skip(question, outcome) for _ in chunk]
                continue
            for answer in answers:
                if isinstance(answer, JudgeInvalid):
                    logger.warning("Judge %s invalid %s, one state passes unjudged: %s", judge.name, question.id,
                                   answer)  # fmt: skip
                    verdicts.append(self._skip(question, "invalid"))
                else:
                    verdicts.append(self._judged(question, answer))
        return verdicts

    def _blocked(self, judge: Judge) -> str | None:
        """Why the judge may not be asked now (``budget``, ``open``), or ``None``. Takes a half-open trial if any."""
        if not self.budget.allows():
            return "budget"
        if not judge.breaker.allow():
            return "open"
        return None

    def _call(
        self, judge: Judge, question: Question, call: Callable[[float], T], *, what: str, batch: bool
    ) -> tuple[T | None, str]:
        """One HTTP request: its result and ``judged``, or ``None`` and the outcome; breaker, budget and seconds."""
        timeout = judge.timeout
        if (left := self.budget.remaining_seconds()) is not None:
            timeout = min(timeout, max(left, 0.001))
        started = time.perf_counter()
        # Stays "error" only if something other than a JudgeError escapes (a caller's bug, a Celery time limit).
        outcome = "error"
        result: T | None = None
        try:
            result = call(timeout)
        except JudgeUnavailable as error:
            judge.breaker.failure()
            outcome = "unreachable"
            logger.warning("Judge %s unreachable, %s passes unjudged: %s", judge.name, what, error)
        except (JudgeRefused, JudgeInvalid) as error:
            # The service answered: it is up, so the breaker closes (and a half-open trial ends).
            judge.breaker.success()
            outcome = "refused" if isinstance(error, JudgeRefused) else "invalid"
            logger.warning("Judge %s %s %s, it passes unjudged: %s", judge.name, outcome, what, error)
        else:
            judge.breaker.success()
            outcome = "judged"
        finally:
            if outcome == "error":
                judge.breaker.release()
            elapsed = time.perf_counter() - started
            self.budget.spend(elapsed)
            attributes = {"question": question.id, "outcome": outcome}
            JUDGE_SECONDS.record(elapsed, attributes | {"batch": "true"} if batch else attributes)
        return result, outcome

    def _judged(self, question: Question, verdict: Verdict) -> Verdict:
        if (thresholds := self.thresholds.get(question.id)) is not None:
            verdict = verdict.model_copy(update={"band": thresholds.band(question, verdict)})
        JUDGE_CALLS.add(1, {"question": question.id, "outcome": "judged", "band": verdict.band or "none"})
        return verdict

    @staticmethod
    def _skip(question: Question, outcome: str) -> None:
        JUDGE_CALLS.add(1, {"question": question.id, "outcome": outcome, "band": "none"})
        return None
