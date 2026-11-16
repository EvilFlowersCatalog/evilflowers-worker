"""One job's judges: the per-job budget, the bands, the metrics, and the rule that a judge never fails the job."""

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from evilflowers_worker.judges.clients import Image, Judge, JudgeInvalid, JudgeRefused, JudgeUnavailable, State
from evilflowers_worker.judges.schema import Question, Thresholds, Verdict
from evilflowers_worker.observability.metrics import Instrument

logger = logging.getLogger(__name__)

#: ``<service>_judge_total{question, outcome, band}``. Outcomes: ``judged``, ``disabled`` (no client configured),
#: ``budget`` (the job's budget is spent), ``open`` (the client's breaker is open), ``unreachable``, ``refused``,
#: ``invalid``. ``band`` is ``none`` unless the outcome is ``judged`` and the question has thresholds.
JUDGE_CALLS = Instrument("counter", "{service}.judge", "{question}", "Questions put to a judge, by outcome and band")
#: ``<service>_judge_seconds{question, outcome}``: the time of each HTTP call made (not of a skipped question).
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

    One per job (``Judges.panel()``): it holds the job's budget. Not thread-safe; a job asks one question at a time.
    """

    budget: Budget
    thresholds: Mapping[str, Thresholds] = field(default_factory=dict)

    def ask(
        self, judge: Judge | None, question: Question, *, state: State = "", images: Sequence[Image] = ()
    ) -> Verdict | None:
        """The verdict, banded when the question has thresholds; ``None`` when no judge answered."""
        if judge is None:
            return self._skip(question, "disabled")
        if not self.budget.allows():
            return self._skip(question, "budget")
        if not judge.breaker.allow():
            return self._skip(question, "open")

        timeout = judge.timeout
        if (left := self.budget.remaining_seconds()) is not None:
            timeout = min(timeout, max(left, 0.001))
        started = time.perf_counter()
        # Stays "error" only if something other than a JudgeError escapes (a caller's bug, a Celery time limit).
        outcome = "error"
        try:
            verdict = judge.ask(question, state=state, images=images, timeout=timeout)
        except JudgeUnavailable as error:
            judge.breaker.failure()
            outcome = "unreachable"
            logger.warning("Judge %s unreachable, %s passes unjudged: %s", judge.name, question.id, error)
        except (JudgeRefused, JudgeInvalid) as error:
            # The service answered: it is up, so the breaker closes (and a half-open trial ends).
            judge.breaker.success()
            outcome = "refused" if isinstance(error, JudgeRefused) else "invalid"
            logger.warning("Judge %s %s %s, it passes unjudged: %s", judge.name, outcome, question.id, error)
        else:
            judge.breaker.success()
            outcome = "judged"
        finally:
            if outcome == "error":
                judge.breaker.release()
            elapsed = time.perf_counter() - started
            self.budget.spend(elapsed)
            JUDGE_SECONDS.record(elapsed, {"question": question.id, "outcome": outcome})

        if outcome != "judged":
            return self._skip(question, outcome)
        if (thresholds := self.thresholds.get(question.id)) is not None:
            verdict = verdict.model_copy(update={"band": thresholds.band(question, verdict)})
        JUDGE_CALLS.add(1, {"question": question.id, "outcome": outcome, "band": verdict.band or "none"})
        return verdict

    @staticmethod
    def _skip(question: Question, outcome: str) -> None:
        JUDGE_CALLS.add(1, {"question": question.id, "outcome": outcome, "band": "none"})
        return None
