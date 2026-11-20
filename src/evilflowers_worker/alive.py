"""``evilflowers-worker-alive``: the exec liveness probe of a worker (evilflowers-worker#11).

The worker touches ``WORKER_HEARTBEAT_FILE`` every ``WORKER_HEARTBEAT_INTERVAL`` seconds (``heartbeat``). This command
exits 0 while the file is fresh and 1 once it is older than ``--max-age`` or missing, so a pod needs neither a shell
nor ``find``::

    livenessProbe:
      exec:
        command: ["evilflowers-worker-alive", "--max-age", "90"]

Standard library only: the probe runs every ``periodSeconds`` and must not import Celery.
"""

import argparse
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path

#: ``/tmp`` is an emptyDir in every Deployment (the root is read-only).
DEFAULT_FILE = Path("/tmp/evilflowers-worker.heartbeat")
DEFAULT_INTERVAL = 30.0
#: The recommended staleness threshold, in intervals: two touches may be missed before the worker counts as dead.
STALE_AFTER_INTERVALS = 3


def heartbeat_age(path: Path, now: float | None = None) -> float | None:
    """Seconds since the worker last touched ``path``; ``None`` when there is no file."""
    try:
        touched = path.stat().st_mtime
    except FileNotFoundError:
        return None
    return (time.time() if now is None else now) - touched


def main(argv: Sequence[str] | None = None) -> int:
    interval = float(os.environ.get("WORKER_HEARTBEAT_INTERVAL") or DEFAULT_INTERVAL)
    parser = argparse.ArgumentParser(
        prog="evilflowers-worker-alive", description="Exit 0 while the worker's heartbeat file is fresh, else 1."
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=Path(os.environ.get("WORKER_HEARTBEAT_FILE") or DEFAULT_FILE),
        help="the heartbeat file (default: $WORKER_HEARTBEAT_FILE or %(default)s)",
    )
    parser.add_argument(
        "--max-age",
        type=float,
        default=STALE_AFTER_INTERVALS * interval,
        help="seconds after which the file is stale (default: 3 x $WORKER_HEARTBEAT_INTERVAL = %(default).0f)",
    )
    args = parser.parse_args(argv)

    age = heartbeat_age(args.file)
    if age is None:
        print(f"dead: no heartbeat file {args.file}", file=sys.stderr)
        return 1
    if age > args.max_age:
        print(f"dead: {args.file} last touched {age:.0f} s ago (max {args.max_age:.0f} s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
