"""One way to build an HTTP session for the services a worker calls."""

import requests
from pydantic import SecretStr

from evilflowers_worker.observability.identity import identity


def user_agent() -> str:
    service = identity()
    return f"evilflowers-{service.service}/{service.version}"


def session(api_key: SecretStr | None = None) -> requests.Session:
    """A session with the service's User-Agent and, when given, the bearer key.

    ``requests`` is instrumented by OpenTelemetry once telemetry is on, so every call made through it carries
    ``traceparent``.
    """
    http = requests.Session()
    http.headers["User-Agent"] = user_agent()
    if api_key is not None and (secret := api_key.get_secret_value()):
        http.headers["Authorization"] = f"Bearer {secret}"
    return http
