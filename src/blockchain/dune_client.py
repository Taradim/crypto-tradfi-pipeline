"""Direct (non-Spice) Dune API client.

This module provides a small, production-friendly wrapper around Dune's REST API:
- Execute a query (optionally with parameters)
- Poll execution status with backoff
- Fetch results and return rows as dictionaries

Design notes:
- Uses only the Python standard library (no `requests` dependency).
- Keeps a narrow interface so we can later swap implementation with Spice.
- Separates "query id parsing" from "network calls" to ease testing.
"""

from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Final, Mapping, MutableMapping, Sequence

from .dune_config import find_project_root

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL: Final[str] = "https://api.dune.com/api/v1"
_DEFAULT_USER_AGENT: Final[str] = "setup-s3-local/0.1 (dune_api_client)"


class DuneApiError(RuntimeError):
    """Raised when the Dune API returns an error response or unexpected shape."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        response_text: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.response_text = response_text


class DuneApiTimeoutError(DuneApiError):
    """Raised when polling exceeds a timeout."""


@dataclass(frozen=True, slots=True)
class DuneExecution:
    """Represents a Dune query execution."""

    query_id: int
    execution_id: str


def parse_query_id(query: str | int) -> int:
    """Parse a Dune query id from a URL or integer/string id.

    Supported inputs:
    - 4388
    - "4388"
    - "https://dune.com/queries/4388"
    - "https://dune.com/queries/4388/anything"
    """

    if isinstance(query, int):
        if query <= 0:
            raise ValueError("query id must be > 0")
        return query

    q = str(query).strip()
    if not q:
        raise ValueError("query must be a non-empty string or integer id")

    if q.isdigit():
        value = int(q)
        if value <= 0:
            raise ValueError("query id must be > 0")
        return value

    parsed = urllib.parse.urlparse(q)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Unsupported query format: {query!r}")

    # Expected path: /queries/<id>(/...)
    parts = [p for p in parsed.path.split("/") if p]
    try:
        idx = parts.index("queries")
        candidate = parts[idx + 1]
    except (ValueError, IndexError) as e:
        raise ValueError(f"Could not parse query id from URL: {query!r}") from e

    if not candidate.isdigit():
        raise ValueError(f"Could not parse numeric query id from URL: {query!r}")
    return int(candidate)


def default_secrets_path(project_root: Any | None = None) -> Any:
    """Return the default `.dlt/secrets.toml` path (project-root resolved)."""

    root = project_root or find_project_root()
    return root / ".dlt" / "secrets.toml"


def load_dune_api_key_from_secrets(*, secrets_path: Any | None = None) -> str:
    """Load Dune API key from `.dlt/secrets.toml` using the agreed convention.

    Expected structure:

    ```toml
    [dune_source]
    api_key = "YOUR_DUNE_API_KEY"
    ```
    """

    import tomllib  # stdlib in Python 3.11+

    path = secrets_path or default_secrets_path()
    try:
        with open(path, "rb") as f:
            secrets = tomllib.load(f)
    except FileNotFoundError as e:
        raise DuneApiError(
            f"Secrets file not found at {path}. Create `.dlt/secrets.toml` first."
        ) from e

    try:
        api_key = secrets["dune_source"]["api_key"]
    except KeyError as e:
        raise DuneApiError(
            "Missing `[dune_source].api_key` in `.dlt/secrets.toml`."
        ) from e

    if not isinstance(api_key, str) or not api_key.strip():
        raise DuneApiError("Invalid Dune API key: expected a non-empty string.")
    return api_key.strip()


class DuneApiClient:
    """Minimal Dune API client (direct HTTP)."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout_seconds: float = 30.0,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
        backoff_max_seconds: float = 20.0,
        user_agent: str = _DEFAULT_USER_AGENT,
    ) -> None:
        if not api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if backoff_base_seconds <= 0:
            raise ValueError("backoff_base_seconds must be > 0")
        if backoff_max_seconds <= 0:
            raise ValueError("backoff_max_seconds must be > 0")

        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_base_seconds = backoff_base_seconds
        self._backoff_max_seconds = backoff_max_seconds
        self._user_agent = user_agent

    def execute_query(
        self, *, query_id: int, parameters: Mapping[str, Any] | None = None
    ) -> DuneExecution:
        """Start a Dune execution for the given query id."""

        path = f"/query/{query_id}/execute"
        payload_attempts = self._build_execute_payload_attempts(parameters or {})

        last_error: DuneApiError | None = None
        for payload in payload_attempts:
            try:
                data = self._request_json(method="POST", path=path, body=payload)
                execution_id = _extract_execution_id(data)
                return DuneExecution(query_id=query_id, execution_id=execution_id)
            except DuneApiError as e:
                # If the request fails due to payload shape, try the next format.
                last_error = e
                if e.status_code in (400, 422):
                    logger.debug(
                        "Execute attempt failed (payload variant). status=%s url=%s",
                        e.status_code,
                        e.url,
                    )
                    continue
                raise

        raise last_error or DuneApiError("Failed to execute query: unknown error.")

    def get_execution_status(self, *, execution_id: str) -> Mapping[str, Any]:
        """Fetch execution status payload."""

        path = f"/execution/{execution_id}/status"
        return self._request_json(method="GET", path=path, body=None)

    def get_execution_results(self, *, execution_id: str) -> Mapping[str, Any]:
        """Fetch execution results payload."""

        path = f"/execution/{execution_id}/results"
        return self._request_json(method="GET", path=path, body=None)

    def wait_for_execution(
        self,
        *,
        execution_id: str,
        poll_timeout_seconds: float = 300.0,
        poll_interval_seconds: float = 2.0,
        poll_max_interval_seconds: float = 15.0,
    ) -> None:
        """Poll until execution completes or fails.

        Notes:
        - We do not assume a single canonical schema for status payloads.
        - We look for a few common fields and treat unknown states conservatively.
        """

        start = time.monotonic()
        interval = max(0.2, poll_interval_seconds)
        while True:
            elapsed = time.monotonic() - start
            if elapsed > poll_timeout_seconds:
                raise DuneApiTimeoutError(
                    f"Polling timed out after {poll_timeout_seconds:.1f}s",
                    url=f"{self._base_url}/execution/{execution_id}/status",
                )

            status = self.get_execution_status(execution_id=execution_id)
            state = _extract_execution_state(status)
            if state in {"QUERY_STATE_COMPLETED", "COMPLETED", "SUCCESS"}:
                return
            if state in {"QUERY_STATE_FAILED", "FAILED", "ERROR"}:
                raise DuneApiError(
                    f"Execution failed with state={state}",
                    url=f"{self._base_url}/execution/{execution_id}/status",
                    response_text=json.dumps(status)[:10_000],
                )

            # Pending/running/unknown state: keep polling with capped exponential backoff.
            sleep_s = min(poll_max_interval_seconds, interval)
            logger.debug(
                "Execution %s state=%s; sleeping %.2fs", execution_id, state, sleep_s
            )
            time.sleep(sleep_s)
            interval = min(poll_max_interval_seconds, interval * 1.5)

    def run_query_rows(
        self,
        *,
        query: str | int,
        parameters: Mapping[str, Any] | None = None,
        poll_timeout_seconds: float = 300.0,
    ) -> list[Mapping[str, Any]]:
        """Execute a query and return its result rows as dictionaries."""

        query_id = parse_query_id(query)
        execution = self.execute_query(query_id=query_id, parameters=parameters)
        self.wait_for_execution(
            execution_id=execution.execution_id,
            poll_timeout_seconds=poll_timeout_seconds,
        )
        payload = self.get_execution_results(execution_id=execution.execution_id)
        return _extract_rows(payload)

    def _build_execute_payload_attempts(
        self, parameters: Mapping[str, Any]
    ) -> list[Mapping[str, Any] | None]:
        """Build payload variants to maximize compatibility with evolving API shapes.

        We try a small number of known shapes. If parameters are empty, we send no body.
        """

        if not parameters:
            return [None]

        # Variant A (common): {"query_parameters": {"date": "2025-01-01"}}
        variant_a: Mapping[str, Any] = {"query_parameters": dict(parameters)}

        # Variant B (sometimes used): {"query_parameters": [{"name": "...", "value": ...}, ...]}
        variant_b_items: list[Mapping[str, Any]] = [
            {"name": k, "value": v} for k, v in parameters.items()
        ]
        variant_b: Mapping[str, Any] = {"query_parameters": variant_b_items}

        return [variant_a, variant_b]

    def _request_json(
        self, *, method: str, path: str, body: Mapping[str, Any] | None
    ) -> Mapping[str, Any]:
        url = f"{self._base_url}{path}"

        data: bytes | None = None
        headers: MutableMapping[str, str] = {
            # Dune docs use `X-Dune-API-Key` (case-insensitive in HTTP).
            "X-Dune-API-Key": self._api_key,
            "Accept": "application/json",
        }
        if self._user_agent:
            headers["User-Agent"] = self._user_agent

        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(
            url=url, method=method, data=data, headers=dict(headers)
        )
        return self._with_retries(lambda: self._do_json_request(req), url=url)

    def _do_json_request(self, req: urllib.request.Request) -> Mapping[str, Any]:
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                raw = resp.read()
                try:
                    decoded = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError as e:
                    raise DuneApiError(
                        "Failed to decode JSON response",
                        status_code=getattr(resp, "status", None),
                        url=req.full_url,
                        response_text=raw[:10_000].decode("utf-8", errors="replace"),
                    ) from e

                if not isinstance(decoded, dict):
                    raise DuneApiError(
                        "Unexpected response shape: expected JSON object",
                        status_code=getattr(resp, "status", None),
                        url=req.full_url,
                        response_text=str(decoded)[:10_000],
                    )
                return decoded
        except urllib.error.HTTPError as e:
            body = None
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001 - best effort to capture body
                body = None
            raise DuneApiError(
                f"HTTP error from Dune API: {e.code}",
                status_code=e.code,
                url=req.full_url,
                response_text=body,
            ) from e
        except urllib.error.URLError as e:
            raise DuneApiError(
                "Network error while calling Dune API",
                url=req.full_url,
                response_text=str(e.reason),
            ) from e

    def _with_retries(self, fn: Any, *, url: str) -> Mapping[str, Any]:
        attempt = 0
        while True:
            try:
                return fn()
            except DuneApiError as e:
                attempt += 1
                retryable = e.status_code in (None, 408, 429, 500, 502, 503, 504)
                if not retryable or attempt > self._max_retries:
                    raise

                # Exponential backoff with jitter.
                base = self._backoff_base_seconds * (2 ** (attempt - 1))
                sleep_s = min(self._backoff_max_seconds, base)
                sleep_s = sleep_s * (0.5 + random.random())  # jitter in [0.5, 1.5)
                logger.warning(
                    "Retrying Dune API call (attempt=%s/%s) in %.2fs: %s",
                    attempt,
                    self._max_retries,
                    sleep_s,
                    url,
                )
                time.sleep(sleep_s)


def _extract_execution_id(payload: Mapping[str, Any]) -> str:
    # Common: {"execution_id": "..."} or {"execution_id": 123}
    raw = payload.get("execution_id")
    if raw is None:
        # Sometimes nested: {"result": {"execution_id": "..."}}
        result = payload.get("result")
        if isinstance(result, dict):
            raw = result.get("execution_id")
    if raw is None:
        raise DuneApiError(
            "Missing `execution_id` in Dune execute response",
            response_text=json.dumps(payload)[:10_000],
        )
    return str(raw)


def _extract_execution_state(payload: Mapping[str, Any]) -> str:
    # Known patterns:
    # - {"state": "QUERY_STATE_COMPLETED"}
    # - {"status": "COMPLETED"}
    # - {"result": {"state": "..."}}
    candidates: Sequence[str] = ("state", "status")
    for key in candidates:
        v = payload.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    result = payload.get("result")
    if isinstance(result, dict):
        for key in candidates:
            v = result.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return "UNKNOWN"


def _extract_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    # Typical pattern: {"result": {"rows": [...], "metadata": {...}}}
    result = payload.get("result")
    if isinstance(result, dict):
        rows = result.get("rows")
        if isinstance(rows, list):
            normalized: list[Mapping[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    raise DuneApiError(
                        "Unexpected row shape: expected object/dict",
                        response_text=str(row)[:10_000],
                    )
                normalized.append(row)
            return normalized

    # Fallback: some APIs may return rows at top-level.
    rows = payload.get("rows")
    if isinstance(rows, list):
        normalized = []
        for row in rows:
            if not isinstance(row, dict):
                raise DuneApiError(
                    "Unexpected row shape: expected object/dict",
                    response_text=str(row)[:10_000],
                )
            normalized.append(row)
        return normalized

    raise DuneApiError(
        "Could not find rows in results payload",
        response_text=json.dumps(payload)[:10_000],
    )
