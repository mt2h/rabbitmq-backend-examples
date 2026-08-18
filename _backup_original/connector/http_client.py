"""Trimmed port of oz-zecore-connector-service's
src/shared/infrastructure/http_client.py — same CircuitBreaker and retry/timeout
behavior, GET/POST only, no logging/redaction plumbing.

Deviation from the original for demo purposes: max_connections/max_keepalive are
constructor params here (real code hardcodes 100/20 in _get_client) so pool
exhaustion is reachable with tens of concurrent requests instead of hundreds.
Everything else (bare-int timeout applying to connect/read/write/pool alike,
CircuitBreaker default threshold=1, exponential backoff across max_retries) is
unchanged.
"""

import asyncio
import time
from typing import Any

import httpx


class HTTPError(Exception):
    def __init__(self, message: str, status_code: int, response_data: Any = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 1, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def can_execute(self) -> bool:
        current_time = time.time()
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if current_time - self.last_failure_time >= self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        else:  # HALF_OPEN
            return True

    def on_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class HTTPClient:
    def __init__(
        self,
        base_url: str,
        timeout: int | httpx.Timeout = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        circuit_breaker: CircuitBreaker | None = None,
        max_connections: int = 100,
        max_keepalive_connections: int = 20,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self._client: httpx.AsyncClient | None = None
        self._max_connections = max_connections
        self._max_keepalive_connections = max_keepalive_connections

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=httpx.Limits(
                    max_keepalive_connections=self._max_keepalive_connections,
                    max_connections=self._max_connections,
                ),
            )
        return self._client

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
    ) -> httpx.Response:
        if not self.circuit_breaker.can_execute():
            raise HTTPError(
                f"Circuit breaker is OPEN for {self.base_url}", status_code=503
            )

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        client = await self._get_client()

        for attempt in range(self.max_retries):
            try:
                response = await client.request(method=method, url=url, json=data)

                if response.status_code < 400:
                    self.circuit_breaker.on_success()
                    return response
                elif response.status_code >= 500:
                    self.circuit_breaker.on_failure()
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (2**attempt))
                        continue
                    raise HTTPError(
                        f"Server error after {self.max_retries} attempts",
                        response.status_code,
                    )
                else:
                    raise HTTPError(f"Client error: {response.text}", response.status_code)

            except httpx.RequestError as e:
                self.circuit_breaker.on_failure()
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2**attempt))
                else:
                    raise HTTPError(
                        f"Request failed after {self.max_retries} attempts: {e}", 0
                    ) from e
            except HTTPError:
                raise

        raise HTTPError(f"Request failed after {self.max_retries} attempts", 0)

    async def post(self, endpoint: str, data: dict[str, Any] | None = None) -> httpx.Response:
        return await self._make_request("POST", endpoint, data=data)

    async def close(self):
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
