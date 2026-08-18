"""Reproduces the queue-congestion mechanism from
oz-connector-load-test-queue-congestion.md at a scale you can run locally.

/endpoint1 == DeliveryCapacityServiceClient pattern:
    HTTPClient(timeout=BAD_TIMEOUT, circuit_breaker=CircuitBreaker(failure_threshold=999999))
    -> a bare int applies to connect/read/write AND pool-wait alike, and the
       breaker never opens.

/endpoint2 == stock_service_client.py pattern:
    HTTPClient(timeout=httpx.Timeout(connect=.., read=.., write=.., pool=..),
               circuit_breaker=CircuitBreaker(failure_threshold=GOOD_CB_THRESHOLD))
    -> pool-wait fails fast and independently of read/write, and the breaker
       actually opens.

WORKER_CONCURRENCY mirrors Celery's `--concurrency=2` with
worker_prefetch_multiplier=1 + task_acks_late=True: a fixed number of "slots"
that each in-flight message occupies for the full duration of the downstream
call. A message queued behind a full set of slots does not get picked up until
one frees -- exactly what the real service does per pod.
"""

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI

from http_client import CircuitBreaker, HTTPClient, HTTPError

DATABASE_URL = os.environ["DATABASE_URL"]
DOWNSTREAM_URL = os.environ["DOWNSTREAM_URL"]

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "2"))
POOL_MAX_CONNECTIONS = int(os.environ.get("POOL_MAX_CONNECTIONS", "5"))

BAD_TIMEOUT = int(os.environ.get("BAD_TIMEOUT", "20"))

GOOD_TIMEOUT = httpx.Timeout(
    connect=float(os.environ.get("GOOD_CONNECT_TIMEOUT", "2")),
    read=float(os.environ.get("GOOD_READ_TIMEOUT", "6")),
    write=float(os.environ.get("GOOD_WRITE_TIMEOUT", "6")),
    pool=float(os.environ.get("GOOD_POOL_TIMEOUT", "1")),
)
GOOD_CB_THRESHOLD = int(os.environ.get("GOOD_CB_THRESHOLD", "3"))

db_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    yield
    await bad_client.close()
    await good_client.close()
    await db_pool.close()


app = FastAPI(lifespan=lifespan)

slot_endpoint1 = asyncio.Semaphore(WORKER_CONCURRENCY)
slot_endpoint2 = asyncio.Semaphore(WORKER_CONCURRENCY)

bad_client = HTTPClient(
    DOWNSTREAM_URL,
    timeout=BAD_TIMEOUT,
    circuit_breaker=CircuitBreaker(failure_threshold=999999),
    max_connections=POOL_MAX_CONNECTIONS,
    max_keepalive_connections=POOL_MAX_CONNECTIONS,
)

good_client = HTTPClient(
    DOWNSTREAM_URL,
    timeout=GOOD_TIMEOUT,
    circuit_breaker=CircuitBreaker(failure_threshold=GOOD_CB_THRESHOLD),
    max_connections=POOL_MAX_CONNECTIONS,
    max_keepalive_connections=POOL_MAX_CONNECTIONS,
)


async def _process(endpoint_name: str, slot: asyncio.Semaphore, client: HTTPClient, message_id: str) -> dict:
    row_id = await db_pool.fetchval(
        "INSERT INTO request_log (message_id, endpoint) VALUES ($1, $2) RETURNING id",
        message_id,
        endpoint_name,
    )

    async with slot:
        await db_pool.execute(
            "UPDATE request_log SET slot_acquired_at = now() WHERE id = $1", row_id
        )

        call_started = time.monotonic()
        await db_pool.execute(
            "UPDATE request_log SET call_started_at = now() WHERE id = $1", row_id
        )

        status = "success"
        error_type = None
        error_message = None
        try:
            await client.post(
                "/api/v1/delivery-capacity/sync", data={"message_id": message_id}
            )
        except HTTPError as e:
            status = "error"
            error_type = type(e).__name__
            error_message = str(e)
        except Exception as e:  # circuit-breaker-open path raises HTTPError too, but be safe
            status = "error"
            error_type = type(e).__name__
            error_message = str(e)

        elapsed = time.monotonic() - call_started

        await db_pool.execute(
            """
            UPDATE request_log
            SET call_ended_at = now(), status = $2, error_type = $3, error_message = $4
            WHERE id = $1
            """,
            row_id,
            status,
            error_type,
            error_message,
        )

    return {
        "message_id": message_id,
        "endpoint": endpoint_name,
        "status": status,
        "error_type": error_type,
        "error_message": error_message,
        "elapsed_seconds": round(elapsed, 3),
        "circuit_breaker_state": client.circuit_breaker.state,
    }


@app.post("/endpoint1")
async def endpoint1(payload: dict | None = None):
    message_id = (payload or {}).get("message_id") or str(uuid.uuid4())
    return await _process("endpoint1", slot_endpoint1, bad_client, message_id)


@app.post("/endpoint2")
async def endpoint2(payload: dict | None = None):
    message_id = (payload or {}).get("message_id") or str(uuid.uuid4())
    return await _process("endpoint2", slot_endpoint2, good_client, message_id)


@app.get("/stats")
async def stats():
    rows = await db_pool.fetch(
        """
        SELECT
            endpoint,
            count(*) FILTER (WHERE slot_acquired_at IS NULL) AS queued,
            count(*) FILTER (WHERE slot_acquired_at IS NOT NULL AND call_ended_at IS NULL) AS in_flight,
            count(*) FILTER (WHERE status = 'success') AS success,
            count(*) FILTER (WHERE status = 'error') AS error
        FROM request_log
        GROUP BY endpoint
        """
    )
    return {
        "worker_concurrency_per_endpoint": WORKER_CONCURRENCY,
        "circuit_breaker": {
            "endpoint1": bad_client.circuit_breaker.state,
            "endpoint2": good_client.circuit_breaker.state,
        },
        "by_endpoint": {r["endpoint"]: dict(r) for r in rows},
    }
