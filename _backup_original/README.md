# oz-timeout-repro

Reproduces, at a runnable scale, the mechanism hypothesized in
`oz-connector-load-test-queue-congestion.md` (section 5): that queue congestion
in `oz-zecore-connector-service` comes from bare-int HTTP timeouts (conflating
connect/read/write/pool-wait into one number) plus a circuit breaker configured
to never open, combined with Celery's `prefetch=1` + `acks_late` + fixed
`--concurrency`.

**All numbers below are from actually running this stack, not predicted.**

Grounded in the real code (`oz-zecore-connector-service`), not guessed:
- `connector/http_client.py` is a trimmed port of
  `src/shared/infrastructure/http_client.py` (same `CircuitBreaker`, same
  retry/backoff loop).
- `/endpoint1` mirrors `DeliveryCapacityServiceClient`
  (`delivery_capacity_service_client.py:196-200`): `timeout=BAD_TIMEOUT` (bare
  int) + `CircuitBreaker(failure_threshold=999999)`.
- `/endpoint2` mirrors `stock_service_client.py`'s pattern: split
  `httpx.Timeout(connect=, read=, write=, pool=)` + a circuit breaker that can
  actually open.
- `WORKER_CONCURRENCY` (default 2) is a semaphore per endpoint standing in for
  Celery's `--concurrency=2` prefork pool: a message occupies a slot for the
  *entire* downstream call and no new message is picked up until a slot frees
  -- what `worker_prefetch_multiplier=1` + `task_acks_late=True` do in the real
  service. Confirmed live in the repo:
  `process_zecore_delivery_capacity_update_task` wraps its async handler in
  `asyncio.run(...)`, so even though the HTTP client is async, each Celery
  worker process blocks synchronously on that call -- the async client does
  **not** buy free concurrency across messages in this path.

Numbers are scaled down 10-25x from production (`BAD_TIMEOUT=20s` instead of
500s, downstream sleeps 8s instead of minutes) so a run finishes in seconds,
keeping the same ratios (see comments in `docker-compose.yml`).

> **Important, honest caveat found while validating this**: `POOL_MAX_CONNECTIONS`
> is a per-`HTTPClient`-instance limit inside one process. Each Celery worker
> in the real service only runs one task at a time per prefork process
> (`asyncio.run()` blocks it), so the "pool exhaustion" symptom below is not
> what caused the observed queue congestion in the real incident -- the
> `WORKER_CONCURRENCY`/slot mechanism (finding further below) is what did.
> Pool separation is still worth fixing for defense-in-depth (any code path
> that fires several concurrent `await`s against the same client -- FastAPI
> request handlers, `asyncio.gather` over a batch -- would hit it), but don't
> present it as *the* root cause of the queue congestion itself.

## Run it

```bash
cd oz-timeout-repro
docker compose up -d --build db downstream connector
```

### 1. Worker-slot exhaustion (the queue-congestion mechanism)

```bash
docker compose run --rm loadtest --endpoint endpoint1 --num 12
docker compose run --rm loadtest --endpoint endpoint2 --num 12
```

Measured: both take **~48s to drain 12 messages** (2 slots x 8s/call x 6
batches). Under a merely slow-but-healthy downstream, splitting the timeout
changes nothing -- confirms the doc's insight is really about the
`WORKER_CONCURRENCY` slots (`prefetch=1`/`acks_late`) holding up new messages,
not the timeout value by itself. Watch the `[stats]` lines the load generator
prints every second -- `queued` is the direct analog of RabbitMQ queue depth
(`zecore_connector_event_listener_queue_high` in the real incident): messages
that arrived but haven't been picked up by a free worker slot. `in_flight` is
messages currently occupying a slot, blocked on the downstream call. You'll see
`queued` drop by exactly `WORKER_CONCURRENCY` every `SLOW_SECONDS` -- that
staircase *is* the congestion.

### 2. Circuit breaker: does it actually open?

Flip the downstream to fail instantly instead of hanging (runtime toggle, no
container recreate needed):

```bash
curl -s -X POST http://localhost:9000/mode -d '{"mode":"erroring"}'

docker compose run --rm loadtest --endpoint endpoint1 --num 6
docker compose run --rm loadtest --endpoint endpoint1 --num 6   # run again
curl -s http://localhost:8000/stats | python3 -m json.tool      # circuit_breaker.endpoint1 == CLOSED
curl -s http://localhost:9000/stats                             # total_requests keeps climbing every run

docker compose run --rm loadtest --endpoint endpoint2 --num 6
docker compose run --rm loadtest --endpoint endpoint2 --num 6   # run again
curl -s http://localhost:8000/stats | python3 -m json.tool      # circuit_breaker.endpoint2 == OPEN
curl -s http://localhost:9000/stats                             # total_requests barely moves after it opens

curl -s -X POST http://localhost:9000/mode -d '{"mode":"slow"}'  # reset for other tests
```

Measured: with `endpoint1`, downstream's `total_requests` went **6 -> 24** for
one 6-message burst (6 messages x 3 retries each; breaker stayed `CLOSED`
throughout -- `failure_threshold=999999` never trips). With `endpoint2`, after
the breaker opened, three more 6-message bursts (18 messages) added **zero**
extra downstream requests -- confirmed by `downstream`'s own `total_requests`
staying flat. That's the concrete difference a real `failure_threshold` makes:
it stops hammering a dead downstream instead of retrying it forever.

> Don't rely on `docker compose up -e DOWNSTREAM_MODE=... service` across
> separate commands -- compose re-resolves env interpolation on every
> invocation (including `run`) and will silently recreate `downstream` back to
> its compose-file default if the var isn't set in *that* shell. That's why
> mode is a runtime `/mode` toggle instead.

### 3. Pool-wait conflated with call-duration (secondary finding, see caveat above)

Temporarily raise `WORKER_CONCURRENCY` above `POOL_MAX_CONNECTIONS` (e.g. 8 vs
5) to force pool contention within one process, downstream in `slow` mode:

```bash
# in docker-compose.yml, set WORKER_CONCURRENCY: "8", then:
docker compose up -d --build connector
docker compose run --rm loadtest --endpoint endpoint1 --num 8
docker compose run --rm loadtest --endpoint endpoint2 --num 8
# revert WORKER_CONCURRENCY to "2" and `docker compose up -d --build connector` when done
```

Measured: `endpoint1` (pool-wait capped at the same 20s as everything else) --
**all 8 succeeded**, but the 3 that had to wait for a connection took ~16s
instead of ~8s, indistinguishable from "the downstream was just slow" (no
distinct error, no signal of *which* problem occurred). `endpoint2` (pool
timeout=1s) -- **5 succeeded, 3 failed fast** with an explicit pool-timeout
`HTTPError` in ~8s total (across 3 retries), and its own breaker opened as a
side effect (it can't tell "downstream is bad" from "our pool is too small" --
worth knowing, not necessarily a problem).

## Reproducing multiple replicas hammering the same downstream

The real incident involved several KEDA-scaled pods all blocked on the same
degraded downstream simultaneously. To approximate that here, drop the fixed
host port on `connector` (comment out its `ports:` in `docker-compose.yml`,
since a fixed port can't be shared across replicas) and scale it:

```bash
docker compose up -d --build --scale connector=3 db downstream connector
```

Docker's embedded DNS round-robins `connector` across the 3 replicas, so
several parallel `loadtest` runs spread messages across them -- each replica
with its own independent `WORKER_CONCURRENCY` slots, all calling the same
`downstream`. Each replica only logs/serves `/stats` for requests routed to
it, so aggregate across replicas by querying Postgres directly:

```bash
docker compose exec db psql -U repro -d repro -c \
  "SELECT endpoint, status, count(*), avg(extract(epoch from call_ended_at - call_started_at)) AS avg_seconds
   FROM request_log GROUP BY endpoint, status;"
```

## Cleanup

```bash
docker compose down -v
```
