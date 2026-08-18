"""Stand-in for a degraded OZ downstream service (e.g. delivery-capacity-service).

Two modes, toggled at runtime via POST /mode (see below), starting from
DOWNSTREAM_MODE at boot:
  - "slow": every request sleeps SLOW_SECONDS then returns 200. Used to show what
    happens to a client whose timeout conflates "waiting for a free pool
    connection" with "waiting for this call to finish".
  - "erroring": every request returns 500 immediately. Used to show whether a
    client's circuit breaker actually opens (or never does, at
    failure_threshold=999999).

Mode is runtime, in-memory state (not just the env var) so it can be flipped
with a curl call instead of recreating the container -- `docker compose
up`/`run` re-resolves environment interpolation on every invocation and will
silently recreate this container back to its default env if you rely on
shell-exported env vars across multiple separate commands.
"""

import asyncio
import os

from fastapi import FastAPI, Response

app = FastAPI()

SLOW_SECONDS = float(os.environ.get("SLOW_SECONDS", "8"))
_mode = os.environ.get("DOWNSTREAM_MODE", "slow")

_active = 0
_lock = asyncio.Lock()
_total_requests = 0


@app.post("/mode")
async def set_mode(payload: dict) -> dict:
    global _mode
    new_mode = payload.get("mode")
    if new_mode not in ("slow", "erroring"):
        return Response(status_code=400, content="mode must be 'slow' or 'erroring'")
    _mode = new_mode
    return {"mode": _mode}


@app.post("/api/v1/delivery-capacity/sync", response_model=None)
async def sync(payload: dict) -> dict | Response:
    global _total_requests
    _total_requests += 1

    if _mode == "erroring":
        return Response(status_code=500, content="downstream overloaded")

    global _active
    async with _lock:
        _active += 1
    try:
        await asyncio.sleep(SLOW_SECONDS)
        return {"status": "ok", "message_id": payload.get("message_id")}
    finally:
        async with _lock:
            _active -= 1


@app.get("/stats")
async def stats() -> dict:
    return {"mode": _mode, "active_calls": _active, "total_requests": _total_requests}
