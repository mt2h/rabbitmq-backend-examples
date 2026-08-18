"""Fires NUM concurrent "messages" at one connector endpoint and reports how the
queue would have behaved: total wall-clock to drain, per-message latency, and a
live poll of /stats (queued vs in_flight) while the run is in progress -- the
direct analog of watching RabbitMQ queue depth during the real load test.

Usage:
  python run_load.py --endpoint endpoint1 --num 20
  python run_load.py --endpoint endpoint2 --num 20
"""

import argparse
import asyncio
import os
import time
import uuid

import httpx

CONNECTOR_URL = os.environ.get("CONNECTOR_URL", "http://connector:8000")


async def send_one(client: httpx.AsyncClient, endpoint: str, message_id: str) -> dict:
    start = time.monotonic()
    try:
        resp = await client.post(f"{CONNECTOR_URL}/{endpoint}", json={"message_id": message_id})
        elapsed = time.monotonic() - start
        return {"message_id": message_id, "elapsed": elapsed, "http_status": resp.status_code, "body": resp.json()}
    except httpx.HTTPError as e:
        elapsed = time.monotonic() - start
        return {"message_id": message_id, "elapsed": elapsed, "http_status": None, "error": str(e)}


async def poll_stats(stop_event: asyncio.Event, interval: float = 1.0):
    async with httpx.AsyncClient(timeout=5) as client:
        while not stop_event.is_set():
            try:
                resp = await client.get(f"{CONNECTOR_URL}/stats")
                print(f"[stats] {resp.json()}")
            except httpx.HTTPError as e:
                print(f"[stats] poll failed: {e}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


async def main(endpoint: str, num: int):
    stop_event = asyncio.Event()
    poller = asyncio.create_task(poll_stats(stop_event))

    # Client-side timeout generous on purpose: we want to measure how long the
    # *server* takes, not have the load generator itself give up first.
    async with httpx.AsyncClient(timeout=120) as client:
        message_ids = [str(uuid.uuid4()) for _ in range(num)]
        wall_start = time.monotonic()
        results = await asyncio.gather(*(send_one(client, endpoint, mid) for mid in message_ids))
        wall_elapsed = time.monotonic() - wall_start

    stop_event.set()
    await poller

    success = [r for r in results if r.get("body", {}).get("status") == "success"]
    error = [r for r in results if r.get("body", {}).get("status") == "error"]
    failed_request = [r for r in results if "error" in r]

    print("\n=== summary ===")
    print(f"endpoint:            {endpoint}")
    print(f"messages fired:      {num}")
    print(f"wall clock to drain: {wall_elapsed:.2f}s")
    print(f"success:             {len(success)}")
    print(f"error (server-side): {len(error)}")
    print(f"failed http request: {len(failed_request)}")
    if results:
        elapsed_values = sorted(r["elapsed"] for r in results)
        p50 = elapsed_values[len(elapsed_values) // 2]
        p95 = elapsed_values[int(len(elapsed_values) * 0.95) - 1]
        print(f"p50 per-message:     {p50:.2f}s")
        print(f"p95 per-message:     {p95:.2f}s")
    if error:
        sample_errors = {r["body"].get("error_type") for r in error}
        print(f"error types seen:    {sample_errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", choices=["endpoint1", "endpoint2"], required=True)
    parser.add_argument("--num", type=int, default=20)
    args = parser.parse_args()
    asyncio.run(main(args.endpoint, args.num))
