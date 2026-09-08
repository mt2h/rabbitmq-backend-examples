# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A step-by-step, no-black-boxes reconstruction of the mechanisms that
`_backup_original/` combines to reproduce a real queue-congestion incident
(see `_backup_original/README.md`). Each
top-level numbered script (`01_*.py`, `02_*.py`, ...) isolates exactly ONE
concept. The full roadmap, current progress, and the exact demo/expected
output for each step live in `RUNBOOK.md` — read it before adding or changing
a numbered script.

`_backup_original/` is reference material, not something to run as part of
this project's own work: it's the original combined docker-compose stack
(Postgres + a FastAPI "connector" + a fake slow "downstream" + a load
generator) that the numbered scripts are reconstructing piece by piece,
without Postgres or docker-compose, using RabbitMQ + plain Python directly.
Its own `docker-compose.yml`/services are independent of the root
`docker-compose.yml` (which only runs RabbitMQ).

## Commands

Start RabbitMQ (management UI included):

```bash
docker compose up -d
```

Install deps (currently just `pika`):

```bash
pip install -r requirements.txt
```

Run a numbered step (each is a standalone, self-contained script — no test
framework, no CLI args):

```bash
python3 01_connect_and_queue.py
```

RabbitMQ management UI: http://localhost:15672 (guest/guest) — every script's
docstring describes the exact queue/connection/channel state expected in the
UI at each point, useful for stepping through with a breakpoint.

To inspect AMQP traffic through mitmproxy, point the scripts at a reverse-TCP
proxy instead of RabbitMQ directly (see "env var convention" below):

```bash
web --mode reverse:tcp://127.0.0.1:5672 --listen-port 5673
# in another terminal:
export RABBITMQ_PORT=5673
python3 01_connect_and_queue.py
```

## Conventions

- **Env vars for connection, never hardcoded**: every numbered script reads
  `RABBITMQ_HOST` (default `localhost`) / `RABBITMQ_PORT` (default `5672`)
  from the environment instead of hardcoding them, specifically so a proxy
  (mitmproxy) can be inserted without touching code. Keep this pattern for
  any new script that talks to RabbitMQ.
- **One concept per script, on purpose**: don't fold multiple mechanisms
  (e.g. prefetch + ack + HTTP timeout) into one script even if it would be
  more "efficient" — the point of the exercise is isolating each mechanism.
  The integration only happens deliberately at step 07 per `RUNBOOK.md`.
- **pika is not thread-safe**: a `BlockingConnection`/`channel` may only be
  used from the thread that created it. Scripts that need multiple concurrent
  consumers (e.g. `03_qos_prefetch.py`) give each one its own connection in
  its own thread — follow that pattern for anything similar.
- Each script's module docstring is intentionally short: 1-2 sentences naming
  the concept plus the exact `Como probar:` command. The concept walkthrough,
  scenario-by-scenario explanation, and the exact RabbitMQ management-UI /
  stdout state to expect at each step live in `onboarding/index.html` (an
  interactive step-by-step page covering steps 01-07) and in `RUNBOOK.md`'s
  per-step bullets — keep those two in sync when editing a script's behavior,
  not the docstring.

## Working in this repo

- `RUNBOOK.md` is the authoritative plan and progress tracker (checkboxes per
  step, blockers, caveats already discovered). Update it when a step's status
  changes; don't duplicate its content elsewhere.
- Scripts are meant to be run by the user, not by Claude — after preparing
  RabbitMQ / code changes, stop and let the user execute the `0N_*.py`
  script themselves and report back what they saw.
