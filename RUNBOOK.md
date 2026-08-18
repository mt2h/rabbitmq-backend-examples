# oz-timeout-repro — runbook / plan de aprendizaje

Objetivo: reconstruir, paso a paso y sin cajas negras, todos los mecanismos que
`_backup_original/` combina para reproducir la congestión de colas real de
`oz-zecore-connector-service` (ver `_backup_original/README.md`). Cada script
numerado aisla UN concepto; al final (paso 07) se juntan todos y se ve el
mecanismo completo con scripts propios, sin depender del stack docker-compose
original.

Mecanismos del incidente real que hay que entender (de `_backup_original`):
1. `http_client.py`: timeout bare-int (aplica igual a connect/read/write/pool)
   vs `httpx.Timeout` partido.
2. `http_client.py`: `CircuitBreaker` con `failure_threshold=999999` (nunca
   abre) vs uno que sí abre.
3. `connector/app.py`: `httpx.Limits` (pool de conexiones) — puede agotarse
   independientemente de si el downstream está lento.
4. `connector/app.py`: `WORKER_CONCURRENCY` (semaphore) simulando Celery
   `--concurrency` + `worker_prefetch_multiplier=1` + `task_acks_late=True` —
   un mensaje ocupa un "slot" durante toda la llamada downstream; nada nuevo
   se recoge hasta que un slot se libera. Este es el mecanismo real de la
   congestión (`queued` sube, `in_flight` tope en N).

## Convención: variables de entorno para conexión

Los scripts numerados leen host/puerto de RabbitMQ desde variables de entorno
(no hardcodeados), para poder apuntar a un proxy intermedio (p. ej. mitmproxy)
sin tocar código:

- `RABBITMQ_HOST` (default `localhost`)
- `RABBITMQ_PORT` (default `5672`)

Ejemplo real usado en este proyecto — mitmweb en modo reverse-proxy TCP
delante de RabbitMQ, escuchando en 5673 y reenviando a 5672, para poder
inspeccionar el trafico AMQP en la UI de mitmproxy:

```bash
mitmweb --mode reverse:tcp://127.0.0.1:5672 --listen-port 5673
```

Y en otra terminal, apuntar los scripts a ese puerto:

```bash
export RABBITMQ_PORT=5673
python3 01_connect_and_queue.py
```

## Progreso

- [x] **01 — `01_connect_and_queue.py`**: Connection, Channel, Queue declare,
  publish/consume mínimo con `basic_get(auto_ack=True)`. Sin timeouts, sin
  pools, sin concurrencia todavía (a propósito).

### Bloque RabbitMQ (falta antes de saltar a HTTP)

- [x] **02 — `02_basic_consume_ack.py`**: `basic_consume` (push) en vez de
  `basic_get` (pull), con `auto_ack=False` y `basic_ack` explícito. Demo:
  publica 3 mensajes, ackea 2 y deja uno sin ackear a propósito, cierra la
  conexión, y en una conexión nueva ese mensaje reaparece solo (redelivery).
  Es el mismo hueco que Celery `acks_late=True` deja abierto si un worker
  muere a mitad de una tarea.
- [ ] **03 — `basic_qos(prefetch_count=N)`**: equivalente exacto de
  `worker_prefetch_multiplier` de Celery. Con `prefetch_count=1` + ack tardío,
  el consumer no recibe el siguiente mensaje hasta confirmar el anterior. Es
  el corazón del "staircase" (`queued` baja de a N) que describe el README
  del incidente.

### Bloque HTTP client (timeout, pool, circuit breaker)

- [ ] **04 — Timeout bare-int vs `httpx.Timeout` partido**: script standalone
  (sin RabbitMQ) contra un servidor propio que duerme N segundos. Comparar
  `timeout=20` (aplica igual a connect/read/write/pool) vs
  `httpx.Timeout(connect=, read=, write=, pool=)`. Ver que con bare-int no se
  puede distinguir "el downstream tardó" de "esperé un slot libre del pool".
- [ ] **05 — `httpx.Limits` / pool exhaustion**: mismo downstream lento, pero
  con más requests concurrentes que `max_connections`. Ver el `PoolTimeout`
  real y cómo con timeout bare-int ese error se disfraza de timeout normal.
  (Caveat honesto del README original: en Celery real esto casi no aplica
  porque cada worker procesa 1 tarea a la vez — sigue valiendo la pena
  entenderlo porque sí aplica a FastAPI/`asyncio.gather`.)
- [ ] **06 — Circuit breaker**: mismo `CircuitBreaker` (CLOSED/OPEN/HALF_OPEN,
  failure_threshold) contra un downstream que falla con 500. Comparar
  `failure_threshold=999999` (nunca abre, sigue masacrando al downstream) vs
  `failure_threshold=3` (abre y protege).

### Bloque integración

- [ ] **07 — Worker slots (semaphore) + prefetch + HTTP lento, todo junto**:
  recrear `WORKER_CONCURRENCY` como semaphore que un consumer de RabbitMQ (02
  + 03) ocupa mientras llama al HTTP client lento (04-06). Acá se ve por fin
  "queued sube, in_flight tope en N, queued baja de a N" — el mecanismo
  completo, con scripts propios, sin Postgres ni docker-compose de por medio.
- [ ] **08 (opcional)** — comparar endpoint1 (bad: bare-int + breaker que
  nunca abre) vs endpoint2 (good: timeout partido + breaker que sí abre) lado
  a lado, replicando los dos findings principales del README con números
  propios.
- [ ] **09 (opcional)** — múltiples replicas/consumers compartiendo la misma
  cola, para la sección final del README (KEDA-scaled pods).

Con 01-07 completos queda reconstruido, pieza por pieza, todo lo que hace
`_backup_original/`. 08 y 09 son profundización, no esenciales para entender
el problema.
