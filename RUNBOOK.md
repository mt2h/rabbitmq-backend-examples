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
- [x] **03 — `03_qos_prefetch.py`**: `basic_qos(prefetch_count=N)`, equivalente
  exacto de `worker_prefetch_multiplier` de Celery. Demo con dos consumers
  (worker_lento 1s, worker_rapido 0.1s): sin `basic_qos` uno acapara los 10
  mensajes si es el único suscrito al publicar; con `prefetch_count=1` en
  ambos nadie acapara, pero el reparto no es 50/50 -- el más rápido termina
  con la mayoría porque en lo que el lento suelta su único slot, el rápido ya
  se comió el resto de la cola. Ojo con correr el script bajo debugger: si un
  run anterior queda pausado (breakpoint/excepción) sin detener el proceso,
  sus conexiones de pika siguen suscritas y compiten por los mensajes del
  siguiente run -- hay que parar del todo la sesión de debug antes de
  re-ejecutar.

### Bloque HTTP client (timeout, pool, circuit breaker)

- [x] **04 — `04_http_timeout.py`**: Timeout bare-int vs `httpx.Timeout`
  partido. Script standalone (sin RabbitMQ) contra un servidor propio
  (stdlib `ThreadingHTTPServer`, sin FastAPI) que duerme `SLEEP_SECONDS` y
  responde 200. `httpx.Limits(max_connections=1)` a propósito para que dos
  requests concurrentes compitan por el mismo slot. Escenario A
  (`timeout=5` bare-int): req-2 espera el slot + su propia llamada (~6s
  total) y termina OK, sin ningún error -- indistinguible de "el downstream
  tardó más esta vez". Escenario B (`httpx.Timeout(connect=2, read=5,
  write=5, pool=1)`, pool corto a propósito): req-2 falla rápido con
  `PoolTimeout` explícito en ~1s en vez de esperar en silencio. Confirma el
  finding de `_backup_original/README.md` (sección 3) con números propios.
- [x] **05 — `05_pool_exhaustion.py`**: mismo downstream lento y mismo patrón
  de `04_http_timeout.py`, pero a escala real: `POOL_SIZE=3` conexiones y
  `N_REQUESTS=12` concurrentes (`asyncio.gather`) → 4 "rondas" de 3 que se
  turnan el pool. Escenario A (`timeout=5` bare-int): las rondas 1-3 (9
  requests) esperan 0s/2s/4s por un slot, todo por debajo de 5s → OK; la
  ronda 4 (3 requests) necesitaría esperar ~6s → supera los 5s y sale
  `PoolTimeout` real y explícito, cortado justo a los 5s. El punto: ese
  `PoolTimeout` queda marcado con el mismo número (5s) que cualquier lectura
  lenta legítima, así que un log genérico de "timeout ~5s" no distingue
  "no había conexiones libres" de "el downstream está lento de verdad".
  Escenario B (`httpx.Timeout(pool=1, read=5, ...)`, pool corto a propósito):
  solo la ronda 1 pasa; las rondas 2-4 (9 requests) fallan rápido a ~1s en
  vez de ~2s/4s/6s — la magnitud de la falla ya delata agotamiento de pool,
  sin tocar la paciencia dada a un downstream lento real. Caveat honesto del
  README original (sección 3): en Celery real esto casi no aplica porque
  cada worker procesa 1 tarea a la vez — sigue valiendo la pena entenderlo
  porque sí aplica a FastAPI/`asyncio.gather`.
- [x] **06 — `06_circuit_breaker.py`**: maquina de tres estados
  (CLOSED/OPEN/HALF_OPEN) contra un downstream propio (stdlib) que responde
  500 al instante mientras esta "enfermo". Sin retries/backoff a proposito
  (eso es otro mecanismo, no se mezcla). Escenario A
  (`failure_threshold=999999`): los 10 requests tocan al downstream y fallan
  los 10 -- el breaker nunca sale de CLOSED. Escenario B
  (`failure_threshold=3`): solo los primeros 3 tocan al downstream (y fallan)
  antes de que el breaker abra; los 7 restantes se cortan local, sin generar
  trafico. Extra: tras `recovery_timeout`, con el downstream ya "sano", un
  request de prueba en HALF_OPEN tiene exito y el breaker vuelve a CLOSED --
  cierra el ciclo de los tres estados.

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
