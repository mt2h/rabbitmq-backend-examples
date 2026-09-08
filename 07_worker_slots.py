"""
Paso 7: WORKER_CONCURRENCY como semaphore -- N hilos consumer
(basic_qos(prefetch_count=1) + ack manual, pasos 02-03) llamando a un
downstream HTTP lento con el timeout partido bueno (leccion del paso 04):
nunca hay mas de N mensajes "in_flight" a la vez, sin importar cuantos queden
esperando (Ready) en la cola.

Como probar:

    docker compose up -d
    pip install -r requirements.txt   # ya incluye httpx
    python3 07_worker_slots.py
"""

import http.server
import os
import threading
import time

import httpx
import pika

QUEUE_NAME = "step7_queue"
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "1"))
N_MESSAGES = int(os.environ.get("N_MESSAGES", "12"))
WORKER_CONCURRENCY_A = int(os.environ.get("WORKER_CONCURRENCY_A", "1"))
WORKER_CONCURRENCY_B = int(os.environ.get("WORKER_CONCURRENCY_B", "3"))

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))

# Timeout partido "bueno" (leccion del paso 04) -- no es el foco de este paso,
# solo evita que el HTTP client en si mismo sea la variable que confunde la
# demo. pool=1 no importa aca: cada worker usa su propio httpx.Client, un solo
# request a la vez, sin compartir pool con nadie (eso es el paso 05).
HTTP_TIMEOUT = httpx.Timeout(connect=2.0, read=5.0, write=5.0, pool=1.0)


class SlowHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(SLEEP_SECONDS)  # simula "el downstream tarda"
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # silenciar el log default de http.server, ensuciaria la demo


def start_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _connect():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return connection, channel


def _purge():
    connection, channel = _connect()
    channel.queue_purge(queue=QUEUE_NAME)
    connection.close()


def _publish(n):
    connection, channel = _connect()
    for i in range(1, n + 1):
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=f"msg-{i}")
    connection.close()
    print(f"{n} mensajes publicados.")


class InFlight:
    """Contador compartido entre los N hilos worker -- cuantos mensajes estan
    siendo procesados (unacked) en este instante, y el maximo que se alcanzo
    en toda la corrida."""

    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0
        self.max_seen = 0

    def incr(self):
        with self._lock:
            self._value += 1
            self.max_seen = max(self.max_seen, self._value)
            return self._value

    def decr(self):
        with self._lock:
            self._value -= 1

    @property
    def value(self):
        with self._lock:
            return self._value


def _worker(worker_id, url, in_flight, procesados, total_esperado, deadline):
    """Un 'proceso Celery' con concurrency=1: su propia connection de pika
    (no es thread-safe: cada connection solo se puede usar desde el hilo que
    la creo) y su propio httpx.Client, prefetch_count=1 -- nunca tiene mas de
    1 mensaje sin confirmar a la vez."""
    connection, channel = _connect()
    channel.basic_qos(prefetch_count=1)
    client = httpx.Client(timeout=HTTP_TIMEOUT)

    def on_message(ch, method_frame, header_frame, body):
        n = in_flight.incr()
        client.get(url)  # la llamada lenta -- el mensaje sigue unacked mientras dura
        in_flight.decr()
        ch.basic_ack(delivery_tag=method_frame.delivery_tag)  # acks_late: recien aca
        procesados.append(body.decode())
        print(f"  [worker-{worker_id}] proceso {body.decode()!r} (in_flight llego a {n})")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    # Loop manual (en vez de start_consuming) para poder parar cuando ya se
    # proceso todo lo esperado, sin bloquear para siempre.
    while time.monotonic() < deadline:
        if len(procesados) >= total_esperado:
            break
        connection.process_data_events(time_limit=0.2)

    client.close()
    connection.close()


def _run_escenario(nombre, worker_concurrency, url):
    print(f"=== {nombre}: WORKER_CONCURRENCY={worker_concurrency} ===")
    _purge()
    in_flight = InFlight()
    procesados = []
    deadline = time.monotonic() + N_MESSAGES * SLEEP_SECONDS + 15

    workers = [
        threading.Thread(
            target=_worker,
            args=(i + 1, url, in_flight, procesados, N_MESSAGES, deadline),
        )
        for i in range(worker_concurrency)
    ]
    for w in workers:
        w.start()
    time.sleep(0.5)  # asegurar que todos ya esten suscritos con su prefetch aplicado

    _publish(N_MESSAGES)

    # Connection propia, solo para consultar cuantos mensajes quedan "Ready"
    # en la cola (queue_declare(passive=True)) -- no cuenta los unacked que
    # ya tiene algun worker en mano, esos ya no son "queued".
    monitor_connection, monitor_channel = _connect()
    inicio = time.monotonic()
    last_print = -1.0
    while len(procesados) < N_MESSAGES and time.monotonic() < deadline:
        ready = monitor_channel.queue_declare(queue=QUEUE_NAME, passive=True).method.message_count
        elapsed = time.monotonic() - inicio
        if elapsed - last_print >= 0.3:
            print(
                f"    t={elapsed:5.1f}s  queued(ready)={ready:2d}  "
                f"in_flight={in_flight.value}/{worker_concurrency}  "
                f"procesados={len(procesados):2d}/{N_MESSAGES}"
            )
            last_print = elapsed
        time.sleep(0.1)
    monitor_connection.close()

    for w in workers:
        w.join()

    print(f"  max in_flight observado: {in_flight.max_seen}")
    assert len(procesados) == N_MESSAGES, (
        f"se esperaban {N_MESSAGES} procesados, salieron {len(procesados)}"
    )
    assert in_flight.max_seen == worker_concurrency, (
        f"se esperaba que in_flight llegara a {worker_concurrency}, "
        f"llego a {in_flight.max_seen}"
    )
    print()


def main():
    server = start_server()
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}/"
    print(f"Downstream propio arriba en {url} (duerme {SLEEP_SECONDS}s por request).\n")

    _run_escenario("Escenario A", WORKER_CONCURRENCY_A, url)
    print(
        f"Confirmado: con WORKER_CONCURRENCY={WORKER_CONCURRENCY_A}, in_flight nunca "
        f"pasa de {WORKER_CONCURRENCY_A} -- los demas mensajes esperan su turno en la "
        f"cola (Ready), de a uno, aunque el downstream pudiera atender varios en "
        f"paralelo.\n"
    )

    _run_escenario("Escenario B", WORKER_CONCURRENCY_B, url)
    print(
        f"Confirmado: con WORKER_CONCURRENCY={WORKER_CONCURRENCY_B}, in_flight sube "
        f"hasta {WORKER_CONCURRENCY_B} y se mantiene ahi -- 'queued sube, in_flight "
        f"tope en N, queued baja de a N', el mecanismo real de la congestion, "
        f"reconstruido con basic_qos(prefetch_count=1) x{WORKER_CONCURRENCY_B} + ack "
        f"manual + un cliente HTTP con timeout partido -- sin Celery ni Postgres de "
        f"por medio."
    )

    server.shutdown()


if __name__ == "__main__":
    main()
