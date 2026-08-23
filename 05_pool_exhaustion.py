"""
Paso 5: agotamiento real del pool de conexiones (`httpx.Limits`) y como el
timeout bare-int disfraza el `PoolTimeout` resultante.

Sigue siendo standalone (sin RabbitMQ), igual que el paso 04, contra el mismo
tipo de servidor propio (stdlib `ThreadingHTTPServer`) que duerme
SLEEP_SECONDS y responde 200. La diferencia con el paso 04 es la escala: ahi
eran 2 requests contra un pool de 1 conexion (solo para ver que el error
`PoolTimeout` existe). Aca son N_REQUESTS concurrentes contra un pool de
POOL_SIZE conexiones con N_REQUESTS > POOL_SIZE -- agotamiento real, en
"tandas": el pool solo puede atender POOL_SIZE requests a la vez, asi que se
forman rondas que esperan su turno.

Con POOL_SIZE=3 y N_REQUESTS=12 se forman 4 rondas de 3 requests cada una.
Si nadie fallara, la ronda K empezaria a correr recien en
t=(K-1)*SLEEP_SECONDS (tuvo que esperar a que las rondas anteriores liberaran
conexion) y terminaria en t=K*SLEEP_SECONDS.

Escenario A -- timeout=BARE_TIMEOUT (bare-int, connect/read/write/pool
comparten el mismo numero). Cada fase tiene su propio presupuesto de
BARE_TIMEOUT segundos (no es un presupuesto acumulado: httpx reinicia el
reloj al pasar de la fase "pool" a la fase "read"), asi que una ronda
sobrevive mientras SU espera de pool, sola, se mantenga por debajo de
BARE_TIMEOUT -- sin importar cuanto hayan tardado las rondas anteriores. Con
BARE_TIMEOUT=5s, las rondas 1-3 esperan 0s/2s/4s por un slot (todas < 5s):
pasan. La ronda 4 necesitaria esperar ~6s por un slot: eso SI supera los 5s,
entonces httpx corta esa espera con un `PoolTimeout` real y explicito a los
5s -- no un timeout generico ni un cuelgue silencioso.

Entonces, si el tipo de excepcion ya es el correcto (`PoolTimeout`), donde
esta el disfraz? En que ese `PoolTimeout` aparece marcado con el MISMO numero
(5s) que cualquier lectura lenta legitima tendria si el downstream de verdad
tardara 5s en responder. Mirando solo "elapsed ~5s, TimeoutException", no hay
forma de saber si el problema fue "no habia conexiones libres" (se arregla
subiendo `max_connections`) o "el downstream esta lento de verdad" (se
arregla optimizando el downstream, o tolerandolo) -- ambos comparten el mismo
presupuesto porque bare-int no permite darles numeros distintos.

Escenario B -- httpx.Timeout(connect=, read=, write=, pool=) con `pool`
deliberadamente corto (1s) y separado de `read` (5s, generoso). Ahora
CUALQUIER ronda que tenga que esperar mas de 1s por un slot falla rapido y
explicito: solo la ronda 1 (espera 0s) pasa: las rondas 2, 3 y 4 (que
necesitarian esperar 2s, 4s y 6s respectivamente) fallan las tres a los ~1s
con `PoolTimeout` -- mucho antes y a una escala de tiempo completamente
distinta (~1s) que la de un `read` lento de verdad (~2s, el que si consigue
slot). La separacion ya no es solo de tipo de excepcion: es de magnitud, y
esa magnitud es la que permite detectar agotamiento de pool sin ambiguedad,
sin tocar para nada la paciencia que se le da a un downstream lento.

Caveat honesto (el mismo del README de `_backup_original`, seccion 3): en un
worker Celery real esto casi no aplica, porque cada worker procesa UNA tarea
a la vez -- no hay "requests concurrentes del mismo proceso" compitiendo por
el mismo pool. Sigue valiendo la pena entenderlo porque si aplica tal cual a
un servicio FastAPI/asyncio que atiende varias requests a la vez con
`asyncio.gather` (o simplemente varias requests HTTP entrantes en paralelo)
compartiendo un mismo `httpx.AsyncClient`.

Como probar:

    pip install -r requirements.txt   # ya incluye httpx
    python3 05_pool_exhaustion.py

No hay RabbitMQ ni UI que mirar en este paso -- toda la evidencia sale por
stdout: por cada request, tipo de resultado y tiempo transcurrido; al final
de cada escenario, un resumen contado por tipo de excepcion.
"""

import asyncio
import http.server
import os
import threading
import time
from collections import Counter

import httpx

SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2"))
POOL_SIZE = int(os.environ.get("POOL_SIZE", "3"))
N_REQUESTS = int(os.environ.get("N_REQUESTS", "12"))
BARE_TIMEOUT = float(os.environ.get("BARE_TIMEOUT", "5"))
SPLIT_CONNECT_TIMEOUT = float(os.environ.get("SPLIT_CONNECT_TIMEOUT", "2"))
SPLIT_READ_TIMEOUT = float(os.environ.get("SPLIT_READ_TIMEOUT", "5"))
SPLIT_WRITE_TIMEOUT = float(os.environ.get("SPLIT_WRITE_TIMEOUT", "5"))
SPLIT_POOL_TIMEOUT = float(os.environ.get("SPLIT_POOL_TIMEOUT", "1"))


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
    # ThreadingHTTPServer: cada conexion entrante es un hilo nuevo, asi que
    # las N_REQUESTS conexiones que sobreviven al pool (las que si consiguen
    # slot) se atienden de verdad en paralelo del lado servidor.
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


async def _timed_get(client, url, nombre):
    inicio = time.monotonic()
    try:
        resp = await client.get(url)
        elapsed = time.monotonic() - inicio
        print(f"  [{nombre}] OK status={resp.status_code} en {elapsed:.1f}s")
        return ("ok", elapsed)
    except httpx.TimeoutException as e:
        elapsed = time.monotonic() - inicio
        print(f"  [{nombre}] {type(e).__name__} en {elapsed:.1f}s")
        return (type(e).__name__, elapsed)


async def _run_oleada(client, url):
    nombres = [f"req-{i + 1:02d}" for i in range(N_REQUESTS)]
    resultados = await asyncio.gather(*(_timed_get(client, url, n) for n in nombres))
    resumen = Counter(tipo for tipo, _elapsed in resultados)
    print(f"  resumen: {dict(resumen)}")
    return resultados, resumen


async def escenario_bare_int(url):
    print(
        f"=== Escenario A: timeout={BARE_TIMEOUT} bare-int, pool de "
        f"{POOL_SIZE} conexiones, {N_REQUESTS} requests concurrentes ==="
    )
    # max_connections=POOL_SIZE: maximo POOL_SIZE (3) conexiones simultaneas
    # para TODO el cliente, compartidas por las N_REQUESTS (12) corrutinas.
    limits = httpx.Limits(max_connections=POOL_SIZE, max_keepalive_connections=POOL_SIZE)
    async with httpx.AsyncClient(timeout=BARE_TIMEOUT, limits=limits) as client:
        return await _run_oleada(client, url)


async def escenario_split(url):
    # Ojo con el nombre repetido: "pool" aparece dos veces y son dos cosas
    # distintas. `pool=SPLIT_POOL_TIMEOUT` (abajo, en httpx.Timeout) es un
    # tiempo en SEGUNDOS -- cuanto esperar por una conexion libre antes de
    # rendirse. `max_connections=POOL_SIZE` (en httpx.Limits, mas abajo) es
    # una CANTIDAD -- cuantas conexiones simultaneas existen. Que ambas
    # variables se llamen "pool" es solo como httpx nombro sus parametros, no
    # una relacion real entre los dos numeros: se podria tener pool=1.0s con
    # POOL_SIZE=3, o pool=5.0s con POOL_SIZE=1, son perillas independientes.
    print(
        f"=== Escenario B: httpx.Timeout(connect={SPLIT_CONNECT_TIMEOUT}, "
        f"read={SPLIT_READ_TIMEOUT}, write={SPLIT_WRITE_TIMEOUT}, "
        f"pool={SPLIT_POOL_TIMEOUT}s [tiempo de espera]) -- pool corto a "
        f"proposito, contra un pool de {POOL_SIZE} conexiones "
        f"[max_connections, una cantidad distinta], {N_REQUESTS} requests "
        f"concurrentes ==="
    )
    timeout = httpx.Timeout(
        connect=SPLIT_CONNECT_TIMEOUT,
        read=SPLIT_READ_TIMEOUT,
        write=SPLIT_WRITE_TIMEOUT,
        pool=SPLIT_POOL_TIMEOUT,
    )
    # max_connections=POOL_SIZE: maximo POOL_SIZE (3) conexiones simultaneas
    # para TODO el cliente, compartidas por las N_REQUESTS (12) corrutinas.
    # El `pool=SPLIT_POOL_TIMEOUT` de arriba (1s) es el presupuesto que tiene
    # CADA request para conseguir una de esas 3 conexiones compartidas -- no
    # es "1s repartido entre las 3", es "1s de espera, y la conexion que te
    # toque es cualquiera de esas 3 que se libere primero".
    limits = httpx.Limits(max_connections=POOL_SIZE, max_keepalive_connections=POOL_SIZE)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        return await _run_oleada(client, url)


async def main_async():
    assert N_REQUESTS > POOL_SIZE, "el punto del demo es tener mas requests que conexiones"
    n_rondas = -(-N_REQUESTS // POOL_SIZE)  # ceil division
    server = start_server()
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}/"
    print(
        f"Servidor propio arriba en {url} (duerme {SLEEP_SECONDS}s por "
        f"request). {N_REQUESTS} requests / pool de {POOL_SIZE} => "
        f"{n_rondas} rondas.\n"
    )

    _resultados_a, resumen_a = await escenario_bare_int(url)
    # Con BARE_TIMEOUT=5, SLEEP_SECONDS=2, POOL_SIZE=3, N_REQUESTS=12: las
    # rondas 1-3 (9 requests) esperan 0s/2s/4s por un slot, todo < 5s => OK.
    # La ronda 4 (3 requests) necesitaria esperar ~6s por un slot: supera los
    # 5s => PoolTimeout real y explicito, cortado justo a los 5s.
    ok_esperados = (n_rondas - 1) * POOL_SIZE
    fallidos_esperados = N_REQUESTS - ok_esperados
    assert resumen_a.get("ok", 0) == ok_esperados, (
        f"se esperaban {ok_esperados} OK en el escenario A, salieron {resumen_a.get('ok', 0)}"
    )
    assert resumen_a.get("PoolTimeout", 0) == fallidos_esperados, (
        f"se esperaban {fallidos_esperados} PoolTimeout en el escenario A, "
        f"salieron {resumen_a.get('PoolTimeout', 0)}"
    )
    print(
        f"Confirmado: {ok_esperados} requests terminaron OK y {fallidos_esperados} "
        f"cayeron en PoolTimeout -- pero los {fallidos_esperados} que fallaron lo "
        f"hicieron marcados con el mismo numero ({BARE_TIMEOUT}s) que cualquier "
        f"lectura lenta legitima tendria. Sin inspeccionar el tipo de excepcion "
        f"(algo que un `except httpx.TimeoutException` generico no hace), un "
        f"'timeout de ~{BARE_TIMEOUT:.0f}s' en los logs es ambiguo: no dice si el "
        f"problema fue 'no habia conexiones libres' (se arregla con mas "
        f"`max_connections`) o 'el downstream esta lento de verdad' (se arregla "
        f"en el downstream) -- comparten el mismo presupuesto porque bare-int no "
        f"deja darles numeros distintos.\n"
    )

    _resultados_b, resumen_b = await escenario_split(url)
    # Con SPLIT_POOL_TIMEOUT=1: la ronda 1 agarra los 3 slots en t=0 y los
    # tiene ocupados hasta t=2 (SLEEP_SECONDS), porque el downstream tarda
    # 2s en responder -- mas que el presupuesto de pool de 1s. Entonces
    # ningun request de las rondas 2, 3 o 4 consigue un slot libre antes de
    # que se les cumpla ese 1s de espera: los 9 caen en PoolTimeout casi al
    # mismo tiempo (~1s), no escalonados como en el escenario A -- ninguno
    # de ellos llega siquiera a "necesitar" esperar 2s/4s/6s, porque el
    # reloj de pool ya los corto antes.
    ok_esperados_b = POOL_SIZE
    fallidos_esperados_b = N_REQUESTS - ok_esperados_b
    assert resumen_b.get("ok", 0) == ok_esperados_b, (
        f"se esperaban {ok_esperados_b} OK en el escenario B, salieron {resumen_b.get('ok', 0)}"
    )
    assert resumen_b.get("PoolTimeout", 0) == fallidos_esperados_b, (
        f"se esperaban {fallidos_esperados_b} PoolTimeout en el escenario B, "
        f"salieron {resumen_b.get('PoolTimeout', 0)}"
    )
    print(
        f"Confirmado: con el presupuesto de 'pool' separado y corto "
        f"({SPLIT_POOL_TIMEOUT:.0f}s), {ok_esperados_b} requests (la unica ronda "
        f"que no tuvo que esperar) terminaron OK con su tiempo de lectura normal "
        f"(~{SLEEP_SECONDS:.0f}s), y los otros {fallidos_esperados_b} fallaron "
        f"rapido, a una escala de tiempo (~{SPLIT_POOL_TIMEOUT:.0f}s) totalmente "
        f"distinta y mucho menor que la de un `read` lento real -- la magnitud "
        f"misma de la falla ya dice 'esto fue agotamiento de pool', sin tocar "
        f"para nada la paciencia (`read={SPLIT_READ_TIMEOUT:.0f}s`) que se le da "
        f"a un downstream lento de verdad."
    )

    server.shutdown()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
