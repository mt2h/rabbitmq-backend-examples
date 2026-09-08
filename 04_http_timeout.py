"""
Paso 4: timeout bare-int vs `httpx.Timeout` partido.

Como probar:

    pip install -r requirements.txt   # agrega httpx
    python3 04_http_timeout.py
"""

import asyncio
import http.server
import os
import threading
import time

import httpx

SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "3"))
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
    # ThreadingHTTPServer: una conexion entrante = un hilo nuevo, asi que dos
    # requests concurrentes de verdad corren en paralelo del lado servidor
    # (si fuera HTTPServer a secas, atenderia una a la vez y arruinaria la
    # demo de contencion de pool).
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


async def escenario_bare_int(url):
    print(f"=== Escenario A: timeout={BARE_TIMEOUT} bare-int (mismo numero para connect/read/write/pool) ===")
    # max_connections=1: pool de una sola conexion a proposito, para que dos
    # requests concurrentes compitan de verdad por el mismo slot.
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    # asyncio.gather lanza las dos corrutinas juntas, "al mismo tiempo" (t=0.0s
    # para ambas) -- de ahi en mas, quien consigue el unico slot del pool y
    # quien tiene que esperar ya depende del pool, no de esta linea.
    async with httpx.AsyncClient(timeout=BARE_TIMEOUT, limits=limits) as client:
        req1, req2 = await asyncio.gather(
            _timed_get(client, url, "req-1"),
            _timed_get(client, url, "req-2"),
        )
    return req1, req2


async def escenario_split(url):
    print(
        f"=== Escenario B: httpx.Timeout(connect={SPLIT_CONNECT_TIMEOUT}, "
        f"read={SPLIT_READ_TIMEOUT}, write={SPLIT_WRITE_TIMEOUT}, "
        f"pool={SPLIT_POOL_TIMEOUT}) -- pool corto a proposito ==="
    )
    # Ojo: estos cuatro numeros son SEGUNDOS (cuanto esperar antes de rendirse
    # en cada fase), no cantidades. connect=2.0 no significa "2 requests", y
    # pool=1.0 no significa "1 conexion" -- esa cantidad la fija por separado
    # max_connections=1 en el httpx.Limits de abajo. Son dos preguntas
    # distintas que en esta config casualmente dan el mismo numero (1):
    # "cuantas conexiones simultaneas hay" (max_connections) vs "si no hay
    # ninguna libre, cuanto espero antes de tirar la toalla" (pool timeout).
    # Con pool=1.0 mas abajo, req-2 va a fallar justo al llegar a 1 segundo de
    # espera -- ni antes ni despues, porque ese es el presupuesto exacto que
    # se le da a esa fase (ver SPLIT_POOL_TIMEOUT).
    timeout = httpx.Timeout(
        connect=SPLIT_CONNECT_TIMEOUT,
        read=SPLIT_READ_TIMEOUT,
        write=SPLIT_WRITE_TIMEOUT,
        pool=SPLIT_POOL_TIMEOUT,
    )
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        req1, req2 = await asyncio.gather(
            _timed_get(client, url, "req-1"),
            _timed_get(client, url, "req-2"),
        )
    return req1, req2


async def main_async():
    server = start_server()
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}/"
    print(f"Servidor propio arriba en {url} (duerme {SLEEP_SECONDS}s por request).\n")

    req1, req2 = await escenario_bare_int(url)
    assert req1[0] == "ok" and req2[0] == "ok", (
        "se esperaba que ambos requests del bare-int terminaran OK (req-2 solo mas lento)"
    )
    # Linea de tiempo real (ejemplo con SLEEP_SECONDS=3, max_connections=1):
    #   t=0.0s -> req-1 y req-2 arrancan "a la vez" (asyncio.gather)
    #   t=0.0s -> req-1 agarra la UNICA conexion del pool y empieza a hablar con el servidor
    #   t=0.0s -> req-2 quiere una conexion, no queda ninguna libre -- espera, sin tocar la red todavia
    #   t=3.1s -> req-1 termina (el servidor tardo ~3s) y libera la conexion
    #   t=3.1s -> req-2 recien ahora agarra la conexion libre y arranca su propia llamada
    #   t=6.1s -> req-2 termina (otros ~3s de respuesta del servidor)
    # req-2 nunca dispara timeout porque cada fase (pool: 0->3.1s, read: 3.1->6.1s)
    # se mantuvo POR SEPARADO por debajo del presupuesto de 5s -- aunque la SUMA
    # de las dos fases (6.1s) sea mayor que el numero del timeout.
    print(
        f"Confirmado: req-2 tardo {req2[1]:.1f}s (espero el slot del pool + su "
        f"propia llamada) y aun asi NO hubo ningun error -- el presupuesto de "
        f"'pool' y el de 'read' son el mismo numero ({BARE_TIMEOUT}s), asi que la "
        f"espera por el slot cupo comoda dentro del mismo margen que el tiempo de "
        f"respuesta real. Mirando solo la duracion total, req-2 es indistinguible "
        f"de 'el downstream tardo mas esta vez' -- no hay forma de saber que en "
        f"realidad paso la mitad de ese tiempo esperando un slot libre.\n"
    )

    req1, req2 = await escenario_split(url)
    assert req1[0] == "ok", "se esperaba que req-1 (sin contencion) terminara OK"
    assert req2[0] == "PoolTimeout", (
        f"se esperaba PoolTimeout en req-2, salio {req2[0]!r}"
    )
    # 1. req-1 pidio una conexion al pool -> como estaba libre, la tomo al
    #    instante (t=0s) -> abrio el socket, mando el GET, espero la respuesta
    #    del servidor (~3s) -> status 200.
    # 2. req-2 tambien pidio una conexion al pool, al mismo tiempo (t=0s) ->
    #    pero ya no quedaba ninguna libre (el pool solo tiene 1, y req-1 se la
    #    quedo) -> se quedo esperando, sin abrir ningun socket, sin mandar
    #    ningun byte.
    # 3. Pasado 1s de espera (el limite de 'pool' en este escenario), httpx le
    #    tiro PoolTimeout -- ahi termino req-2, sin haber hecho nada hacia la
    #    red.
    print(
        f"Confirmado: con el presupuesto de 'pool' separado y corto "
        f"({SPLIT_POOL_TIMEOUT}s), req-2 fallo RAPIDO con PoolTimeout en "
        f"{req2[1]:.1f}s en vez de esperar en silencio -- una senal explicita y "
        f"distinta de 'estuve esperando un slot', separada del presupuesto de "
        f"'read' (que sigue siendo generoso, {SPLIT_READ_TIMEOUT}s, para tolerar "
        f"un downstream lento de verdad como el de req-1)."
    )

    server.shutdown()


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
