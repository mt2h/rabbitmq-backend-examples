"""
Paso 6: CircuitBreaker (CLOSED / OPEN / HALF_OPEN) -- comparar un breaker que
nunca abre contra uno que si protege al downstream.

Como probar:

    python3 06_circuit_breaker.py
"""

import http.server
import os
import threading
import time
import urllib.error
import urllib.request

FAILURE_THRESHOLD_BAD = int(os.environ.get("FAILURE_THRESHOLD_BAD", "999999"))
FAILURE_THRESHOLD_GOOD = int(os.environ.get("FAILURE_THRESHOLD_GOOD", "3"))
RECOVERY_TIMEOUT = float(os.environ.get("RECOVERY_TIMEOUT", "2"))
N_REQUESTS = int(os.environ.get("N_REQUESTS", "10"))


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 1, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def can_execute(self) -> bool:
        current_time = time.time()
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if current_time - self.last_failure_time >= self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        else:  # HALF_OPEN
            return True

    def on_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class DownstreamState:
    """Estado compartido del downstream propio: cuantos requests recibio en
    total, y si en este momento esta "enfermo" (500) o "sano" (200)."""

    def __init__(self):
        self.healthy = False
        self.total_requests = 0


def make_handler(state):
    class FlakyHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            state.total_requests += 1
            if state.healthy:
                body = b"ok"
                self.send_response(200)
            else:
                body = b"error"
                self.send_response(500)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass  # silenciar el log default de http.server, ensuciaria la demo

    return FlakyHandler


def start_server(state):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def call_downstream(breaker, url):
    """Una sola llamada, sin retries -- ver el docstring del modulo sobre por
    que se deja afuera el loop de reintentos del original."""
    if not breaker.can_execute():
        return "short_circuited"

    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            status = resp.status
    except urllib.error.HTTPError as e:
        status = e.code

    if status >= 500:
        breaker.on_failure()
        return "downstream_error"

    breaker.on_success()
    return "ok"


def escenario_a_nunca_abre(url, state):
    print(f"=== Escenario A: CircuitBreaker(failure_threshold={FAILURE_THRESHOLD_BAD}) ===")
    state.healthy = False
    state.total_requests = 0
    breaker = CircuitBreaker(failure_threshold=FAILURE_THRESHOLD_BAD)

    resultados = []
    for i in range(N_REQUESTS):
        resultado = call_downstream(breaker, url)
        resultados.append(resultado)
        print(f"  request-{i + 1:02d}: {resultado} (breaker.state={breaker.state})")

    print(f"  downstream.total_requests = {state.total_requests}")
    assert resultados.count("downstream_error") == N_REQUESTS
    assert state.total_requests == N_REQUESTS
    assert breaker.state == "CLOSED"
    print(
        f"Confirmado: los {N_REQUESTS} requests tocaron de verdad al downstream y "
        f"fallaron los {N_REQUESTS} -- con failure_threshold={FAILURE_THRESHOLD_BAD}, "
        f"failure_count nunca alcanza el umbral, asi que el breaker se queda CLOSED "
        f"para siempre y sigue mandando trafico a un downstream que ya demostro que "
        f"esta caido.\n"
    )


def escenario_b_abre_y_protege(url, state):
    print(f"=== Escenario B: CircuitBreaker(failure_threshold={FAILURE_THRESHOLD_GOOD}) ===")
    state.healthy = False
    state.total_requests = 0
    breaker = CircuitBreaker(
        failure_threshold=FAILURE_THRESHOLD_GOOD, recovery_timeout=RECOVERY_TIMEOUT
    )

    resultados = []
    for i in range(N_REQUESTS):
        resultado = call_downstream(breaker, url)
        resultados.append(resultado)
        print(f"  request-{i + 1:02d}: {resultado} (breaker.state={breaker.state})")

    print(f"  downstream.total_requests = {state.total_requests}")
    assert resultados.count("downstream_error") == FAILURE_THRESHOLD_GOOD
    assert resultados.count("short_circuited") == N_REQUESTS - FAILURE_THRESHOLD_GOOD
    assert state.total_requests == FAILURE_THRESHOLD_GOOD
    assert breaker.state == "OPEN"
    print(
        f"Confirmado: solo los primeros {FAILURE_THRESHOLD_GOOD} requests tocaron al "
        f"downstream (y fallaron); en cuanto failure_count llego a "
        f"{FAILURE_THRESHOLD_GOOD} el breaker abrio, y los "
        f"{N_REQUESTS - FAILURE_THRESHOLD_GOOD} requests restantes se cortaron LOCAL "
        f"-- el downstream no vio ni uno mas.\n"
    )

    print(
        f"--- Recuperacion: esperando {RECOVERY_TIMEOUT:.0f}s (recovery_timeout) y "
        f"marcando el downstream como sano ---"
    )
    time.sleep(RECOVERY_TIMEOUT + 0.1)
    state.healthy = True

    resultado = call_downstream(breaker, url)
    print(f"  request de prueba tras recovery_timeout: {resultado} (breaker.state={breaker.state})")
    assert resultado == "ok"
    assert breaker.state == "CLOSED"
    assert breaker.failure_count == 0
    print(
        "Confirmado: pasado recovery_timeout, can_execute() dejo pasar UN request de "
        "prueba (HALF_OPEN); como el downstream ya respondia 200, ese request "
        "disparo on_success() y el breaker volvio a CLOSED con el contador en 0."
    )


def main():
    state = DownstreamState()
    server = start_server(state)
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}/"
    print(f"Downstream propio arriba en {url} (arranca 'enfermo', responde 500).\n")

    escenario_a_nunca_abre(url, state)
    escenario_b_abre_y_protege(url, state)

    server.shutdown()


if __name__ == "__main__":
    main()
