"""
Paso 3: basic_qos(prefetch_count=N) -- el limite de "cuantos mensajes sin
confirmar puede tener un consumer a la vez".

En 02 vimos que un mensaje entregado queda "unacked" hasta que se manda el
ack. Lo que no vimos: por defecto RabbitMQ NO limita cuantos mensajes puede
tener "unacked" un consumer al mismo tiempo. Sin limite, Rabbit empuja
mensajes al consumer tan rapido como puede, sin importar si el consumer ya
esta ocupado procesando el anterior -- se acumulan del lado del cliente,
esperando a que el callback les llegue el turno.

basic_qos(prefetch_count=N) le dice a Rabbit: "no le entregues a este
consumer un mensaje nuevo si ya tiene N sin confirmar". Es el control de flujo
que evita que un consumer se sature (o acapare trabajo que otro podria hacer
mas rapido).

Esto ES el mismo mecanismo que Celery usa: `worker_prefetch_multiplier`
(por defecto 4) multiplicado por `--concurrency` define el prefetch_count
real que Celery le pide a Rabbit. Por ejemplo, con el default (multiplier=4)
y `--concurrency=2`, ese worker Celery le pide a Rabbit "dame maximo 8 tareas
sin confirmar" -- el doble de tareas de las que en verdad puede procesar en
paralelo (2), quedan 6 reservadas y esperando turno del lado del worker.
Con multiplier=1 y concurrency=2, en cambio, cada worker Celery le dice a
Rabbit "dame maximo 2 tareas sin confirmar" -- ni una mas, aunque haya 500
esperando en la cola. Eso es EXACTAMENTE lo que probamos aqui con dos
consumers propios (usando multiplier=1, o sea prefetch_count=1 por consumer).

Diferencia con el semaphore que vamos a construir en el paso 07
(WORKER_CONCURRENCY): prefetch_count lo hace cumplir RabbitMQ (a nivel de
protocolo AMQP, controla cuantos mensajes le entrega al canal). El semaphore
del paso 07 lo hace cumplir NUESTRO codigo de aplicacion (a nivel de cuantas
llamadas HTTP concurrentes dejamos correr). En Celery real ambos coinciden:
prefetch_count = worker_prefetch_multiplier * concurrency, asi que un worker
nunca tiene mas mensajes reservados de los que puede llegar a procesar en
paralelo.

Demo (dos "workers" -- dos consumers con su propia conexion, cada uno en su
propio hilo, sobre la MISMA cola):

    worker_lento: tarda 1s en procesar cada mensaje (simula downstream lento).
    worker_rapido: tarda 0.1s en procesar cada mensaje.

Escenario 1 -- SIN limite (sin basic_qos, el default es ilimitado):
    worker_lento se suscribe primero, solo. Publicamos 10 mensajes. Como es
    el UNICO consumer en ese momento y no hay limite, Rabbit le entrega los
    10 de una sola vez (quedan "unacked" del lado de worker_lento aunque
    todavia no los haya procesado). Cuando worker_rapido se suscribe medio
    segundo despues, la cola ya esta vacia -- no hay nada que darle. Resultado
    esperado: worker_lento procesa los 10, worker_rapido procesa 0. Acaparo
    todo el trabajo aunque worker_rapido lo hubiera hecho 10x mas rapido.

Escenario 2 -- CON prefetch_count=1 en ambos:
    Los dos se suscriben ANTES de publicar, cada uno con basic_qos(
    prefetch_count=1). Publicamos los mismos 10 mensajes. Ahora Rabbit solo le
    da 1 mensaje a la vez a cada uno; en cuanto uno ackea, le manda el
    siguiente. Como worker_rapido ackea mucho mas rapido, termina procesando
    la mayoria -- pero worker_lento SI alcanza a procesar varios (a diferencia
    del escenario 1, donde se quedo en cero). El trabajo se reparte segun
    quien puede de verdad, en vez de por quien llego primero.

Como probar:

    docker compose up -d
    python3 03_qos_prefetch.py

Host/puerto salen de RABBITMQ_HOST/RABBITMQ_PORT igual que en los pasos 01/02.

Que vas a ver en la UI en cada escenario (pestana Queues -> step3_queue, y
la pestana Connections/Channels para ver el detalle por canal):

    - Escenario 1, justo despues de publicar (con solo worker_lento
      suscrito): Ready=0, Unacked=10 -- TODOS entregados de una, aunque el
      callback los vaya a procesar de a uno cada 1s. Si entras a la pestana
      Channels y abres el canal de worker_lento, su "Prefetch count" saldra
      en 0 (0 = ilimitado en la UI de RabbitMQ, no "cero mensajes").
    - Escenario 1, cuando worker_rapido se suscribe: aparece un consumer mas
      (consumers=2 en la cola) pero no le llega nada -- Ready sigue en 0.
    - Escenario 2, justo despues de publicar (ambos ya suscritos con QoS 1):
      Unacked se mantiene en 2 la mayor parte del tiempo (1 por worker), y
      Ready va bajando de a poco segun se van confirmando -- un patron mucho
      mas parejo que el "todo de una" del escenario 1. Si abres cada canal en
      la pestana Channels, "Prefetch count" saldra en 1 para ambos.
"""

import os
import threading
import time

import pika

QUEUE_NAME = "step3_queue"
NUM_MESSAGES = 10

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))


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


def _worker(nombre, prefetch_count, sleep_seconds, procesados, total_esperado, deadline):
    """Corre en su propio hilo, con su propia connection (pika no es
    thread-safe: cada connection solo se puede usar desde el hilo que la
    creo)."""
    connection, channel = _connect()
    if prefetch_count is not None:
        channel.basic_qos(prefetch_count=prefetch_count)

    def on_message(ch, method_frame, header_frame, body):
        time.sleep(sleep_seconds)  # simula el "trabajo" de procesar el mensaje
        ch.basic_ack(delivery_tag=method_frame.delivery_tag)
        procesados[nombre].append(body.decode())
        print(f"  [{nombre}] proceso {body.decode()!r} (lleva {len(procesados[nombre])})")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    # Loop manual (en vez de start_consuming) para poder parar cuando ya se
    # proceso todo lo esperado, sin bloquear para siempre.
    while time.monotonic() < deadline:
        if sum(len(v) for v in procesados.values()) >= total_esperado:
            break
        connection.process_data_events(time_limit=0.2)

    connection.close()


def escenario_1_sin_limite():
    print("=== Escenario 1: SIN basic_qos (default = ilimitado) ===")
    _purge()
    procesados = {"worker_lento": [], "worker_rapido": []}
    deadline = time.monotonic() + 15

    t_lento = threading.Thread(
        target=_worker,
        args=("worker_lento", None, 1.0, procesados, NUM_MESSAGES, deadline),  # sin prefetch_count (ilimitado)
    )
    t_lento.start()  # lanza el hilo, no bloquea
    time.sleep(0.5)  # asegurar que worker_lento ya esta suscrito

    _publish(NUM_MESSAGES)
    time.sleep(0.5)  # darle tiempo a Rabbit de entregarle TODO a worker_lento

    t_rapido = threading.Thread(
        target=_worker,
        args=("worker_rapido", None, 0.1, procesados, NUM_MESSAGES, deadline),  # sin prefetch_count (ilimitado)
    )
    t_rapido.start()  # lanza el hilo, no bloquea

    t_lento.join()  # bloquea aca ~10s; en la UI, Unacked=10 bajando de a uno cada 1s
    t_rapido.join()  # no espera casi nada: worker_rapido ya termino con 0 procesados
    # (en la UI, aca es donde la cola ya mostro Consumers=2 -- ambos suscritos --
    # pero a worker_rapido nunca le llego nada porque Rabbit ya le habia dado
    # los 10 a worker_lento antes de que este se suscribiera)

    print(f"worker_lento proceso: {len(procesados['worker_lento'])}")
    print(f"worker_rapido proceso: {len(procesados['worker_rapido'])}")
    assert len(procesados["worker_rapido"]) == 0, "se esperaba que worker_rapido no procesara nada"
    assert len(procesados["worker_lento"]) == NUM_MESSAGES
    print(
        "Confirmado: sin prefetch_count, Rabbit ya le habia entregado los 10 a "
        "worker_lento (unico consumer al momento de publicar) -- worker_rapido "
        "se quedo sin nada que hacer aunque procesa 10x mas rapido.\n"
    )


def escenario_2_con_prefetch_1():
    print("=== Escenario 2: CON basic_qos(prefetch_count=1) en ambos ===")
    _purge()
    procesados = {"worker_lento": [], "worker_rapido": []}
    deadline = time.monotonic() + 15

    t_lento = threading.Thread(
        target=_worker,
        args=("worker_lento", 1, 1.0, procesados, NUM_MESSAGES, deadline),  # prefetch_count=1
    )
    t_rapido = threading.Thread(
        target=_worker,
        args=("worker_rapido", 1, 0.1, procesados, NUM_MESSAGES, deadline),  # prefetch_count=1
    )
    t_lento.start()  # lanza el hilo, no bloquea
    t_rapido.start()  # lanza el hilo, no bloquea
    time.sleep(0.5)  # asegurar que AMBOS ya esten suscritos con su QoS aplicado

    _publish(NUM_MESSAGES)

    t_lento.join()  # espera a que ese hilo termine
    t_rapido.join()  # espera a que ese hilo termine

    print(f"worker_lento proceso: {len(procesados['worker_lento'])}")
    print(f"worker_rapido proceso: {len(procesados['worker_rapido'])}")
    # Lo unico que prefetch_count=1 garantiza es que nadie acapara los 10 de
    # una (a diferencia del Escenario 1). Cuanto le toca a cada worker depende
    # de una carrera de red al momento de basic_consume -- no determinista,
    # asi que NO se puede asegurar que worker_lento siempre procese > 0.
    # Tampoco es un reparto equitativo: worker_rapido (0.1s) suele terminar
    # con la mayoria -- en el 1s que worker_lento tarda en soltar su unico
    # slot, worker_rapido ya tuvo tiempo de comerse casi todo el resto de la
    # cola. prefetch_count es control de flujo (cuantos "en vuelo" a la vez),
    # no un balanceador de carga.
    assert len(procesados["worker_lento"]) + len(procesados["worker_rapido"]) == NUM_MESSAGES
    print(
        "Confirmado: con prefetch_count=1, ningun worker puede acaparar mas de "
        "1 mensaje sin confirmar -- el trabajo se reparte segun quien ackea "
        "primero. Cuantos le tocan a cada uno varia de corrida en corrida "
        "(depende de una carrera al momento de suscribirse), pero nunca los "
        "10 de una como en el escenario 1."
    )


def main():
    escenario_1_sin_limite()
    escenario_2_con_prefetch_1()


if __name__ == "__main__":
    main()
