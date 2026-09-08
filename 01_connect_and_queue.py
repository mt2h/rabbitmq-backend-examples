"""
Paso 1: lo minimo indispensable para hablar con RabbitMQ.

Como probar:

    docker compose up -d
    pip install -r requirements.txt
    python3 01_connect_and_queue.py
"""

import os

import pika

QUEUE_NAME = "step1_queue"

# Host/puerto configurables por variable de entorno para poder meter un
# proxy en medio (p.ej. mitmproxy) sin tocar el codigo.
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))


def main():
    # 1. Abrir la connection (TCP + login) contra el RabbitMQ que levantamos
    #    con docker-compose. guest/guest son las credenciales por defecto de
    #    la imagen oficial, validas solo para conexiones locales.
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    print(f"Conexion abierta ({RABBITMQ_HOST}:{RABBITMQ_PORT}).")

    # 2. Abrir un channel sobre esa connection.
    channel = connection.channel()
    print("Channel abierto.")

    # 3. Declarar la cola (crearla si no existe).
    channel.queue_declare(queue=QUEUE_NAME)
    print(f"Cola '{QUEUE_NAME}' declarada.")

    # 4. Publicar un mensaje de prueba.
    #    exchange="" es el "default exchange" de RabbitMQ: un exchange
    #    implicito que enruta cada mensaje directo a la cola cuyo nombre
    #    coincide exactamente con routing_key. En realidad TODO publish pasa
    #    por un exchange (nunca se publica directo a una cola); usar "" es el
    #    atajo para "sin exchange propio, entrega por nombre de cola". Es lo
    #    mismo que usa el incidente real que reconstruimos (ver README de
    #    `_backup_original`), asi que todos los scripts numerados se quedan
    #    con "" -- no hay ningun paso planeado que use exchanges tipados
    #    (fanout/direct/topic).
    mensaje = "hola desde step1"
    channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=mensaje)
    print(f"Mensaje publicado: {mensaje!r}")

    # 5. Consumirlo de vuelta (basic_get = "dame un mensaje si hay, sin
    #    bloquear esperando"; distinto de basic_consume, que veremos despues).
    method_frame, header_frame, body = channel.basic_get(
        # auto_ack=True: apenas RabbitMQ nos entrega el mensaje lo da por
        # confirmado (ack) automaticamente y lo borra de la cola. Con
        # auto_ack=False el mensaje queda "entregado pero no confirmado": si
        # el consumidor se cae antes de mandar el ack explicito (channel.
        # basic_ack), RabbitMQ lo vuelve a poner en la cola para reintentar.
        # Aqui usamos True porque este script no hace nada que pueda fallar
        # entre recibir y procesar; el manejo real de acks (y sus riesgos:
        # duplicados, mensajes perdidos) lo vemos mas adelante.
        queue=QUEUE_NAME, auto_ack=True
    )
    if method_frame:
        print(f"Mensaje recibido de vuelta: {body.decode()!r}")
    else:
        print("No habia ningun mensaje en la cola (raro, algo fallo).")

    # 6. Cerrar prolijo.
    connection.close()
    print("Conexion cerrada.")


if __name__ == "__main__":
    main()
