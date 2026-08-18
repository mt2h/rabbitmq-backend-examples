"""
Paso 1: lo minimo indispensable para hablar con RabbitMQ.

Conceptos que este script toca (y nada mas que estos, a proposito):

- Connection: la conexion TCP real contra el servidor RabbitMQ (localhost:5672
  porque el docker-compose expone ese puerto). Es "pesada": abrirla cuesta un
  handshake TCP + autenticacion.
- Channel: un "canal virtual" que viaja DENTRO de una connection. Casi todas
  las operaciones (declarar colas, publicar, consumir) se hacen sobre un
  channel, no sobre la connection directamente. Una connection puede tener
  muchos channels; asi una sola conexion TCP sirve para varias operaciones
  concurrentes sin abrir un socket por cada una.
- Queue: el buffer de mensajes en si. "Declarar" una cola es idempotente: si
  no existe la crea, si ya existe con la misma config no hace nada.
- Publish / Consume: publicar deja un mensaje en la cola; consumir lo saca.
  Aqui hacemos ambas cosas en el mismo script solo para comprobar que el viaje
  redondo funciona.

Nada de timeouts, pools ni concurrencia todavia -- eso viene despues.

Como probar:

    docker compose up -d
    pip install -r requirements.txt
    python3 01_connect_and_queue.py

Host/puerto salen de las variables de entorno RABBITMQ_HOST/RABBITMQ_PORT
(default localhost:5672). Util para meter un proxy en medio, p.ej. mitmproxy:

    RABBITMQ_PORT=8080 python3 01_connect_and_queue.py

Salida esperada:

    Conexion abierta.
    Channel abierto.
    Cola 'step1_queue' declarada.
    Mensaje publicado: 'hola desde step1'
    Mensaje recibido de vuelta: 'hola desde step1'
    Conexion cerrada.

Para ver el estado en la UI entre paso y paso, pon un breakpoint (o F5 con el
debugger) entre las lineas que te interesen y revisa
http://localhost:15672 (guest/guest, pestana Queues -> step1_queue).

Que vas a ver en la UI en cada paso (pestana Queues -> step1_queue):

    - Antes de queue_declare(): la cola puede ni aparecer en el listado
      (si es la primera vez que corres el script).
    - Despues de queue_declare(): aparece con Ready=0, Unacked=0, Total=0,
      consumers=0.
    - Despues de basic_publish(): Ready pasa a 1 -- el mensaje esta ahi
      sentado, nadie lo ha tocado.
    - Despues de basic_get(auto_ack=True): Ready vuelve a 0. A diferencia
      del "Get messages" de la UI con Ack Mode "Nack message requeue true"
      (que lo regresa a la cola), auto_ack=True lo borra para siempre en
      cuanto se entrega -- no hay ventana donde quede "Unacked".
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
    #    atajo para "sin exchange propio, entrega por nombre de cola". Cuando
    #    veamos exchanges de verdad (fanout/direct/topic) dejaremos de usar "".
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
