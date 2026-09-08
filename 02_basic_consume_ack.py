"""
Paso 2: basic_consume + ack manual.

Como probar:

    docker compose up -d
    python3 02_basic_consume_ack.py
"""

import os

import pika

QUEUE_NAME = "step2_queue"

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))


def connect():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return connection, channel


def ronda_1():
    print("=== Ronda 1: publicar 3, ackear 2, dejar 1 sin ack a proposito ===")
    connection, channel = connect()

    for i in range(1, 4):
        channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=f"mensaje-{i}")
    print("3 mensajes publicados.")

    procesados = []

    # Este callback lo invoca pika UNA VEZ POR MENSAJE, no una sola vez con
    # los 3 juntos. Cada mensaje que RabbitMQ empuja dispara su propia
    # llamada a on_message, en orden (primero mensaje-1, luego mensaje-2,
    # luego mensaje-3).
    def on_message(ch, method_frame, header_frame, body):
        # Llegar hasta aqui es "entregado", nada mas -- RabbitMQ ya nos dio
        # el mensaje, pero todavia no se borro de verdad. Con auto_ack=False,
        # "entregado" y "consumido" (borrado permanente) son dos momentos
        # DISTINTOS: entregado pasa siempre que on_message se ejecuta;
        # consumido de verdad pasa recien en la linea de basic_ack de abajo
        # -- y si esa linea nunca se ejecuta (como con mensaje-2 aqui abajo),
        # el mensaje jamas se consume, aunque ya lo hayamos "recibido" e
        # impreso.
        texto = body.decode()
        print(f"Recibido: {texto!r} (delivery_tag={method_frame.delivery_tag})")

        if texto == "mensaje-2":
            # A proposito NO llamamos basic_ack. Simula un consumer que se
            # cae (crash, kill -9, excepcion no capturada) despues de recibir
            # el mensaje pero antes de confirmarlo -- el mismo hueco que
            # acks_late=True deja abierto en Celery.
            print("  -> simulando crash: NO se manda ack para este mensaje.")
        else:
            # AQUI es donde el mensaje se consume/borra de verdad -- no en
            # la linea de arriba donde lo "recibimos". mensaje-1 y mensaje-3
            # pasan por aqui; mensaje-2 nunca (ver el "if" de arriba), por
            # eso el unico que sobrevive para la ronda 2.
            ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            print("  -> ack enviado.")

        procesados.append(texto)
        # stop_consuming() es la UNICA razon por la que start_consuming()
        # (mas abajo) no se queda escuchando para siempre. Sin este corte,
        # el script procesaria los 3 mensajes igual, pero despues se
        # quedaria bloqueado esperando un 4to mensaje que nunca llega -- asi
        # es como corre un worker de verdad en produccion: no sabe cuantos
        # mensajes van a llegar, simplemente se queda escuchando.
        if len(procesados) == 3:
            ch.stop_consuming()

    # basic_consume() NO consume nada por si solo: solo SUSCRIBE este canal
    # a la cola ("de ahora en adelante, empujame lo que llegue y avisame por
    # on_message"). Retorna casi al instante, no bloquea.
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)

    # start_consuming() es lo que de verdad bloquea: entra a un loop que lee
    # frames del socket y dispara on_message por cada uno, sin saber de
    # antemano cuantos van a llegar. Se queda "escuchando" indefinidamente
    # hasta que algo (aqui, el propio callback via stop_consuming()) le
    # diga que pare.
    channel.start_consuming()

    # Cerramos SIN haber ackeado "mensaje-2". Ese mensaje sigue en estado
    # "unacked" ligado a este canal; al cerrar la conexion, RabbitMQ lo
    # detecta y lo vuelve a poner en la cola para el proximo consumer.
    connection.close()
    print("Conexion de ronda 1 cerrada (mensaje-2 nunca fue ackeado).\n")


def ronda_2():
    print("=== Ronda 2: nueva conexion, ver que mensaje-2 reaparecio ===")
    connection, channel = connect()

    recibidos = []

    def on_message(ch, method_frame, header_frame, body):
        texto = body.decode()
        print(f"Recibido: {texto!r} (delivery_tag={method_frame.delivery_tag})")
        # Aqui SI se consume/borra de verdad "mensaje-2" -- la primera vez
        # (ronda_1) se entrego pero nunca se llego a este punto.
        ch.basic_ack(delivery_tag=method_frame.delivery_tag)
        print("  -> ack enviado (esta vez si).")
        recibidos.append(texto)
        # Igual que en ronda_1: sin este stop_consoming(), start_consuming()
        # de abajo se quedaria escuchando para siempre esperando otro
        # mensaje que nunca va a llegar (la cola solo tenia el 1 redeliverado).
        if len(recibidos) == 1:
            ch.stop_consuming()

    # basic_consume() solo suscribe (no bloquea); start_consuming() es el
    # que bloquea de verdad escuchando lo que llegue -- ver los comentarios
    # equivalentes en ronda_1() para el detalle completo.
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    channel.start_consuming()

    assert recibidos == ["mensaje-2"], (
        f"Se esperaba que solo 'mensaje-2' quedara pendiente, llego {recibidos}"
    )
    print("Confirmado: 'mensaje-2' fue redeliverado porque nunca se ackeo.")

    connection.close()
    print("Conexion de ronda 2 cerrada.")


def main():
    ronda_1()
    ronda_2()


if __name__ == "__main__":
    main()
