"""
Paso 2: basic_consume + ack manual.

En 01 usamos basic_get(auto_ack=True): "dame un mensaje si hay, y en cuanto me
lo entregas dalo por confirmado y borralo". Eso es un atajo de demo. El
mecanismo real que nos interesa (y que Celery implementa con
task_acks_late=True) depende de un estado intermedio que auto_ack=True se
salta por completo:

    entregado (delivered) != confirmado (acked)

Cuando un consumer recibe un mensaje con auto_ack=False, RabbitMQ lo marca
como "unacked" y lo saca de la cola visualmente, PERO no lo borra de verdad.
Ese mensaje sigue "perteneciendo" a esa conexion/canal hasta que:

  - el consumer manda channel.basic_ack(delivery_tag) -> RabbitMQ lo borra
    para siempre.
  - el consumer manda channel.basic_nack(delivery_tag, requeue=True) -> vuelve
    a la cola inmediatamente para que otro consumer lo tome.
  - la conexion/canal se cierra (crash, kill -9, excepcion no capturada) SIN
    haber mandado ack -> RabbitMQ lo redelivera automaticamente (a este u
    otro consumer) porque nunca se confirmo.

Ese ultimo caso es exactamente lo que hace Celery con acks_late=True: si el
worker muere a la mitad de una tarea, la tarea nunca se dio por completada y
Rabbit la vuelve a poner en la cola. Es una garantia de "al menos una vez"
(at-least-once), pagada con el riesgo de reprocesar tareas si el worker muere
DESPUES de terminar el trabajo pero ANTES de mandar el ack.

Tambien cambiamos de basic_get a basic_consume: basic_get es "pull" (yo pido
un mensaje cuando quiero). basic_consume es "push" -- te registras con un
callback y RabbitMQ te empuja mensajes segun van llegando. Es el modelo que
usan los consumers reales (incluido kombu/Celery por debajo), y es el que
necesitamos para que el prefetch (paso 03) tenga sentido.

Demo de este script (todo con la MISMA conexion, en dos rondas):

    Ronda 1: publicamos 3 mensajes. Los consumimos con basic_consume /[
    auto_ack=False. Al msg 1 y 3 los ackeamos normal. Al msg 2 lo procesamos
    pero A PROPOSITO no lo ackeamos (simulamos que el consumer "muere" ahi).
    Cerramos la conexion sin ackearlo.

    Ronda 2: abrimos una conexion nueva y consumimos de nuevo. El msg 2
    reaparece solo -- Rabbit lo redelivero porque nadie lo habia confirmado.
    Esta vez si lo ackeamos.

Como probar:

    docker compose up -d
    python3 02_basic_consume_ack.py

Host/puerto salen de RABBITMQ_HOST/RABBITMQ_PORT igual que en el paso 01.

Para ver el estado en la UI entre paso y paso, pon un breakpoint (o F5 con
el debugger) entre las lineas que te interesen y revisa
http://localhost:15672 (guest/guest, pestana Queues -> step2_queue).

Que vas a ver en la UI en cada paso (pestana Queues -> step2_queue):

    - Despues de publicar los 3 mensajes: Ready=3, Unacked=0, consumers=0
      (todavia nadie se suscribio).
    - En cuanto arranca basic_consume (antes de que el callback procese
      nada): consumers=1, y Ready/Unacked se mueven juntos: Rabbit le
      empuja los 3 de una al consumer, asi que veras Ready=0, Unacked=3
      un instante (los 3 "entregados pero no confirmados").
    - Justo despues de ackear mensaje-1: Unacked baja a 2 (Total=2).
    - Justo despues de "no ackear" mensaje-2 (a proposito): Unacked se
      queda en esa cuenta -- ese mensaje no se mueve de ahi.
    - Justo despues de ackear mensaje-3: Unacked baja a 1 (solo queda
      mensaje-2, unacked, ligado a este canal).
    - Al cerrar la conexion de ronda 1 SIN ackear mensaje-2: consumers
      vuelve a 0, y Ready sube a 1 de nuevo -- Rabbit detecto el canal
      muerto con un mensaje unacked y lo re-encolo solo. Este es el
      momento clave: nadie "reenvio" nada, Rabbit lo hizo por su cuenta.
    - En ronda 2, apenas basic_consume arranca: Ready=0, Unacked=1
      (mensaje-2, redeliverado -- nota que la propiedad "redelivered"
      del mensaje viene en True si lo miras con Get messages en la UI).
    - Despues del ack final: Ready=0, Unacked=0, cola vacia.
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
