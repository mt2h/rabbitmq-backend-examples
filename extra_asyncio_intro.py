"""
async/await/gather explicado con el ejemplo mas chico posible.

Asincronico quiere decir esto: en vez de que el programa se quede parado
esperando a que una tarea termine antes de arrancar la siguiente (eso es
codigo sincronico, secuencial), varias tareas pueden quedar "en progreso" al
mismo tiempo, y el programa aprovecha los tiempos muertos de una (mientras
espera una respuesta, por ejemplo) para avanzar con otra.

Ojo con quien hace ese aprovechamiento: no es la CPU la que decide usar el
tiempo muerto -- la CPU no sabe nada de corrutinas, solo ejecuta lo que el
interprete le va dando. Quien decide es el event loop (el despachador de
asyncio, un solo hilo): cuando una corrutina hace await sobre algo que
implica esperar una respuesta externa, le devuelve el control al event loop
en ese instante, y este busca otra corrutina lista para avanzar y le da el
turno. Asi la CPU, que si no hubiera quedado ociosa esperando la red, hace
trabajo util con la otra tarea mientras tanto.

Si vienes de Node: asyncio.gather(...) es el equivalente directo de
Promise.all([...]) -- corre varias tareas async al mismo tiempo (concurrentes,
no en threads separados) y espera a que todas terminen.

Una diferencia real con JS, no solo de sintaxis: en Node, una async function
arranca a correr sincronicamente apenas la llamas, hasta el primer await, y en
ese momento te devuelve una Promise pendiente. En Python, llamar a una
`async def` sin await no corre nada del cuerpo -- ni la primera linea. Se
queda como un objeto "corrutina" fria hasta que alguien le hace await, o hasta
que asyncio.run()/asyncio.gather() la programa.

async def define una funcion que no corre sola -- hay que hacerle await.
await pausa esa funcion y le deja el turno a otra cosa mientras espera.
asyncio.gather corre varias de esas funciones a la vez, aprovechando esas
pausas, en vez de esperarlas una por una.

Corrutina no es lo mismo que hilo (thread). Un hilo lo maneja el sistema
operativo, que puede interrumpirlo en cualquier momento para darle el turno a
otro (multitarea preventiva) -- por eso los hilos si pueden llegar a correr en
paralelo de verdad, cada uno en un core distinto. Una corrutina vive dentro de
UN SOLO hilo: nadie la interrumpe a la fuerza, ella misma decide en que puntos
soltar el turno (cada await es uno de esos puntos, elegido por el codigo, no
por el sistema operativo). Por eso asyncio da concurrencia, no paralelismo:
nunca hay dos corrutinas ejecutando Python al mismo tiempo, solo se turnan
aprovechando las esperas.

Y multiprocessing es otra cosa todavia, un escalon mas arriba: son procesos
del sistema operativo completamente separados, cada uno con su propia
memoria y su propio interprete de Python -- esos si pueden ejecutar Python
en paralelo de verdad, uno por core. Pero son pesados de crear y no
comparten memoria entre si (hay que mandarse datos explicitamente). En orden
de "cuanto paralelismo real dan" queda: corrutina (ninguno, todo en un solo
hilo) < hilo (limitado en Python por el GIL para codigo Python puro, aunque
si ayuda con I/O) < proceso (paralelismo real de CPU). Para el caso de este
proyecto (esperar respuestas de red) asyncio alcanza y sobra -- no hace
falta ni hilos ni procesos.
"""

import asyncio
import time


async def saludar(nombre, segundos):
    print(f"{nombre}: empiezo, espero {segundos}s")
    await asyncio.sleep(segundos)
    print(f"{nombre}: termine")


async def main():
    inicio = time.monotonic()
    # Uno despues del otro: los tiempos se suman (~2s en total).
    await saludar("A", 1)
    await saludar("B", 1)
    print(f"secuencial: {time.monotonic() - inicio:.1f}s\n")

    inicio = time.monotonic()
    # Los dos juntos con gather: se solapan, tarda lo que tarda el mas lento (~1s).
    await asyncio.gather(saludar("C", 1), saludar("D", 1))
    print(f"gather: {time.monotonic() - inicio:.1f}s")


# El puente entre el mundo sincronico (donde arranca el script) y el
# asincronico (donde viven las corrutinas). Arranca el event loop, corre
# main() hasta que termina, y lo cierra. Se llama UNA sola vez, aca arriba
# de todo -- adentro de codigo async ya no se vuelve a llamar asyncio.run(),
# ahi se hace simplemente await.
asyncio.run(main())
