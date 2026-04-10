"""

NOMBRE, MATRICULA: Cristina Forchue Martich, 23-0865
PROFESOR: Joerlyn Mariano Morfe Ureña 

"""

import threading
import time

base_de_datos = {}
historial_eventos = []

## Lock global para evitar conflictos cuando varios hilos escriben al mismo tiempo
lock = threading.Lock()


def publicar_evento(tipo, detalle):
    ## guarda un registro de lo que ocurrio en el sistema
    historial_eventos.append({"tipo": tipo, "detalle": detalle, "hora": time.strftime("%H:%M:%S")})


def insertar(tabla, datos):
    with lock:
        if tabla not in base_de_datos:
            base_de_datos[tabla] = []
        base_de_datos[tabla].append(datos)
    publicar_evento("INSERT", f"{tabla} -> {datos}")
    print(f"[INSERT] {tabla}: {datos}")


def consultar(tabla, filtros=None):
    registros = base_de_datos.get(tabla, [])
    if filtros:
        ## solo devuelve registros donde todos los campos que coincidan
        registros = [r for r in registros if all(r.get(k) == v for k, v in filtros.items())]
    return registros


def actualizar(tabla, filtros, nuevos_datos):
    with lock:
        for registro in base_de_datos.get(tabla, []):
            if all(registro.get(k) == v for k, v in filtros.items()):
                registro.update(nuevos_datos)
    publicar_evento("UPDATE", f"{tabla} donde {filtros} -> {nuevos_datos}")
    print(f"[UPDATE] {tabla} donde {filtros}: {nuevos_datos}")


def eliminar(tabla, filtros):
    with lock:
        antes = base_de_datos.get(tabla, [])
        ## Conserva solo los registros que NO coincidan con el filtro
        base_de_datos[tabla] = [r for r in antes if not all(r.get(k) == v for k, v in filtros.items())]
    publicar_evento("DELETE", f"{tabla} donde {filtros}")
    print(f"[DELETE] {tabla} donde {filtros}")


def transaccion(operaciones):
    ## ejecuta varias operaciones como un bloque. Si algo falla, no se aplica ninguna.
    try:
        for op in operaciones:
            op()
        publicar_evento("COMMIT", f"{len(operaciones)} operaciones")
        print("[COMMIT] Transaccion completada")
    except Exception as e:
        publicar_evento("ROLLBACK", str(e))
        print(f"[ROLLBACK] Error: {e}")


## PRUEBAS

print("Base de Datos en Tiempo Real\n")

print("PRUEBA 1: CRUD Basico")
insertar("usuarios", {"nombre": "Yamel", "edad": 25})
insertar("usuarios", {"nombre": "Cris", "edad": 30})

resultado = consultar("usuarios", {"nombre": "Yamel"})
print(f"Consulta: {resultado}")

actualizar("usuarios", {"nombre": "Yamel"}, {"edad": 26})
print(f"Despues de actualizar: {consultar('usuarios', {'nombre': 'Yamel'})}")

eliminar("usuarios", {"nombre": "Joerlyn"})
print(f"Despues de eliminar Joerlyn: {consultar('usuarios')}")


print("PRUEBA 2: Transaccion")
transaccion([
    lambda: insertar("productos", {"nombre": "Laptop", "precio": 800}),
    lambda: insertar("productos", {"nombre": "Mouse", "precio": 20}),
])

## Transaccion que falla a proposito
transaccion([
    lambda: insertar("productos", {"nombre": "Teclado"}),
    lambda: (_ for _ in ()).throw(Exception("Error simulado en la transaccion")),
])


print("PRUEBA 3 varios hilos insertando a la vez")
def insertar_pedido(i):
    insertar("pedidos", {"cliente": f"Cliente-{i}", "monto": i * 10})

hilos = [threading.Thread(target=insertar_pedido, args=(i,)) for i in range(5)]
for h in hilos: h.start()
for h in hilos: h.join()
print(f"Total pedidos insertados: {len(consultar('pedidos'))}")

