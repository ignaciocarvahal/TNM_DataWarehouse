
import psycopg2
from queue import Queue
import time
import threading
import psycopg2
import sqlalchemy as sa
import pandas as pd

from load_server import *
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from delete_data_server import *

from connection import connectionDB

import psycopg2
from queue import Queue
import time
import threading
import sqlalchemy as sa
import pandas as pd
from incremental_load_server import online_load

from utils_operaciones import hora_a_minutos, extract_data, descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from delete_data_server import *

from connection import connectionDB


def get_last_id():
    # Datos de conexión
    host = "190.171.188.230"
    port = "5432"
    database = "topusDB"
    
    user = "user_solo_lectura"
    password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"
    
    # Establece la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Abre un cursor para ejecutar consultas SQL
    cursor = conn.cursor()

    try:
        # Consulta SQL para obtener el id más alto actual
        query = "SELECT MAX(id) FROM public.servicios_logs;"

        # Ejecuta la consulta SQL
        cursor.execute(query)
        
        # Obtiene el resultado de la consulta
        last_id = cursor.fetchone()[0]
        return last_id if last_id is not None else 0

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error al obtener el último ID: {e}")
        return 0

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()

def get_fk_servicio_values(last_id, numero_de_indices):
    # Datos de conexión
    host = "190.171.188.230"
    port = "5432"
    database = "topusDB"
    
    user = "user_solo_lectura"
    password = "4l13nW4r3.C0ntr4s3n4.S0l0.L3ctur4"
    
    # Establece la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Abre un cursor para ejecutar consultas SQL
    cursor = conn.cursor()

    try:
        # Consulta SQL para obtener los nuevos valores de fk_servicio desde servicios_logs
        query = f"""
            SELECT id, fk_servicio
            FROM public.servicios_logs
            WHERE id > {last_id}
            ORDER BY id ASC
            LIMIT {numero_de_indices};
        """

        # Ejecuta la consulta SQL
        cursor.execute(query)
        
        # Obtiene los resultados de la consulta
        new_records = cursor.fetchall()
        return new_records

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error al obtener valores de fk_servicio: {e}")
        return None

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()



def get_unique_values_from_queue(queue):
    unique_values = set()
    while not queue.empty():
        unique_values.add(queue.get())
        print(list(unique_values))
    return list(unique_values)


def create_queue_with_new_records(queue, last_id=0, polling_interval=0.1, numero_de_indices=10000):
    while True:
        # Obtiene los nuevos registros
        new_records = get_fk_servicio_values(last_id, numero_de_indices)
        fk_servicios = get_unique_values_from_queue(queue)
        
        if new_records:
            print(fk_servicios)
            for record in new_records:
                if record[1] not in fk_servicios:
                    queue.put(record[1])  # Agrega fk_servicio a la cola
            last_id = record[0]  # Actualiza last_id al id del último registro procesado
        
        # Espera un intervalo antes de la siguiente consulta
        time.sleep(polling_interval)




def process_queue(queue):
    while True:
        # Si la cola no está vacía, procesa y muestra el siguiente elemento
        if not queue.empty():
            fk_servicio = queue.get()
            try:
                # Llama a la función online_load con el fk_servicio
                online_load(fk_servicio)
                print(f"Procesado y eliminado fk_servicio: {fk_servicio}")
            except Exception as e:
                # Maneja cualquier error que pueda ocurrir durante el procesamiento
                print(f"Error al procesar fk_servicio {fk_servicio}: {e}")
            finally:
                queue.task_done()  # Marca el elemento como procesado


def carga_online():
    # Obtener el último ID actual al inicio
    initial_last_id = get_last_id()
    print(f"Último ID inicial: {initial_last_id}")
    
    # Crear una cola
    record_queue = Queue()
    
    # Iniciar el proceso para llenar la cola con nuevos registros
    polling_thread = threading.Thread(target=create_queue_with_new_records, args=(record_queue, initial_last_id))
    polling_thread.start()
    
    # Iniciar el proceso para mostrar los elementos de la cola
    processing_thread = threading.Thread(target=process_queue, args=(record_queue,))
    processing_thread.start()
    
    # Esperar a que los hilos terminen (esto no sucederá a menos que los hilos terminen por algún motivo)
    polling_thread.join()
    processing_thread.join()
"""

def carga_online(stop_hour, stop_minute):
    # Obtener el último ID actual al inicio
    initial_last_id = get_last_id()
    print(f"Último ID inicial: {initial_last_id}")
    
    # Crear una cola
    record_queue = Queue()
    
    # Iniciar el proceso para llenar la cola con nuevos registros
    polling_thread = threading.Thread(target=create_queue_with_new_records, args=(record_queue, initial_last_id))
    polling_thread.start()
    
    # Iniciar el proceso para mostrar los elementos de la cola
    processing_thread = threading.Thread(target=process_queue, args=(record_queue,))
    processing_thread.start()
    
    # Iniciar el proceso para detener los hilos a una hora específica
    stop_thread = threading.Thread(target=stop_at_specific_time, args=(stop_hour, stop_minute))
    stop_thread.start()
    
    # Esperar a que los hilos terminen (esto no sucederá a menos que los hilos terminen por algún motivo)
    polling_thread.join()
    processing_thread.join()
    stop_thread.join()

"""
 
carga_online()


'''
import threading
from queue import Queue

def process_queue(queue, unique_set):
    while True:
        # Si la cola no está vacía, procesa y muestra el siguiente elemento
        if not queue.empty():
            fk_servicio = queue.get()
            try:
                # Llama a la función online_load con el fk_servicio
                online_load(fk_servicio)
                print(f"Procesado y eliminado fk_servicio: {fk_servicio}")
            except Exception as e:
                # Maneja cualquier error que pueda ocurrir durante el procesamiento
                print(f"Error al procesar fk_servicio {fk_servicio}: {e}")
            finally:
                unique_set.remove(fk_servicio)  # Eliminar el elemento del conjunto
                queue.task_done()  # Marca el elemento como procesado

def create_queue_with_new_records(queue, initial_last_id, unique_set):
    while True:
        # Suponiendo que get_new_records devuelve una lista de nuevos fk_servicio
        new_records = list(get_fk_servicio_values(initial_last_id, 10000000))
        for record in new_records:
            if record not in unique_set:
                unique_set.add(record)
                queue.put(record)
        # Actualizar el último ID procesado
        if new_records:
            initial_last_id = new_records[-1]

# Obtener el último ID actual al inicio
initial_last_id = get_last_id()
print(f"Último ID inicial: {initial_last_id}")

# Crear una cola
record_queue = Queue()
# Crear un conjunto para rastrear valores únicos
unique_set = set()

# Iniciar el proceso para llenar la cola con nuevos registros
polling_thread = threading.Thread(target=create_queue_with_new_records, args=(record_queue, initial_last_id, unique_set))
polling_thread.start()

# Iniciar el proceso para mostrar los elementos de la cola
processing_thread = threading.Thread(target=process_queue, args=(record_queue, unique_set))
processing_thread.start()

# Esperar a que los hilos terminen (esto no sucederá a menos que los hilos terminen por algún motivo)
polling_thread.join()
processing_thread.join()


'''

