# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 15:59:25 2024

@author: Ignacio Carvajal
"""


import pandas as pd
import schedule
import time
import datetime
from incremental_load_server import incremental_batch_load_online, incremental_batch_load, incremental_batch_load_tomorrow, massive_load
from online_load_queue import *

def job_planificacion():
    print("Ejecutando el script...")
    incremental_batch_load_tomorrow(1000, 1)
    
    print("Script ejecutado.")
    
def job_222():
    
    print("Ejecutando el script...")
    incremental_batch_load(5000,200)
    
    
    print("Script ejecutado.")

def mini_job():
    
    print("Ejecutando el script...")
    incremental_batch_load(400,200)
    
    print("Script ejecutado.")

def nitan_mini_job():
    
    print("Ejecutando el script...")
    incremental_batch_load_online(400,200)
    
    print("Script ejecutado.")



 
def mega_job():
    
    print("Ejecutando el script...")
    
   
    incremental_batch_load(400, 1)
    #massive_load()
    print("Script ejecutado.")
    carga_online()
   
# Programar la ejecución del script todos los días a las 13:00 y 16:30
#schedule.every().day.at("09:16").do(nitan_mini_job)
#schedule.every().day.at("10:20").do(mini_job)
#schedule.every().day.at("10:30").do(mini_job)
#schedule.every().day.at("14:00").do(mini_job)
#schedule.every().day.at("14:15").do(mini_job)

#schedule.every().day.at("17:31").do(mini_job)
#schedule.every().day.at("10:20").do(job_222)
#mega carga de datos:
#schedule.every().day.at("01:00").do(mega_job)
#mega_job()

mega_job()
#carga de actualización media
#schedule.every().day.at("01:20").do(mega_jo)

schedule.every().day.at("11:00").do(mega_job)
schedule.every().day.at("14:00").do(mega_job)
schedule.every().day.at("23:00").do(mega_job)
"""
mega_job()
# Medir el tiempo de ejecución
start_time = time.time()  # Tomar el tiempo inicial
nitan_mini_job()  # Llamar a la función o proceso que quieres medir
end_time = time.time() 

elapsed_time = end_time - start_time  # Calcular el tiempo transcurrido
print(f"El proceso tomó {elapsed_time:.2f} segundos")

# Programar la ejecución cada 10 minutos
schedule.every(10).minutes.do(nitan_mini_job)

# Mantener el script ejecutándose para que el scheduler siga funcionando
while True:
    schedule.run_pending()  # Ejecuta las tareas pendientes
    
    time.sleep(1) 

#mini cargas para data online prueba
#schedule.every().day.at("23:13").do(mega_job)
#schedule.every().day.at("15:15").do(mini_job)
#schedule.every().day.at("15:30").do(mini_job)
#schedule.every().day.at("15:45").do(mini_job)
#schedule.every().day.at("16:00").do(mini_job)
#schedule.every().day.at("16:15").do(mini_job)

#schedule.every().day.at("16:30").do(mini_job)
#schedule.every().day.at("16:45").do(mini_job)
#schedule.every().day.at("16:45").do(mini_job)
#schedule.every().day.at("17:00").do(mini_job)

#schedule.every().day.at("17:15").do(mini_job)
#schedule.every().day.at("15:30").do(mega_job)
#schedule.every().day.at("17:45").do(mini_job)



#schedule.every().day.at("08:50").do(mini_job)
#schedule.every().day.at("12:46").do(job_planificacion)
#schedule.every().day.at("13:30").do(job_planificacion)
#schedule.every().day.at("14:00").do(job_planificacion)
#schedule.every().day.at("15:59").do(job_planificacion)
#schedule.every().day.at("16:59").do(job_planificacion)
#schedule.every().day.at("17:59").do(job_planificacion)
#schedule.every().day.at("21:59").do(job_planificacion)
#schedule.every().day.at("19:59").do(job_planificacion)
#schedule.every().day.at("20:59").do(job_planificacion) 
#schedule.every().day.at("21:59").do(job_planificacion)

"""
while True:
    schedule.run_pending()
    time.sleep(1)
