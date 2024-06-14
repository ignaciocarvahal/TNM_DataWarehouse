# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 15:59:25 2024

@author: Ignacio Carvajal
"""


import pandas as pd
import schedule
import time
import datetime
from incremental_load_server import incremental_batch_load, incremental_batch_load_tomorrow


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
    incremental_batch_load(4000,200)
    
    print("Script ejecutado.")


 
def mega_job():
    
    print("Ejecutando el script...")
    incremental_batch_load(10000, 200)
    
    print("Script ejecutado.")
   
# Programar la ejecución del script todos los días a las 13:00 y 16:30
#schedule.every().day.at("09:16").do(nitan_mini_job)
#schedule.every().day.at("10:20").do(mini_job)
#schedule.every().day.at("10:30").do(mini_job)
#schedule.every().day.at("14:00").do(mini_job)
#schedule.every().day.at("14:15").do(mini_job)
#schedule.every().day.at("17:31").do(mini_job)
#schedule.every().day.at("10:20").do(job_222)
#mega carga de datos:
#schedule.every().day.at("14:09").do(mega_job)


#carga de actualización media
schedule.every().day.at("20:30").do(mega_job)
#schedule.every().day.at("09:40").do(job)


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
schedule.every().day.at("14:00").do(job_planificacion)
#schedule.every().day.at("15:59").do(job_planificacion)
#schedule.every().day.at("16:59").do(job_planificacion)
#schedule.every().day.at("17:59").do(job_planificacion)
#schedule.every().day.at("21:59").do(job_planificacion)
#schedule.every().day.at("19:59").do(job_planificacion)
#schedule.every().day.at("20:59").do(job_planificacion) 
#schedule.every().day.at("21:59").do(job_planificacion)


while True:
    schedule.run_pending()
    time.sleep(1)
