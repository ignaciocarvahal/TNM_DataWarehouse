# -*- coding: utf-8 -*-
"""
Created on Mon Jan 22 13:06:22 2024

@author: Ignacio Carvajal
"""


import pandas as pd
import schedule
import time
import datetime
from facturas import incremental_batch_load_facturas

def job():
    
    print("Ejecutando el script de conductores...")
    incremental_batch_load_facturas()
    

    print("Script de conductores ejecutado.")




# Programar la ejecución del script todos los días a las 13:00 y 16:30


schedule.every().day.at("08:30").do(job)
schedule.every().day.at("09:40").do(job)
schedule.every().day.at("11:40").do(job)
schedule.every().day.at("10:23").do(job)
schedule.every().day.at("16:48").do(job)
schedule.every().day.at("18:40").do(job)
schedule.every().day.at("20:40").do(job)
schedule.every().day.at("22:40").do(job)

#schedule.every().day.at("01:30").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
