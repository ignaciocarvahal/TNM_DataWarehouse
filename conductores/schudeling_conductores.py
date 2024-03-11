


import pandas as pd
import schedule
import time
import datetime
from conductores2 import incremental_batch_load_conductores

def job():
    
    print("Ejecutando el script de conductores...")
    incremental_batch_load_conductores()
    

    print("Script de conductores ejecutado.")





# Programar la ejecución del script todos los días a las 13:00 y 16:30


schedule.every().day.at("04:30").do(job)
schedule.every().day.at("09:20").do(job)
schedule.every().day.at("11:20").do(job)
schedule.every().day.at("10:28").do(job)
schedule.every().day.at("14:40").do(job)
schedule.every().day.at("18:20").do(job)
schedule.every().day.at("20:20").do(job)
schedule.every().day.at("21:49").do(job)
schedule.every().day.at("22:00").do(job)

#schedule.every().day.at("01:30").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
