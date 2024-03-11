# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 10:23:27 2023

@author: Ignacio Carvajal
"""

from datetime import datetime, MINYEAR
import pandas as pd
import numpy as np
import random
import os
import psycopg2
from datetime import datetime, time
from connection import *
import psycopg2
import pandas as pd
import numpy as np


def hora_a_minutos(hora_str):
    # Divide la cadena de hora en horas, minutos y segundos
    partes = hora_str.split(":")

    # Convierte las partes en enteros
    horas = int(partes[0])
    minutos = int(partes[1])
    segundos = int(partes[2])

    # Calcula los minutos totales
    minutos_totales = horas * 60 + minutos

    return minutos_totales


def extract_data():

    directory = os.getcwd()
    with open(directory + "\\queries\\query_dw.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido

    df = connectionDB_todf(query)

    # Ahora, guardamos el DataFrame en un archivo Excel llamado 'data_plana.xlsx'
    nombre_del_archivo = 'data_plana.xlsx'
    # El argumento "index=False" evita que se escriba el índice en el archivo
    #df.to_excel(nombre_del_archivo, index=False)

    return df


def descomponer_fechas(df):
    # Eliminar filas con 'etapa_1_fecha' vacía
    # df = df.dropna(subset=['etapa_1_fecha'])
    # df = df.dropna(subset=['etapa_1_hora'])

    # Lista de columnas de fecha y hora a descomponer
    columnas_fecha_hora = ['eta_fecha', 'etapa_1_fecha',
                           'etapa_1_hora', 'hora_real_arribo', 'hora_real_salida']

    for columna in columnas_fecha_hora:
        # Verificar si la columna contiene valores no vacíos
        try:
            # Utilizar apply con una función personalizada para manejar nulos
            df[columna] = df[columna].apply(lambda x: pd.to_datetime(
                x, format='%d-%m-%Y' if 'fecha' in columna else '%H:%M', errors='coerce'))

            # Crea nuevas columnas para año, mes, día, hora y minuto
            df[f'{columna}_anio'] = df[columna].dt.year
            df[f'{columna}_mes_str'] = df[columna].dt.strftime(
                '%B')  # Formatea el nombre completo del mes
            df[f'{columna}_mes'] = df[columna].dt.month
            df[f'{columna}_dia'] = df[columna].dt.day
            df[f'{columna}_hora'] = df[columna].dt.hour
            df[f'{columna}_minuto'] = df[columna].dt.minute
        except ValueError:
            # Si la conversión falla, establece un valor nulo
            df[columna] = np.nan

    # Ahora, guardamos el DataFrame en un archivo Excel llamado 'data_plana.xlsx'
    nombre_del_archivo = 'data.xlsx'
    #El argumento "index=False" evita que se escriba el índice en el archivo
    #df.to_excel(nombre_del_archivo, index=False)
    return df


def unir_fecha_hora(etapa_1_fecha, etapa_1_hora):
    print("1", etapa_1_fecha, etapa_1_hora)
    # Verificar si las fechas son None o iguales a 0
    if etapa_1_fecha is None or etapa_1_fecha.year == 0:
        etapa_1_fecha = datetime(3000, 1, 1)
    # Verificar si las horas son None o iguales a 0
    if etapa_1_hora is None or etapa_1_hora.hour == 0 and etapa_1_hora.minute == 0 and etapa_1_hora.second == 0:
        etapa_1_hora = time(0, 0)
    print("2", etapa_1_fecha, etapa_1_hora)
    # Combinar la fecha y la hora en un solo objeto datetime
    etapa_1_fecha_hora = datetime(
        int(etapa_1_fecha.year), int(
            etapa_1_fecha.month), int(etapa_1_fecha.day),
        int(etapa_1_hora.hour), int(
            etapa_1_hora.minute), int(etapa_1_hora.second)
    )

    #print("Resultado:", etapa_1_fecha_hora)

    return etapa_1_fecha_hora


def unir_fecha_hora(etapa_1_fecha, etapa_1_hora):

    # Verificar si las fechas son NaT o iguales a 0
    if pd.isna(etapa_1_fecha) or etapa_1_fecha.year == 0:
        etapa_1_fecha = datetime(3000, 1, 1)
    # Verificar si las horas son NaT o iguales a 0
    if pd.isna(etapa_1_hora) or etapa_1_hora.hour == 0 and etapa_1_hora.minute == 0 and etapa_1_hora.second == 0:
        etapa_1_hora = time(0, 0)

    # Combinar la fecha y la hora en un solo objeto datetime
    etapa_1_fecha_hora = datetime(
        etapa_1_fecha.year, etapa_1_fecha.month, etapa_1_fecha.day,
        etapa_1_hora.hour, etapa_1_hora.minute, etapa_1_hora.second
    )

    #print("Resultado:", etapa_1_fecha_hora)

    return etapa_1_fecha_hora


def formatear_a_fecha(fecha):
    try:
        # Intenta convertir a datetime
        fecha_formateada = pd.to_datetime(fecha, errors='coerce')
        if pd.notna(fecha_formateada):
            return fecha_formateada
    except ValueError:
        pass  # Ignora errores de conversión y continúa

    # Si no se puede convertir, devuelve la fecha mínima
    return datetime(3000, 1, 1)
