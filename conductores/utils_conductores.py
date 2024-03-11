# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 10:21:40 2023

@author: Ignacio Carvajal
"""
import pandas as pd
from sqlalchemy import create_engine
from connection import connectionDB_todf
import os


def extract_data():
    directory = os.getcwd()
    with open(directory + "\\queries\\conductores_disp_3.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido
    df = connectionDB_todf(query)
    return df


def formatear_fechas(df, date_columns):

    # Iterate over each date column
    for column in date_columns:
        try:
            df[column] = pd.to_datetime(df[column], format='%d-%m-%Y %H:%M', errors='raise')
        except ValueError as e:
            # Handle the conversion error
            print(f"Error converting {column}: {e}")
            # You can decide what to do with problematic rows, for example, set them to NaN or replace with a default value
            df[column] = pd.to_datetime(df[column], format='%d-%m-%Y %H:%M', errors='coerce')
        
        # Convert to tz-naive if the column is tz-aware
        if df[column].dt.tz is not None:
            df[column] = df[column].dt.tz_localize(None)

    return df