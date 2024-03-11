# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 14:16:49 2023

@author: Ignacio Carvajal
"""
import pandas as pd
from sqlalchemy import create_engine
from connection import connectionDB_todf
import os


def extract_data():
    directory = os.getcwd()
    with open(directory + "\\queries\\facturas2.txt", "r") as archivo:
        contenido = archivo.read()
    query = contenido
    df = connectionDB_todf(query)
    return df
