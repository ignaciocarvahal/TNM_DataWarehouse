# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 14:16:11 2023

@author: Ignacio Carvajal
"""

import pandas as pd
from sqlalchemy import create_engine
from connection import connectionDB_todf
from utils_facturas import extract_data
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def incremental_batch_load_facturas():
    # Extracción de la data desde la BBDD
    data_df = extract_data()
    print(data_df)
    data_df = data_df.drop_duplicates()
    
    # Supongamos que ya tienes un DataFrame llamado 'data_df' con las mismas columnas que las tablas
    
    # Configuración de la conexión a PostgreSQL
    db_config = {
        # Datos de conexión
    
        'host': "3.91.152.225",
        'database': "dw",
        'user': "postgres",
        'password': "ignacio",
        'port': "5432"
    }
    
    # Crear conexión a la base de datos
    conn = psycopg2.connect(**db_config)
    
    # Crear un motor SQLAlchemy para facilitar la carga de datos en la base de datos
    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
    
    
    # Dimension Comercial
    dimension_comercial_df = data_df[['id','comercial_rut', 'comercial']]
    dimension_comercial_df.to_sql('dimension_comercial', con=engine, if_exists='replace', index=True, schema='facturas')
    
    # Dimension Cliente
    dimension_cliente_df = data_df[['id', 'cliente_despacho_codigo', 'cliente_despacho_rut', 'cliente_despacho_nombre', 'cliente_facturacion', 'fecha_conversion_cliente']].drop_duplicates()
    dimension_cliente_df.to_sql('dimension_cliente', con=engine, if_exists='replace', index=True, schema='facturas')
    
    # Dimension Nave
    dimension_nave_df = data_df[['id', 'nave_id', 'nave_nombre']]
    dimension_nave_df.to_sql('dimension_nave', con=engine, if_exists='replace', index=True, schema='facturas')
    
    # Dimension Contenedor
    dimension_contenedor_df = data_df[['id', 'contenedor', 'cont_tamano']]
    dimension_contenedor_df.to_sql('dimension_contenedor', con=engine, if_exists='replace', index=True, schema='facturas')
    
    # Tabla de Hechos (Facturas)
    facturas_df = data_df[['id', 'fk_servicio', 'sii_factura', 'estado', 'sii_fecha', 'fk_responsable', 'fecha_envio_fact', 'total_servicio', 'total_cobros_extras', 'cant_servicios', 'total_servicios', 'total_cuadrilla', 'total_sobrepeso', 'total_sobreeestadia', 'total_almacenaje', 'total_con_refer', 'total_otros_cobros', 'createdAt', 'updatedAt', 'age_nombre', 'total_viajes_1', 'total_viajes_1_refeer', 'total_viajes_2', 'cuadrilla', 'cuadrilla_cantidad', 'sobre_peso', 'sobre_estadia_valor', 'sobreestadia_horas_libres', 'sobreestadia_costo_hora', 'almacenaje_dias', 'almacenaje_dias_valor', 'almacenaje_dias_valor_conexion', 'almacenaje_dias_conexion_horas', 'cliente_despacho_codigo', 'cliente_despacho_rut', 'cliente_despacho_nombre', 'cliente_facturacion', 'fk_cliente', 'nave_id', 'nave_nombre', 'contenedor', 'cont_tamano']].drop_duplicates(subset=['fk_servicio'])
    facturas_df.to_sql('facturas', con=engine, if_exists='replace', index=True, schema='facturas')
    
    
    
    
    
    
    # Cerrar la conexión a la base de datos
    conn.close()
    
    
    
    
    
