# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 23:14:30 2024

@author: Ignacio Carvajal
"""

import time
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
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from datetime import datetime, MINYEAR
import pytz
from datetime import datetime, timezone, timedelta




import time
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
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from datetime import datetime, MINYEAR
import pytz
from datetime import datetime, timezone, timedelta

def execute_bulk_insert(cursor, query, values):
    """
    Inserta múltiples registros en una tabla usando un solo comando INSERT.

    Parameters
    ----------
    cursor : psycopg2 cursor object
        Cursor de la conexión a la base de datos.
    query : str
        La consulta SQL base para el comando INSERT.
    data : list of tuples
        Los datos a insertar en la tabla.

    Returns
    -------
    None
    """
    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "dw"  # Reemplazar por el nombre real de la base de datos
    user = "postgres"
    password = "ignacio"

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


    # Uso de psycopg2 para ejecutar la consulta en Python
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    
    # Supongamos que 'conn' es tu conexión a la base de datos
    with conn.cursor() as cursor:
        execute_values(cursor, query, values)
        conn.commit()
















def load_dimensions(df, initial_index, tipo_carga='increental'):
    """
    Parameters
    ----------
    df : DataFrame pandas object
        DESCRIPTION.
    initial_index : int
        sera el indice inicial de la carga
    stop_index : int
        sera el ultimo indice en ser llenado

    Returns None
    -------
    index : int
        es la llave que une el datawarehouse.
    """
    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "dw"  # Reemplazar por el nombre real de la base de datos
    user = "postgres"
    password = "ignacio"

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

    comercial_values = []
    factura_values = []
    cliente_facturacion_values = []
    cliente_despacho_values = []
    conductor_values = []
    nave_values = []
    caracteristicas_values = []
    direccion_salida_values = []
    direccion_llegada_values = []
    programacion_values = []
    real_time_arribo_values = []
    real_time_salida_values = []
    

    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():
        index = int(index + initial_index)

        fk_servicio = row['fk_servicio']

        ##################################################### comercial #################################################3
        nombre_comercial = row['comercial_nombre']
        rut_comercial = row['rut_comercial']

        comercial_values.append((index, nombre_comercial, rut_comercial, fk_servicio))

        ###################################################### factura ###################################################
        # Extracción de datos
        sii_factura = row['sii_factura']
        sii_fecha = row['sii_fecha']
        try:
            # Convertir a objeto de fecha
            fecha_objeto = datetime.strptime(sii_fecha, "%d-%m-%Y")

        except:
            # Convertir a objeto de fecha
            fecha_objeto = datetime.strptime('01-01-1900', "%d-%m-%Y")

        # Formatear como "YYYY-MM-DD"
        sii_fecha = fecha_objeto.strftime("%Y-%m-%d")

        total_servicio = row['total_servicio']
        total_cobros_extras = row['total_cobros_extras']

        creacion_factura = formatear_a_fecha(row['creacion_factura'])
        actualizacion_factura = formatear_a_fecha(row['actualizacion_factura'])


        factura_values.append((index, sii_factura, sii_fecha, total_servicio,
                       total_cobros_extras, creacion_factura, actualizacion_factura, fk_servicio))
        # print(index)

        ###################################################################### cliente_facturacion #####################################3
        nombre_cliente = row['cli_fact_nombre']
        rut_cliente = row['cli_fact_rut']
        fecha_conversion = row['fecha_conversion_fact']

        #print(type(fecha_conversion), fecha_conversion, "fecha_conversion_fact")

        try:
            # Supongamos que fecha_conversion es una variable de tipo Timestamp de Pandas
            fecha_conversion = pd.Timestamp(fecha_conversion)

        except:
            fecha_conversion = pd.Timestamp('01-01-2100 00:00:00')
        # Obtener la fecha actual
        fecha_actual = pd.Timestamp.now()

        # Obtener la fecha actual con la misma información de zona horaria que fecha_conversion
        fecha_actual = pd.Timestamp.now(tz=fecha_conversion.tz)

        # Calcular la diferencia en días
        diferencia_en_dias = (fecha_actual - fecha_conversion).days

        antiguedad = "error"
        # Comprobar si la diferencia es mayor que 30 días (aproximadamente un mes)
        if diferencia_en_dias < 30:
            antiguedad = "menos de un mes"
        elif diferencia_en_dias >= 30 and diferencia_en_dias < 3*30:
            antiguedad = "menos de 3 meses"

        elif diferencia_en_dias >= 3*30 and diferencia_en_dias < 6*30:
            antiguedad = "menos de 6 meses"

        elif diferencia_en_dias >= 6*30 and diferencia_en_dias < 365:
            antiguedad = "más de 6 meses"

        elif diferencia_en_dias >= 365 and diferencia_en_dias < 3*365:
            antiguedad = "más de 1 año"

        elif diferencia_en_dias > 3*365:
            antiguedad = "más de 3 años"

        else:
            antiguedad = "error"

        # Consulta SQL para insertar un nuevo registro en la tabla 'public.cliente_facturacion'
        cliente_facturacion_values.append((index, nombre_cliente,
                       rut_cliente, fk_servicio, fecha_conversion, antiguedad))


        ###################################################3 cliente_despacho ###################################################
        nombre_despacho = row['cli_desp_nombre']
        rut_despacho = row['cli_desp_rut']
        fecha_conversion = row['fecha_conversion_desp']
        #print(type(fecha_conversion), fecha_conversion, "fecha_conversion_desp")

        try:
            # Supongamos que fecha_conversion es una variable de tipo Timestamp de Pandas
            fecha_conversion = pd.Timestamp(fecha_conversion)
        except:
            fecha_conversion = pd.Timestamp('01-01-2100 00:00:00')

        # Obtener la fecha actual
        fecha_actual = pd.Timestamp.now()

        # Obtener la fecha actual con la misma información de zona horaria que fecha_conversion
        fecha_actual = pd.Timestamp.now(tz=fecha_conversion.tz)

        # Calcular la diferencia en días
        diferencia_en_dias = (fecha_actual - fecha_conversion).days

        antiguedad = "error"
        # Comprobar si la diferencia es mayor que 30 días (aproximadamente un mes)
        if diferencia_en_dias < 30:
            antiguedad = "menos de un mes"
        elif diferencia_en_dias >= 30 and diferencia_en_dias < 3*30:
            antiguedad = "menos de 3 meses"

        elif diferencia_en_dias >= 3*30 and diferencia_en_dias < 6*30:
            antiguedad = "menos de 6 meses"

        elif diferencia_en_dias >= 6*30 and diferencia_en_dias < 365:
            antiguedad = "más de 6 meses"

        elif diferencia_en_dias >= 365 and diferencia_en_dias < 3*365:
            antiguedad = "más de 1 año"

        elif diferencia_en_dias > 3*365:
            antiguedad = "más de 3 años"

        else:
            antiguedad = "error"

        
        cliente_despacho_values.append((index, nombre_despacho,
                       rut_despacho, fk_servicio, str(fecha_conversion), antiguedad))


        ###################################################### conductor ####################################################################
        nombre_conductor = row['etapa_1_conductor_nombre']
        rut_conductor = row['etapa_1_conductor_rut']
        tipo_conductor = row['ult_empt_tipo']
        # Consulta SQL para insertar un nuevo registro en la tabla 'public.conductor'

        
        conductor_values.append((index, nombre_conductor,
                       rut_conductor, fk_servicio, tipo_conductor))


        ###################################################### nave ##############################################33
        nombre_nave = str(row['servicio_nave_nombre'])
        
        ETA_nave = row['eta_fecha'] if not pd.isnull(row['eta_fecha']) else datetime(1900, 1, 1)
       

        # Consulta SQL para insertar un nuevo registro en la tabla 'public.conductor'

        nave_values.append((index, nombre_nave, fk_servicio, ETA_nave))
        


        ################################################ caracteristicas ############################################

        tamano_contenedor = row['cont_tamano']

        peso_carga = row['contenedor_peso_carga']

        dry = row['cont_tipo_nombre']

        impo_expo = row['servicio_codigo']
        numero_contenedor = row['numero_contenedor']
        full_lcl = row['fk_tipo_carga']
        fila = row['posicion_fila']

        columna = row['posicion_columna']

        posicion = row['posicion']

        # Consulta SQL para insertar un nuevo registro en la tabla 'public.conductor'

        
        caracteristicas_values.append((index, tamano_contenedor, peso_carga,
                       impo_expo, dry, numero_contenedor, full_lcl, fk_servicio, fila, columna, posicion))

        ###########################################33 direccion_salida############################################################3333

        comuna = row['comuna_salida_nombre']
        direccion = str(row['direccion_salida_nombre']) + \
            " " + str(row['direccion_salida_numero'])
        longitud = row['direccion_salida_long']
        latitud = row['direccion_salida_lat']



        try:
            longitud = str(longitud)
            latitud = str(latitud)
        except ValueError:
            # Handle the case where the conversion fails
            print("Error: Invalid coordinates format")
            # You may choose to set default values or handle the error in another way
            longitud = "0.0"
            latitud = "0.0"

        direccion_salida_values.append( (index, comuna, direccion, fk_servicio, longitud, latitud))
        # Now you can use longitud and latitud in your execute statement
       

        #######################################################3 direccion_llegada ###################################################3

        comuna = row['comuna_llegada_nombre']
        direccion = str(row['direccion_llegada_nombre']) + \
            " " + str(row['direccion_llegada_numero'])
        longitud = row['direccion_llegada_long']
        latitud = row['direccion_llegada_lat']


        try:
            longitud = str(longitud)
            latitud = str(latitud)
        except ValueError:
            # Handle the case where the conversion fails
            print("Error: Invalid coordinates format")
            # You may choose to set default values or handle the error in another way
            longitud = "0.0"
            latitud = "0.0"

        # Now you can use longitud and latitud in your execute statement
        direccion_llegada_values.append((index, comuna, direccion, fk_servicio, longitud, latitud))

        #########################################################################33 time ################################3
        etapa_1_fecha = row['etapa_1_fecha']
        etapa_1_hora = row['etapa_1_hora']
        #print(etapa_1_fecha)
        etapa_1_fecha = unir_fecha_hora(etapa_1_fecha, etapa_1_hora)


        
        programacion_values.append((index, etapa_1_fecha, fk_servicio))


        #########################################3 real_arribo ######################
        try:

            # Convierte la cadena a un objeto de fecha y hora
            fecha_hora_arribo = datetime.strptime(
                row['hora_real_arribo'], "%Y-%m-%d %H:%M:%S")

            # Obtiene solo la hora como una cadena
            hora_str = fecha_hora_arribo.strftime("%H:%M:%S")
            # etapa_programacion
            fecha_hora_str = f"{row['fecha_real_arribo']} {row['hora_real_arribo']}"
            # Convierte la cadena a un objeto de fecha y hora
            fecha_hora_formateada = datetime.strptime(
                fecha_hora_str, "%Y-%m-%d %H:%M:%S", errors='coerse')
            # Convierte la fecha y hora formateadas a timestamp
            fecha_real_arribo = fecha_hora_formateada.timestamp()

        except:
            fecha_real_arribo = datetime(MINYEAR, 1, 1)


        real_time_arribo_values.append((index, fecha_real_arribo, fk_servicio))

        



        ##############################################3 real_salida ###########################
        try:

            # Obtiene solo la hora como una cadena
            hora_str = fecha_hora.strftime("%H:%M:%S")

            # etapa_programacion
            fecha_hora_str = f"{row['fecha_real_salida']} {hora_str}"

            # Convierte la cadena a un objeto de fecha y hora
            fecha_hora_formateada = datetime.strptime(
                fecha_hora_str, "%Y-%m-%d %H:%M:%S", errors='coerse')

            # Convierte la fecha y hora formateadas a timestamp
            fecha_real_salida = fecha_hora_formateada.timestamp()

        except:
            fecha_real_salida = datetime(MINYEAR, 1, 1)

        
        real_time_salida_values.append((index, fecha_real_salida, fk_servicio))


    #definicion de inserts
    # Consulta SQL para insertar un nuevo registro en la tabla 'public.comercial'
    insert_comercial_query = """INSERT INTO public.comercial (id_comercial, name, rut, fk_servicio)
                                    VALUES %s
                                    ON CONFLICT (id_comercial) DO UPDATE
                                    SET
                                        
                                        name = EXCLUDED.name,
                                        rut = EXCLUDED.rut,
                                        fk_servicio = EXCLUDED.fk_servicio; """
    
        # Consulta SQL para insertar un nuevo registro en la tabla 'public.factura'
    insert_factura_query = """INSERT INTO public.facturas (id_factura, sii_factura, sii_fecha, total_servicio, total_cobros_extras, creacion_factura, actualizacion_factura, fk_servicio)
                                VALUES %s
                                ON CONFLICT (id_factura) DO UPDATE
                                SET
                                    sii_factura = EXCLUDED.sii_factura,
                                    sii_fecha = EXCLUDED.sii_fecha,
                                    total_servicio = EXCLUDED.total_servicio,
                                    total_cobros_extras = EXCLUDED.total_cobros_extras,
                                    
                                    creacion_factura = EXCLUDED.creacion_factura,
                                    actualizacion_factura = EXCLUDED.actualizacion_factura, 
                                    fk_servicio = EXCLUDED.fk_servicio; """
    
    insert_cliente_facturacion_query = """INSERT INTO public.cliente_facturacion (id_customer, name, rut, fk_servicio, fecha_conversion, antiguedad)
                                        VALUES %s
                                        ON CONFLICT (id_customer) DO UPDATE
                                        SET
                                            name = EXCLUDED.name,
                                            rut = EXCLUDED.rut,
                                            fk_servicio =  EXCLUDED.fk_servicio,
                                            fecha_conversion = EXCLUDED.fecha_conversion,
                                            antiguedad = EXCLUDED.antiguedad;
                                                """


    # Consulta SQL para insertar un nuevo registro en la tabla 'public.cliente_despacho'
    insert_cliente_despacho_query = """INSERT INTO public.cliente_despacho (id_cliente_despacho, nombre, rut, fk_servicio, fecha_conversion, antiguedad)
                                            VALUES %s
                                            ON CONFLICT (id_cliente_despacho) DO UPDATE
                                            SET
                                                nombre = EXCLUDED.nombre,
                                                rut = EXCLUDED.rut,
                                                fk_servicio =  EXCLUDED.fk_servicio,
                                                fecha_conversion = EXCLUDED.fecha_conversion,
                                                antiguedad = EXCLUDED.antiguedad;
                                            """
                                            
    insert_conductor_query = """INSERT INTO public.conductor (id_conductor, nombre, rut, fk_servicio, tipo_conductor)
                                    VALUES %s
                                    
                                    ON CONFLICT (id_conductor) DO UPDATE
                                    SET
                                        nombre = EXCLUDED.nombre,
                                        rut = EXCLUDED.rut,
                                        fk_servicio = EXCLUDED.fk_servicio,
                                        tipo_conductor = EXCLUDED.tipo_conductor;
                                    """
    
    insert_nave_query = """INSERT INTO public.nave (id_nave, nombre, fk_servicio, "ETA_nave")
                            VALUES %s
                            ON CONFLICT (id_nave) DO UPDATE
                            SET
                                nombre = EXCLUDED.nombre,
                                fk_servicio = EXCLUDED.fk_servicio,
                                "ETA_nave" = EXCLUDED."ETA_nave";
                    """
    
    insert_caracteristicas_query = """INSERT INTO public.caracteristicas (id_caracteristicas, tamano_contenedor, peso_carga, impo_expo, dry, numero_contenedor, full_lcl, fk_servicio, posicion_fila, posicion_columna, posicion)
                                        VALUES %s
                                        ON CONFLICT (id_caracteristicas) DO UPDATE
                                        SET
                                            tamano_contenedor = EXCLUDED.tamano_contenedor,
                                            peso_carga = EXCLUDED.peso_carga,
                                            impo_expo = EXCLUDED.impo_expo,
                                            dry = EXCLUDED.dry,
                                            numero_contenedor = EXCLUDED.numero_contenedor,
                                            full_lcl = EXCLUDED.full_lcl,
                                            posicion_fila = EXCLUDED.posicion_fila,
                                            posicion_columna = EXCLUDED.posicion_columna,
                                            posicion = EXCLUDED.posicion
                                            ;
                                        """
                                        
    insert_direccion_salida_query = """INSERT INTO public.direccion_salida (id_direccion_salida, comuna, direccion, fk_servicio, longitud, latitud)
                                            VALUES %s
                                            ON CONFLICT (id_direccion_salida) DO UPDATE
                                            SET
                                                comuna = EXCLUDED.comuna,
                                                direccion = EXCLUDED.direccion,
                                                longitud = EXCLUDED.longitud,
                                                latitud = EXCLUDED.latitud;
                                            ;"""
                                            
    insert_direccion_llegada_query = """INSERT INTO public.direccion_llegada (id_direccion, comuna, direccion, fk_servicio, longitud, latitud)
                                        VALUES %s
                                        ON CONFLICT (id_direccion) DO UPDATE
                                        SET
                                            comuna = EXCLUDED.comuna,
                                            direccion = EXCLUDED.direccion,
                                            longitud = EXCLUDED.longitud,
                                            latitud = EXCLUDED.latitud;
                                        """

    insert_programacion_query = """INSERT INTO public.time (id_time, etapa_1_fecha, fk_servicio)
                                    VALUES %s
                                    ON CONFLICT (id_time) DO UPDATE
                                    SET
                                        etapa_1_fecha = EXCLUDED.etapa_1_fecha;

                                    """
    


    # etapa_tiempo_real_arribo
    insert_real_time_arribo_query = """INSERT INTO public.real_time_arribo (id_real_time, fecha_real_arribo, fk_servicio)
                                            VALUES %s
                                            ON CONFLICT (id_real_time) DO UPDATE
                                            SET
                                                fecha_real_arribo = EXCLUDED.fecha_real_arribo;
                                            """
    
    
    insert_real_time_salida_query = """INSERT INTO public.real_time_salida (id_real_time, fecha_real_salida, fk_servicio)
                                            VALUES %s
                                            ON CONFLICT (id_real_time) DO UPDATE
                                            SET
                                                fecha_real_salida = EXCLUDED.fecha_real_salida;
                                            """





 
  #  try:

    # Insertar en la tabla 'comercial'
    execute_bulk_insert(cursor, insert_comercial_query, comercial_values)

    # Insertar en la tabla 'facturas'
    execute_bulk_insert(cursor, insert_factura_query, factura_values)

    # Insertar en la tabla 'cliente_facturacion'
    execute_bulk_insert(cursor, insert_cliente_facturacion_query, cliente_facturacion_values)

    # Insertar en la tabla 'cliente_despacho'
    execute_bulk_insert(cursor, insert_cliente_despacho_query, cliente_despacho_values)

    # Insertar en la tabla 'conductor'
    execute_bulk_insert(cursor, insert_conductor_query, conductor_values)

    # Insertar en la tabla 'nave'
    execute_bulk_insert(cursor, insert_nave_query, nave_values)

    # Insertar en la tabla 'caracteristicas'
    execute_bulk_insert(cursor, insert_caracteristicas_query, caracteristicas_values)

    # Insertar en la tabla 'direccion_salida'
    execute_bulk_insert(cursor, insert_direccion_salida_query, direccion_salida_values)

    # Insertar en la tabla 'direccion_llegada'
    execute_bulk_insert(cursor, insert_direccion_llegada_query, direccion_llegada_values)

    # Insertar en la tabla 'programacion'
    execute_bulk_insert(cursor, insert_programacion_query, programacion_values)

    # Insertar en la tabla 'real_time_arribo'
    execute_bulk_insert(cursor, insert_real_time_arribo_query, real_time_arribo_values)

    # Insertar en la tabla 'real_time_salida'
    execute_bulk_insert(cursor, insert_real_time_salida_query, real_time_salida_values)

    #except:
        #print("error en los execute")





    # Confirma los cambios en la base de datos
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

    return 1


























































def load_fact_table(df, initial_index, tipo_carga='incremental'):

    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"  # Puerto predeterminado de PostgreSQL
    database = "dw"  # Reemplazar por el nombre real de la base de datos
    user = "postgres"
    password = "ignacio"

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


    etapas_values = []
    servicios_values = []
    # Itera a través de las filas del DataFrame 'df' e inserta los datos en ambas tablas
    for index, row in df.iterrows():

        index = int(index + initial_index)
        
        #initial_index = 0
        # definimos el indice principal
        fk_servicio = row['fk_servicio']
        id_de_la_etapa = row['id_etapa']
        # etapa
        titulo = row['etapa_titulo']
        codigo = row['etapa_tipo']
        
        kilometros = row['distancia_mts']
        #print(row['causa_atrasos'])
        causa_atrasos = row['causa_de_atraso']
        comentarios = row['comentario_causas_atrasos']

        # Reemplazar NaN con 0 si es necesario
        # print(type(kilometros))
        if pd.isna(kilometros) or kilometros is None:
            kilometros = 0

        # Suponiendo que 'fecha' y 'hora' son strings en formato 'YYYY-MM-DD' y 'HH:MM:SS' respectivamente
        fecha_str = row['etapa_1_fecha']
        hora_str = row['etapa_1_hora']

        # Concatenar la fecha y la hora en un solo string con formato completo 'YYYY-MM-DD HH:MM:SS'
        fecha_hora_str = str(hora_str)[11:16]

        # Repite el proceso para 'fecha_arribo' y 'hour_arribo'
        fecha_arribo_str = row['fecha_real_arribo']
        hora_arribo_str = row['hora_real_arribo']
        tiempo_estadia = row['tiempo_estadia']
        fecha_hora_arribo_str = str(hora_arribo_str)[11:16]

        

        try:
            fecha_hora_datetime = datetime.strptime(fecha_hora_str, '%H:%M')
            fecha_hora_arribo_datetime = datetime.strptime(
                fecha_hora_arribo_str, '%H:%M')
            tiempo_estadia = hora_a_minutos(tiempo_estadia)
        except:
            fecha_hora_datetime = datetime.strptime('00:00', '%H:%M')
            fecha_hora_arribo_datetime = datetime.strptime('00:00', '%H:%M')
            tiempo_estadia = 0
        
        from datetime import timedelta
        
        if fecha_hora_arribo_datetime  > fecha_hora_datetime + timedelta(minutes=15):
            atraso = "Atrasado"
        else:
            atraso = "En tiempo"
            
            
            
            
        # Repite el proceso para 'fecha_arribo' y 'hour_arribo'
        fecha_arribo_auto_str = row['fecha_real_arribo_auto']
        hora_arribo_auto_str = row['hora_real_arribo_auto']
       
        fecha_hora_arribo_auto_str = str(hora_arribo_auto_str)#[11:16]

        

        try:
            fecha_hora_auto_datetime = datetime.strptime(fecha_hora_auto_str, '%H:%M')
            fecha_hora_arribo_auto_datetime = datetime.strptime(
                fecha_hora_arribo_auto_str, '%H:%M')
            
        except:
            fecha_hora_auto_datetime = datetime.strptime('00:00', '%H:%M')
            fecha_hora_arribo_auto_datetime = datetime.strptime('00:00', '%H:%M')
            
        
        from datetime import timedelta
        
        if fecha_hora_arribo_auto_datetime  > fecha_hora_auto_datetime + timedelta(minutes=15):
            atraso_auto = "Atrasado"
        else:
            atraso_auto = "En tiempo"           
            
            
            
        try:
            if atraso == 'Atrasado' or atraso_auto == 'Atrasado':
                atraso_auto = 'Atrasado'
        except:
            pass
        
        try:
            if (fecha_hora_arribo_auto_str=='0' or fecha_hora_arribo_auto_str=='') and  (fecha_hora_arribo_str=='' or fecha_hora_arribo_str=='0'):

                atraso_auto = 'No calculado'

        
        except:
            pass
            
            
        try:
         
            # Calcula la diferencia de tiempo en segundos
            diferencia_segundos = (
                fecha_hora_arribo_datetime - fecha_hora_datetime).total_seconds()
        
        except:
            print("error")
        
        try:
 
            # Calcula la diferencia de tiempo en segundos
            diferencia_segundos_auto = (
                fecha_hora_arribo_auto_datetime - fecha_hora_auto_datetime).total_seconds()
       
        except:
            print("error")

        try:

            # diferencia horas de facturacion
            creacion_factura = row['creacion_factura']
            creacion_factura = formatear_a_fecha(row['creacion_factura'])
         
            # Convertir creacion_factura a datetime
           
            #print(4)
            etapa_1_fecha = row['etapa_1_fecha']
            etapa_1_hora = row['etapa_1_hora']

            etapa_1_fecha = unir_fecha_hora(etapa_1_fecha, etapa_1_hora)
            #print(5)
            etapa_1_fecha = pd.Timestamp(etapa_1_fecha)
            ##print(6)
            # Eliminar la información de zona horaria
            etapa_1_fecha = etapa_1_fecha.tz_localize(None)
            creacion_factura = creacion_factura.tz_localize(None)

            #print(creacion_factura, etapa_1_fecha)
            # Ahora puedes restar ambos objetos
            horas_facturacion = -(etapa_1_fecha - creacion_factura).total_seconds()/86400
            if horas_facturacion>100:
                horas_facturacion = -100
                
        
        except:
            horas_facturacion = -100

        # Convierte la diferencia de segundos a minutos
        diferencia_minutos = diferencia_segundos / 60
        # Convierte la diferencia de segundos a minutos
        diferencia_minutos_auto = diferencia_segundos_auto / 60




        #print("SQL Query:", insert_etapa_query)
        #print("Values:", (index, titulo, codigo, kilometros, atraso, diferencia_minutos, tiempo_estadia, fk_servicio, horas_facturacion))
        etapas_values.append((index, titulo, codigo, kilometros, atraso,
                       diferencia_minutos, tiempo_estadia, fk_servicio, horas_facturacion, atraso_auto, diferencia_minutos_auto, causa_atrasos, comentarios, id_de_la_etapa))


        # fact_servicios
        fk_servicio = row['fk_servicio']
        estado = row['estado']
        # Consulta SQL para insertar un nuevo registro en la tabla 'public.etapa'

                                        
                                        
        servicios_values.append((index, index, index,
                       index, index, fk_servicio, index, index, index, index, index, estado))
 


    # Consulta SQL para insertar un nuevo registro en la tabla 'public.etapa'
    insert_etapa_query = """INSERT INTO public.etapa (id_etapa, titulo, codigo, kilometros, atraso, diferencia_minutos, tiempo_estadia, servicio, horas_facturacion, atraso_auto, diferencia_minutos_auto, causa_atrasos, comentarios, id_de_la_etapa )
                                VALUES %s
                                ON CONFLICT (id_etapa) DO UPDATE
                                SET
                                    titulo = EXCLUDED.titulo,
                                    codigo = EXCLUDED.codigo,
                                    kilometros = EXCLUDED.kilometros,
                                    atraso = EXCLUDED.atraso,
                                    diferencia_minutos = EXCLUDED.diferencia_minutos,
                                    tiempo_estadia = EXCLUDED.tiempo_estadia, 
                                    servicio = EXCLUDED.servicio,
                                    horas_facturacion = EXCLUDED.horas_facturacion,
                                    atraso_auto = EXCLUDED.atraso_auto,
                                    diferencia_minutos_auto = EXCLUDED.diferencia_minutos_auto,
                                    causa_atrasos = EXCLUDED.causa_atrasos,
                                    comentarios = EXCLUDED.comentarios,
                                    id_de_la_etapa = EXCLUDED.id_de_la_etapa;
                                """
                                
    insert_servicio_query = """INSERT INTO public.fact_servicios (
                                        fk_cliente, fk_conductor, fk_comercial, fk_agencia, fk_etapa, fk_servicio,
                                        fk_direccion, fk_cliente_despacho, fk_caracteristicas, fk_nave, id, estado
                                    ) VALUES  %s
                                    ON CONFLICT (id) DO UPDATE
                                    SET
                                        fk_cliente = EXCLUDED.fk_cliente,
                                        fk_conductor = EXCLUDED.fk_conductor,
                                        fk_comercial = EXCLUDED.fk_comercial,
                                        fk_agencia = EXCLUDED.fk_agencia,
                                        fk_etapa = EXCLUDED.fk_etapa,
                                        fk_servicio = EXCLUDED.fk_servicio,
                                        fk_direccion = EXCLUDED.fk_direccion,
                                        fk_cliente_despacho = EXCLUDED.fk_cliente_despacho,
                                        fk_caracteristicas = EXCLUDED.fk_caracteristicas,
                                        fk_nave = EXCLUDED.fk_nave,
                                        estado = EXCLUDED.estado;
                                        
                                    """

    try:
        
        execute_bulk_insert(cursor, insert_etapa_query, etapas_values)
        execute_bulk_insert(cursor, insert_servicio_query, servicios_values)
    except:
        pass


    #print("index salida", index)
    # Confirma los cambios en la base de datos
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()


    return 1





































import time

def loader(df, initial_index, batch_size, tipo_carga):

    lenght = len(df)
    # print(lenght)

    index_dimension = initial_index
    index_fact = initial_index
    index = initial_index

    while index < lenght + initial_index:
        
        try:
            index_dimension = index
            print("index_dimension", index_dimension,
                  index_dimension + batch_size)
            index_dimension = load_dimensions(
                df, index_dimension)
        except:
            print("fallo dimension")

            # Pausa de 5 segundos
            time.sleep(5)
            pass

        #try:
        index_fact = index

        index_fact = load_fact_table(
            df, index_fact)
       # except:
       #     print("fallo hechos")
      #      time.sleep(5)
       #     pass
        initial_index2 = initial_index2 + batch_size
        index = min(index_dimension, index_fact)