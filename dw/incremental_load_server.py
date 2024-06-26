

import psycopg2
import sqlalchemy as sa
import pandas as pd

from load_server import *
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from delete_data_server import *

from connection import connectionDB







def get_fk_servicio_values_next_day():
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
        # Consulta SQL para obtener los últimos 10000 valores de fk_servicio desde servicios_logs
        query = f"""
            SELECT
              ser.id AS fk_servicio
              
            FROM
              public.servicios AS ser
            INNER JOIN public.servicios_etapas AS eta_1 ON ser.id = eta_1.fk_servicio
            LEFT JOIN public.servicios_etapas AS eta_0 ON eta_1.fk_etapa_anterior = eta_0.id
            LEFT JOIN public.direcciones AS dir_salida ON eta_0.fk_direccion = dir_salida.id
            LEFT JOIN public.comunas AS com_1 ON dir_salida."comunaComunaId" = com_1.comuna_id
            WHERE
              ser.estado != 999
              AND (eta_1.fecha = TO_CHAR(CURRENT_DATE + INTERVAL '1 day', 'DD-MM-YYYY') or eta_1.fecha = TO_CHAR(CURRENT_DATE + INTERVAL '0 day', 'DD-MM-YYYY'))
            
            ORDER BY
              ser.id ASC;
        """

        # Ejecuta la consulta SQL
        cursor.execute(query)
       

        # Obtiene los resultados de la consulta
        fk_servicio_values = [result[0] for result in cursor.fetchall()]

        return fk_servicio_values

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error al obtener valores de fk_servicio: {e}")
        return None

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()
        








def get_fk_servicio_values(numero_de_indices):
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
        # Consulta SQL para obtener los últimos 10000 valores de fk_servicio desde servicios_logs
        query = f"""
            SELECT fk_servicio
            FROM public.servicios_logs
            ORDER BY id DESC
            limit {numero_de_indices}
            ;
        """

        # Ejecuta la consulta SQL
        cursor.execute(query)
       

        # Obtiene los resultados de la consulta
        fk_servicio_values = [result[0] for result in cursor.fetchall()]

        return fk_servicio_values

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir durante la ejecución de la consulta
        print(f"Error al obtener valores de fk_servicio: {e}")
        return None

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()
        



def obtener_max_id_etapa():
    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"
    database = "dw"
    user = "postgres"
    password = "ignacio"

    # Crear la cadena de conexión
    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    # Consulta SQL
    sql_query = "SELECT MAX(id_etapa) as max_id_etapa FROM public.etapa;"

    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df = pd.read_sql_query(sql_query, engine)

    # Obtener el valor máximo de id_etapa
    max_id_etapa = df['max_id_etapa'].values[0]

    return max_id_etapa



def obtener_fk_servicios_sin_factura():
    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"
    database = "dw"
    user = "postgres"
    password = "ignacio"

    # Crear la cadena de conexión
    engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    # Consulta SQL
    sql_query = """SELECT DISTINCT fk_servicio
                        FROM public.facturas
                        WHERE sii_factura IS NULL;"""

    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df = pd.read_sql_query(sql_query, engine)

    # Obtener el valor máximo de id_etapa
    fk_servicio_lista = list(df['fk_servicio'])

    return fk_servicio_lista




def incremental_batch_load(fk_servicio_amount, batch_size):
    # Lista de valores de fk_servicio
    lista_fk_servicio = list(set(get_fk_servicio_values(fk_servicio_amount)))
    lista_sin_factura = list(set(obtener_fk_servicios_sin_factura()))
    lista = [83384, 83382, 83126, 82786, 86682, 85937, 86681, 86679, 86680, 86683,
    86677, 86678, 86676, 88176, 87404, 87729, 88300, 88475, 87865, 88069,
    86939, 87597, 88175, 88246, 88408, 88040, 88425, 87412, 86686, 86687,
    86685, 87868, 86684, 90670, 89074, 89449, 89819, 89483, 90172, 90440,
    89770, 89768, 89290, 89818, 90211, 90071, 90238, 90305, 82765, 81872,
    80263, 75310, 75312, 75314, 75332, 75311, 74337]
    lista_sin_factura = lista_sin_factura + lista
    lista_fk_servicio = list(set(lista_fk_servicio + lista_sin_factura))
    # Formatear la lista como una cadena separada por comas
    lista_fk_servicio_str = ', '.join(map(str, lista_fk_servicio))
    # Obtiene los valores de fk_servicio desde la consulta

    if lista_fk_servicio_str:
        # Llama a la función para eliminar registros
        delete_records_by_fk_servicio(lista_fk_servicio_str)
    else:
        print("No se pudieron obtener los valores de fk_servicio.")
        
    # Consulta SQL con la lista de valores de fk_servicio
    query = f"""
    select
    ser.id as fk_servicio

    , ser.estado /* 0 pre servicio 1 transito 2 terminado 999 eliminado */


    /* comercial */
    , concat ( case when TRIM(comer.usu_nombre) LIKE '% %' then left(TRIM(comer.usu_nombre), strpos(TRIM(comer.usu_nombre), ' ') - 1) else TRIM(comer.usu_nombre) end ,' '
    , case when TRIM(comer.usu_apellido) LIKE '% %' then left(TRIM(comer.usu_apellido), strpos(TRIM(comer.usu_apellido), ' ') - 1) else TRIM(comer.usu_apellido) end ) as comercial_nombre /* ejecutivo comercial */
    ,comer.usu_rut as rut_comercial

    /* clienbte de facturacion */
    , cli_fact.cli_nombre as cli_fact_nombre /* clienbte de facturacion */
    , cli_fact.cli_rut as cli_fact_rut /* clienbte de facturacion */
    , cli_fact."createdAt" as fecha_conversion_fact

    /* cliente para el despacho */
    , cli_desp.cli_nombre as cli_desp_nombre /* cliente para el despacho */
    , cli_desp.cli_rut as cli_desp_rut /* cliente para el despacho */
    , cli_fact."createdAt" as fecha_conversion_desp


    /* nombre del barco */
    , coalesce(nave.nave_nombre,'') as servicio_nave_nombre /* nombre del barco */
    , coalesce(eta.eta_fecha,'') as eta_fecha /* fecha de llegada del barco */


    /* caracteristicas */
    , ser.numero_contenedor
    , ser.fk_tipo_servicio as servicio_codigo /* importacion exportacion almacenamiento desconsolidado etc*/
    , ser.fk_tipo_carga /* fcl contenedor full, lcl contenedor parcial, vehi auto */
    , coalesce(cont_tip.cont_nombre,'') as cont_tipo_nombre /* si es dry es seco, refeer es con temperatura */
    , coalesce(cont_tam.conttam_tamano,'') as cont_tamano /* 20 40 60 */
    , ser.contenedor_peso_carga




    /* etapas del servicio */
    , coalesce(eta_1.tipo, 0) as etapa_tipo /* 1 retiro, 2 presentacion, 3 devolucion, 0 almacenaje */
    , coalesce (eta_1.titulo, '') as etapa_titulo
    , coalesce(eta_1.fecha, '') as etapa_1_fecha
    , coalesce(eta_1.hora, '') as etapa_1_hora
    , coalesce(eta_1.fecha_real_arribo, '') as fecha_real_arribo
    , coalesce(eta_1.hora_real_arribo, '') as hora_real_arribo
    , coalesce(eta_1.fecha_real_salida, '') as fecha_real_salida
    , coalesce(eta_1.hora_real_salida, '') as hora_real_salida
    , coalesce(eta_1.tiempo_estadia, '') as tiempo_estadia


    /* factura */
    , b.sii_factura
    , b.sii_fecha
    , d.total as total_servicio
    , d.total_cobros_extras
    , b."createdAt" as creacion_factura
    , b."updatedAt" as actualizacion_factura

    -- direccion de salida
    , coalesce(dir_salida.direccion, '') as direccion_salida_nombre
    , coalesce(dir_salida.numero, '') as direccion_salida_numero
    , coalesce(dir_salida.lat, '') as direccion_salida_lat
    , coalesce(dir_salida."long", '') as direccion_salida_long
    , coalesce(com_1.comuna_nombre, '') as comuna_salida_nombre -- Nombre de la comuna de salida

    -- direccion de llegada
    , coalesce(dir_llegada.direccion, '') as direccion_llegada_nombre
    , coalesce(dir_llegada.numero, '') as direccion_llegada_numero
    , coalesce(dir_llegada.lat, '') as direccion_llegada_lat
    , coalesce(dir_llegada."long", '') as direccion_llegada_long
    ,coalesce(com_2.comuna_nombre, '') as comuna_llegada_nombre -- Nombre de la comuna de llegada


    , (SELECT temp1.tiempo FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as tiempo_minutos
    , (SELECT temp1.distancia FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as distancia_mts
    , coalesce(dir_1.nombre,'') as etapa_1_lugar_nombre
    , concat(dir_1.nombre) as etapa_1_direccion_texto


    , concat(cond_1.usu_rut) as etapa_1_conductor_rut
    , concat ( TRIM(coalesce(cond_1.usu_nombre,'')),' ',TRIM(coalesce(cond_1.usu_apellido,'')) ) as etapa_1_conductor_nombre
    , cond_1.ult_empt_tipo

    , coalesce(tract_1.patente,'') as etapa_1_tracto


    , coalesce(ser.almacenaje_principal,'') as almacenaje_principal /* donde se guardo el contenedor cuando se fue a retirar, y antes de la presentacion en cliente */
    , concat( coalesce(ser.cont_fila,''),'-',coalesce(ser.cont_columna,''),'-',coalesce(ser.cont_posicion) ) as posicion_ubicacion /* ultima ubicacion en el patio de TNM */

    , coalesce(ser.cont_fila,'') as posicion_fila
    , coalesce(ser.cont_columna,'') as posicion_columna
    , coalesce(ser.cont_posicion) as posicion

    , case 
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='VACIO' THEN concat('S-V ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='LLENO' THEN concat('S-F ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='LLENO' THEN 'A-F'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='LLENO' THEN 'A-F'
    else '' end as posicion_tipo /* tipo de la ultima posicion, si se guardo vacio, o lleno, etc */

    from
    public.servicios as ser
    inner join public.usuarios as comer on ser.fk_comercial=comer.usu_rut
    left join public.clientes as cli_fact on ser.fk_cliente_facturacion=cli_fact.cli_codigo
    left join public.clientes as cli_desp on ser.fk_cliente_despacho=cli_desp.cli_codigo
    left join public.naves as nave on ser.fk_nave=nave.nave_id
    left join public.naves_etas as eta on ser.fk_eta=eta.eta_id
    left join public.contenedores_tipos as cont_tip on ser.fk_tipo_contenedor=cont_tip.cont_id
    left join public.contenedores_tamanos as cont_tam on ser.fk_contenedor_tamano=cont_tam.conttam_id

    left join public.servicios_etapas as eta_1 on ser.id=eta_1.fk_servicio
    left join public.direcciones as dir_1 on eta_1.fk_direccion=dir_1.id
    ---left join public.comunas as com_1 on dir_1."comunaComunaId"=com_1.comuna_id

    left join public.servicios_etapas_conductores as cond_eta_1 on eta_1.id=cond_eta_1.fk_etapa
    left join public.usuarios as cond_1 on cond_eta_1.fk_conductor=cond_1.usu_rut
    left join public.taller_equipos as tract_1 on cond_eta_1.fk_tracto=tract_1.id

    left join public.servicios_etapas as eta_0 on eta_1.fk_etapa_anterior=eta_0.id


    -- Tu consulta SQL actual

    -- Agrega un cruce para obtener la dirección de salida
    LEFT JOIN public.direcciones AS dir_salida ON eta_0.fk_direccion = dir_salida.id

    -- Agrega un cruce para obtener la dirección de llegada
    LEFT JOIN public.direcciones AS dir_llegada ON dir_1.id = dir_llegada.id

    LEFT JOIN public.comunas as com_1 ON dir_salida."comunaComunaId" = com_1.comuna_id
    LEFT JOIN public.comunas as com_2 ON dir_llegada."comunaComunaId" = com_2.comuna_id

    left join public.facturacion_liquidaciones_detalles AS d on ser.id=d.fk_servicio
    LEFT JOIN public.facturacion_liquidaciones_bases AS b ON b.id = d.fk_liquidacion



    WHERE ser.estado != 999
      AND ser.id > 0
      AND ser.id IN ({lista_fk_servicio_str})  -- Insertar la lista de valores de fk_servicio aquí

    ORDER BY 
      ser.id ASC;
    """


    # Llamada a la función
    max_id_etapa = obtener_max_id_etapa()
    initial_index = max_id_etapa
    
    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df = connectionDB_todf2(query)
    df = descomponer_fechas(df)
    max_lenght = len(df) + max_id_etapa
    
    while True:
        try:
            
            if initial_index + batch_size >= max_lenght:
                print("final")
                df2 = df.iloc[initial_index - (max_id_etapa+1):]
                
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
                break
                
            else:
                df2 = df.iloc[initial_index - (max_id_etapa+1):initial_index - (max_id_etapa+1) + batch_size]
                print("indice_maximo_dw: ", initial_index)
                print("largo de la  carga: ", len(df))
                # Imprimir el DataFrame resultante
    
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
    
    
    
            initial_index += batch_size
        
        except Exception as e:
            # Acceder a la información de la excepción a través de la variable 'e'
            print(f"Se ha producido una excepción: {e}")
            print(f"error en la subida del batch {initial_index}")

        
        

    
    
    
    
    
def incremental_batch_load_tomorrow(fk_servicio_amount, batch_size):

    
    lista_fk_servicio = list(set(get_fk_servicio_values_next_day()))
    # Formatear la lista como una cadena separada por comas
    lista_fk_servicio_str = ', '.join(map(str, lista_fk_servicio))
    # Obtiene los valores de fk_servicio desde la consulta

    if lista_fk_servicio_str:
        # Llama a la función para eliminar registros
        delete_records_by_fk_servicio(lista_fk_servicio_str)
    else:
        print("No se pudieron obtener los valores de fk_servicio.")
        
    # Consulta SQL con la lista de valores de fk_servicio
    query = f"""
    select
    ser.id as fk_servicio

    , ser.estado /* 0 pre servicio 1 transito 2 terminado 999 eliminado */


    /* comercial */
    , concat ( case when TRIM(comer.usu_nombre) LIKE '% %' then left(TRIM(comer.usu_nombre), strpos(TRIM(comer.usu_nombre), ' ') - 1) else TRIM(comer.usu_nombre) end ,' '
    , case when TRIM(comer.usu_apellido) LIKE '% %' then left(TRIM(comer.usu_apellido), strpos(TRIM(comer.usu_apellido), ' ') - 1) else TRIM(comer.usu_apellido) end ) as comercial_nombre /* ejecutivo comercial */
    ,comer.usu_rut as rut_comercial

    /* clienbte de facturacion */
    , cli_fact.cli_nombre as cli_fact_nombre /* clienbte de facturacion */
    , cli_fact.cli_rut as cli_fact_rut /* clienbte de facturacion */
    , cli_fact."createdAt" as fecha_conversion_fact

    /* cliente para el despacho */
    , cli_desp.cli_nombre as cli_desp_nombre /* cliente para el despacho */
    , cli_desp.cli_rut as cli_desp_rut /* cliente para el despacho */
    , cli_fact."createdAt" as fecha_conversion_desp


    /* nombre del barco */
    , coalesce(nave.nave_nombre,'') as servicio_nave_nombre /* nombre del barco */
    , coalesce(eta.eta_fecha,'') as eta_fecha /* fecha de llegada del barco */


    /* caracteristicas */
    , ser.numero_contenedor
    , ser.fk_tipo_servicio as servicio_codigo /* importacion exportacion almacenamiento desconsolidado etc*/
    , ser.fk_tipo_carga /* fcl contenedor full, lcl contenedor parcial, vehi auto */
    , coalesce(cont_tip.cont_nombre,'') as cont_tipo_nombre /* si es dry es seco, refeer es con temperatura */
    , coalesce(cont_tam.conttam_tamano,'') as cont_tamano /* 20 40 60 */
    , ser.contenedor_peso_carga




    /* etapas del servicio */
    , coalesce(eta_1.tipo, 0) as etapa_tipo /* 1 retiro, 2 presentacion, 3 devolucion, 0 almacenaje */
    , coalesce (eta_1.titulo, '') as etapa_titulo
    , coalesce(eta_1.fecha, '') as etapa_1_fecha
    , coalesce(eta_1.hora, '') as etapa_1_hora
    , coalesce(eta_1.fecha_real_arribo, '') as fecha_real_arribo
    , coalesce(eta_1.hora_real_arribo, '') as hora_real_arribo
    , coalesce(eta_1.fecha_real_salida, '') as fecha_real_salida
    , coalesce(eta_1.hora_real_salida, '') as hora_real_salida
    , coalesce(eta_1.tiempo_estadia, '') as tiempo_estadia


    /* factura */
    , b.sii_factura
    , b.sii_fecha
    , d.total as total_servicio
    , d.total_cobros_extras
    , b."createdAt" as creacion_factura
    , b."updatedAt" as actualizacion_factura

    -- direccion de salida
    , coalesce(dir_salida.direccion, '') as direccion_salida_nombre
    , coalesce(dir_salida.numero, '') as direccion_salida_numero
    , coalesce(dir_salida.lat, '') as direccion_salida_lat
    , coalesce(dir_salida."long", '') as direccion_salida_long
    , coalesce(com_1.comuna_nombre, '') as comuna_salida_nombre -- Nombre de la comuna de salida

    -- direccion de llegada
    , coalesce(dir_llegada.direccion, '') as direccion_llegada_nombre
    , coalesce(dir_llegada.numero, '') as direccion_llegada_numero
    , coalesce(dir_llegada.lat, '') as direccion_llegada_lat
    , coalesce(dir_llegada."long", '') as direccion_llegada_long
    ,coalesce(com_2.comuna_nombre, '') as comuna_llegada_nombre -- Nombre de la comuna de llegada


    , (SELECT temp1.tiempo FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as tiempo_minutos
    , (SELECT temp1.distancia FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as distancia_mts
    , coalesce(dir_1.nombre,'') as etapa_1_lugar_nombre
    , concat(dir_1.nombre) as etapa_1_direccion_texto


    , concat(cond_1.usu_rut) as etapa_1_conductor_rut
    , concat ( TRIM(coalesce(cond_1.usu_nombre,'')),' ',TRIM(coalesce(cond_1.usu_apellido,'')) ) as etapa_1_conductor_nombre
    , cond_1.ult_empt_tipo

    , coalesce(tract_1.patente,'') as etapa_1_tracto


    , coalesce(ser.almacenaje_principal,'') as almacenaje_principal /* donde se guardo el contenedor cuando se fue a retirar, y antes de la presentacion en cliente */
    , concat( coalesce(ser.cont_fila,''),'-',coalesce(ser.cont_columna,''),'-',coalesce(ser.cont_posicion) ) as posicion_ubicacion /* ultima ubicacion en el patio de TNM */

    , coalesce(ser.cont_fila,'') as posicion_fila
    , coalesce(ser.cont_columna,'') as posicion_columna
    , coalesce(ser.cont_posicion) as posicion

    , case 
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='VACIO' THEN concat('S-V ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='LLENO' THEN concat('S-F ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='LLENO' THEN 'A-F'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='LLENO' THEN 'A-F'
    else '' end as posicion_tipo /* tipo de la ultima posicion, si se guardo vacio, o lleno, etc */

    from
    public.servicios as ser
    inner join public.usuarios as comer on ser.fk_comercial=comer.usu_rut
    left join public.clientes as cli_fact on ser.fk_cliente_facturacion=cli_fact.cli_codigo
    left join public.clientes as cli_desp on ser.fk_cliente_despacho=cli_desp.cli_codigo
    left join public.naves as nave on ser.fk_nave=nave.nave_id
    left join public.naves_etas as eta on ser.fk_eta=eta.eta_id
    left join public.contenedores_tipos as cont_tip on ser.fk_tipo_contenedor=cont_tip.cont_id
    left join public.contenedores_tamanos as cont_tam on ser.fk_contenedor_tamano=cont_tam.conttam_id

    left join public.servicios_etapas as eta_1 on ser.id=eta_1.fk_servicio
    left join public.direcciones as dir_1 on eta_1.fk_direccion=dir_1.id
    ---left join public.comunas as com_1 on dir_1."comunaComunaId"=com_1.comuna_id

    left join public.servicios_etapas_conductores as cond_eta_1 on eta_1.id=cond_eta_1.fk_etapa
    left join public.usuarios as cond_1 on cond_eta_1.fk_conductor=cond_1.usu_rut
    left join public.taller_equipos as tract_1 on cond_eta_1.fk_tracto=tract_1.id

    left join public.servicios_etapas as eta_0 on eta_1.fk_etapa_anterior=eta_0.id


    -- Tu consulta SQL actual

    -- Agrega un cruce para obtener la dirección de salida
    LEFT JOIN public.direcciones AS dir_salida ON eta_0.fk_direccion = dir_salida.id

    -- Agrega un cruce para obtener la dirección de llegada
    LEFT JOIN public.direcciones AS dir_llegada ON dir_1.id = dir_llegada.id

    LEFT JOIN public.comunas as com_1 ON dir_salida."comunaComunaId" = com_1.comuna_id
    LEFT JOIN public.comunas as com_2 ON dir_llegada."comunaComunaId" = com_2.comuna_id

    left join public.facturacion_liquidaciones_detalles AS d on ser.id=d.fk_servicio
    LEFT JOIN public.facturacion_liquidaciones_bases AS b ON b.id = d.fk_liquidacion



    WHERE ser.estado != 999
      AND ser.id > 0
      AND ser.id IN ({lista_fk_servicio_str})  -- Insertar la lista de valores de fk_servicio aquí

    ORDER BY 
      ser.id ASC;
    """


    # Llamada a la función
    max_id_etapa = obtener_max_id_etapa()
    initial_index = max_id_etapa
    
    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df = connectionDB_todf2(query)
    df = descomponer_fechas(df)
    max_lenght = len(df) + max_id_etapa
    
    while True:
        try:
            
            if initial_index + batch_size >= max_lenght:
                print("final")
                df2 = df.iloc[initial_index - (max_id_etapa+1):]
                
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
                break
                
            else:
                df2 = df.iloc[initial_index - (max_id_etapa+1):initial_index - (max_id_etapa+1) + batch_size]
                print("indice_maximo_dw: ", initial_index)
                print("largo de la  carga: ", len(df))
                # Imprimir el DataFrame resultante
    
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
    
    
    
            initial_index += batch_size
        
        except Exception as e:
            # Acceder a la información de la excepción a través de la variable 'e'
            print(f"Se ha producido una excepción: {e}")
            print(f"error en la subida del batch {initial_index}")








#incremental_batch_load(12000, 500)





def online_load(fk_servicio):
    
    #lista_fk_servicio = list(set(get_fk_servicio_values(1)))
    # Lista de valores de fk_servicio
    lista_fk_servicio = [fk_servicio]
    batch_size = 1

    # Formatear la lista como una cadena separada por comas
    lista_fk_servicio_str = ', '.join(map(str, lista_fk_servicio))
    # Obtiene los valores de fk_servicio desde la consulta

    if lista_fk_servicio_str:
        # Llama a la función para eliminar registros
        delete_records_by_fk_servicio(lista_fk_servicio_str)
    else:
        print("No se pudieron obtener los valores de fk_servicio.")
        
    # Consulta SQL con la lista de valores de fk_servicio
    query = f"""
    select
    ser.id as fk_servicio

    , ser.estado /* 0 pre servicio 1 transito 2 terminado 999 eliminado */


    /* comercial */
    , concat ( case when TRIM(comer.usu_nombre) LIKE '% %' then left(TRIM(comer.usu_nombre), strpos(TRIM(comer.usu_nombre), ' ') - 1) else TRIM(comer.usu_nombre) end ,' '
    , case when TRIM(comer.usu_apellido) LIKE '% %' then left(TRIM(comer.usu_apellido), strpos(TRIM(comer.usu_apellido), ' ') - 1) else TRIM(comer.usu_apellido) end ) as comercial_nombre /* ejecutivo comercial */
    ,comer.usu_rut as rut_comercial

    /* clienbte de facturacion */
    , cli_fact.cli_nombre as cli_fact_nombre /* clienbte de facturacion */
    , cli_fact.cli_rut as cli_fact_rut /* clienbte de facturacion */
    , cli_fact."createdAt" as fecha_conversion_fact

    /* cliente para el despacho */
    , cli_desp.cli_nombre as cli_desp_nombre /* cliente para el despacho */
    , cli_desp.cli_rut as cli_desp_rut /* cliente para el despacho */
    , cli_fact."createdAt" as fecha_conversion_desp


    /* nombre del barco */
    , coalesce(nave.nave_nombre,'') as servicio_nave_nombre /* nombre del barco */
    , coalesce(eta.eta_fecha,'') as eta_fecha /* fecha de llegada del barco */


    /* caracteristicas */
    , ser.numero_contenedor
    , ser.fk_tipo_servicio as servicio_codigo /* importacion exportacion almacenamiento desconsolidado etc*/
    , ser.fk_tipo_carga /* fcl contenedor full, lcl contenedor parcial, vehi auto */
    , coalesce(cont_tip.cont_nombre,'') as cont_tipo_nombre /* si es dry es seco, refeer es con temperatura */
    , coalesce(cont_tam.conttam_tamano,'') as cont_tamano /* 20 40 60 */
    , ser.contenedor_peso_carga




    /* etapas del servicio */
    , coalesce(eta_1.tipo, 0) as etapa_tipo /* 1 retiro, 2 presentacion, 3 devolucion, 0 almacenaje */
    , coalesce (eta_1.titulo, '') as etapa_titulo
    , coalesce(eta_1.fecha, '') as etapa_1_fecha
    , coalesce(eta_1.hora, '') as etapa_1_hora
    , coalesce(eta_1.fecha_real_arribo, '') as fecha_real_arribo
    , coalesce(eta_1.hora_real_arribo, '') as hora_real_arribo
    , coalesce(eta_1.fecha_real_salida, '') as fecha_real_salida
    , coalesce(eta_1.hora_real_salida, '') as hora_real_salida
    , coalesce(eta_1.tiempo_estadia, '') as tiempo_estadia


    /* factura */
    , b.sii_factura
    , b.sii_fecha
    , d.total as total_servicio
    , d.total_cobros_extras
    , b."createdAt" as creacion_factura
    , b."updatedAt" as actualizacion_factura

    -- direccion de salida
    , coalesce(dir_salida.direccion, '') as direccion_salida_nombre
    , coalesce(dir_salida.numero, '') as direccion_salida_numero
    , coalesce(dir_salida.lat, '') as direccion_salida_lat
    , coalesce(dir_salida."long", '') as direccion_salida_long
    , coalesce(com_1.comuna_nombre, '') as comuna_salida_nombre -- Nombre de la comuna de salida

    -- direccion de llegada
    , coalesce(dir_llegada.direccion, '') as direccion_llegada_nombre
    , coalesce(dir_llegada.numero, '') as direccion_llegada_numero
    , coalesce(dir_llegada.lat, '') as direccion_llegada_lat
    , coalesce(dir_llegada."long", '') as direccion_llegada_long
    ,coalesce(com_2.comuna_nombre, '') as comuna_llegada_nombre -- Nombre de la comuna de llegada


    , (SELECT temp1.tiempo FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as tiempo_minutos
    , (SELECT temp1.distancia FROM public.tiempodistanciadirecciones as temp1 where eta_0.fk_direccion=temp1.dir1 and temp1.dir2=dir_1.id order by id desc limit 1) as distancia_mts
    , coalesce(dir_1.nombre,'') as etapa_1_lugar_nombre
    , concat(dir_1.nombre) as etapa_1_direccion_texto


    , concat(cond_1.usu_rut) as etapa_1_conductor_rut
    , concat ( TRIM(coalesce(cond_1.usu_nombre,'')),' ',TRIM(coalesce(cond_1.usu_apellido,'')) ) as etapa_1_conductor_nombre
    , cond_1.ult_empt_tipo

    , coalesce(tract_1.patente,'') as etapa_1_tracto


    , coalesce(ser.almacenaje_principal,'') as almacenaje_principal /* donde se guardo el contenedor cuando se fue a retirar, y antes de la presentacion en cliente */
    , concat( coalesce(ser.cont_fila,''),'-',coalesce(ser.cont_columna,''),'-',coalesce(ser.cont_posicion) ) as posicion_ubicacion /* ultima ubicacion en el patio de TNM */

    , coalesce(ser.cont_fila,'') as posicion_fila
    , coalesce(ser.cont_columna,'') as posicion_columna
    , coalesce(ser.cont_posicion) as posicion

    , case 
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='VACIO' THEN concat('S-V ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='SALIDA' and ser.cont_tipo='LLENO' THEN concat('S-F ',coalesce(ser.cont_hora,''))
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='INGRESO' and ser.cont_tipo='LLENO' THEN 'A-F'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='VACIO' THEN 'A-V'
    when ser.cont_tipo_mov='CAMBIO POSICION' and ser.cont_tipo='LLENO' THEN 'A-F'
    else '' end as posicion_tipo /* tipo de la ultima posicion, si se guardo vacio, o lleno, etc */

    from
    public.servicios as ser
    inner join public.usuarios as comer on ser.fk_comercial=comer.usu_rut
    left join public.clientes as cli_fact on ser.fk_cliente_facturacion=cli_fact.cli_codigo
    left join public.clientes as cli_desp on ser.fk_cliente_despacho=cli_desp.cli_codigo
    left join public.naves as nave on ser.fk_nave=nave.nave_id
    left join public.naves_etas as eta on ser.fk_eta=eta.eta_id
    left join public.contenedores_tipos as cont_tip on ser.fk_tipo_contenedor=cont_tip.cont_id
    left join public.contenedores_tamanos as cont_tam on ser.fk_contenedor_tamano=cont_tam.conttam_id

    left join public.servicios_etapas as eta_1 on ser.id=eta_1.fk_servicio
    left join public.direcciones as dir_1 on eta_1.fk_direccion=dir_1.id
    ---left join public.comunas as com_1 on dir_1."comunaComunaId"=com_1.comuna_id

    left join public.servicios_etapas_conductores as cond_eta_1 on eta_1.id=cond_eta_1.fk_etapa
    left join public.usuarios as cond_1 on cond_eta_1.fk_conductor=cond_1.usu_rut
    left join public.taller_equipos as tract_1 on cond_eta_1.fk_tracto=tract_1.id

    left join public.servicios_etapas as eta_0 on eta_1.fk_etapa_anterior=eta_0.id


    -- Tu consulta SQL actual

    -- Agrega un cruce para obtener la dirección de salida
    LEFT JOIN public.direcciones AS dir_salida ON eta_0.fk_direccion = dir_salida.id

    -- Agrega un cruce para obtener la dirección de llegada
    LEFT JOIN public.direcciones AS dir_llegada ON dir_1.id = dir_llegada.id

    LEFT JOIN public.comunas as com_1 ON dir_salida."comunaComunaId" = com_1.comuna_id
    LEFT JOIN public.comunas as com_2 ON dir_llegada."comunaComunaId" = com_2.comuna_id

    left join public.facturacion_liquidaciones_detalles AS d on ser.id=d.fk_servicio
    LEFT JOIN public.facturacion_liquidaciones_bases AS b ON b.id = d.fk_liquidacion



    WHERE ser.estado != 999
      AND ser.id > 0
      AND ser.id IN ({lista_fk_servicio_str})  -- Insertar la lista de valores de fk_servicio aquí

    ORDER BY 
      ser.id ASC;
    """


    # Llamada a la función
    max_id_etapa = obtener_max_id_etapa()
    initial_index = max_id_etapa
    
    # Ejecutar la consulta y almacenar los resultados en un DataFrame
    df = connectionDB_todf2(query)
    df = descomponer_fechas(df)
    max_lenght = len(df) + max_id_etapa
    
    while True:
        try:
            
            if initial_index + batch_size >= max_lenght:
                print("final")
                df2 = df.iloc[initial_index - (max_id_etapa+1):]
                
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
                break
                
            else:
                df2 = df.iloc[initial_index - (max_id_etapa+1):initial_index - (max_id_etapa+1) + batch_size]
                print("indice_maximo_dw: ", initial_index)
                print("largo de la  carga: ", len(df))
                # Imprimir el DataFrame resultante
    
                index_dimension = load_dimensions(df2, initial_index)
                index_fact = load_fact_table(df2, initial_index)
    
    
    
            initial_index += batch_size
        
        except Exception as e:
            # Acceder a la información de la excepción a través de la variable 'e'
            print(f"Se ha producido una excepción: {e}")
            print(f"error en la subida del batch {initial_index}")




