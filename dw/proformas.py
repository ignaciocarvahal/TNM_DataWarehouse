

import psycopg2
import sqlalchemy as sa
import pandas as pd

from load_server import *
from utils_operaciones import hora_a_minutos, extract_data,  descomponer_fechas, formatear_a_fecha, unir_fecha_hora
from delete_data_server import *

from connection import connectionDB






def cargar_proformas():
    proformas = """
               WITH servicios_union AS (
                   SELECT 
                       id, 
                       CONCAT(tipo, '-', fk_servicio) AS tipo_servicio, 
                       estado, 
                       tipo,
                       fechahora, 
                       posicion, 
                       fecha, 
                       hora, 
                       titulo, 
                       "createdAt", 
                       "updatedAt", 
                       fk_direccion, 
                       fk_servicio, 
                       fk_responsable, 
                       fk_titulo, 
                       comision, 
                       fecha_real_arribo, 
                       hora_real_arribo, 
                       hora_real_salida, 
                       fechahora_salida, 
                       fechahora_real_arribo, 
                       fechahora_real_salida, 
                       fecha_real_salida, 
                       hora_est_llegada, 
                       inicio_gps, 
                       tiempo_estadia, 
                       fecha_real_salida_presentacion, 
                       hora_real_salida_presentacion, 
                       fin_gps, 
                       fk_etapa_anterior, 
                       fk_etapa_posterior, 
                       fecha_real_arribo_auto, 
                       hora_real_arribo_auto, 
                       fecha_real_salida_auto, 
                       hora_real_salida_auto
                   FROM public.servicios_etapas
               ),
               fact_asociados_union AS (
                   SELECT 
                       id, 
                       CONCAT(tipo, '-', fk_servicio) AS tipo_servicio, 
                       estado, 
                       fk_servicio, 
                       referencia, 
                       cliente_facturacion_codigo, 
                       cliente_despacho_codigo,
                       cliente_facturacion_nombre, 
                       cliente_despacho_nombre, 
                       contenedor_numero, 
                       contenedor_tipo, 
                       contenedor_peso, 
                       servicio_tipo, 
                       carga_tipo, 
                       valor, 
                       fecha, 
                       hora, 
                       lugar, 
                       fk_empresa, 
                       "createdAt", 
                       "updatedAt", 
                       fk_responsable, 
                       fk_cabecera
                   FROM public.fact_asociados_detalles
               ),
               fecha_tipo_2 AS (
                   SELECT 
                       fk_servicio,
                       TO_DATE(fecha, 'DD-MM-YYYY') AS fecha_tipo_2
                   FROM servicios_union
                   WHERE tipo = 2
                   GROUP BY fk_servicio, TO_DATE(fecha, 'DD-MM-YYYY')
               )
               SELECT distinct on(su.id)
                   su.id AS etapa_id,
               	su.fk_servicio as fk_servicio,
               	
                   su.fecha,
                   
                   su.tipo,
                   su.titulo,
               	su.posicion AS etapa_posicion,
               	cab.id as proforma_id,
               	fu.id AS fact_asociado_id,
               		cab.estado as estado_proforma
               	, cab.numero_factura
               	, cab.fecha_factura
               	, cab.total_neto as TOTAL_NETO
               	, cab.cant_servicios
               	, cab.fk_empresa
               	
               	
                   ,fu.valor AS monto_proforma,
               	fu.fecha as fecha_proforma,
                   sec.fk_conductor,
                   sec.fk_emp_trans,
                   sec.fk_emp_trans_tipo,
                   sec.fk_conductor,
                   empt_trans.empt_nombre,
                   empt_trans.rut as empresa_rut,
               	
               	 
               	fu."createdAt" as createdat,
                   serv.estado AS servicio_estado,
                   fu.estado AS fact_estado,
                   fu.cliente_facturacion_nombre,
                   fu.cliente_despacho_nombre,
                   fu.contenedor_numero,
                   
                   fu.fecha AS fact_fecha,
                   fu.hora AS fact_hora,
                   fu.lugar AS fact_lugar,
                   fu.referencia,
                   ft2.fecha_tipo_2 
               	
               FROM servicios_union su
               LEFT JOIN fact_asociados_union fu 
               ON su.tipo_servicio = fu.tipo_servicio
               
               left join public.servicios as serv on serv.id=su.fk_servicio
               LEFT JOIN public.servicios_etapas_conductores sec
               ON su.id = sec.fk_etapa
               
               LEFT JOIN fecha_tipo_2 ft2
               ON su.fk_servicio = ft2.fk_servicio
               
               left join public.fact_asociados_cabeceras as cab on cab.id = fu.fk_cabecera
               LEFT JOIN public.empresas_transportes as empt_trans
               ON empt_trans.id = cab.fk_empresa 
               
               where serv.estado!=999
			and ft2.fecha_tipo_2 != '0001-01-01 BC'
               --and fu.estado = 'true' or fu.estado IS NULL 
               --and cab.id=5833
               ORDER BY su.id DESC
				
                ;


                


    """
    df = connectionDB_todf2(proformas)
    print(df)
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
    proformas_values = []
    for index, row in df.iterrows():
        
        
        
        etapa_id = row['etapa_id']
        fk_servicio = row['fk_servicio']
        proforma_id = row['proforma_id']
        fact_asociado_id = row['fact_asociado_id']
        estado_proforma = row['estado_proforma']
        numero_factura = row['numero_factura']
        fecha_factura = row['fecha_factura']
        cant_servicios = row['cant_servicios']
        total_neto = row['total_neto']
        monto_proforma = row['monto_proforma']
        fecha_proforma = row['fecha_proforma']
        empresa_nombre = row['empt_nombre']
        empresa_rut = row['empresa_rut']
        created_at = row['createdat']
        fact_estado = row['fact_estado']
        cliente_facturacion_nombre = row['cliente_facturacion_nombre']
        cliente_despacho_nombre = row['cliente_despacho_nombre']
        contenedor_numero = row['contenedor_numero']
        fact_fecha = row['fact_fecha']
        fecha_tipo_2 = row['fecha_tipo_2']
        
        # Crear una tupla con los valores de la fila actual
        proforma_tuple = (
            etapa_id, fk_servicio, proforma_id, fact_asociado_id, 
            estado_proforma, numero_factura, fecha_factura, cant_servicios, 
            total_neto, monto_proforma, fecha_proforma, empresa_nombre, 
            empresa_rut, created_at, fact_estado, cliente_facturacion_nombre, 
            cliente_despacho_nombre, contenedor_numero, fact_fecha, fecha_tipo_2
        )
        
        # Agregar la tupla a la lista de valores
        proformas_values.append(proforma_tuple)
        
        print(proformas_values)
        
        query = """
        INSERT INTO public.servicios_facturacion (
        etapa_id, 
        fk_servicio, 
        proforma_id, 
        fact_asociado_id, 
        estado_proforma, 
        numero_factura, 
        fecha_factura, 
        cant_servicios, 
        total_neto, 
        monto_proforma, 
        fecha_proforma, 
        empresa_nombre, 
        empresa_rut, 
        created_at, 
        fact_estado, 
        cliente_facturacion_nombre, 
        cliente_despacho_nombre, 
        contenedor_numero, 
        fact_fecha, 
        fecha_tipo_2
    ) 
    VALUES 
        %s
    ON CONFLICT (etapa_id) DO UPDATE
    SET
        fk_servicio = EXCLUDED.fk_servicio,
        proforma_id = EXCLUDED.proforma_id,
        fact_asociado_id = EXCLUDED.fact_asociado_id,
        estado_proforma = EXCLUDED.estado_proforma,
        numero_factura = EXCLUDED.numero_factura,
        fecha_factura = EXCLUDED.fecha_factura,
        cant_servicios = EXCLUDED.cant_servicios,
        total_neto = EXCLUDED.total_neto,
        monto_proforma = EXCLUDED.monto_proforma,
        fecha_proforma = EXCLUDED.fecha_proforma,
        empresa_nombre = EXCLUDED.empresa_nombre,
        empresa_rut = EXCLUDED.empresa_rut,
        created_at = EXCLUDED.created_at,
        fact_estado = EXCLUDED.fact_estado,
        cliente_facturacion_nombre = EXCLUDED.cliente_facturacion_nombre,
        cliente_despacho_nombre = EXCLUDED.cliente_despacho_nombre,
        contenedor_numero = EXCLUDED.contenedor_numero,
        fact_fecha = EXCLUDED.fact_fecha,
        fecha_tipo_2 = EXCLUDED.fecha_tipo_2;
        """

  
    execute_bulk_insert(cursor, query, proformas_values)

    # Confirma los cambios en la base de datos
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

    return 1

cargar_proformas()
