import pandas as pd
from sqlalchemy import create_engine
from connection import connectionDB_todf
from utils_conductores import extract_data, formatear_fechas
import os

def incremental_batch_load_conductores():
    # Extracción de la data desde la BBDD
    df = extract_data()
    
    # Set display options to show all rows and columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    
    # Conversion de fechas:
    # List of date columns to be converted
    date_columns = ['fecha_desde', 'fecha_hasta', 'dia_pedido', 'dia_actualizado', 'usu_createdat', 'usu_updatedat']
    df = formatear_fechas(df, date_columns)
    
    # Print the DataFrame
    # print(df.columns)
    
    # Separamos el df en dimensiones
    # Transformación de dimensiones
    dimension_usuario = df[['dia_libre_id', 'rut_conductor', 'nombre_completo_conductor', 'tipo_empleado', 'estado_empleado',
                            'estado_empleado2', 'empleado_tipo', 'usu_createdat', 'usu_updatedat',
                            'rut_empresa', 'empresa_nombre', 'empresa_razon_social', 'empresa_tipo',
                            'empresa_id', 'empresa_estado', 'responsable']]
    dimension_fecha = df[['dia_libre_id', 'fecha_desde', 'fecha_hasta', 'dia_pedido', 'dia_actualizado']]
    dimension_tipo_permiso = df[['dia_libre_id', 'fk_tipo_permiso', 'tipo_permiso']]
    
    # Ahora la tabla de hechos:
    hechos = df[['dia_libre_id']]
    
    hechos.loc[:, 'dias_anticipacion'] = (dimension_fecha['fecha_desde'] - dimension_fecha['dia_pedido']).dt.days
    
    
    for index, row in dimension_fecha.iterrows():
        start_date = row['fecha_desde']
        end_date = row['fecha_hasta']
    
        # Check if both start_date and end_date are not NaT
        if not pd.isnull(start_date) and not pd.isnull(end_date):
            days_range = pd.date_range(start_date, end_date, freq='D')
    
            for day in days_range:
                hechos = pd.concat([hechos, pd.DataFrame({
                    'dia_libre_id': [row['dia_libre_id']],
                    'dias_anticipacion': [(day - row['dia_pedido']).days],
                    'dia': day
                })], ignore_index=True)
    
    hechos['dia'] = hechos['dia'].dt.strftime('%Y-%m-%d')
    hechos['dia'] = pd.to_datetime(hechos['dia'], errors='coerce')
    
    # Datos de conexión
    host = "3.86.83.8"
    port = "5432"
    database = "dw"
    user = "postgres"
    password = "ignacio"
    
    # Crear la conexión a la base de datos PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    
    # Guardar las dimensiones en la base de datos en el esquema "personas"
    dimension_usuario.to_sql('dimension_usuario', con=engine, if_exists='replace', index=True, schema='personas')
    dimension_fecha.to_sql('dimension_fecha', con=engine, if_exists='replace', index=True, schema='personas')
    dimension_tipo_permiso.to_sql('dimension_tipo_permiso', con=engine, if_exists='replace', index=True, schema='personas')
    
    try:
        hechos.to_sql('tabla_hechos', con=engine, if_exists='replace', index=True, schema='personas')
    except Exception as e:
        print(f"Error during insertion: {e}")
    
    # Mostrar la tabla de hechos después de mapear las llaves foráneas
    print(hechos)