import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F
import datetime

def main(session: snowpark.Session): 
    
    # Stage y tabla destino
    stage_name = "@DATALAKE"
    tabla_control = "archivos_procesados"
    
    # 1. Obtener lista de archivos en el stage
    archivos_stage = session.sql(f"LIST {stage_name}").collect()
    
    # 2. Obtener nombres de archivos ya procesados
    procesados_df = session.table(tabla_control).select("nombre_archivo").collect()
    procesados = {row["NOMBRE_ARCHIVO"] for row in procesados_df}
    
    # 3. Filtrar solo archivos nuevos
    archivos_nuevos = [archivo["name"] for archivo in archivos_stage if archivo["name"] not in procesados]
    
    # Obtener fecha actual del sistema
    fecha_etl = datetime.datetime.now()
    
    for archivo in archivos_nuevos:
        ruta_completa = f"@{archivo}"
    
        df = session.read.format("csv").options({
            "header": True
        }).load(ruta_completa)
                
        # Agregar columna 'etl_last_updated_date' con la fecha actual
        df = df.with_column("etl_last_updated_date", F.lit(fecha_etl))

        # Insertar en la tabla destino
        columnas = columnas = [col.strip().replace('"', '').upper() for col in df.columns]

        print("Columnas encontradas:", columnas)
        
        if 'ORDER_ID' in columnas and 'SHIPPING_COST' in columnas:
            destino = "E_COMMERCE.PRESENTATION.FACT_TABLE"
        elif 'PRODUCT_NAME' in columnas:
            destino = "E_COMMERCE.PRESENTATION.DIM_PRODUCT"
        elif 'CATEGORY_NAME' in columnas:
            destino = "E_COMMERCE.PRESENTATION.DIM_CATEGORY"
        elif 'CUSTOMER_LOGIN_TYPE' in columnas:
            destino = "E_COMMERCE.PRESENTATION.DIM_COSTUMER"
        else:
            raise ValueError(f"No se reconoce la estructura del archivo: {archivo}")

        # Cargar en la tabla correspondiente 'destino'
        df.write.mode("append").save_as_table(destino)   
    
        # Registrar archivo como procesado
        df_insert = session.create_dataframe(
            [(archivo, fecha_etl)],
            schema=["nombre_archivo", "etl_last_updated_date"]
        )
        
        df_insert.write.mode("append").save_as_table(tabla_control)


    return session.create_dataframe([(f,) for f in archivos_nuevos], schema=["archivo_procesado"])