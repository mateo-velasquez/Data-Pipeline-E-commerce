___
# <font color= #003366> **Data-Pipeline-E-commerce** </font>
___

# <font color= #003366> **Explicación de ejecución del código** </font>

Este repositorio contiene la información del DataPipeline trabajado en la materia de Proyecto de Ingeniería de Datos en el Instituto Tecnológico de Estudios Superiores de Occidente (ITESO). En el periodo de Primavera 2025

Dentro del repositorio se encuentran diferentes carpetas: 
- Data-Created: Dentro de esta carpeta se encuentran los archivos de python para la generación de los archivos csv.
- Data-Original: Dentro de esta carpeta se encuentra el archivo con los datos originales obtenidos de la plataforma Kaggle y con los que se trabajó a lo largo del proyecto. 
- Data-Processed: Dentro de esta carpeta se encuentran las 3 dimensiones que se crearon con el archivo de ProcessData. Las dimensiones que se crearon fueron de categoría, de cliente, de producto y la Fact_Table
- jobs: Dentro de esta carpeta se encuentran los archivos de la creación de tablas(sql) y la función de cargar los archivos a Datawarehouse(py).

De igual manera se encuentran diferentes archivos:
- `requirements.txt`: Se encuentran la lista de las dependencias(librerías y versiones) que se necesitan para que el proyecto funcione correctamente. En este caso **pyspark** y **findspark**.
- `ProcessData.ipynb`: Este archivo contiene código en pyspark en donde se hizo la ingesta de datos y la limpieza de datos y normalización que permitió crear las diferentes dimensiones así como la Fact_Table. Este fue el primer archivo que se creó para posteriormente poder almacenar los datos en Snowflake.

El proceso de uso de los diferentes códigos fue el siguiente:

1. Se realizó la limpieza de datos con el archivo de `ProcessData.ipynb`, así como para elborar las dimensiones.

2. Los datos fueron almacenados en snowflake por lo que se utilizó el archivo de `createTables.sql`para generar las diferentes tablas. Una vez creadas las tablas se usó el archivo de `load_files_to_Datawarehouse.py` para cargar los datos a las diferentes tablas.

3. Se usaron los 4 archivos dentro de la carpeta `DataCreated` para generar nuevos datos en formato csv.
