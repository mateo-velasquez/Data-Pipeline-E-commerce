# Código para hacer la parte del ETL
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Creamos la sesión Spark
spark = SparkSession.builder.appName("E-commerce_Data_Transformation").getOrCreate()

sc = spark.sparkContext

# Por las dudas guardo la dirección del archivo, que más adelante lo voy a necesitar
csv_path = "Data-Original/E-commerce Dataset.csv" 

# Ahora leemos el csv:
archivo = spark.read.csv(csv_path,header=True)
archivo.show(5)


