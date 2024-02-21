import findspark
import pyspark
import pandas as pd
from IPython.display import display 
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, lit
from pyspark import RDD


findspark.init()

spark = SparkSession \
    .builder \
    .appName("etl_datos") \
    .getOrCreate()

sqlContext = SQLContext(spark)

df = spark.createDataFrame([
    (14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
df.show(2)

path = "./Dataset_ETL_Pyspark/"
players_21 = spark.read.csv(path+"players_21.csv")
display(players_21)


# players_21 = spark.read.format("csv").option("header", "true").load("./Dataset_ETL_Pyspark/players_21.csv")

players_21 = players_21.select('_c3', '_c4','_c37','_c38','_c39','_c40','_c41','_c42')
print(players_21)
# players_21.show()

def dropFirstRow(index, iterator):
# def dropFirstRow(iterator):
    print("Paso FUNCION")
    return iter(list(iterator)[1:])

rdd = players_21.rdd
# rdd.collect()
# recopilacion = rdd.collect()
# print(recopilacion)
# print(rdd.collect())

rdd = rdd.mapPartitionsWithIndex(dropFirstRow)
# rdd = rdd.mapPartitions(dropFirstRow)
# print(rdd.collect())
# recopilacion = rdd.collect() #Esto no va 

schema = StructType([
    StructField("name",StringType(),False),
    StructField("Positition",StringType(),True),
    StructField("PAC",StringType(),True),
    StructField("SHO",StringType(),True),
    StructField("PASS",StringType(),True),
    StructField("DRI",StringType(),True),
    StructField("DEF",StringType(),True),
    StructField("PHY",StringType(),True),
])

#Creacion del dataframe 35:45
dataframe = sqlContext.createDataFrame(rdd,schema)
dataframe.printSchema()
# dataframe = dataframe.select('name', 'Positition','PAC','SHO','PASS','DRI','DEF','PHY')

#Elimina los valores nulos o NAN 42:50
# dataframe = dataframe.dropna()

# cambia los valores nulos en ceros de cada row
dataframe = dataframe.fillna(0)

#convertir los datos del dataframe de string a integer 43:35
dataframe = dataframe.withColumn("PAC",col("PAC").cast(IntegerType()))
dataframe = dataframe.withColumn("SHO",col("SHO").cast(IntegerType()))
dataframe = dataframe.withColumn("PASS",col("PASS").cast(IntegerType()))
dataframe = dataframe.withColumn("DRI",col("DRI").cast(IntegerType()))
dataframe = dataframe.withColumn("DEF",col("DEF").cast(IntegerType()))
dataframe = dataframe.withColumn("PHY",col("PHY").cast(IntegerType()))
dataframe.printSchema()

#calcular el promedio de los valores de los jugadores 47:50
dataframe = dataframe.withColumn("mean", ((col("PAC") + col("SHO") + col("PASS") + col("DRI") + col("DEF") + col("PHY")) / lit(6)))

print("DATAFRAME con columna nueva")
print(dataframe) #solo se imprime la estructira, así es el tutorial

#AQUÍ ROMPE TAMBIEN AL PEDIR QUE IMPRIMA EL DATAFRAME CON DATAFRAME.SHOW()

dataframe.show()
# print(dataframe)
# display(dataframe)

#exportar un archivo csv con los datos del dataframe 51:56
# dataframe = dataframe.repartition(1).write.csv("output.csv", sep=",")

# # spark.read.csv("output.csv")
# final = pyspark.read.csv("output.csv")
# print(final)
# print("Paso 11")

# ERROR Resuelto en 1:02:20