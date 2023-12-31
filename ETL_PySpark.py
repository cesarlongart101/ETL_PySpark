import pyspark
from IPython.display import display 
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, lit
from pyspark import RDD

spark = SparkSession.builder.appName("etl_datos").getOrCreate()
# spark = SparkSession.builder.appName("etl_datos").config("spark.sql.debug.maxToStringFields", "10").getOrCreate()
sqlContext = SQLContext(spark)
print("Paso 1")

path = "./Dataset_ETL_Pyspark/"
players_21 = spark.read.csv(path+"players_21.csv")
print("Paso 2")
display(players_21)

print("Paso 3")
# players_21 = spark.read.format("csv").option("header", "true").load("./Dataset_ETL_Pyspark/players_21.csv")
players_21.show()
print("Paso 4")



players_21 = players_21.select('_c3', '_c16','_c33','_c34','_c35','_c36','_c37','_c38')
# print(players_21)
# players_21.show()

# 
# def dropFirstRow(index, iterator):
def dropFirstRow(iterator):
    print("Paso FUNCION")
    return iter(list(iterator)[1:])


rdd = players_21.rdd
# rdd.collect()
# recopilacion = rdd.collect()
# print(recopilacion)
print(rdd.collect())


print("Paso 5")
# rdd = rdd.mapPartitionsWithIndex(dropFirstRow)
# rdd = rdd.mapPartitions(dropFirstRow)
# print(rdd.collect())
# recopilacion = rdd.collect() #Esto no va 

print("Paso 6")
# schema = StructType([
#     StructField("name",StringType(),False),
#     StructField("Positition",StringType(),True),
#     StructField("PAC",StringType(),False),
#     StructField("SHO",StringType(),False),
#     StructField("PASS",StringType(),False),
#     StructField("DRI",StringType(),False),
#     StructField("DEF",StringType(),False),
#     StructField("PHY",StringType(),False),
# ])
schema = StructType([
    StructField("_c3",StringType(),False),
    StructField("_c16",StringType(),True),
    StructField("_c33",StringType(),False),
    StructField("_c34",StringType(),False),
    StructField("_c35",StringType(),False),
    StructField("_c36",StringType(),False),
    StructField("_c37",StringType(),False),
    StructField("_c38",StringType(),False),
])
print("Paso 7")
#Creacion del dataframe 35:45
dataframe = sqlContext.createDataFrame(rdd,schema)
dataframe.printSchema()
# dataframe = dataframe.select('name', 'Positition','PAC','SHO','PASS','DRI','DEF','PHY')
# dataframe.show()

print("Paso 8")
#Elimina los valores nulos o NAN 42:50
dataframe = dataframe.dropna()

#convertir los datos del dataframe de string a integer 43:35
# dataframe = dataframe.withColumn("PAC",col("PAC").cast(IntegerType()))
# dataframe = dataframe.withColumn("SHO",col("SHO").cast(IntegerType()))
# dataframe = dataframe.withColumn("PASS",col("PASS").cast(IntegerType()))
# dataframe = dataframe.withColumn("DRI",col("DRI").cast(IntegerType()))
# dataframe = dataframe.withColumn("DEF",col("DEF").cast(IntegerType()))
# dataframe = dataframe.withColumn("PHY",col("PHY").cast(IntegerType()))


dataframe = dataframe.withColumn("_c33",col("_c33").cast(IntegerType()))
dataframe = dataframe.withColumn("_c34",col("_c34").cast(IntegerType()))
dataframe = dataframe.withColumn("_c35",col("_c35").cast(IntegerType()))
dataframe = dataframe.withColumn("_c36",col("_c36").cast(IntegerType()))
dataframe = dataframe.withColumn("_c37",col("_c37").cast(IntegerType()))
dataframe = dataframe.withColumn("_c38",col("_c38").cast(IntegerType()))
dataframe.printSchema()

print("Paso 9")
#calcular el promedio de los valores de los jugadores 47:50
# dataframe = dataframe.withColumn("mean", ((col("PAC") + col("SHO") + col("PASS") + col("DRI") + col("DEF") + col("PHY")) / lit(6)))
dataframe = dataframe.withColumn("mean", ((col("_c33") + col("_c34") + col("_c35") + col("_c36") + col("_c37") + col("_c38")) / lit(6)))

print("DATAFRAME con columna nueva")
print(dataframe) #solo se imprime la estructira, así es el tutorial
dataframe.show()
print("Paso 10")

#exportar un archivo csv con los datos del dataframe 51:56
# dataframe = dataframe.repartition(1).write.csv("output.cvs", sep=",")

# # spark.read.csv("output.csv")
# final = pyspark.read.csv("output.csv")
# print(final)
# print("Paso 11")

# ERROR Resuelto en 1:02:20