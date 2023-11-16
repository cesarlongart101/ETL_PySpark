import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("etl_datos").getOrCreate()
# spark = SparkSession.builder.appName("etl_datos").config("spark.sql.debug.maxToStringFields", "10").getOrCreate()
sqlcontext = SQLContext(spark)
print("Paso 1")

path = "./Dataset_ETL_Pyspark/"
players_21 = spark.read.csv(path+"players_21.csv")
print("Paso 2")
print(players_21)
print("Paso 3")
# players_21 = spark.read.format("csv").option("header", "true").load("./Dataset_ETL_Pyspark/players_21.csv")
players_21.show()
print("Paso 4")



players_21 = players_21.select('_c3', '_c16','_c33','_c34','_c35','_c36','_c37','_c38')
print(players_21)
players_21.show()


def dropfirstrow(index, iterator):
    return iter(list(iterator)[1:])


rdd = players_21.rdd

# recopilacion = rdd.collect()
# print(recopilacion)
print("Paso 5")

rdd = rdd.mapPartitionsWithIndex(dropfirstrow)

# recopilacion = rdd.collect() #Esto no va 

print("Paso 6")
schema = StructType([
    StructField("name",StringType(),False),
    StructField("Positition",StringType(),True),
    StructField("PAC",StringType(),False),
    StructField("SHO",StringType(),False),
    StructField("PASS",StringType(),False),
    StructField("DRI",StringType(),False),
    StructField("DEF",StringType(),False),
    StructField("PHY",StringType(),False)
])
print("Paso 7")
dataframe = sqlcontext.createDataFrame (rdd,schema)
dataframe.printSchema()
# print(dataframe)

print("Paso 8")
dataframe = dataframe.dropna()

dataframe = dataframe.withColumn("PAC",col("PAC").cast(IntegerType()))
dataframe = dataframe.withColumn("SHO",col("SHO").cast(IntegerType()))
dataframe = dataframe.withColumn("PASS",col("PASS").cast(IntegerType()))
dataframe = dataframe.withColumn("DRI",col("DRI").cast(IntegerType()))
dataframe = dataframe.withColumn("DEF",col("DEF").cast(IntegerType()))
dataframe = dataframe.withColumn("PHY",col("PHY").cast(IntegerType()))

dataframe.printSchema()

print("Paso 9")

dataframe = dataframe.withColumn("mean", ((col("PAC") + col("SHO") + col("PASS") + col("DRI") + col("DEF") + col("PHY")) / lit(6)))

print("DATAFRAME con columna nueva")
print(dataframe)



print("Paso 10")

dataframe = dataframe.repartition(1).write.csv("output.cvs", sep=",")

# spark.read.csv("output.csv")
final = pyspark.read.csv("output.csv")
print(final)
print("Paso 11")