import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# spark = SparkSession.builder.appName("etl_datos").getOrCreate()
spark = SparkSession.builder.appName("etl_datos").config("spark.sql.debug.maxToStringFields", "10").getOrCreate()
sqlcontext = SQLContext(spark)


path = "./Dataset_ETL_Pyspark/"
players_21 = spark.read.csv(path+"players_21.csv")
# print(players_21)
# players_21 = spark.read.format("csv").option("header", "true").load("./Dataset_ETL_Pyspark/players_21.csv")
# players_21.show()

players_21 = players_21.select('_c3', '_c16','_c33','_c34','_c35','_c36','_c37','_c38')
# print(players_21)

def dropfirstrow(index, iterator):
    return iter(list(iterator)[1:])

rdd = players_21.rdd

# print(rdd)

dropfirstrow()
rdd = rdd.mapPartitionsWithIndex(dropfirstrow)

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

dataframe = SQLContext.createDataFrame (rdd,schema)
dataframe.printSchema()
print(dataframe)