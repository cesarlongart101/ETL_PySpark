import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import IntegerType

# spark = SparkSession.builder.appName("etl_datos").getOrCreate()
spark = SparkSession.builder.appName("etl_datos").config("spark.sql.debug.maxToStringFields", "10").getOrCreate()
sqlcontext = SQLContext(spark)


# path = "./Dataset_ETL_Pyspark/"
# players_21 = spark.read.csv(path+"players_21.csv")
# print(players_21)
players_21 = spark.read.format("csv").option("header", "true").load("./Dataset_ETL_Pyspark/players_21.csv")
players_21.show()
