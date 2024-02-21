import findspark
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

findspark.init()

SpSession = SparkSession \
    .builder \
    .appName("etl_ventas") \
    .getOrCreate()

SpContext = SpSession.SparkContext

data = SpSession.read.csv('Ventas.csv')

data.show()