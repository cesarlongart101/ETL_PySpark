import findspark
import pyodbc
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, lit
from pyspark import SparkContext
from IPython.display import display

findspark.init()
findspark.find()

# SpSession = SparkSession \
#     .builder \
#     .appName("etl_ventas") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("SQL_Prueba") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://DESKTOP-46PH49A\SQLEXPRESS:1433;database=DGT_SQL_PRUEBA") \
    .option("dbtable", "USUARIOS") \
    .option("user", "sa") \
    .option("password", "1234") \
    .load()


# SpContext = SQLContext(SpSession)
# SpContext = SpSession.SparkContext
# SpContext = SpSession.sparkContext


# data = SpSession.read.csv('./Dataset_ETL_Pyspark/Ventas.csv', header = True, sep=';')
# data.show(truncate=False)
# print(data)
# print('Tipo de dato:')
# print(type(data))
# display(data)

# data.printSchema()
# print(data.columns)
# print(data.dtypes)

# data.select(data.Country).show(5,truncate=False)
# data.select(col('Country')).show(5,truncate=False)
# data.select(data.Country, data.Date).show(truncate=False)
# data.select(col('COUNTRY'), col('DATE')).show(5,truncate=False)

# df = data.withColumn('First_Column', lit(1))
# df = data.withColumn('Second_Column', lit(2)) \
#           .withColumn('Third_Column', lit('Third_Column'))

# df.groupBy('Country').count().show()
# df.groupBy(' Product ', 'Country').count().show()

# df = df.drop('Second_Column', 'Third_Column')

# df.orderBy(' Product ').show(5)
# df.orderBy(' Product ', ascending=False).show(5)
# df.groupby('Segment').count().orderBy('count', ascending=False).show(5)

# df.count()
# total_count = df.count()
# print('Total de registros:',total_count )
# #Col es una libreria que se debe importar
# Germany_total = df.filter(col('Country')=='Germany').count()
# print('Registros en Germany:', Germany_total )
# df.filter(col('Country')=='Germany').show()
# # df.select(data.Country).show(5,truncate=False)

# total_count = df.count()
# print('Total de registros:',total_count )
# Germany_carretera = df.filter((col('Country')=='Germany')&(col(' Product ')==' Carretera ')).count()
# print('Total de Germany_carretera:',Germany_carretera )
# df.filter((col('Country')=='Germany')&(col(' Product ')==' Carretera ')).show()