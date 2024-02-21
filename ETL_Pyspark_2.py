import findspark
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from IPython.display import display 

findspark.init()

SpSession = SparkSession \
    .builder \
    .appName("etl_ventas") \
    .getOrCreate()

SpContext = SQLContext(SpSession)

data = SpSession.read.csv('./Dataset_ETL_Pyspark/Ventas.csv', header = True, sep=';')
# data.show(truncate=False)
# print(data)
# print('Tipo de dato:')
# print(type(data))

data.printSchema()
print(data.columns)
print(data.dtypes)



# dataframe = SpContext.createDataFrame(data)
# display(data)
