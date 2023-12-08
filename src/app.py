from pyspark.sql import SparkSession
from pyspark import SparkContext
sc = SparkContext.getOrCreate()


spark = SparkSession.builder.appName("PySPARK").master("local[*]").getOrCreate(sc)



spark.stop()
