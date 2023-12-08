import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.1-bin-hadoop2.7"

import findspark
findspark.init()


spark = SparkSession.builder.appName("PySPARK").master("local[*]").getOrCreate()


