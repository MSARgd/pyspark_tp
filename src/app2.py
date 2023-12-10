import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

spark = SparkSession.builder.appName("SPARK SQL with JSON").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema_json = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("ville", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("apartments", IntegerType(), True)
])

# Lire le fichier JSON en utilisant le schéma spécifié
df = spark.read.option("multiLine", True).schema(schema_json).json("data.json")
# df.show()

revenue_per_city = df.groupBy("ville").sum("revenue").withColumnRenamed("sum(revenue)", "total_revenue")
top_2_ville = revenue_per_city.orderBy(col("total_revenue").desc()).limit(2)

# print("Afficher les deux première villes ou l’entreprise a réalisé plus de ventes (en terme de revenues).")
# top_2_ville.show()
print("fficher la liste des clients qui possèdent plus qu’un appartement à rabat.")
client_with_more_than_1apart_in_rabat_df = df.filter((col("ville") == "Rabat")).filter(col("apartments") > 1).orderBy(
    col("apartments").desc())

client_with_more_than_1apart_in_rabat_df.show()

print("Afficher le nombre d’appartements vendues à Casablanca.")
apartments_casa_df = df.filter(col("ville") == "Casablanca")
print(apartments_casa_df.count())

