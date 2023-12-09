from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialisation de la session Spark
spark = SparkSession.builder.appName("TopCitiesByRevenue").getOrCreate()

# Chargement des données JSON dans un DataFrame
sales_data = spark.read.json("chemin_vers_sales_data.json")

# Calcul du revenu total par ville
revenue_per_city = sales_data.groupBy("city").sum("revenue").withColumnRenamed("sum(revenue)", "total_revenue")

# Affichage des deux premières villes avec les revenus les plus élevés
top_cities = revenue_per_city.orderBy(col("total_revenue").desc()).limit(2)
top_cities.show()
