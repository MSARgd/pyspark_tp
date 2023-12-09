import os
from pyspark.sql import SparkSession
from datetime import datetime

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

spark = SparkSession.builder.appName("PySPARK").master("local[*]") \
    .config("spark.driver.extraClassPath", "/home/msa/Documents/M2/S3/Big_Data/TP_pyspark/mysql-connector-j-8.0.31.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


db_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/DB_IMOMAROC",
    "user": "root",
    "password": "1234@@@@"
}
query_1 = """
SELECT * FROM  Project 
"""
query_2 = """

select P.ID_PROJECT , P.TITRE,count(T.ID_TACHES) as NUMBER from Project  as P 
                     INNER JOIN TACHES AS T ON P.ID_PROJECT = T.ID_PROJECT
                    WHERE DATEDIFF(T.DATE_FIN, T.DATE_DEBIT) > 30 
                    group by P.ID_PROJECT 

"""



df_query_1 = spark.read.format("jdbc").options(**db_properties).option("query", query_1).load()
df_query_2 = spark.read.format("jdbc").options(**db_properties).option("query", query_2).load()
df_query_2.show()
    # .option("dbtable", "Project").load()
# print("Afficher pour chaque projet, le nombre de tâches dont la durée dépasse un mois ")
# projctesDf.show()

'''
filtred_project = projctesDf.filter(projctesDf['DATE_FIN']> datetime.now().date())
print(" Afficher les projets en cours de réalisation.")
filtred_project.show()

'''

