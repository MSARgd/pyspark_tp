# projctesDf = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("url", "jdbc:mysql://localhost:3306/DB_IMOMAROC") \
#     .option("user", "root").option("password", "1234@@@@") \
#     .option("query","select P.ID_PROJECT , P.TITRE,count(T.ID_TACHES) as NUMBER from Project  as P "
#                     "INNER JOIN TACHES AS T ON P.ID_PROJECT = T.ID_PROJECT"
#                     " WHERE DATEDIFF(T.DATE_FIN, T.DATE_DEBIT) > 30 "
#                     ""
#                     "group by P.ID_PROJECT ").load()