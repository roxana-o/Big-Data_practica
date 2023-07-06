from pyspark.sql import SparkSession

#creare Spark Session
spark = SparkSession.builder.master("local[1]")\
                    .appName('Spark1')\
                    .getOrCreate()

#citire fisier Erasmus.csv intr-un DataFrame
df = spark.read.options(header='True', inferSchema='True') \
            .csv("C:/Users/user/Desktop/IBM practica/Erasmus.csv")

#afisare continut (fara a trunchia)
df.show(truncate=False)


