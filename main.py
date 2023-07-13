from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Creare Spark Session
spark = SparkSession.builder.master("local[1]")\
                    .appName('Spark1')\
                    .getOrCreate()

# Citire fisier Erasmus.csv intr-un DataFrame
df = spark.read.options(header='True', inferSchema='True') \
            .csv("C:/Users/user/Desktop/IBM practica/Erasmus.csv")

# Afisare continut initial (fara a trunchia)
# df.show(truncate=False)

# Grupare studenti in functie de codurile tarilor, numararea acestora pentru fiecare caz si ordonarea crescatoare dupa coduri
df2 = df.groupBy("Receiving Country Code", "Sending Country Code") \
                .agg(count("*")) \
                .orderBy("Receiving Country Code", "Sending Country Code")

# Filtrare dupa coduri tari (daca sunt LV MK MT)
df2_filtrat = df2.filter(df2['Receiving Country Code'].isin('LV', 'MK', 'MT'))

# Afisare finala
df2_filtrat.show(df2_filtrat.count())
