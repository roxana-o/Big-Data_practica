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

# Filtrare dupa coduri tari (daca sunt LV MK MT)
df_filtrat = df.filter(df['Receiving Country Code'].isin('LV', 'MK', 'MT'))

# Grupare studenti in functie de codurile tarilor, numararea acestora pentru fiecare caz si ordonarea crescatoare dupa coduri
df2 = df_filtrat.groupBy("Receiving Country Code", "Sending Country Code") \
                .agg(count("*")) \
                .orderBy("Receiving Country Code", "Sending Country Code")


# Afisare finala
df2.show(df2.count())
