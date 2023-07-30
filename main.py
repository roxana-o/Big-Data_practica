from flask import Flask, render_template
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')

# Creare Spark Session
spark = SparkSession.builder.master("local[1]")\
                    .appName('Spark1')\
                    .getOrCreate()


# Citire fisier Erasmus.csv intr-un DataFrame
df = spark.read.options(header='True', inferSchema='True') \
            .csv('templates/Erasmus.csv')

# Afisare continut initial (fara a trunchia)
# df.show(truncate=False)

# Filtrare dupa coduri tari (daca sunt LV MK MT)
df_filtered = df.filter(df['Receiving Country Code'].isin('LV', 'MK', 'MT'))

# Grupare studenti in functie de codurile tarilor, numararea acestora pentru fiecare caz si ordonarea crescatoare dupa coduri
df2 = df_filtered.groupBy("Receiving Country Code", "Sending Country Code") \
                 .agg(count("*").alias("Count")) \
                 .orderBy("Receiving Country Code", "Sending Country Code")

# Configuratii conectare la baza de date MySQL
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Exportare date in 3 tabele separate in functie de tara (LV, MK, MT)
countries = ["LV", "MK", "MT"]
for country in countries:
    df3 = df2.filter(df2['Receiving Country Code'] == country).drop("Receiving Country Code")
    df3.write.jdbc("jdbc:mysql://localhost:3306/erasmus?useSSL=false", "Erasmus_" + country, "overwrite", properties)

# Afisare finala
df2.show(df2.count())

# ------------------------------- Displaying data in a tabular format on the webpage----------------------------

app = Flask(__name__)


@app.route('/')
def show_results():
    results = df2.collect()
    return render_template('index.html', results=results)


if __name__ == '__main__':
    app.run(debug=True)
