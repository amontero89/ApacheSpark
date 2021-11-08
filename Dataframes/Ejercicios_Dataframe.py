# Databricks notebook source
# MAGIC %md #Apache Spark - Dataframes

# COMMAND ----------

# MAGIC %md <p><strong>Objetivo: </strong> El objetivo de este cuaderno es crear un dataframe de Spark y aplicar distintos tipos de transformaciones y acciones</p>

# COMMAND ----------

# MAGIC %md ## Cargar los datos
# MAGIC Para este ejercicio se va a utilizar un conjunto de datos Movies en formato Parquet:

# COMMAND ----------

# MAGIC %md Leer los datos

# COMMAND ----------

df_movies = spark.read.parquet("/FileStore/tables/movies.parquet")

# COMMAND ----------

# MAGIC %md Imprimir el esquema

# COMMAND ----------

df_movies.printSchema

# COMMAND ----------

# MAGIC %md Mostrar los datos

# COMMAND ----------

display(df_movies)

# COMMAND ----------

# MAGIC %md Mostrar una cantidad específica de datos

# COMMAND ----------

df_movies.take(10)

# COMMAND ----------

display(df_movies.take(10))

# COMMAND ----------

display(df_movies.tail(5))

# COMMAND ----------

# MAGIC %md Mostrar una fracción aleatoria de los datos

# COMMAND ----------

display(df_movies.sample(fraction=0.070))

# COMMAND ----------

# MAGIC %md Mostrar los nombres de las columnas en el dataframe

# COMMAND ----------

df_movies.columns

# COMMAND ----------

# MAGIC %md ## select(columns)

# COMMAND ----------

# MAGIC %md Seleccionar algunas columnas

# COMMAND ----------

display(df_movies.select("movie_title","produced_year"))

# COMMAND ----------

df_movies.select("movie_title","produced_year").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ##selectExpr(expressions)

# COMMAND ----------

# MAGIC %md Añadir una columna calculada utilizando una expresión SQL

# COMMAND ----------

df_movies.selectExpr("*","(produced_year - (produced_year % 10)) as decade").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Quedarme con los datos del nuevo dataframe

# COMMAND ----------

df_new = df_movies.selectExpr("*","(produced_year - (produced_year % 10)) as decade")

# COMMAND ----------

display(df_new)

# COMMAND ----------

# MAGIC %md Utilizando funciones dentro de una expresión SQL

# COMMAND ----------

df_movies.selectExpr("count(distinct(movie_title)) as movies","count(distinct(actor_name)) as actors").show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##filler(condition), where(condition)

# COMMAND ----------

# MAGIC %md Filtrar filas utilizando operadore lógicos y los valores de las columnas

# COMMAND ----------

df_filter = df_movies.filter(df_movies.produced_year < 2000)
display(df_filter)

# COMMAND ----------

df_movies.filter(df_movies.produced_year == 2000).show(5)

# COMMAND ----------

df_movies.select("movie_title","produced_year").filter(df_movies.produced_year != 2000).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Count

# COMMAND ----------

# MAGIC %md Contar los elementos en el dataframe

# COMMAND ----------

df_movies.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##distinct y sort

# COMMAND ----------

# MAGIC %md Elimino los que son iguales y despues los ordeno

# COMMAND ----------

display(df_movies.select("movie_title").distinct().sort("movie_title"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##OrderBy

# COMMAND ----------

# MAGIC %md
# MAGIC Ordenando los valores de una columna

# COMMAND ----------

display(df_movies.orderBy("actor_name"))

# COMMAND ----------

display(df_movies.orderBy("actor_name", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Describe

# COMMAND ----------

# MAGIC %md
# MAGIC Sometimes it is useful to have a general sense of the basic statistics of the data you
# MAGIC are working with. The basic statistics this transformation can compute for string and
# MAGIC numeric columns are count, mean, standard deviation, minimum, and maximum.
# MAGIC You can pick and choose which string or numeric columns to compute the statistics for.

# COMMAND ----------

df_movies.describe("produced_year").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Guardar en un archivo Parquet en el sistema de archivos

# COMMAND ----------

df_filter.write.parquet("/FileStore/Filter.parquet")

# COMMAND ----------

parquetFile = spark.read.parquet("/filter.parquet")
display(parquetFile)

# COMMAND ----------

dbutils.fs.rm("/FileStore/Filter.parquet", True)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Ahora tu</h3>

# COMMAND ----------

# MAGIC %md
# MAGIC Selecciona otro comando de Spark y aplícalo en el conjunto de datos

# COMMAND ----------

# MAGIC %md Cambiar el nombre a una columna

# COMMAND ----------

df_movies = df_movies.withColumnRenamed("movie_title", "movieTitle")

# COMMAND ----------

df_movies.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtrando por atributo numérico y Alias

# COMMAND ----------

df_movies.select("movieTitle","produced_year",(df_movies.produced_year.between(1980, 2000)).alias("Between")).show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtrando por atributo categórico y Alias

# COMMAND ----------

df_movies.select("actor_name", (df_movies.actor_name.startswith("Coo")).alias("Starts with Coo")).show(50)

# COMMAND ----------

df_movies.select("actor_name", (df_movies.actor_name.startswith("Coo")).alias("True")).filter( df_movies.actor_name.startswith("Coo")== "true").show(50)

# COMMAND ----------

# MAGIC %md
# MAGIC Agrupamiento, contar y ordenar

# COMMAND ----------

per_year = df_movies.groupBy("produced_year").count().sort("produced_year")
per_year.show()
display(per_year)
