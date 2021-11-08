# Databricks notebook source
# MAGIC %md #Apache Spark - Introducción

# COMMAND ----------

# MAGIC %md <p><strong>Objetivo: </strong> El objetivo de este cuaderno es crear un dataframe de Spark y utilizar código SQL en un notebook de Python:</p>

# COMMAND ----------

# MAGIC %md ##Cargar los datos
# MAGIC Para este ejercicio se va a utilizar un conjunto de datos ejemplo de los que provee Databricks. Se cargan los datos de tipo CSV y se infiere el esquema de los datos:

# COMMAND ----------

# Use the Spark CSV datasource with options specifying:
#  - First line of file is a header
#  - Automatically infer the schema of the data
data = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header="true", inferSchema="true") 

# COMMAND ----------

# MAGIC %md ##Visualizar los datos

# COMMAND ----------

# MAGIC %md
# MAGIC Para visualizar los datos que se han cargado se utiliza el comando <b>display</b>:

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md ##Crear una vista de los datos

# COMMAND ----------

# MAGIC %md
# MAGIC El comando <b>createOrReplaceTempView</b> crea una nueva vista temporal usando un SparkDataFrame en la sesión de Spark. Si ya existe una vista temporal con el mismo nombre, la reemplaza. U la vista se elimina cuando terminamos el cluster, es una vista temporal. Permite realizar consultas SQL sobre los datos. Se pueden crear una vista de toda el dataframe o de una parte: 

# COMMAND ----------

data.createOrReplaceTempView("data_geo")

# COMMAND ----------

# MAGIC %md ##Ejecutar sentencias SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora se puedes ejecutar consultar SQL sobre la vista <b>data_geo</b> igual que si fuera una tabla. Para ejecutar código SQL en un cuaderno Python se debe poner el comando <b>%sql</b> para indicar que se va a ejecutar otro tipo de código dentro del notebook. En la consulta se puede ver los precios medios por cada estado para el año 2015 y se puede visualizar como un mapa:

# COMMAND ----------

# MAGIC %sql
# MAGIC select `State Code`, `2015 median sales price` from data_geo

# COMMAND ----------

# MAGIC %md
# MAGIC En esta otra consulta se pueden observar las top 10 ciudades con los precios medios más altos para 2015 y la población estimada en 2014:

# COMMAND ----------

# MAGIC %sql
# MAGIC select City, `2014 Population estimate`/1000 as `2014 Population Estimate (1000s)`, `2015 median sales price` as `2015 Median Sales Price (1000s)` from data_geo order by `2015 median sales price` desc limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC En esta conuslta se muestra un histograma de los precios medios de las ventas para 2015:

# COMMAND ----------

# MAGIC %sql
# MAGIC select `State Code`, `2015 median sales price` from data_geo order by `2015 median sales price` desc;
