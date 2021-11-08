# Databricks notebook source
# MAGIC %md #Apache Spark - Comandos DBFS

# COMMAND ----------

# MAGIC %md <p><strong>Objetivo: </strong> El objetivo de este cuaderno es utilizar los distintos comandos que nos ofrece DBFS para trabajar con el sistema de archivos.</p>

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>Índice</h2>
# MAGIC 
# MAGIC <div class="alert alert-block alert-info" style="margin-top: 20px">
# MAGIC <ul>
# MAGIC   <li>Acceder al sistema de archivos</li>
# MAGIC   <li>Trabajando con carpetas</li>
# MAGIC   <li>Mover, copiar y eliminar archivos dentro de DBFS</li>
# MAGIC   <li>Ayuda</li>
# MAGIC </ul>
# MAGIC 
# MAGIC Tiempo estimado: <strong>20 min</strong>
# MAGIC 
# MAGIC </div>
# MAGIC <hr>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Acceder al sistema de archivos

# COMMAND ----------

# MAGIC %md El <b>signo de porcentaje % </b> y <b>fs</b> le dicen a Databricks que el comando que sigue debe ejecutarse en el sistema de archivos de Databricks. Es comando mágico de databricks. Se comienza ejecutando un comando de lista, <b>ls</b>. El primer argumento dice qué carpeta se quiere investigar. La barra indica que se está mirando en la raíz:

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %md Ahora se va a acceder a una de las carpetas que están en la raiz, para ello ponemos el nombre dentro de las barras. Se accede a la carpeta <b>FileStore</b> que es donde usualmente se van a ubicar los ficheros de datos:

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %md
# MAGIC Aquí se obtiene la lista de conjuntos de datos y luego enumeramos el contenido de la carpeta que contiene el ejemplo de las aerolíneas:

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %md
# MAGIC Usamos otro comando, <b>head</b>, para ver el archivo <b>README.md</b> en la carpeta:

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/README.md

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/part-00000

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trabajando con carpetas

# COMMAND ----------

# MAGIC %md
# MAGIC El comando <b>mkdirs</b> crea una carpeta dentro del sistema de archivos. Se va a crear una carpeta Prueba para el ejercicio:

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/Prueba2

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %md
# MAGIC Si han creado carpetas y desean eliminarlas pueden ejecutar la siguiente sentencia <b>rm -r</b>. Se debe especificar la ruta donde se encuentra la carpeta. Se devuelve un <b>Boolean = true</b> si fue exitoso el borrado:

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/Prueba2

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mover, copiar y eliminar archivos dentro de DBFS

# COMMAND ----------

# MAGIC %md Para copiar un archivo de una carpeta a otra se utiliza el comando <b>cp</b>. Debe especificar la ruta donde está el archivo, un espacio y la ruta para donde será copiado:

# COMMAND ----------

# MAGIC %fs cp /databricks-datasets/airlines/README.md /FileStore/Prueba

# COMMAND ----------

# MAGIC %fs ls /FileStore/Prueba

# COMMAND ----------

# MAGIC %md Para eliminar un archivo de una carpeta se utiliza el comando <b>rm</b>. Debe especificar la ruta donde está el archivo:

# COMMAND ----------

# MAGIC %fs rm /FileStore/Prueba/README.md

# COMMAND ----------

# MAGIC %fs rm /FileStore/Prueba/test.ipynb

# COMMAND ----------

# MAGIC %md
# MAGIC Para guardar un archivo en FileStore, colóquelo en el directorio / FileStore en DBFS:

# COMMAND ----------

dbutils.fs.put("/FileStore/Prueba/test.ipynb", "Contents of my file")

# COMMAND ----------

dbutils.fs.head("/FileStore/Prueba/test.ipynb")

# COMMAND ----------

# MAGIC %fs ls /FileStore/Prueba

# COMMAND ----------

# MAGIC %md Para mover un archivo de una carpeta a otra se utiliza el comando <b>mv</b>. Debe especificar la ruta donde está el archivo, un espacio y la ruta para donde será movido. No puede mover los archivos de prueba de databricks, prueba a mover alguno de los archivos que tiene en su sistema de archivos:

# COMMAND ----------

# MAGIC %fs mv /FileStore/Prueba/test.ipynb /FileStore/tables

# COMMAND ----------

# MAGIC %fs ls /FileStore/Prueba

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs rm /FileStore/tables/test.ipynb

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ayuda

# COMMAND ----------

# MAGIC %md
# MAGIC Con estos comandos puedes acceder a la ayuda y descripción de los comandos anteriores, y tambien se puede acceder a la ayuda de un comando en especial:

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Links de ayuda interesantes
# MAGIC <ul>
# MAGIC     <li>Databricks Utilities: https://docs.databricks.com/dev-tools/databricks-utils.html</li>
# MAGIC     <li>Databricks File System (DBFS): https://docs.databricks.com/data/databricks-file-system.html</li> 
# MAGIC     <li>Comandos mágicos: https://docs.databricks.com/notebooks/notebooks-use.html#language-magic</li>  
# MAGIC <ul>
