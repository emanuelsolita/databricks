# Databricks notebook source
# MAGIC %md
# MAGIC ### Del 2 - Silver

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import sha1, col, initcap, to_timestamp, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC Berika data - Kalender

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.bronze.calendar_bronze

# COMMAND ----------

spark_cal = _sqldf

# COMMAND ----------

spark_cal = spark_cal.withColumn('date_start', to_date(col('date')))
spark_cal.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC Läs in data som vi sparade i tidigare del 
# MAGIC
# MAGIC ```%sql select * from <catalog>.<schema>.<table>```
# MAGIC
# MAGIC eller 
# MAGIC
# MAGIC ```spark.read.table("<catalog>.<schema>.<table>")```

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.bronze.elpriser_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC **Notera** att resultatet från SQL-frågan sparas i variabeln ```_sqldf```när man exekvera en SQL fråga med ```%sql select ...```

# COMMAND ----------

# MAGIC %md
# MAGIC Skriv resultatet till en bestämd variabel föra att inte skriva över den senare

# COMMAND ----------

spark_elpris = _sqldf 

# COMMAND ----------

spark_elpris = spark_elpris.withColumn('date_start', to_date(col('time_start')))
spark_elpris.show()

# COMMAND ----------

spark_elpris.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Nu är vi klara med berikningen och vill spara vår nya DF i schemat silver
# MAGIC
# MAGIC Använd ```spark.sql("<Query>")``` för att skapa schemat i din catalog

# COMMAND ----------

spark.sql("create schema if not exists emanuel_db.silver")

# COMMAND ----------

# MAGIC %md 
# MAGIC Skriv data till ditt nya schema

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gör dessa transfomrationer med hjälp av ```spark.readStream```

# COMMAND ----------

# MAGIC %md
# MAGIC ###Berika kalendern

# COMMAND ----------

name = "_".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0:2])
deltaTablesDirectory = '/Users/'+name+'/elpriser/'

database = 'emanuel_db'

source_schema = 'bronze'
source_table = 'calendar_bronze'

target_schema = 'silver'
target_table = 'calendar'

(spark.readStream
        .table(f'{database}.{source_schema}.{source_table}')
        .withColumn("date_start", to_date(col("date")))
      .writeStream
        .option("checkpointLocation", f"{deltaTablesDirectory}/checkpoint/calendar")
        .trigger(once=True)
        .table(f"{database}.{target_schema}.{target_table}").awaitTermination())

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Berika el-data

# COMMAND ----------

database = 'emanuel_db'

source_schema = 'bronze'
source_table = 'elpriser_bronze'

target_schema = 'silver'
target_table = 'elpriser'

(spark.readStream
        .table(f'{database}.{source_schema}.{source_table}')
        .withColumnRenamed("EXR", "exchange_rate")
        .withColumn("date_start", to_date(col("time_start")))
        .withColumn("time_end", to_timestamp(col("time_end")))
        .withColumn("time_start", to_timestamp(col("time_start")))
        .drop(col("_rescued_data"))
      .writeStream
        .option("checkpointLocation", f"{deltaTablesDirectory}/checkpoint/elpriser")
        .trigger(once=True)
        .table(f"{database}.{target_schema}.{target_table}").awaitTermination())

# COMMAND ----------

spark.read.table(f"{database}.{schema}.{table_name}").show()
