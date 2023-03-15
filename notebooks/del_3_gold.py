# Databricks notebook source
# MAGIC %md
# MAGIC Sista delen i vår "Pipeline" - Gold

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md 
# MAGIC Läs in vår berikade tabell

# COMMAND ----------

catalog = 'emanuel_db'
source_schema = 'silver'
source_table_name = 'elpris_ber'

# COMMAND ----------

spark_df = spark.read.table(f"{catalog}.{source_schema}.{source_table_name}")
display(spark_df)

# COMMAND ----------

spark_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Skapa vårt nya schema **Gold**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists emanuel_db.gold

# COMMAND ----------

# MAGIC %md 
# MAGIC Skapa en vy över tabellen

# COMMAND ----------

target_schema = 'gold'
target_table_name = 'view_elpris'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view emanuel_db.gold.view_elpris
# MAGIC as 
# MAGIC select 
# MAGIC   date_start,
# MAGIC   SEK_per_kWh,
# MAGIC   EUR_per_kWh,
# MAGIC   EXR,
# MAGIC   elzoon,
# MAGIC   time_start,
# MAGIC   time_end,
# MAGIC   Date,
# MAGIC   Day_Name,
# MAGIC   Day,
# MAGIC   Week,
# MAGIC   Month_Name,
# MAGIC   Month,
# MAGIC   Quarter,
# MAGIC   Year,
# MAGIC   Year_half,
# MAGIC   FY,
# MAGIC   EndOfMonth
# MAGIC 
# MAGIC from emanuel_db.silver.elpris_ber

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.gold.view_elpris

# COMMAND ----------


