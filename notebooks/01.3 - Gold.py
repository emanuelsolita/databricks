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
source_table_name = 'elpriserr'

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

database = 'emanuel_db'

source_schema = 'silver'
source_table = 'elpriser'

target_schema = 'gold'
target_view = 'view_elpriser'

spark.sql(
  f"""
    CREATE OR REPLACE view {database}.{target_schema}.{target_view} AS
        SELECT
            date_start,
            SEK_per_kWh,
            EUR_per_kWh,
            exchange_rate,
            elzon,
            time_start,
            time_end,
            Date,
            Day_Name,
            Day,
            Week,
            Month_Name,
            Month,
            Quarter,
            Year,
            Year_half,
            FY,
            EndOfMonth
        FROM {database}.{source_schema}.{source_table} el
        left JOIN 
        (select *, to_date(date) as date_start_cal from emanuel_db.staging.calendar) cal
        on el.date_start = cal.date_start_cal
  """
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.gold.view_elpriser

# COMMAND ----------


