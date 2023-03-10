# Databricks notebook source

import pandas as pd 
import numpy as np 

# COMMAND ----------

spark_df = spark.read.option('inferSchema', True).json(f"abfss://landing@landing123emhol.dfs.core.windows.net/emanuel/SE1/")

spark_df.write.mode("overwrite").saveAsTable("emanuel_db.staging.stg_elpris")
