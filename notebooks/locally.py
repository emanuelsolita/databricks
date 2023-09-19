# Databricks notebook source
import pandas as pd 
import numpy as np 

# COMMAND ----------

#columns = ["name","age"]
data = [{"SEK_per_kWh":0.82517,"EUR_per_kWh":0.07361,"EXR":11.210032,"time_start":"2023-03-08T00:00:00+01:00","time_end":"2023-03-08T01:00:00+01:00"},
{"SEK_per_kWh":0.79602,"EUR_per_kWh":0.07101,"EXR":11.210032,"time_start":"2023-03-08T01:00:00+01:00","time_end":"2023-03-08T02:00:00+01:00"},
{"SEK_per_kWh":0.78549,"EUR_per_kWh":0.07007,"EXR":11.210032,"time_start":"2023-03-08T02:00:00+01:00","time_end":"2023-03-08T03:00:00+01:00"},
{"SEK_per_kWh":0.7717,"EUR_per_kWh":0.06884,"EXR":11.210032,"time_start":"2023-03-08T03:00:00+01:00","time_end":"2023-03-08T04:00:00+01:00"},
{"SEK_per_kWh":0.76721,"EUR_per_kWh":0.06844,"EXR":11.210032,"time_start":"2023-03-08T04:00:00+01:00","time_end":"2023-03-08T05:00:00+01:00"},
{"SEK_per_kWh":0.76934,"EUR_per_kWh":0.06863,"EXR":11.210032,"time_start":"2023-03-08T05:00:00+01:00","time_end":"2023-03-08T06:00:00+01:00"}]

#df = spark.createDataFrame(data).toDF(*columns)

#(df)

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

type(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = spark.read.table("emanuel_db.staging.stg_elpris")
display(df)

