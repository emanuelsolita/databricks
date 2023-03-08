# Databricks notebook source

import pandas as pd 
import numpy as np 

# COMMAND ----------

df = pd.read_json("./data/SE1/eldata_2023-03-07.json")

print(df.head())
print(df.columns)