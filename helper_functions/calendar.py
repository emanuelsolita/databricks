# Databricks notebook source
import pandas as pd
from pandas.tseries.offsets import MonthEnd

# COMMAND ----------

def create_date_table2(start='2000-01-01', end='2099-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Day_Name"] = df.Date.dt.day_name()
    df["Day"] = df.Date.dt.day
    df["Week"] = df.Date.dt.isocalendar().week
    df["Month_Name"] = df.Date.dt.month_name()
    df["Month"] = df.Date.dt.month
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df["Year_half"] = (df.Quarter + 1) // 2
    df['FY'] = df.Date.dt.to_period('Q-AUG').dt.qyear
    df['EndOfMonth'] = pd.to_datetime(df['Date'], format="%Y%m") + MonthEnd(0)

    return df

df = create_date_table2() 
print(df.tail())
print(len(df))

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.show()

# COMMAND ----------

spark_df.write.saveAsTable("emanuel_db.staging.calendar")

# COMMAND ----------


