# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1projectdatalakenew.dfs.core.windows.net",
    "")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1projectdatalakenew.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1projectdatalakenew.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# read first 10 rows from the csv file above
display(spark.read.csv("abfss://demo@f1projectdatalakenew.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# the the csv file in a dataframe
circuits_df = spark.read.csv("abfss://demo@f1projectdatalakenew.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

# read first 10 ros of the dataframe

display(circuits_df.head(10))

# COMMAND ----------

