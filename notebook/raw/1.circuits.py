# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                     StructField("circuitRef", StringType(), True), 
                     StructField("name", StringType(), True), 
                     StructField("location", StringType(), True),
                     StructField("country", StringType(), True), 
                     StructField("lat", DoubleType(), True), 
                     StructField("lng", DoubleType(), True), 
                     StructField("alt", IntegerType(), True), 
                     StructField("url", StringType(), True)])

# COMMAND ----------

df = spark.read.schema(schema).csv("file:/Workspace/Users/user-cqxogaawrjjk@oreilly-cloudlabs.com/TreinamentoDatabricksTCS/landing/circuito.csv", header='true')

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("/mnt/treinamento/raw/circuito")

# COMMAND ----------

df_final = spark.read.format("delta").load("/mnt/treinamento/raw/circuito")
df_final.display()
df_final.printSchema()
