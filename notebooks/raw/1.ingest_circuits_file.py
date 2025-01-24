# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

user_path

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{user_path}TreinamentoDatabricksTCS/landing/circuito.csv")

# COMMAND ----------

circuits_df.write.mode("overwrite").format("delta").save("/mnt/formula1dl/raw/circuits")
