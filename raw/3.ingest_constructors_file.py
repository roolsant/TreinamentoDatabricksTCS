# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{user_path}/TreinamentoDatabricks/landing/constructors.json")

# COMMAND ----------

constructor_df.write.mode("overwrite").format("delta").save("/mnt/formula1dl/raw/constructors")
