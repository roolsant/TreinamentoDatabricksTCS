# Databricks notebook source
### Importando o módulo para definir o schema

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

# COMMAND ----------

### Listando os arquivos no diretorio

User = 'user-fwuoqkflpbbz@oreilly-cloudlabs.com'
path = f"file:/Workspace/Users/{User}/TreinamentoDatabricksTCS/landing"

display(dbutils.fs.ls(path))

# COMMAND ----------

# Definindo o esquema (schema) para o DataFrame de circuitos,
# especificando os nomes das colunas, tipos de dados e se os campos são obrigatórios.

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
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

# Lendo o arquivo CSV "circuito.csv" no caminho especificado, aplicando o schema previamente definido
# e habilitando a opção para considerar a primeira linha como cabeçalho

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{path}/circuito.csv")

# COMMAND ----------

# Salvando o DataFrame circuits_df no formato Delta na camada raw do Data Lake, 
# com o modo de gravação configurado para sobrescrever os dados existentes.

circuits_df.write \
    .mode("overwrite")\
    .format("delta")\
    .save("/mnt/formula1dl/raw/circuits")
