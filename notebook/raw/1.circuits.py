# Databricks notebook source
### Importando bibliotecas para manipulação de dados
from pyspark.sql.types import *

# COMMAND ----------

### Definindo o schema para a tabela de dados
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

### Lendo os dados da tabela

# Definição do usuário
User = 'user-rsyscppecwxv@oreilly-cloudlabs.com'

# Construção do caminho do arquivo com interpolação usando f-string
df = spark.read.schema(schema).csv(
    f"file:/Workspace/Users/{User}/TreinamentoDatabricksTCS/landing/circuito.csv",
    header='true'
)

# COMMAND ----------

### Criando um datafrme com os dados
df.write.mode("overwrite")\
        .format("delta")\
        .save("/mnt/treinamento/raw/circuito")

# COMMAND ----------

### Leitura do arquivo no formato delta
df_final = spark.read\
    .format("delta")\
    .load("/mnt/treinamento/raw/circuito")

### Exibicao dos dados no dataframe
df_final.display()

### Imprimindo na tela o schema
df_final.printSchema()
