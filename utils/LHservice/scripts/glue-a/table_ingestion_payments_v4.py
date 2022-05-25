#=============================================================================#
#                                                                             #
# Objetivo : Ingerir dados oriundos do dump do Datamart Payments.             #
# Autor: Edinor Cunha Júnior - NTT DATA                                       #
# Data : Dez/2021                                                             #
# Data de Modificação: 18/Mar/2022                                            #
# Versão: 4.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Extrai o schema dos dados para aplicar nos dados a serem ingeridos via dump,#
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> [JDBC_USERNAME, JDBC_PASSWORD] = Credenciais para o sql server.     	  #
#>>> STRING_CONNECTION = String de conexão com o banco de dados. 			  #
#>>> S3_SOURCE = Bucket s3 onde se encontra o arquivo de dump.                #
#>>> S3_TARGET = Caminho do bucket raw zone onde os dados serão gravados.     #
#>>> DATABASE = Database de onde os dados serão extraídos. 					  #
#>>> TABLE = Table do catálago do SQL SERVER.                                 #
#>>> PARTITION_COL = Coluna de data que usada para particionar.               #
#                                                                             #
#=============================================================================#

import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col, asc,desc
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth, date_format, to_date, lpad, lit, when

# Inicialização do Spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'JDBC_USERNAME', 'JDBC_PASSWORD', 'STRING_CONNECTION', 'S3_SOURCE', 'S3_TARGET', 'DATABASE', 'TABLE', 'PARTITION_COL'])

user                = args['JDBC_USERNAME']
password            = args['JDBC_PASSWORD']
bucket_source       = args['S3_SOURCE']
bucket_target       = args['S3_TARGET']
database_name       = args['DATABASE']
table_name          = args['TABLE']
conn_string         = args['STRING_CONNECTION']
partition_col       = args['PARTITION_COL']


#######################################################################################################################
#Definição de funções

def getStructType(columName, schemaArray) :
    '''Retorna o tipo de dado da coluna consultada.'''

    for schema in schemaArray :
        if columName.lower() == schema.name.lower() :
            return schema

def standardize_column_names(df):
    '''Padroniza os nomes das colunas.'''

    return df.toDF(*[c.strip("\t") for c in df.columns])

def lower_column_names(df):
    '''Padroniza em minúsculo os nomes das colunas.'''  
    return df.toDF(*[c.lower() for c in df.columns])

#######################################################################################################################
# Script principal

# Lista para receber o schema
customSchema =  []

# RDD that takes the title of file
rddBucketSource = sc.textFile(bucket_source)

# Taking the header out of file
rddBucketSource = rddBucketSource.map(lambda x : x.split(';'))
header = rddBucketSource.first()

# Making a custom schema with file header
for column in header : 
    customSchema.append(StructField(column, StringType(), True))

# Tranforming into the schema type
customSchema = StructType(customSchema)

# Getting data to get the schema from sql server
dfDB = spark.read \
        .format("jdbc") \
        .option("url", f"{conn_string};database={database_name}") \
        .option("dbtable", f"(SELECT TOP 1 * FROM {table_name}) as temp") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("user", user) \
        .option("password", password)\
        .load()

# Reading the entire RDD
output = glueContext.spark_session.read.option("delimiter",";") \
        .option("header", True) \
        .option("enforceSchema", True) \
        .option("encoding", "UTF-8") \
        .schema(customSchema) \
        .csv(bucket_source)

# Normalizando as colunas dos dataframes
dfDB = standardize_column_names(dfDB)
dfDB = lower_column_names(dfDB)

output = standardize_column_names(output)
output = lower_column_names(output)

# Changing the final Data Frame Schema
for column  in output.columns :
    schemaField = getStructType(column, dfDB.schema)
    if column == "assets":
        output = output.withColumn(column, col(column).cast("integer"))
        output = output.withColumnRenamed(column, schemaField.name)
    elif column == "refdate":
        output = output.withColumn(column, col(column).cast("int"))
        output = output.withColumnRenamed(column, schemaField.name)
    else:
        output = output.withColumn(column, col(column).cast(schemaField.dataType))
        output = output.withColumnRenamed(column, schemaField.name)

# Tratando os dados de pagamento que não tem agência
output = output.fillna(-1, subset=["agencyid"])
output = output.fillna('Não especificado', subset=["paymentmethod"])

# Coleta por cash novo ou não
output = output.withColumn("cashtype", when(col("installmentnumber") == 1, \
        lit('cash_novo')).otherwise(lit('colchao')))

output = output.withColumn("lastmodificationdate", lit(None).cast('timestamp'))

# Escrevendo o Data Frame final
if partition_col == "n/a":
    output.write.format("parquet") \
         .option("header", True) \
         .option("spark.sql.parquet.compression.codec", "snappy") \
         .option("encoding", "UTF-8") \
         .mode("overwrite") \
         .save(bucket_target)
else:
    output = output.withColumn("year", year(partition_col).cast("int"))\
                    .withColumn("month", lpad(month(partition_col), 2, "0"))\
                    .withColumn("day", lpad(dayofmonth(partition_col), 2, "0"))\
                    .repartition("year", "month", "day")

    output.write.format("parquet") \
         .option("header", True) \
         .option("spark.sql.parquet.compression.codec", "snappy") \
         .option("encoding", "UTF-8") \
         .partitionBy("year", "month", "day")\
         .mode("overwrite") \
         .save(bucket_target)

job.commit()