#=============================================================================#
#                                                                             #
# Objetivo: Ingerir dados originalizados dos dumps fornecidos para tabelas    #
# que contém o tipo VARIANT.                                                  #
# Autor: Renato Candido Kurosaki - NTT DATA                                   #
# Data: Out/2021                                                              #
# Versão: 3.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Extrai o schema dos dados para aplicar nos dados a serem ingeridos via dump.#
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> [JDBC_USER, JDBC_PASSWORD] = Credenciais para o sql server.              #
#>>> STRING_CONNECTION = String de conexão com o banco de dados.              #
#>>> S3_SOURCE = Bucket s3 onde se encontra o arquivo de dump.                #
#>>> S3_TARGET = Caminho do buckt raw zone onde os dados serão gravados.      #
#>>> DATABASE = Database de onde será extraído o schema.                      #
#>>> PARTITION_COL = Coluna de data que usada para particionar.               #
#>>> QUERY = Key do dicionário queries.                                       #
#                                                                             #
#=============================================================================#

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, asc,desc
from pyspark.sql.types import *
from pyspark.sql.functions import year, month, dayofmonth

# Making sure that the order is correct
def getStructType(columName, schemaArray) :
    '''Nome da Tabela a ser carregada.'''

    for schema in schemaArray :
        if columName.lower() == schema.name.lower() :
            return schema

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'JDBC_USERNAME', 'JDBC_PASSWORD', 'STRING_CONNECTION', 'S3_SOURCE', 'S3_TARGET', 'DATABASE', 'TABLE', 'PARTITION_COL', 'QUERY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

user                = args['JDBC_USERNAME']
password            = args['JDBC_PASSWORD']
bucket_source       = args['S3_SOURCE']
bucket_target       = args['S3_TARGET']
database_name       = args['DATABASE']
table_name          = args['TABLE']
conn_string         = args['STRING_CONNECTION']
partition_col       = args['PARTITION_COL']
query               = args['QUERY']

# Dict with custom queries
queries = { 
    "ticketoperations_query" : f"""
        (SELECT TOP 1
        CAST(parameter1 AS VARCHAR) as parameter1,
        CAST(parameter2 AS VARCHAR) as parameter2,
        CAST(parameter3 AS VARCHAR) as parameter3,
        CAST(parameter4 AS VARCHAR) as parameter4,
        CAST(parameter5 AS VARCHAR) as parameter5,
        agentsid,
        affectedrecords,
        ticketoperationid,
        ticketcid,
        operationdate,
        resultid,
        methodname,
        dataversion,
        spid,
        resultcode
        FROM {table_name}
        )
        as temp
        """
        }

# List with schema definition
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


dfDB = spark.read \
        .format("jdbc") \
        .option("url", f"{conn_string};database={database_name}") \
        .option("dbtable", queries[query]) \
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

# Changing the final Data Frame Schema
for column  in output.columns :
    schemaField = getStructType(column, dfDB.schema)
    if column == "DataVersion":
        output = output.withColumn(column, col(column).cast("string"))
        output = output.withColumnRenamed(column, schemaField.name)
    else:
        output = output.withColumn(column, col(column).cast(schemaField.dataType))
        output = output.withColumnRenamed(column, schemaField.name)

if partition_col == "n/a":

    # Writing the output Data Frame
    output.write.format("parquet") \
         .option("header", True) \
         .option("spark.sql.parquet.compression.codec", "snappy") \
         .option("encoding", "UTF-8") \
         .mode("overwrite") \
         .save(bucket_target)

else:
    output = output.withColumn("year", year(partition_col))\
                    .withColumn("month", month(partition_col))\
                    .withColumn("day", dayofmonth(partition_col))\
                    .repartition("year", "month", "day")

    # Writing the output Data Frame
    output.write.format("parquet") \
         .option("header", True) \
         .option("spark.sql.parquet.compression.codec", "snappy") \
         .option("encoding", "UTF-8") \
         .partitionBy("year", "month", "day")\
         .mode("overwrite") \
         .save(bucket_target)

job.commit()