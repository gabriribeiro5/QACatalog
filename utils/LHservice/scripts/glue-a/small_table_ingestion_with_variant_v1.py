#=============================================================================#
#                                                                             #
# Objetivo: Ingerir dados para tabelas que contém o tipo VARIANT.             #
# Autor: Moyses Santos - NTT DATA                                             #
# Data: Out/2021                                                              #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Extrai o schema dos dados para aplicar nos dados a serem ingeridos sem dump.#
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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'JDBC_USERNAME', 'JDBC_PASSWORD', 'STRING_CONNECTION', 'S3_TARGET', 'DATABASE', 'TABLE', 'QUERY'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

user                = args['JDBC_USERNAME']
password            = args['JDBC_PASSWORD']
bucket_target       = args['S3_TARGET']
database_name       = args['DATABASE']
table_name          = args['TABLE']
conn_string         = args['STRING_CONNECTION']
query               = args['QUERY']

# Dict with custom queries
queries = { 
    "campaignoptions_query" : f"""
        (SELECT 
        creationdate,
        campaignid,
        campaignoptionid,
        CAST(propertyvalue AS  VARCHAR) as propertyvalue,
        campaignpropertyid,
        dataversion
        FROM {table_name}
        )
        as temp
        """
        }

dfDB = spark.read \
        .format("jdbc") \
        .option("url", f"{conn_string};database={database_name}") \
        .option("dbtable", queries[query]) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("user", user) \
        .option("password", password)\
        .load()

output = glueContext.spark_session.read.option("delimiter",";") \
        .option("header", True) \
        .option("enforceSchema", True) \
        .option("encoding", "UTF-8") \
        .schema(customSchema) \
        .csv(bucket_source)

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