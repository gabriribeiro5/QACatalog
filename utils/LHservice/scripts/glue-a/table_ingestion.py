#=============================================================================#
#                                                                             #
# Objetivo: Ingetão dos dados originalizados dos dumps fornecidos             #
# Autor: Lucas Carvalho Roncoroni - NTT DATA                                  #
# Data: Out/2021                                                              #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Do catálogo, extrai o schema para aplicar nos dados ingeridos via dump.     #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> CRAWLER_TABLE = Tabela do catálago de dados.                             #
#>>> S3_SOURCE = Bucket s3 onde se encontra o arquivo de dump.                #
#>>> S3_TARGET = Caminho do buckt raw zone onde os dados serão gravados.      #
#>>> CRAWLER_DATABASE = Database do catálago de dados.                        #
#                                                                             #
#=============================================================================#

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, asc,desc
import boto3


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CRAWLER_TABLE', 'S3_SOURCE', 'S3_TARGET', 'CRAWLER_DATABASE'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


crawler_catalog = args['CRAWLER_TABLE']
bucket_source = args['S3_SOURCE']
bucket_target = args['S3_TARGET']
crawler_database = args['CRAWLER_DATABASE']

# This ensures that we get the right schema from the PIC Data Catalog
ingestion = glueContext.create_data_frame.from_catalog( 
    database = crawler_database, 
    table_name = crawler_catalog,
    additional_options =  {"connectionType": "s3", "paths": bucket_source})

ingestion = ingestion.toDF(*[c.lower() for c in ingestion.columns])

ingestion.write.format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("overwrite") \
        .save(bucket_target)
job.commit()