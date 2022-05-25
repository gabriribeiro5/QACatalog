#=============================================================================#
#                                                                             #
# Objetivo: Ingestão dos dados originalizados dos dumps fornecidos.            #
# Autor: Lucas Carvalho Roncoroni - NTT DATA                                  #
# Data: Out/2021                                                              #
# Versão: 3.0                                                                 #
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
from pyspark.sql.types import *

# Making sure that the order is correct
def getStructType(columName, schemaArray) :
    '''Retorna o tipo de dado da coluna consultada.'''

    for schema in schemaArray:
        if columName.lower() == schema.name.lower() :
            return schema

def standardize_column_names(df):
    '''Padroniza os nomes das colunas.'''

    return df.toDF(*[c.strip("\t") for c in df.columns])

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CRAWLER_TABLE', 'S3_SOURCE', 'S3_TARGET', 'CRAWLER_DATABASE'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

crawler_catalog = args['CRAWLER_TABLE']
bucket_source = args['S3_SOURCE']
bucket_target = args['S3_TARGET']
crawler_database = args['CRAWLER_DATABASE']

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

# Getting the data catalog
dfDB = glueContext.create_data_frame.from_catalog( 
        database = crawler_database, 
        table_name = crawler_catalog, 
        transformation_ctx = "df")

# Reading the entire RDD
output = glueContext.spark_session.read.option("delimiter",";") \
	    .option("header", True) \
	    .option("enforceSchema", True) \
        .option("encoding", "UTF-8") \
        .schema(customSchema) \
	    .csv(bucket_source)

dfDB = standardize_column_names(dfDB)
output = standardize_column_names(output)

# Changing the final Data Frame Schema
for column  in output.columns :    
    schemaField = getStructType(column, dfDB.schema)
    if column == "DataVersion":
        output = output.withColumn(column, col(column).cast("string"))
        output = output.withColumnRenamed(column, schemaField.name)
    else:
        output = output.withColumn(column, col(column).cast(schemaField.dataType))
        output = output.withColumnRenamed(column, schemaField.name)

# Writing the output Data Frame
output.write.format("parquet") \
     .option("header", True) \
     .option("spark.sql.parquet.compression.codec", "snappy") \
     .option("encoding", "UTF-8") \
     .mode("overwrite") \
     .save(bucket_target)

job.commit()