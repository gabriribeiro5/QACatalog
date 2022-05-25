#=============================================================================#
#                                                                             #
# Objetivo : Ingerir os dados na área Silver                                  #
# Autor : Edinor Santos da Cunha Júnior - NTT DATA                            #
# Data : Mai/2022                                                             #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Script PySpark que aplica as mudanças nos arquivos das coligadas            #
# da área Bronze e ingerindo o resultando na área Silver                      #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> CRAWLER_TABLE = Tabela do catálago de dados.                             #
#>>> S3_TARGET = Caminho do buckt raw zone onde os dados serão gravados.      #
#>>> CRAWLER_DATABASE = Database do catálago de dados.                        #
#                                                                             #
#=============================================================================#

import sys
from datetime import date
from math import ceil

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

import boto3
import json


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE', 'S3_TARGET', 'S3_JSON', 'JSON_FILE','FILE_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


bucket_source = args['S3_SOURCE']
bucket_target = args['S3_TARGET']
bucket_json = args['S3_JSON']
file_name = args['FILE_NAME']
json_file = args['JSON_FILE']


# Parametros principais
s3 = boto3.resource('s3')

############### Definição de funções #######################
def load_json(bucket_name, bucket_key):
    content_object = s3.Object(bucket_name, bucket_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_file = json.loads(file_content)
    return json_file

def load_csv(path, col, spark):
    df = spark.read.option("delimiter", "|") \
                    .csv(path)
    df = df.toDF(*col)
    return df  

def getStructType(columName, schemaArray) :
    '''Retorna o tipo de dado da coluna consultada.'''

    for schema in schemaArray:
        if columName.lower() == schema.name.lower() :
            return schema

def standardize_column_names(df):
    '''Padroniza os nomes das colunas.'''

    return df.toDF(*[c.strip("\t") for c in df.columns])


def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)

    return df
################# inicio do script principal ###############
# Parametros principais
MAX_REGISTERS_REPARTITION = 250000

try:
    max_id = spark.read.format("parquet") \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("spark.sql.parquet.compression.codec", "snappy") \
            .option("encoding", "UTF-8") \
            .load(bucket_target) \
            .agg({"identitynumber": "max"}).collect()[0][0]
except:
    max_id = 0

# Carrega o json
json_schema = load_json(bucket_json, json_file)
columns = [x["name"] for x in json_schema["columns"]]

# Carrega o arquivo csv para validação
df = load_csv(bucket_source, columns, spark)

output = standardize_column_names(df)

output.printSchema()

# Altera o dataframe final
output = output.withColumn("mailtypedescription", 
                    F.when(output["mailtype"] == "1", "PESSOAL")\
                    .when(output["mailtype"] == "2", "COMERCIAL")\
                    .when(output["mailtype"] == "3", "REFERENCIA")\
                    .when(output["mailtype"] == "4", "OUTROS"))  

output = output.withColumn("mailcategorydescription", 
                    F.when(output["mailcategory"] == "1", "PREFERENCIAL")\
                    .when(output["mailcategory"] == "2", "NAO_PREFERENCIAL")\
                    .when(output["mailcategory"] == "3", "NAO_INFORMADO"))


output = output.select("identitynumber", "mail", "mailtype", "mailtypedescription", \
    "mailcategory", "mailcategorydescription", "mailstatus", "notes", "sourcemail", \
    "lastmodificationdate", "processingdate", "sourcedirection", "sourcename")

output.printSchema()

num_repartitions = ceil(output.count() / MAX_REGISTERS_REPARTITION)
output = output.repartition(num_repartitions).cache()

output.write.format("parquet") \
    .option("header", True) \
    .option("spark.sql.parquet.compression.codec", "snappy") \
    .option("encoding", "UTF-8") \
    .mode("append") \
    .save(bucket_target)

job.commit()
