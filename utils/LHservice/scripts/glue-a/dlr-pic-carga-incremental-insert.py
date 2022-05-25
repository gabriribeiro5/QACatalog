#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark para ingestão incremental via job Glue (glue-a)    #
# Autor: Renato Candido Kurosaki - NTT DATA                                   #
# Data: Set/2021                                                              #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Determina tipo da operação, consultando no S3 a última data ou id, carrega  #
# os registros com data maior, ou id superiores e grava em Parquet.   		  #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> PATH_S3 = Caminho do S3 dos aquivos que serão consultados e inseridos.   #
#>>> DATABASE = Database do catálago do Glue.                                 #
#>>> TABLE_NAME = Table do catálago do Glue.                                  #
#>>> REFERENCE_COL = Nome da coluna de referência para atualizar.             #
#>>> TYPE_REF_COL = Tipo do dado  da coluna de referência para atualizar.     #
#                   (timestamp ou id)                                         #
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

def query_from_id(reference_col: str, bucket_path: str):
	'''Consulta o último id dos registro do Data lake.'''

	df = spark.read.format('parquet').load(bucket_path)

	# get last id number as reference
	last_id = df.agg({reference_col.lower(): "max"}).collect()[0][0]

	return last_id

def query_from_date(reference_col: str, bucket_path: str) -> str:
	'''Consulta a última data dos registro do Data lake.'''

	df = spark.read.format('parquet').load(bucket_path)

	# get last date/timestamp as reference
	last_date = df.orderBy(col(reference_col.lower()).desc()).collect()[0][reference_col.lower()]

	return last_date

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'TYPE_REF_COL', 'BATCH_SIZE'])

sc 			= SparkContext()
glueContext = GlueContext(sc)
spark 		= glueContext.spark_session
job 		= Job(glueContext)

bucket_path 	  = args['PATH_S3']
database_name 	  = args['DATABASE']
table_name 		  = args['TABLE_NAME']
reference_col 	  = args['REFERENCE_COL']
type_ref_col 	  = args['TYPE_REF_COL']

reference_value = ''

if type_ref_col != 'date':
	reference_value = query_from_id(reference_col, bucket_path)
else:
    reference_value = query_from_date(reference_col, bucket_path)
    
temp_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)
temp_df = temp_df.toDF()
incrementals = temp_df.where( temp_df[reference_col.lower()] > reference_value )
	

# lowercase all column names
incrementals = incrementals.toDF(*[c.lower() for c in incrementals.columns])

incrementals.write.format("parquet").mode("append").save(bucket_path)

job.commit()