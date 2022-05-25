#=============================================================================#
# 																			  #
# Objetivo: Script PySpark que extrai dados por batches do SQL SERVER 		  #
# Autor: Renato Candido Kurosaki - NTT DATA 						 	  	  #
# Data: Set/2021 															  #
# Alteração: Edinor Cunha Junior  											  #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script						  #
# Versão: 4.0                                                                 #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Determina a operação, consultando no S3 a última data ou id e pega do 	  #
# banco os registros com data maior, ou id superiores e grava em Parquet.     #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> PATH_S3 = Caminho do S3 dos aquivos que serão consultados e inseridos.   #
#>>> DATABASE = Database de onde os dados serão extraídos.					  #
#>>> TABLE_NAME = Table do Database. 										  #
#>>> REFERENCE_COL = Coluna de data ou id para consultar e extrair os dados.  #
#>>> TYPE_REF_COL = Identificador da coluna de referência, se é data ou id.	  #
#>>> BATCH_SIZE = Número de tuplas consultadas por interação. 				  #
#>>> STRING_CONNECTION = String de conexão com o banco de dados. 			  #
#>>> [JDBC_USER, JDBC_PASSWORD] = Credenciais para o sql server.			  #
#                                                                             #
#=============================================================================#


import csv
from datetime import datetime, timedelta
from math import ceil
import sys
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

import boto3
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, lpad, col


# Inicialização do S3
s3 = boto3.resource('s3')


# Inicialização do Spark
sc 			= SparkContext()
glueContext = GlueContext(sc)
spark 		= glueContext.spark_session
job 		= Job(glueContext)


# Parâmetros Dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'TYPE_REF_COL', 'BATCH_SIZE',\
                                    'STRING_CONNECTION', 'JDBC_USERNAME', 'JDBC_PASSWORD',
									'LOG_PATH', 'PROCESS_TYPE'])


job_name          = args['JOB_NAME']
job_run_id        = args['JOB_RUN_ID']
log_path		  = args['LOG_PATH']
process_type	  = args['PROCESS_TYPE']
bucket_path 	  = args['PATH_S3']
database_name 	  = args['DATABASE']
table_name 		  = args['TABLE_NAME']
reference_col 	  = args['REFERENCE_COL']
type_ref_col 	  = args['TYPE_REF_COL']
batch_size		  = args['BATCH_SIZE']

string_connection = args['STRING_CONNECTION']
username 		  = args['JDBC_USERNAME']
password 		  = args['JDBC_PASSWORD']


#######################################################################################################################
#Definição de funções


def save_log(proccess_type, job_run_id, status, n_rows, max_id, max_creation_date, last_modification_date, start_time, end_time, elapsed_time):
    log_data = {
        'proccess_type': proccess_type, 'job_id': job_run_id, 'status': status, 'total_rows': n_rows, 
        'max_id': max_id, 'max_creation_date': max_creation_date, 
        'last_modification_date': last_modification_date, 
        'start_time': start_time, 'end_time': end_time, 'elapsed_time': elapsed_time
        }
    print(json.dumps(log_data, indent=4, sort_keys=True))
    try: 
        df1 = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}.csv'.format(log_path, job_name), index=False)
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}.csv'.format(log_path, job_name), index=False)


def lower_column_names(df):
	'''Padroniza em minúsculo os nomes das colunas.'''	
	return df.toDF(*[c.lower() for c in df.columns])


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


#######################################################################################################################
# Script principal


#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


if type_ref_col != 'date':
	reference_value = query_from_id(reference_col, bucket_path)
else:
    reference_value = query_from_date(reference_col, bucket_path)

incrementals = spark.read.format("jdbc")\
		.option("url", f"{string_connection};database={database_name}")\
		.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
		.option("user", username)\
		.option("password", password)\
		.option("dbtable", f"(SELECT * FROM {table_name} WHERE {reference_col} > {reference_value}) as temp")\
		.option("fetchSize", int(batch_size))\
		.load()

incrementals = incrementals.toDF(*[c.lower() for c in incrementals.columns])

incrementals.write.format("parquet").mode("append").save(bucket_path)

count_insert_datalake = incrementals.count()

#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = incrementals.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_id = incrementals.agg({reference_col: "max"}).collect()[0][0]
max_creation_date = incrementals.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, count_insert_datalake, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


#Finaliza script
print(f"Tempo de execução: {elapsed_time}")
job.commit()