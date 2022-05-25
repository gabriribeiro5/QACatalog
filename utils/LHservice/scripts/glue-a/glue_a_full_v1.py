#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que conecta no SQL SERVER e atualiza o Parquet no  #
# S3, total, via job Glue (glue-a)			 								  #
# Autor: Edinor Santos Junior - NTT DATA 								  	  #
# Data: 25/03/2022															  #
# Versão: 1.0  																  #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Faz o parse dos argumentos e determina qual tipo de update será realizado.  #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> PATH_S3 = Caminho do S3 para os aquivos consultados e inseridos.		  #
#>>> DATABASE = Database de onde os dados serão extraídos. 					  #
#>>> TABLE_NAME = Table do Database.  										  #
#>>> REFERENCE_COL = Nome da coluna de data para consultar e extrair os dados.#
#>>> ID_COL = Coluna de id do Parquet. 										  #
#>>> TRANSIENT_PATH = Caminho S3 para a transient zone.						  #
#>>> IS_FULL_UPDATE = O fator que determina qual update será realizado.		  #
#>>> STRING_CONNECTION = String de conexão com o banco de dados. 			  #
#>>> [JDBC_USER, JDBC_PASSWORD] = Credenciais para o sql server.			  #
#                                                                             #
#=============================================================================#

from operator import index
import sys
from datetime import datetime, timedelta
from math import ceil
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


# Inicilização do Spark
sc 			= SparkContext()
glueContext = GlueContext(sc)
spark 		= glueContext.spark_session
job 		= Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Inicialização do S3
s3 = boto3.resource('s3')

# Parâmetros Dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'ID_COL', 'TRANSIENT_PATH', 'IS_FULL_UPDATE', 'PROCESS_TYPE',
                                    'STRING_CONNECTION', 'JDBC_USERNAME', 'JDBC_PASSWORD', 'LOG_PATH'])

job_name		  = args['JOB_NAME']
job_run_id        = args['JOB_RUN_ID']
bucket_path 	  = args['PATH_S3']
database_name 	  = args['DATABASE']
table_name 		  = args['TABLE_NAME']
reference_col 	  = args['REFERENCE_COL']
id_col 			  = args['ID_COL']
transient_path    = args['TRANSIENT_PATH']
full_update 	  = args['IS_FULL_UPDATE']
process_type	  = args['PROCESS_TYPE']
log_path		  = args['LOG_PATH']

string_connection = args['STRING_CONNECTION']
username 		  = args['JDBC_USERNAME']
password 		  = args['JDBC_PASSWORD']

#######################################################################################################################
#Definição de funções


def get_last_timestamp():
    try:
        df = pd.read_csv('{}{}.csv'.format(log_path, args['JOB_NAME']))
        max_last_modification = df.loc[df['status'] == 'SUCCESS', 'lastmodificationdate'].max().strftime('%Y-%m-%d')
        return max_last_modification

    except:
        max_last_modification = datetime(1970,1,1).strftime('%Y-%m-%d')
        return max_last_modification
	

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


#######################################################################################################################
# Script principal


# Parâmetros principais
MAX_REGISTERS_REPARTITION = 250000


# Data hora do inicio do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


# Carrega registros atualizados do PIC
df = spark.read.format("jdbc")\
	.option("url", f"{string_connection};database={database_name}")\
	.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
	.option("user", username)\
	.option("password", password)\
	.option("dbtable", table_name)\
	.load()


# Alterando o datatype da coluna DataVersion para leitura no Redshift
for column  in df.columns:
	if column == "dataversion":
		df = df.withColumn(column, col(column).cast("string"))


# Ajusta nomes da coluna para lowercase
df = lower_column_names(df)


# Contagem total dos registros
total_registers = df.count()
print(f'Contagem total de registros: {total_registers}')


# Reparticiona e salva tabela atualizada no S3
num_repartitions = ceil(total_registers / MAX_REGISTERS_REPARTITION)
df = df.repartition(num_repartitions)		

df.write.format('parquet')\
		.mode("overwrite")\
		.option("maxRecordsPerFile", MAX_REGISTERS_REPARTITION)\
		.save(bucket_path)


# Data Hora final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG no processo
status = 'SUCCESS'
last_modification_date = None
max_id = df.agg({id_col: "max"}).collect()[0][0]
max_creation_date = df.agg({'creationdate': "max"}).collect()[0][0]
n_rows = total_registers
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, n_rows, max_id, max_creation_date, last_modification_date, start_time, end_time, elapsed_time)


# Finaliza o script
print(f'Tempo de execução: {elapsed_time}')
job.commit()
