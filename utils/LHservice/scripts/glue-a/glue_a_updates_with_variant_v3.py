#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que conecta no SQL SERVER e atuliza o Parquet no   #
# S3, total ou parcial que contém o tipo VARIANT, via job Glue (glue-a).	  #
# Autor: Moyses Santos - NTT DATA                                             #
# Data: Dez/2021                                                              #
# Alteração: Edinor Cunha Junior 											  #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script						  #
# Versão: 3.0                                                                #
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
#>>> QUERY = Key do dicionário queries.                                       #
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


# Inicialização do Spark
sc 			= SparkContext()
glueContext = GlueContext(sc)
spark 		= glueContext.spark_session
job 		= Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'ID_COL', 'TRANSIENT_PATH', 'IS_FULL_UPDATE',
                                    'STRING_CONNECTION', 'JDBC_USERNAME', 'JDBC_PASSWORD','QUERY', 
									'LOG_PATH', 'PROCESS_TYPE'])

job_name		  = args['JOB_NAME']
job_run_id		  = args['JOB_RUN_ID']
log_path		  = args['LOG_PATH']
process_type	  = args['PROCESS_TYPE']
bucket_path 	  = args['PATH_S3']
database_name 	  = args['DATABASE']
table_name 		  = args['TABLE_NAME']
reference_col 	  = args['REFERENCE_COL']
id_col 			  = args['ID_COL']
transient_path    = args['TRANSIENT_PATH']
full_update 	  = args['IS_FULL_UPDATE']
query             = args['QUERY']

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


def create_temp_updates(df, database_name, table_name, reference_col, transient_path):
	'''Consulta os dados atualizados do SQL SERVER e grava os dados na transient zone.'''

	df = df.withColumn("year", year(reference_col))\
			.withColumn("month", lpad(month(reference_col), 2, "0"))\
			.withColumn("day", lpad(dayofmonth(reference_col), 2, "0"))

	df.write.format('parquet')\
			.mode("append")\
			.partitionBy("year", "month", "day")\
			.save(transient_path)

#######################################################################################################################
# Script principal


# Parametros principais
MAX_REGISTERS_REPARTITION = 250000
grouped_by = {id_col}


#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


# Dicionario com as queries customizadas
queries = { 
	"update_campaignoptions_query" : f"""
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

# Lendo os dados da tabela original
updates = spark.read.format("jdbc")\
	.option("url", f"{string_connection};database={database_name}")\
	.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
	.option("user", username)\
	.option("password", password)\
	.option("dbtable", queries[query]) \
	.load()


# Transforma as colunas em lower case
updates = lower_column_names(updates)
create_temp_updates(updates, database_name, table_name, reference_col, transient_path)


old_df = spark.read.parquet(bucket_path).cache()
old_df = lower_column_names(old_df)


# Reparticiona a tabela
total_registers = old_df.count()
num_repartitions = ceil(total_registers / MAX_REGISTERS_REPARTITION)


# Join entre as tabelas
new_df = old_df.unionByName(updates)
new_df = new_df.groupBy(*grouped_by)\
				.agg(*[F.last(c).alias(c) for c in set(new_df.columns)-grouped_by])\
				.orderBy(id_col)


# Escreve os dados no S3
new_df = new_df.repartition(num_repartitions)
new_df.write.format('parquet')\
			.mode("overwrite")\
			.option("maxRecordsPerFile", MAX_REGISTERS_REPARTITION)\
			.save(bucket_path)


# Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = new_df.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_id = new_df.agg({id_col: "max"}).collect()[0][0]
max_creation_date = new_df.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Finaliza script
print(f"Tempo de execução: {elapsed_time}")
job.commit()