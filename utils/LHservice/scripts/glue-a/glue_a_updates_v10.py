#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que conecta no SQL SERVER e atuliza o Parquet no   #
# S3, total ou parcial, via job Glue (glue-a) 								  #
# Autor: Renato Candido Kurosaki - NTT DATA 								  #
# Data: Set/2021                                                              #
# Alteração: Thiago Nascimento Nogueira										  #
# Data de Modificação: 24/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script						  #
# Versão: 10.0                                                                #
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

# Inicialização do S3
s3 = boto3.resource('s3')

# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'ID_COL', 'TRANSIENT_PATH', 'IS_FULL_UPDATE', 
                                    'PROCESS_TYPE','STRING_CONNECTION', 'JDBC_USERNAME', 
                                    'JDBC_PASSWORD', 'LOG_PATH'])

job_name 		  = args['JOB_NAME']
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

#Parâmetros principais
MAX_REGISTERS_REPARTITION = 250000
grouped_by = {id_col}


#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


#Obtém último valor de atualização do processo anterior
lastmodificationdate = get_last_timestamp()
print("lastmodificationdate inicial: ", lastmodificationdate)


# Carrega registros atualizados do PIC
updates_origem = spark.read.format("jdbc")\
	.option("url", f"{string_connection};database={database_name}")\
	.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
	.option("user", username)\
	.option("password", password)\
	.option("dbtable", f"(SELECT * FROM {table_name} WHERE '{reference_col}' >= '{lastmodificationdate}') as temp")\
	.load()


# Ajusta Nomes da coluna para lowercase
updates_origem = lower_column_names(updates_origem)
for column  in updates_origem.columns:
	if column == "dataversion":
		updates_origem = updates_origem.withColumn(column, col(column).cast("string"))

updates_origem = updates_origem.cache()
count_udates_origem  = updates_origem.count()
print(f"Contagem updates origem: {count_udates_origem}")


#Carrega Tabela completa do Datalake
datalake_table = spark.read.parquet(bucket_path)
datalake_table = lower_column_names(datalake_table)
for column  in datalake_table.columns:
	if column == "dataversion":
		datalake_table = datalake_table.withColumn(column, col(column).cast("string"))
        
count_datalake_table  = datalake_table.count()
print(f"Contagem Inicial datalake: {count_datalake_table}")


#Seleciona os registros do Datalake que serão atualizados (Remove da origem os novos registros)
datalake_id = datalake_table.select((id_col))
updates_datalake = updates_origem.join(datalake_id, [id_col], 'inner')
count_updates_datalake  = updates_datalake.count()
print(f"Contagem updates datalake: {count_updates_datalake}")


#Seleciona os registros do Datalake que não serão atualizados
updates_id = updates_datalake.select((id_col))
# not_updated = datalake_table.join(F.broadcast(updates_id), [id_col], 'leftanti')
not_updated = datalake_table.join(updates_id, [id_col], 'leftanti')


# Faz union dos dataframes gerando a tabela final atualizada
final_table = not_updated.unionByName(updates_datalake).cache()
count_final_table = final_table.count()
print(f"Contagem Final datalake: {count_final_table}")


#Faz contagem da origem agregada por mês e ano baseada na data de update
print(f"Contagem agrupada origem:")
data_count_grouped2 = updates_origem\
        .withColumn('up_year', F.year(F.col("lastmodificationdate")))\
        .withColumn('up_month', F.month(F.col("lastmodificationdate")))

data_count_grouped2 = data_count_grouped2\
        .groupby('up_year', 'up_month')\
        .agg(F.count(F.lit(1)).alias("counts"))\
        .sort(F.col("up_year").desc(),F.col("up_month").desc())\
        .show(truncate=False)


#Faz contagem do destino agregada por mês e ano baseada na data de update
print(f"Contagem agrupada destino:")
data_count_grouped = final_table\
        .withColumn('up_year', F.year(F.col("lastmodificationdate")))\
        .withColumn('up_month', F.month(F.col("lastmodificationdate")))

data_count_grouped = data_count_grouped\
        .groupby('up_year', 'up_month')\
        .agg(F.count(F.lit(1)).alias("counts"))\
        .sort(F.col("up_year").desc(),F.col("up_month").desc())\
        .show(truncate=False)


## Reparticiona e salva tabela atualizada no S3
print(f"Salvando dados no S3.")
num_repartitions = ceil(count_final_table / MAX_REGISTERS_REPARTITION)
final_table = final_table.repartition(num_repartitions)	

final_table.write.format('parquet')\
			.mode("overwrite")\
			.option("maxRecordsPerFile", MAX_REGISTERS_REPARTITION)\
			.save(bucket_path)


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = final_table.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_id = final_table.agg({id_col: "max"}).collect()[0][0]
max_creation_date = final_table.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, count_updates_datalake, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


#Finaliza script
print(f"Tempo de execução: {elapsed_time}")
job.commit()