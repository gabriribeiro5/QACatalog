#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que conecta no SQL SERVER e atuliza o Parquet no   #
# S3, total ou parcial, via job Glue (glue-a) 								  #
# Autor: Renato Candido Kurosaki - NTT DATA 								  #
# Data: Set/2021 															  #
# Versão: 1.0																  #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Faz o parse dos argumentos e determina qual tipo de update será realizado.  #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> PATH_S3 = Caminho do S3 dos aquivos que serão consultados e inseridos.   #
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

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from math import ceil
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, dayofmonth

def get_yesterday_timestamp():
	'''Retorna um timestamp do dia anterior'''

	return datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d %H:%M:%S')

def update_table(glueContext, spark, bucket_path, database_name, table_name, reference_col, id_col, transient_path):
	'''
	Define os parâmetros de atualização, pega dos dados atualizados do SQL SERVER, grava os dados na transient zone, aplica os updates nos dados da raw zone e grava no bucket. 		
	'''

	MAX_REGISTERS_REPARTITION = 250000

	grouped_by = {id_col}

	temp_df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)
	temp_df = temp_df.toDF()
	yesterday_timestamp = get_yesterday_timestamp()

	updates = temp_df.where(f"{reference_col} > '{yesterday_timestamp}'")

	create_temp_updates(updates, database_name, table_name, reference_col, transient_path)

	old_df = spark.read.parquet(bucket_path).cache()
	temp_var = old_df.take(1)

	total_registers = old_df.count()
	num_repartitions = ceil(total_registers / MAX_REGISTERS_REPARTITION)

	new_df = old_df.unionByName(updates)
	new_df = new_df.groupBy(*grouped_by)\
					.agg(*[F.last(c).alias(c) for c in set(new_df.columns)-grouped_by])\
					.orderBy(id_col)

	new_df = new_df.repartition(num_repartitions)
	new_df.write.format('parquet')\
				.mode("overwrite")\
				.option("maxRecordsPerFile", MAX_REGISTERS_REPARTITION)\
				.save(bucket_path)


def full_table_update(glueContext, spark, bucket_path, database_name, table_name, transient_path):
	'''Consulta todos os dados do SQL SERVER e sobrescreve o Parquet.'''

	MAX_REGISTERS_REPARTITION = 250000
	df = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)
	df = df.toDF()

	total_registers = df.count()
	num_repartitions = ceil(total_registers / MAX_REGISTERS_REPARTITION)

	df = df.repartition(num_repartitions)
	df.write.format('parquet')\
			.mode("overwrite")\
			.option("maxRecordsPerFile", MAX_REGISTERS_REPARTITION)\
			.save(bucket_path)


def create_temp_updates(df, database_name, table_name, reference_col, transient_path):
	'''Consulta os dados atualizados do SQL SERVER e grava os dados na transient zone.'''

	transient_path = transient_path + f"{database_name}/{table_name}/"
	df = df.withColumn("year", year(reference_col))\
			.withColumn("month", month(reference_col))\
			.withColumn("day", dayofmonth(reference_col))


	df.write.format('parquet')\
			.mode("append")\
			.partitionBy("year", "month", "day")\
			.save(transient_path)




args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'ID_COL', 'TRANSIENT_PATH', 'IS_FULL_UPDATE'])

sc 			= SparkContext()
glueContext = GlueContext(sc)
spark 		= glueContext.spark_session
job 		= Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bucket_path 	  = args['PATH_S3']
database_name 	  = args['DATABASE']
table_name 		  = args['TABLE_NAME']
reference_col 	  = args['REFERENCE_COL']
id_col 			  = args['ID_COL']
transient_path    = args['TRANSIENT_PATH']
full_update 	  = args['IS_FULL_UPDATE']

if full_update == 'true':
	full_table_update(glueContext, spark, bucket_path, database_name, table_name, transient_path)
else:
	update_table(glueContext, spark, bucket_path, database_name, table_name, reference_col, id_col, transient_path)
	


job.commit()