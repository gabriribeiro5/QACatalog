#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que atualiza datamart debtcontacts                 #
# Autor: Matheus Soares Rodrigues - NTT DATA                                  #
# Data: Jan/2021                                                              #
# Alteração: Thiago Nascimento Nogueira					      #
# Data de Modificação: 24/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script		              #
# Versão: 5.0                                                                 # 
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da transient zone e atualiza o datamart debtcontacts na            #
# refined zone.                                                               #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> UPDATE_DEBTS = Referência no S3 onde estão os dados da tabela            #
# FTCRM_DEBTS na trasient zone.                                               #
#>>> DATAMART_DEBTCONTACTS = Referência no S3 onde os dados do datamart de    #
# DEBTCONTACTS serão inseridos.                                               #
#                                                                             #        
#=============================================================================#

from datetime import datetime, timedelta
from math import ceil
import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import json
import pandas as pd

from pyspark.context import SparkContext
import pyspark.sql.functions as F


# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 
                                     'UPDATE_DEBTS',
                                     'DATAMART_DEBTCONTACTS',
                                     'LOG_PATH',
                                     'PROCESS_TYPE'])

job_name                 = args['JOB_NAME']
job_run_id               = args['JOB_RUN_ID']
ftcrm_debts_update       = args['UPDATE_DEBTS']
datamart_debtcontacts    = args['DATAMART_DEBTCONTACTS']
log_path                 = args['LOG_PATH']
process_type             = args['PROCESS_TYPE']


#Parâmetros principais
MAX_REGISTERS_REPARTITION = 250000

id_col = "debtid"
update_column_list = [
        "debtid",
        "lastmodificationdate",
        "portfolioid",
        "originaldebtnumber",
        "debtstatus",
        "originalfirstdefaultdate",
        "firstdefaultdate",
        "opendate",
        "originalfirstdefaultbalance",
        "initialbalance",
        "currentbalance",
        "settlementdate",
        "recalldate",
        "selldate"
        ]


# Inicialização do Spark
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(job_name, args)


# Inicialização do S3
s3 = boto3.resource('s3')

#spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

#######################################################################################################################
#Definição de funções

def get_last_timestamp():
    try:
        df = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        max_last_modification = df.loc[df['status'] == 'SUCCESS', 'lastmodificationdate'].max()
        return max_last_modification
    except:
        max_last_modification = "1970-01-01 00:00:00"
        return max_last_modification


def save_log(job_run_id, status, n_rows, max_id, max_creation_date, last_modification_date, start_time, end_time):
    log_data = {
        'id': job_run_id, 'status': status, 'total_rows': n_rows, 
        'max_id': max_id, 'maxcreationdate': max_creation_date, 
        'lastmodificationdate': last_modification_date, 
        'start_time': start_time, 'end_time': end_time
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




#######################################################################################################################
# Script principal

#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script (UTC): {start_time_str}")


#Obtém último valor de atualização do processo anterior
lastmodificationdate = get_last_timestamp()
print("lastmodificationdate inicial: ", lastmodificationdate)


# Carrega Datamart original
datamart_original = glueContext.spark_session.read.format("parquet") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .load(datamart_debtcontacts) \
        .cache()

print("Contagem datamart original: ", datamart_original.count())


# Carrega tabela origem, filtra registros que sofreram updates, renomeia colunas para join
updates_origem = glueContext.spark_session.read.format("parquet") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .load(ftcrm_debts_update) \
        .select(update_column_list)

updates_origem = updates_origem.filter(F.date_format(updates_origem["lastmodificationdate"], "yyyy-MM-dd") >= lastmodificationdate)
updates_origem = updates_origem.select([F.col(x).alias( '_' + x) for x in update_column_list])

count_udates_origem  = updates_origem.count()
print(f"Contagem updates origem: {count_udates_origem}")


# Contrói dataframe de updates
updates_datamart = datamart_original.join(updates_origem, datamart_original[id_col] == updates_origem['_' + id_col], how="inner")
updates_datamart = updates_datamart.drop(*update_column_list)

for col in updates_datamart.columns:
    if col.startswith('_'):
        updates_datamart = updates_datamart.withColumnRenamed(col, col[1:])

print(f"Schema dataframe updates_datamart:")
updates_datamart.printSchema()

count_udates_datalake  = updates_datamart.count()
print(f"Contagem updates datamart: {count_udates_datalake}")


# Contrói Datamart com registros não atualizados
updates_id = updates_datamart.select(id_col)
not_updated = datamart_original.join(updates_id, [id_col], 'leftanti')


# Contrói tabela final atualizada, a partir do union dos dataframes
final_table = not_updated.unionByName(updates_datamart).cache()

count_final_table = final_table.count()
print(f"Contagem Final datalake: {count_final_table}")

max_modification_date = final_table.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_modification_date_str = max_modification_date.strftime('%Y-%m-%d %H:%M:%S')
print(f"lastmodificationdate atualizado: {max_modification_date}")


## Reparticiona e salva tabela atualizada no S3
# print(f"Salvando dados no S3.")
# num_repartitions = ceil(count_final_table / MAX_REGISTERS_REPARTITION)
# final_table = final_table.repartition(num_repartitions)	

# final_table.write.format("parquet") \
#         .option("header", True) \
#         .option("spark.sql.parquet.compression.codec", "snappy") \
#         .option("encoding", "UTF-8") \
#         .mode("overwrite") \
#         .save(datamart_debtcontacts)

#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_id = final_table.agg({id_col: "max"}).collect()[0][0]
max_creation_date = final_table.agg({"debtcreationdate": "max"}).collect()[0][0]
max_creation_date_str = max_creation_date.strftime('%Y-%m-%d %H:%M:%S')
save_log(job_run_id, status, count_udates_datalake, max_id, max_creation_date_str, max_modification_date_str, start_time_str, end_time_str)


#######################################################################################################################
#DEBUG:

print(f"Schema dataframe final:")
final_table.printSchema()


# Faz contagem da origem agregada por mês e ano baseada na data de update
print(f"Contagem agrupada PIC:")

data_count_grouped1 = updates_origem\
        .withColumn('up_year', F.year(F.col("_lastmodificationdate")))\
        .withColumn('up_month', F.month(F.col("_lastmodificationdate")))

data_count_grouped1 = data_count_grouped1\
        .groupby('up_year', 'up_month')\
        .agg(F.count(F.lit(1)).alias("counts"))\
        .sort(F.col("up_year").desc(),F.col("up_month").desc())\
        .show(truncate=False)


#Faz contagem da origem agrupada agregada por mês e ano baseada na data de update
print(f"Contagem agrupada datamart atualizada:")

data_count_grouped2 = updates_datamart\
        .withColumn('up_year', F.year(F.col("lastmodificationdate")))\
        .withColumn('up_month', F.month(F.col("lastmodificationdate")))

data_count_grouped2 = data_count_grouped2\
        .groupby('up_year', 'up_month')\
        .agg(F.count(F.lit(1)).alias("counts"))\
        .sort(F.col("up_year").desc(),F.col("up_month").desc())\
        .show(truncate=False)


#Faz contagem do destino agregada por mês e ano baseada na data de update
print(f"Contagem agrupada destino:")

data_count_grouped3 = final_table\
        .withColumn('up_year', F.year(F.col("lastmodificationdate")))\
        .withColumn('up_month', F.month(F.col("lastmodificationdate")))

data_count_grouped3 = data_count_grouped3\
        .groupby('up_year', 'up_month')\
        .agg(F.count(F.lit(1)).alias("counts"))\
        .sort(F.col("up_year").desc(),F.col("up_month").desc())\
        .show(truncate=False)
#######################################################################################################################

#Finaliza script
print(f"Tempo de execução: {end_time - start_time}")
job.commit()
