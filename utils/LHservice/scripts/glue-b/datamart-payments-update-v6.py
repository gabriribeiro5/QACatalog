#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que atualiza o datamart arrangements               #
# Autor: Matheus Soares Rodrigues - NTT DATA                                  #
# Data: Jan/2022                                                              #
# Versão: 6.0                                                                 # 
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da trasient zone e atualiza o datamart payments na refined zone.   #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> UPDATE_DEBT_TRANSACTIONS = Referência no S3 onde estão os dados da       #
# tabela DEBT_TRANSACTIONS na trasient zone.                                  #
#>>> DATAMART_PAYMENTS = Referência no S3 onde os dados do datamart de        #
#  PAYMENTS estão inseridos.                                                  #
#                                                                             # 
#=============================================================================#

import sys
from datetime import datetime, timedelta
from math import ceil

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import pandas as pd
from botocore.client import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F



# Parâmetros Dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PATH_S3', 'DATABASE', 'TABLE_NAME', \
                                    'REFERENCE_COL', 'ID_COL', 'TRANSIENT_PATH', 'IS_FULL_UPDATE', 'PROCESS_TYPE', \
                                    'STRING_CONNECTION', 'JDBC_USERNAME', 'JDBC_PASSWORD', 'LOG_PATH'])

job_name          = args['JOB_NAME']
job_run_id        = args['JOB_RUN_ID']
bucket_path       = args['PATH_S3']
database_name     = args['DATABASE']
table_name        = args['TABLE_NAME']
reference_col     = args['REFERENCE_COL']
id_col            = args['ID_COL']
transient_path    = args['TRANSIENT_PATH']
full_update       = args['IS_FULL_UPDATE']
process_type      = args['PROCESS_TYPE']
log_path          = args['LOG_PATH']
string_connection = args['STRING_CONNECTION']
username          = args['JDBC_USERNAME']
password          = args['JDBC_PASSWORD']
datamart_payments   = args['DATAMART_PAYMENTS']
ftcrm_debts_transaction_update = args['UPDATE_DEBT_TRANSACTIONS']


#Parâmetros principais
MAX_REGISTERS_REPARTITION = 250000

id_col = "debttransactionid"
update_column_list = [
        "debttransactionid",
        "lastmodificationdate",
        "reversedtransactionid",
        "accountingdate",
        "cleardate",
        "processingdate",
        "transactionsource",
        "transactioncode",
        "creationdate"
        ]

# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")


#######################################################################################################################
#Definição de funções

def save_log(process_type, job_run_id, status, n_rows, max_id, max_creation_date, last_modification_date, start_time, end_time, elapsed_time):
    log_data = {
        'process_type': process_type, 'job_id': job_run_id, 'status': status, 'total_rows': n_rows, 
        'max_id': max_id, 'max_creation_date': max_creation_date, 
        'last_modification_date': last_modification_date, 
        'start_time': start_time, 'end_time': end_time, 
        'elapsed_time': elapsed_time
        }
    print(log_data)
    try: 
        df1 = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}.csv'.format(log_path, job_name), index=False)
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}.csv'.format(log_path, job_name), index=False)

def get_last_timestamp():
    try:
        df = pd.read_csv('{}{}.csv'.format(log_path, args['JOB_NAME']))
        max_last_modification = df.loc[df['status'] == 'SUCCESS', 'lastmodificationdate'].max().strftime('%Y-%m-%d')
        return max_last_modification
    except:
        max_last_modification = datetime(1970,1, 1).strftime('%Y-%m-%d')
        return max_last_modification

def folder_exists(s3_path):
    s3 = boto3.client('s3')

    complete_path = s3_path.split('s3://')[1]
    bucket = complete_path.split('/')[0]
    path = '/'.join(complete_path.split('/')[1:]).rstrip('/')

    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/', MaxKeys=1)

    return 'CommonPrefixes' in resp

def read_data_from_lake(s3_path, base_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("basePath", base_path) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path).cache()

    return df


def lower_column_names(df):
	'''Padroniza em minúsculo os nomes das colunas.'''	
	return df.toDF(*[c.lower() for c in df.columns])

#######################################################################################################################
#Definição de funções

#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script (UTC): {start_time_str}")


#Obtém último valor de atualização do processo anterior
lastmodificationdate = get_last_timestamp()
print("lastmodificationdate inicial: ", lastmodificationdate)


# Carrega tabela origem, filtra registros que sofreram updates, renomeia colunas para join
updates_origem = glueContext.spark_session.read.format("parquet") \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("spark.sql.parquet.compression.codec", "snappy") \
            .option("encoding", "UTF-8") \
            .load(ftcrm_debts_transaction_update) \
            .select(update_column_list)

updates_origem = lower_column_names(updates_origem)
updates_origem = updates_origem.filter(F.date_format(updates_origem["lastmodificationdate"], "yyyy-MM-dd") >= lastmodificationdate)
updates_origem = updates_origem.select([F.col(x).alias( '_' + x) for x in update_column_list])

count_udates_origem  = updates_origem.count()
print(f"Contagem updates origem: {count_udates_origem}")


#Constroi caminhos do S3 de partições com update
update_partitions = updates_origem.withColumn("_year", F.date_format(F.col("_creationdate"), "yyyy")) \
                        .withColumn("_month", F.date_format(F.col("_creationdate"), "MM")) \
                        .withColumn("_day", F.date_format(F.col("_creationdate"), "dd")) \
                        .select("_year", "_month", "_day").distinct().collect()
print("Dates partition update: ", update_partitions)


update_partition_paths = [datamart_payments + "year=" + x._year + "/month=" + x._month + "/day=" + x._day + "/" for x in update_partitions]
update_partition_paths = [s3_path for s3_path in update_partition_paths if folder_exists(s3_path)]


#######################################################################################################################
# Carrega Datamart original(sem partições)
datamart_original = read_data_from_lake(datamart_payments, datamart_payments)
datamart_original = lower_column_names(datamart_original)

count_datamart_original  = datamart_original.count()
print("Contagem datamart original (partições carregadas): ", count_datamart_original)

updates_datamart = datamart_original.join(updates_origem, datamart_original[id_col] == updates_origem["_" + id_col], how="inner")
updates_datamart = updates_datamart.drop(*update_column_list + ["_creationdate"]) 

for col in updates_datamart.columns:
    if col.startswith('_'):
        updates_datamart = updates_datamart.withColumnRenamed(col, col[1:])

count_updates_datalake  = updates_datamart.count()
print(f"Contagem updates datamart: {count_updates_datalake}")


# Contrói Datamart com registros não atualizados
updates_id = updates_datamart.select((id_col))
not_updated = datamart_original.join(updates_id, [id_col], 'leftanti')

# Contrói tabela final atualizada, a partir do union dos dataframes
final_table = not_updated.unionByName(updates_datamart).cache()

count_final_table = final_table.count()
print(f"Contagem Final datalake: {count_final_table}")


## Reparticiona e salva tabela atualizada no S3
print(f"Salvando dados no S3.")
num_repartitions = ceil(count_final_table / MAX_REGISTERS_REPARTITION)
final_table = final_table.repartition(num_repartitions)	

final_table.write.format("parquet") \
        .partitionBy("year", "month", "day") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("overwrite") \
        .save(datamart_payments)

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


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = final_table.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_modification_date_str = max_modification_date.strftime('%Y-%m-%d %H:%M:%S')
print(f"lastmodificationdate atualizado: {max_modification_date}")


max_id = final_table.agg({id_col: "max"}).collect()[0][0]
max_creation_date = final_table.agg({"date": "max"}).collect()[0][0]
max_creation_date_str = max_creation_date.strftime('%Y-%m-%d %H:%M:%S')


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")
elapsed_time = end_time - start_time


save_log(process_type, job_run_id, status, count_updates_datalake, max_id, max_creation_date, max_modification_date_str, start_time, end_time, elapsed_time)


#Finaliza script
print(f"Tempo de execução: {end_time - start_time}")
job.commit()



