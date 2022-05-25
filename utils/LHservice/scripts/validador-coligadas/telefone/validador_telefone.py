#=============================================================================#
#                                                                             #
# Objetivo: Script principal do validador de arquivos de telefone coligadas   #
# Autor: Matheus Soares Rodrigues - NTT DATA      							  #
# Data: Mai/2022                                                              #
# Versão: 1.0                                                                 #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Carrega os modulos de validação e aplica suas funções no arquivo csv.       #
# Gerando um arquivo de rechaço se houver registros incorretos e envia        #
# para ingestão os dados previamente configurados                             #
#                                                                             #
#=============================================================================#

from ast import arg
import csv
from datetime import datetime, timedelta
from fileinput import filename
from itertools import product
import sys
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

import boto3
import pandas as pd
from functools import reduce

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from modules.in_out.input_files import load_csv, load_json
from modules.in_out.output_file import save_log, save_nok_data, save_ok_data
from modules.required_fields.required_fields import validate_required_fields
from modules.column_types.column_types import validate_column_types
from modules.content import cpf_cnpj, date_columns, ddd_numbers, notes, \
    phone_category, phone_extension, phone_numbers, phone_status, \
    phone_type, source_phone, sourcedirection, sourcename


# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Inicialização do S3
s3 = boto3.resource('s3')

# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 
                                     'BUCKET_JSON', 
                                     'BUCKET_KEY', 
                                     'DATA_PATH', 
                                     'LOG_PATH', 
                                     'BUCKET_BRONZE', 
                                     'FILE_NAME'])

job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
bucket_json = args['BUCKET_JSON'] #bucket onde esta o arquivo json
bucket_key = args['BUCKET_KEY'] #nome do arquivo json
data_path = args['DATA_PATH'] #bucket onde esta o arquivo csv
log_path = args['LOG_PATH'] #bucket onde o log será gravado
bucket_bronze = args['BUCKET_BRONZE'] #bucket bronze
file_name = args['FILE_NAME']

# Data e hora de inicio do processo
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Carrega arquivo json
metadata_dict = load_json(bucket_json, bucket_key)
columns = [x["name"] for x in metadata_dict["columns"]]

# Carrega o arquivo csv para validação
df = load_csv(data_path, columns, spark)

# Valida se o arquivo possui os campos obrigatorios
output_df, error_df = validate_required_fields(df, metadata_dict, columns)

if (output_df.count() == 0):
    # Salva o arquivo de rechaço quando não possui os campos obrigatorios
    save_nok_data(error_df, log_path, file_name)
    error_count = error_df.count()
    output_count = 0 
    status = "FAILED"
    status_message = "Some mandatory fields missing or with null values"
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)
else:
    column_error = validate_column_types(output_df, metadata_dict)
    if len(column_error) == 0:
        df_ok, df_nok_1 = cpf_cnpj.validate(columns[0], output_df)
        df_ok, df_nok_2 = ddd_numbers.validate(columns[1], df_ok)
        df_ok, df_nok_3 = phone_numbers.validate(columns[2], df_ok)
        df_ok, df_nok_4 = phone_type.validate(columns[3], df_ok)
        df_ok, df_nok_5 = phone_category.validate(columns[4], df_ok)
        df_ok, df_nok_6 = phone_status.validate(columns[5], df_ok)
        df_ok, df_nok_7 = phone_extension.validate(columns[6], df_ok)
        df_ok, df_nok_8 = notes.validate(columns[7], df_ok)
        df_ok, df_nok_9 = source_phone.validate(columns[8], df_ok)
        df_ok, df_nok_10 = date_columns.validate(columns[9], df_ok)
        df_ok, df_nok_11 = date_columns.validate(columns[10], df_ok)
        df_ok, df_nok_12 = sourcedirection.validate(columns[11], df_ok)
        df_ok, df_nok_13 = sourcename.validate(columns[12],df_ok)
        dfs_nok = [df_nok_1, df_nok_2, df_nok_3, df_nok_4, df_nok_5, df_nok_6, df_nok_7, 
                    df_nok_8, df_nok_9, df_nok_10, df_nok_11, df_nok_12, df_nok_13]
        df_nok = reduce(DataFrame.unionAll, dfs_nok)
        if(df_ok.count() > 0):
            date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_ok_data(df_ok, bucket_bronze, file_name)
            status = "SUCCESS"
            status_message = "Records imported successfully"
        else:
            status = "FAILED"
            status_message = "Invalid data content"
        save_nok_data(df_nok, log_path, file_name)
        error_count = df_nok.count()
        output_count = df_ok.count()
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)
    else:
        save_nok_data(output_df, log_path, file_name)
        error_count = output_df.count()
        output_count = 0
        status = "FAILED"
        status_message = "Invalid column type: " + column_error
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)

job.commit()