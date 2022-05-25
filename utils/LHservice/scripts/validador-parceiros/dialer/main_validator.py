#=============================================================================#
#                                                                             #
# Objetivo: Script principal do validador de arquivos de parceiros			  #
# Autor: Edinor Cunha Júnior - NTT DATA      								  #
# Data: Mar/2022                                                              #
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
from functools import reduce

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, lpad, col

from modules.in_out.input_files import load_csv, load_json
from modules.in_out.output_file import save_log, save_nok_data, save_ok_data
from modules.required_fields.validate_required_fields import validate_required_fields
from modules.minimum_rows.validate_min_rows import get_min_rows, validate_min_rows
from modules.column_types.validate_column_types import validate_column_types
from modules.content import call_direction, call_status, call_status_10, call_time, conversation_status, cpf_cnpj, \
    document_type, document_code, ddd_numbers, phone_numbers, ura_capture, ura_path, ura_route
from modules.column_length.validate_column_length import validate as validate_column_length


# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Inicialização do S3
s3 = boto3.resource('s3')

# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'BUCKET_PATH',
                                     'BUCKET_KEY', 'DATA_PATH', 'DF_ID', 'LOG_PATH',
                                     'DOC_TYPE', 'DOC_COL', 'CAL_DIR', 'CALL_STATUS',
                                     'STATUS_COL', 'AGENT_COL', 'CONVERSATION_COL', 'CONVERSATION_STATUS', 
                                     'VOICE_START_COL', 'DDD_COL', 'PHONE_NUMBERS', 'DOC_CODE', 'URA_CAPTURE',
                                     'URA_PATH', 'URA_ROUTE', 'BUCKET_NAME'])

job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
bucket_name = args['BUCKET_NAME']
bucket_path = args['BUCKET_PATH']
bucket_key = args['BUCKET_KEY']
data_path = args['DATA_PATH']
df_id = args['DF_ID']
log_path = args['LOG_PATH']
doc_type_col = args['DOC_TYPE']
document_col = args['DOC_COL']
call_dir = args['CAL_DIR']
call_st = args['CALL_STATUS']
agent_column = args['AGENT_COL']
conversation_st = args['CONVERSATION_STATUS']
voice_start_column = args['VOICE_START_COL']
task_start_time = args['TASK_START_TIME']
task_end_time = args['TASK_END_TIME']
voice_start_time = args['VOICE_START_TIME']
ddd_col = args['DDD_COL']
phone_num = args['PHONE_NUMBERS']
doc_code = args['DOC_CODE']
capture_ura = args['URA_CAPTURE']
path_ura = args['URA_PATH']
route_ura = args['URA_ROUTE']
bucket_bronze = args['BUCKET_BRONZE']
file_name = args['FILE_NAME']

# Data e hora de inicio do processo
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Carrega arquivo json
metadata_dict = load_json(bucket_name, bucket_key)
columns = [x["name"] for x in metadata_dict["columns"]]

# Carrega o arquivo csv para validação
df = load_csv(data_path, columns, spark)

# Valida se o arquivo possui os campos obrigatorios
output_df, error_df = validate_required_fields(df, metadata_dict)

if len(output_df) == 0:
    # Salva o arquivo de rechaço quando não possui os campos obrigatorios
    save_nok_data(error_df, log_path, bucket_key)
    error_count = error_df.count()
    output_count = 0 
    status = "FAILED"
    status_message = "Some mandatory fields missing or with null values"
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)
else:
    # Valida se as colunas possuem o tamanho maximo estipulado
    output_df, error_df = validate_column_length(output_df, metadata_dict)

    if len(output_df) == 0:
        # Salva o arquivo de rechaço quando não possui os campos obrigatorios
        save_nok_data(error_df, log_path, bucket_key)
        error_count = error_df.count()
        output_count = 0 
        status = "FAILED"
        status_message = "Data beyond the maximum size allowed"
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)
    else:
        # Valida se o arquivo possui a quantidade de linhas minimas para ingestão
        min_rows = get_min_rows(log_path, job_name)
        is_valid = validate_min_rows(output_df, min_rows)
        if is_valid:
            column_error = validate_column_types(output_df, metadata_dict)
            if len(column_error) == 0:
                df_ok, df_nok_1 = document_type.validate(document_col, output_df)
                df_ok, df_nok_2 = cpf_cnpj.validate(df_id, doc_type_col, document_col, df_ok)
                df_ok, df_nok_3 = call_direction.validate(call_dir, df_ok)
                df_ok, df_nok_4 = call_status.validate(call_st, df_ok)
                df_ok, df_nok_5 = call_status_10.validate(call_st, agent_column, conversation_st, voice_start_column, df_ok)
                df_ok, df_nok_6 = call_time.validate(task_start_time, task_end_time, voice_start_time, df_ok)
                df_ok, df_nok_7 = conversation_status.validate(conversation_st, df_ok)
                df_ok, df_nok_8 = ddd_numbers.validate(ddd_col, df_ok)
                df_ok, df_nok_9 = phone_numbers.validate(phone_num, df_ok)
                df_ok, df_nok_10 = document_code.validate(doc_code, df_ok)
                df_ok, df_nok_11 = ura_capture.validate(capture_ura, df_ok)
                df_ok, df_nok_12 = ura_path.validate(path_ura, df_ok)
                df_ok, df_nok_13 = ura_route.validate(route_ura, df_ok)
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
        else:
            save_nok_data(output_df, log_path, file_name)
            error_count = output_df.count()
            output_count = 0 
            status = "FAILED"
            status_message = "File without the minimum of valid lines"
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            save_log(job_run_id, output_count, error_count, status, status_message, start_time, end_time, log_path, job_name)

job.commit()