#======================================================================#
# Objetivo: Output dos dados referentes do arquivo de parceiros         #
# Autor: Matheus Soares Rodrigues - NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#

import sys
import uuid
from math import ceil
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# Definição das funções
def save_ok_data(df_ok, path, file_name):
    df_ok.toPandas().to_csv('{}{}'.format(path, file_name), index=False)

def save_nok_data(df_nok, path, file_name):
    df_nok.toPandas().to_csv('{}rechaço-{}'.format(path, file_name), index=False)

def save_log(job_run_id, accepted_rows, rejected_rows, status, status_message, start_time, end_time, log_path, job_name):
    log_data = {
        "id": job_run_id,
        "status": status,
        "status_message": status_message,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "start_time": start_time,
        "end_time": end_time
        }
    
    try: 
        df1 = pd.read_csv('{}{}-log.csv'.format(log_path, job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}-log.csv'.format(log_path, job_name), index=False)
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}-log.csv'.format(log_path, job_name), index=False)