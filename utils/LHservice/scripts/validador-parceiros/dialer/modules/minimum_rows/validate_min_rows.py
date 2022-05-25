#======================================================================#
# Objetivo: Valida se o arquivo possui numero de registros maior que o #
# valor minimo definido.                                               #
# Autor: Matheus Soares Rodrigues - NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#


import sys
import pandas as pd

# Definição das funções
def get_min_rows(log_path, job_name):
    try:
        df = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        min_rows = df.loc[df['status'] == 'SUCCESS', 'total_rows'].max()  * 0.9
    except:
        min_rows = 1000
    return min_rows

def validate_min_rows(df, min_rows):
    df_rows = df.count()
    return df_rows >= min_rows