#======================================================================#
# Objetivo: Verifica as colunas do arquivo em busca de valores nulos   #
# nos campos de definidos como obrigatórios.                           #
# Autor: Matheus Soares Rodrigues - NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#


import sys
import pyspark.sql.functions as F

# Definição das funções
def validate_required_fields(df, schema):
    output_df = df
    all_columns = df.columns

    try:
        for column in schema["columns"]:
            if(column["required"]):
                output_df = output_df.filter(F.col(column["name"]).isNotNull())
        error_df = df.join(output_df, all_columns, "leftanti")
    except:
        output_df = 0
        error_df = df
    return output_df, error_df
