#======================================================================#
# Objetivo: Valida o tipo de dado em cada coluna do arquivo.           #
# Autor: Matheus Soares Rodrigues - NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#


import sys
import pyspark.sql.functions as F

# Definição das funções
def validate_column_types(df, schema):
    error_columns = []
    for column in schema["columns"]:
        try:
            if(column["type"] != "json"):
                df = df.withColumn(column["name"], F.col(column["name"]).cast(column["type"]))
        except:
            error_columns.append(column["name"])
    return error_columns