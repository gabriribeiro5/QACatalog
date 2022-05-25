#======================================================================#
# Objetivo: Valida o tipo de dado em cada coluna do arquivo.           #
# Autor: Matheus Soares Rodrigues - NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#


import sys
import pyspark.sql.functions as F

# Definição das funções
def validate(df, schema):
    df_ok = df
    df_nok = df

    for column in schema["columns"]:
        df_ok = df_ok.filter((F.length(F.col(column["name"])) <= column["size"]))
    df_nok = df.join(df_ok, df.columns, "leftanti")
    return df_ok, df_nok
