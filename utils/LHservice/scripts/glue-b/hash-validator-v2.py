#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark para comparação de hash entre os dados do datalake #
# e  do PIC.                                                                  #
# Autor: Matheus Soares Rodrigues - NTT DATA                                  #
# Data: mai/2022                                                              #
# Versão: 1.0                                                                 # 
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# O script lê os dados da base pic (sql server) e os mesmo dados presentes no #
# datalake, gera um hash a partir dos dados e compara os dois.                #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> PATH_S3 = Caminho do S3 dos aquivos que serão consultados no datalake.   #
#>>> LOG_PATH = Caminho do S3 onde a comparação final será salva.             #
#>>> DATABASE = Database de onde os dados serão extraídos.		      #
#>>> TABLE_NAME = Table do Database. 				              #
#>>> LAST_MODIFICATION_COL = coluna referente a data de atualização do dado.  #
#>>> DATE_COL = coluna referente a data de criação do dado.      	      #
#>>> ID_COL = coluna referente ao campo de id do datamart.       	      #
#>>> STRING_CONNECTION = String de conexão com o banco de dados. 	      #
#>>> [JDBC_USER, JDBC_PASSWORD] = Credenciais para o sql server.	      #
#                                                                             #
#=============================================================================#

import sys
from datetime import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql import functions as F, DataFrame
from dateutil.relativedelta import relativedelta
from functools import reduce

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                    'PATH_S3',
                                    'LOG_PATH',
                                    'DATABASE',
                                    'TABLE_NAME',
                                    'LAST_MODIFICATION_COL',
                                    'DATE_COL',
                                    'ID_COL',
                                    'JDBC_USERNAME',
                                    'JDBC_PASSWORD',
                                    'STRING_CONNECTION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

path = args['PATH_S3']
log_path = args['LOG_PATH']
database_name = args['DATABASE']
table_name = args['TABLE_NAME']
mod_column = args['LAST_MODIFICATION_COL']
date_column = args['DATE_COL']
id_column = args['ID_COL']
username = args['JDBC_USERNAME']
password = args['JDBC_PASSWORD']
string_connection = args['STRING_CONNECTION']

# Definindo data de referência para busca na base PIC
try:
    log_df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(log_path)
    reference_date = datetime.now() - relativedelta(months=1)
except:
    year = datetime.now().year
    reference_date = datetime(year,1,1)

# Query a ser realizada no PIC
query = f"""
SELECT *
FROM {table_name}
WHERE DATEPART(YEAR, {date_column}) = {reference_date.year}
AND DATEPART(MONTH, {date_column}) >= {reference_date.month}
"""

# Carregando dados do PIC 
pic_df = spark.read.format("jdbc")\
                .option("url", f"{string_connection};database={database_name}")\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                .option("user", username)\
                .option("password", password)\
                .option("query", query)\
                .load()

# Carregando dados do Datalake
lake_df = spark.read.format("parquet") \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("spark.sql.parquet.compression.codec", "snappy") \
                .option("encoding", "UTF-8") \
                .load(path)

print("init bi: ", pic_df.count())
#Separando dados e extraindo 1% de dados de cada mês
months = pic_df.select(F.month(date_column).alias("month")).distinct().collect()
list_dfs = [pic_df.filter(F.month(date_column) == x["month"]) \
                  .sample(False, 0.1, seed=0) for x in months]

# Reunindo dados do PIC novamente em um unico dataframe 
pic_df = reduce(DataFrame.unionAll, list_dfs)


# Filtrando dados do datalake pelos trazidos no PIC 
rows = pic_df.select(F.col(id_column)).collect()
lista = [x[id_column] for x in rows]
print("ids bi: ", len(lista))

lake_df = lake_df.filter(F.col(id_column).isin(lista))
lake_df = lake_df.drop("year", "month", "day")

print("Count do BI: ", pic_df.count())
print("Count do lake: ", lake_df.count())
print("ids unicos no lake: ", lake_df.select(id_column).distinct().count())

# Gerando colunas de hash em ambos os dataframes e realizando o join entre eles
pic_df = pic_df.select(id_column, date_column, F.col(mod_column).alias("pic_modification_date"), F.md5(F.lower(F.concat_ws(*pic_df.columns))).alias("hash_pic"))
lake_df = lake_df.select(id_column, date_column, F.col(mod_column).alias("sandbox_modification_date"), F.md5(F.lower(F.concat_ws(*lake_df.columns))).alias("hash_sandbox"))

output_df = pic_df.join(lake_df, [id_column, date_column], "inner")

# Gerando coluna de validação dos hashs
output_df = output_df.withColumn("is_valid", 
                        F.when(F.col("hash_pic") == F.col("hash_sandbox"), True) \
                        .otherwise(False))
print("Count saida: ", output_df.count())

# Salvando dataframe final
output_df.write.format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("append") \
        .save(log_path)

job.commit()