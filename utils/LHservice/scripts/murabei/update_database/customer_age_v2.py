#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark para processo ETL dos modelos da Murabei           #
# Autor: Renato Candido Kurosaki - NTT DATA                                   #
# Data: Nov/2021                                                              #
# Versão: 1.0                                                                 #
# Versão Python: 3                                                            #
# Versão Spark: 2.4                                                           #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Processo ETL para transformação dos dados da FTContacts People que          #
# irá alimentar o modelo preditivo da Murabei.                                #
# obs: a versão 1.0 é para homologação da lógica aplicada ao processo         #
#----------------------------- Parâmetros ------------------------------------#
#                                                                             #
#>>> FTCONTACTS_PEOPLE = Referência no S3 onde estão os dados da tabela       #
# People.                                                                     #
#>>> CUSTOMER_TIME_NUMERICAL = Referência no S3 onde estão os dados da tabela #
# customer_time_numerical                                                     #
#>>> DEBT_TIME_NUMERICAL = Referência no S3 onde serão armazenados os dados   #
# da tabela debt_time_numerical.                                              #
#                                                                             #
#=============================================================================#

import sys
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from math import ceil
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import *
from math import ceil

MAX_REGISTERS_REPARTITION = 250000

def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)

    return df

def write_data_bucket(df, path_s3, write_mode):
    df.write.format("parquet") \
                    .option("header", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .mode(write_mode) \
                    .save(path_s3)


if __name__ == '__main__':

    args        = getResolvedOptions(sys.argv, ['JOB_NAME',
                                                'FTCONTACTS_PEOPLE',
                                                'CUSTOMER_TIME_NUMERICAL',
                                                'DEBT_TIME_NUMERICAL'])
    sc 			= SparkContext()
    glueContext = GlueContext(sc)
    spark 		= glueContext.spark_session
    job 		= Job(glueContext)
    logger      = glueContext.get_logger()

    ftcontacts_dbo_people       = args["FTCONTACTS_PEOPLE"]
    debt_time_numerical         = args["DEBT_TIME_NUMERICAL"]
    customer_time_numerical     = args["CUSTOMER_TIME_NUMERICAL"]


    # Verificando até que mês foram processados os pagamentos
    try:
        df_customer_time_numerical = read_data_from_lake(customer_time_numerical)
        range_time = df_customer_time_numerical.select(
                                F.min("time").alias("min"),
                                F.max("time").alias("max")
        )

        start_date = range_time.collect()[0]["max"].replace(day=1).strftime("%Y-%m-%d")
    except:
        start_date = "1990-01-01"

    df_ftcontacts_people = read_data_from_lake(ftcontacts_dbo_people)
    df_ftcontacts_people.createOrReplaceTempView("ftcontacts_people")

    df_debt_time_numerical = read_data_from_lake(debt_time_numerical)
    df_debt_time_numerical.createOrReplaceTempView("database_debt_time_numerical")

    today = datetime.today().strftime("%Y-%m-%d")

    nascimento_customer = spark.sql(f"""
                                    SELECT ContactID AS customer_id,
                                            BirthDate AS birth_date
                                    FROM ftcontacts_people
                                    """)

    nascimento_customer = nascimento_customer.withColumn("birth_date",
                                        F.when(
                                        (F.col("birth_date") >= "1900-01-01") &\
                                        (F.col("birth_date") <= today), F.col("birth_date"))
                                        .otherwise(None))

    # Convertendo todas as datas para o primeiro dia do mês.
    nascimento_customer = nascimento_customer.withColumn("birth_date",
                                            F.trunc("birth_date", "month"))

    range_time_query_debt_time = f"""
            SELECT MIN(time) as min_time,
                    MAX(time) as max_time
            FROM database_debt_time_numerical
            WHERE variable_id = 1
            AND '{start_date}' <= time
            """

    range_time = spark.sql(range_time_query_debt_time)

    min_time = range_time.collect()[0]["min_time"]
    max_time = range_time.collect()[0]["max_time"]

    process_months = list(pd.date_range(
        start=min_time, end=max_time,
        freq="MS"))

    unique_customer = df_debt_time_numerical.select("customer_id", "time")\
                        .filter(
                            (F.col("variable_id") == 1) &\
                            (F.col("time").isin(process_months))
                        ).distinct()

    unique_customer = unique_customer.withColumn("time", F.col("time").cast("date"))\
                                     .withColumn("variable_id", F.lit(61).cast("int"))

    unique_customer = unique_customer.join(nascimento_customer, ["customer_id"] , how="left")
    unique_customer = unique_customer.withColumn("value", F.datediff(
                                        F.col("time"), F.col("birth_date"))/365.25)

    # Remove valores nulos e menores ou iguais a 0
    unique_customer = unique_customer.filter("value IS NOT NULL AND value > 0")

    # Seleciona apenas as colunas de interesse
    unique_customer = unique_customer.select("time", "customer_id", "variable_id", "value")

    num_repartitions = ceil(unique_customer.count() / MAX_REGISTERS_REPARTITION)

    to_upload = unique_customer.repartition(num_repartitions)

    # Salva no bucket
    write_data_bucket(to_upload, customer_time_numerical, "append")

    job.commit()
