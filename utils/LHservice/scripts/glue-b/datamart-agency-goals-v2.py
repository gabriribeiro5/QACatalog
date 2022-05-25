#=============================================================================#
# 																			  #
# Objetivo: Script PySpark que cria datamart Agency Goals 				      #
# Autor: Renato Candido Kurosaki - NTT DATA 								  #
# Data: Dez/2021 															  #
# Versão: 1.0 																  #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart 		  #
# agency_goals na refined zone.  										      #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> FTCRM_PORTFOLIOS = Referência no S3 onde estão os dados da 	          #
# tabela FTCRM_PORTFOLIOS.											          #
#>>> FTCRM_BUSSINESSUNITS = Referência no S3 onde estão os dados da tabela 	  #
# FTCRM_BUSSINESSUNITS.														  #
#>>> FTWAREHOUSE_AGENCIESGOALS = Referência no S3 onde estão os  			  #
# dados da tabela FTWAREHOUSE_AGENCIESGOALS                                   #
#>>> AGENCYGOALS = Referência no S3 onde os dados do datamart de 		      #
# AGENCYGOALS serão inseridos.								         		  #
#																			  #
#=============================================================================#

import sys
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
    args = getResolvedOptions(sys.argv, ['JOB_NAME',
									 'FTCRM_PORTFOLIOS',
									 'FTCRM_BUSINESSUNITS',
                                     'FTWAREHOUSE_AGENCIESGOALS',
									 'AGENCYGOALS'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    ftcrm_portfolios          = args['FTCRM_PORTFOLIOS']
    ftcrm_businessunits 	  = args['FTCRM_BUSINESSUNITS']
    ftwarehouse_agenciesgoals = args['FTWAREHOUSE_AGENCIESGOALS']
    datamart_agencygoals 	  = args['AGENCYGOALS']

    try:
        agency_goals = read_data_from_lake(datamart_agencygoals)
        last_date = agency_goals.select(F.max('date').alias('date'))\
                                .collect()[0]['date']\
                                .strftime("%Y-%m-%d")

    except:
        last_date = '1900-01-01'


    df_portfolios = read_data_from_lake(ftcrm_portfolios)
    df_businessunits = read_data_from_lake(ftcrm_businessunits)
    df_ftw_agenciesgoals = read_data_from_lake(ftwarehouse_agenciesgoals)

    df_ftw_agenciesgoals = df_ftw_agenciesgoals.withColumn("refdate",
                                        F.date_format("month", "yyyyMM").cast("int").alias("refdate"))\
                                        .withColumn("date", F.col('month').cast('date'))


    df_portfolios.createOrReplaceTempView("Portfolios")
    df_businessunits.createOrReplaceTempView("BusinessUnits")
    df_ftw_agenciesgoals.createOrReplaceTempView("AgenciesGoals")

    agencygoals = spark.sql(f"""
        SELECT  P.portfolioid,
                P.alias AS portfolio,
                AG.refdate,
                AG.date,
                BU.businessunitid AS agencyid,
                BU.alias AS agency,
                AG.goal
        FROM AgenciesGoals AS AG
        INNER JOIN Portfolios AS P ON AG.portfolioid = P.portfolioid
        INNER JOIN BusinessUnits AS BU ON AG.businessunitid = BU.businessunitid
        WHERE P.businessunitid IN ( 672,  743 )
        AND
        AG.date > '{last_date}'
    """)

    agencygoals = agencygoals.withColumn("goaluntiltoday", F.lit(None).cast('decimal(16,2)'))

    num_repartitions = ceil(agencygoals.count() / MAX_REGISTERS_REPARTITION)

    agencygoals = agencygoals.repartition(num_repartitions)

    write_data_bucket(agencygoals, datamart_agencygoals, "append")

    job.commit()
