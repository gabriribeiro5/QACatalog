import sys
import pandas as pd
import pyspark.sql.functions as F
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from math import ceil
from datetime import datetime, timedelta
from pyspark.sql.functions import year, month, dayofmonth, when
from pandas.tseries.offsets import MonthEnd, MonthBegin
from functools import reduce
from pyspark.sql import DataFrame

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

def union_list_dataframes(df_list):
    df = reduce(DataFrame.unionByName, df_list)
    return df

if __name__ == '__main__':

    args        = getResolvedOptions(sys.argv, ['JOB_NAME',
                                                'DEBTCONTACTS',
                                                'ARRANGEMENTINSTALLMENTS',
                                                'FTCRM_PORTFOLIOS',
                                                'DEBT_TIME_CATEGORICAL'])
    sc 			= SparkContext()
    glueContext = GlueContext(sc)
    spark 		= glueContext.spark_session
    job 		= Job(glueContext)
    logger      = glueContext.get_logger()

    datamart_debtcontacts            = args["DEBTCONTACTS"]
    datamart_arrangementinstallments = args["ARRANGEMENTINSTALLMENTS"]
    ftcrm_porfolios                  = args["FTCRM_PORTFOLIOS"]
    debt_time_categorical            = args["DEBT_TIME_CATEGORICAL"]

    df_debtcontacts = read_data_from_lake(datamart_debtcontacts)
    df_arrangementinstallments = read_data_from_lake(datamart_arrangementinstallments)

    # Verificando a última data que foi atualizada a base de dados.
    ultima_data_query = """
                        (SELECT MAX(TIME) AS time
                        FROM database_debt_time_categorical)
                        as temp
                        """

    try:
        df_debt_time_categorical = read_data_from_lake(debt_time_categorical)
        df_debt_time_categorical.createOrReplaceTempView("database_debt_time_categorical")

        last_date = spark.sql(ultima_data_query)

        last_date = last_date.collect()[0]["time"]
        start_processing_date = (last_date - MonthBegin(2))
        query_start_date = \
            start_processing_date.to_pydatetime().date().isoformat()

    except:
        query_start_date = "1900-01-01"

    df_debtcontacts.createOrReplaceTempView("debts")
    df_arrangementinstallments.createOrReplaceTempView("inst")

    # Olhando a tabela que contem todos os installments
    print("# Verificando os acordos que estão quebrados")
    today = datetime.today()
    quebrados_installments = spark.sql(f"""
            SELECT inst.ArrangementID AS arrangement_id,
                   inst.AgencyID AS agency_id,
                   inst.PortfolioID AS portfolio_id,
                   inst.DebtID AS debt_id,
                   debts.CustomerID AS customer_id,
                   MAX(inst.TotalInstallments) AS total_installments,
                   MIN(inst.InstallmentDate) AS installment_date,
                   MIN(inst.InstallmentNumber) AS installment_number
            FROM inst
            JOIN debts
                ON debts.DebtID = inst.DebtID
            WHERE InstallmentStatus = 'Quebrado'
                AND inst.CreationDate >= DATE('{today}' - INTERVAL 1 YEAR)
            GROUP BY inst.ArrangementID,
                     inst.AgencyID,
                     inst.PortfolioID,
                     inst.DebtID,
                     debts.CustomerID
            HAVING '{query_start_date}' <= MIN(inst.InstallmentDate)
            """)

    print("# Verificando os acordos que foram quebrados")
    pagos_installments = spark.sql(f"""
            SELECT inst.ArrangementID AS arrangement_id,
                   inst.AgencyID AS agency_id,
                   inst.PortfolioID AS portfolio_id,
                   inst.DebtID AS debt_id,
                   debts.CustomerID AS customer_id,
                   inst.TotalInstallments AS total_installments,
                   inst.InstallmentDate AS installment_date,
                   inst.InstallmentNumber AS installment_number
            FROM  inst
            JOIN  debts
                ON debts.DebtID = inst.DebtID
            WHERE InstallmentStatus = 'Pago'
              AND inst.CreationDate >= DATE('{today}' - INTERVAL 1 YEAR)
              AND '{query_start_date}' <= inst.InstallmentDate
            """)

    # Quebra
    print("# Calculando o status das dívidas")
    quebrados_installments = quebrados_installments\
                            .withColumn("status",
                            F.when(
                                F.col("installment_number") == 1, "quebra_promessa"
                            ).otherwise("quebra"))

    # Pagamento
    pagos_installments = pagos_installments\
                        .withColumn("status",
                        F.when(
                            (F.col("installment_number") == 1) &\
                            (F.col("installment_number") == F.col("total_installments")), "avista"
                        ).when(
                            (F.col("installment_number") != 1) &\
                            (F.col("installment_number") == F.col("total_installments")), "quitada"
                        ).when(
                            F.col("installment_number") == 1, "cash_novo"
                        ).otherwise("pago_colchao"))

    # Unindo bases
    all_installments = quebrados_installments.unionByName(pagos_installments)

    # de-para status
    values = [('quebra_promessa', 1),
                ('quebra', 2),
                ('cash_novo', 3),
                ('pago_colchao', 4),
                ('avista', 5),
                ('quitada', 6)]
    cols = ['status', 'status_id']
    de_para = spark.createDataFrame(values, cols)

    length_before = all_installments.count()
    all_installments = all_installments.join(F.broadcast(de_para), "status")
    assert all_installments.count() == length_before,\
        "Erro ao fazer o de_para dos status da dívida"

    # Todas as datas são convertidas para o primeiro dia do mês
    all_installments = all_installments.withColumn("installment_date",
                        F.trunc("installment_date", "month"))

    agg_all_data = all_installments.groupBy([
        "installment_date", "portfolio_id", "debt_id", "customer_id"
        ]).agg(F.max("status_id").alias("max"),
                F.min("status_id").alias("min"))


    # Seleciona todas as tuplas onde min != max e divide a quantidade de
    # registros retornados pelo total de registros (equivalente ao mean())

    diff_status = agg_all_data.filter("max != min")
    diff_status = diff_status.count() / agg_all_data.count()
    print("% de dividas com status distintos no mês: {:.2%}".format(
        diff_status))

    max_status = agg_all_data.withColumnRenamed("max", "status_id")\
                            .withColumnRenamed("installment_date", "time")\
                            .drop("min")

    length_before = max_status.count()
    max_status = max_status.join(F.broadcast(de_para), "status_id")
    error_msg = (
        "O tamanho dos dataframes não coencide: "
        "max_status.count() == length_before")
    assert max_status.count() == length_before, error_msg


    # Verificando as datas finais e cortando arrangement que estão
    # depois de hoje

    today = datetime.today().strftime("%Y-%m-%d")
    max_status = max_status.filter(F.col("time").cast("date") < today)
    min_time, max_time = max_status.select(F.min("time").cast("date"),
                                           F.max("time").cast("date")).first()


    # Criando a variável de status máximo
    finished = max_status.filter(F.col("status_id").isin([5, 6]))
    finished = finished.withColumnRenamed("time", "finished__time")

    month_list = list(pd.date_range(start=min_time, end=max_time, freq="MS"))

    temp_data = max_status.filter(F.col("time").isin(month_list))\
                          .groupBy("portfolio_id", "debt_id", "customer_id", "time")\
                          .agg(F.max("status_id").alias("status_id"))

    temp_data = temp_data.withColumn("time", F.col("time").cast("date"))

    finished_debt_ids = finished.select(F.col("debt_id"))\
                        .filter(F.col("finished__time") <= month_list[-1])

    max_status_all_time = temp_data.join(finished_debt_ids, "debt_id", "left_anti")

    length_before = max_status_all_time.count()
    max_status_all_time = max_status_all_time.join(F.broadcast(de_para), "status_id")
    error_msg = (
        "O tamanho dos dataframes não coencide: "
        "max_status_all_time.count() == length_before")
    assert max_status_all_time.count() == length_before, error_msg

    # Colocando os nomes nas variáveis e unindo os data sets
    max_status = max_status.withColumn("variable", F.lit("arrangement_status"))
    max_status = max_status.drop("status_id")

    max_status_all_time = max_status_all_time.withColumn("variable", F.lit("max_arrangement_status"))
    max_status_all_time = max_status_all_time.drop("status_id")
    all_data = max_status.unionByName(max_status_all_time)
    all_data = all_data.withColumnRenamed("status", "value")

    # Ajustando os ids das variáveis
    all_data = all_data.withColumn("variable_id",
                        F.when(F.col("variable") == "arrangement_status", 17)\
                        .otherwise(18))
    all_data = all_data.drop("variable")

    # Verificando o portfolio age
    df_ftcrm_portfolios = read_data_from_lake(ftcrm_porfolios)
    df_ftcrm_portfolios.createOrReplaceTempView("ftcrm_dbo_porfolios")
    portfolios = spark.sql("""
                SELECT PortfolioID AS portfolio_id,
                       cast(PortfolioDate As Date) AS portfolio_date
                FROM ftcrm_dbo_porfolios
                """)

    portfolios = portfolios.withColumn("portfolio_date", F.trunc("portfolio_date", "month"))

    all_data = all_data.join(portfolios, "portfolio_id")
    all_data = all_data.withColumn("portfolio_age", F.round(\
                        F.datediff( F.col("time"), F.col("portfolio_date") ) / 365.25)\
                        .cast("int"))
    all_data = all_data.drop("portfolio_date")


    num_repartitions = ceil(all_data.count() / MAX_REGISTERS_REPARTITION)

    all_data = all_data.repartition(num_repartitions)

    # Salva os dados no bucket
    write_data_bucket(all_data, debt_time_categorical, "append")

    job.commit()
