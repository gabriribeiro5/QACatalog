#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que atualiza os dados de recorrência de customers. #
# Autor: Edinor Cunha Junior - NTT DATA                                       #
# Data: Dez/2021                                                              #
# Versão: 2.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
# A versão 2.0 tem o foco em apresentar uma solução otimizada do processo de  #
# ETL para os modelos da Murabei.                                             #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> FTCRM_PORTFOLIOS = Referência no S3 onde estão os dados da tabela        #
# FTCRM_PORTFOLIOS.                                                           #
#>>> FTCRM_DEBTS = Referência no S3 onde estão os dados da tabela FTCRM_DEBTS #
#>>> FTCRM_BINDINGS = Referência no S3 onde estão os dados da tabela          #
# FTCRM_BINDINGS.                                                             #
#>>> FTCRM_DEBTTRANSACTIONS = Referência no S3 onde estão os dados da         #
# tabela FTCRM_DEBTTRANSACTIONS.                                              #
#>>> FTCRM_DEBTTRANSACTIONCODES = Referência no S3 onde estão os dados da     #
# tabela FTCRM_DEBTTRANSACTIONCODES.                                          #
#                                                                             #
#=============================================================================#

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from math import ceil
from datetime import datetime, timedelta
from pyspark.sql.functions import date_format, col, month, when, to_date, countDistinct, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                    'FTCRM_PORTFOLIOS',
                                    'FTCRM_DEBTS',
                                    'FTCRM_BINDINGS',
                                    'FTCRM_DEBTTRANSACTIONS',
                                    'FTCRM_DEBTTRANSACTIONCODES',
                                    'CUSTOMER_TIME_NUMERICAL'
                                    ])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
ftcrm_debts = args["FTCRM_DEBTS"]
ftcrm_bindings = args["FTCRM_BINDINGS"]
ftcrm_debttransactioncodes = args["FTCRM_DEBTTRANSATIONCODES"]
ftcrm_debttransactions = args['FTCRM_DEBTTRANSACTIONS']
customer_time_numerical = args['CUSTOMER_TIME_NUMERICAL']

'''função para carregar arquivos Parquet'''
def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)
    return df

''' Função para validar o último processamento '''
def verifica_start_date(s3_path, database, *args): 
    try:
            df = read_data_from_lake(s3_path)

            df.createOrReplaceTempView(database)

            range_time = spark.sql("""
                    SELECT MIN(time),
                            MAX(time)
                    FROM {database}
                    WHERE variable_id IN ({args})
                    """)

            """Pega o primeiro dia do mês da maior data e converte para string"""
            start_date = range_time.collect()[0]["max"].replace(day=1).strftime("%Y-%m-%d")
            return start_date
    except:
            start_date = "1990-01-01"
            return start_date


''' Função para filtrar o mês para processar '''
def read_month(list_month):
        index = (len(list_month) - 1)
        return index

''' Função que gera o arquivo parquet '''
def update_customer_recurrence(output, s3_path):
        output.write.format("parquet"
        ).option("header", True
        ).option("spark.sql.parquet.compression.codec", "snappy"
        ).option("encoding", "UTF-8"
        ).mode("append"
        ).save(s3_path)

''' Load das tabelas armazenadas '''
portfolio = read_data_from_lake(ftcrm_portfolios)
debts = read_data_from_lake(ftcrm_debts)
bindings = read_data_from_lake(ftcrm_bindings)
transaction_codes = read_data_from_lake(ftcrm_debttransactioncodes)
transactions = read_data_from_lake(ftcrm_debttransactions)

''' Criação das Views Temporárias '''
portfolio.createOrReplaceTempView("dbo_portfolio")
debts.createOrReplaceTempView("dbo_debts")
bindings.createOrReplaceTempView("dbo_bindings")
transactions.createOrReplaceTempView("dbo_transaction")
transaction_codes.createOrReplaceTempView("dbo_transaction_code")

"""Verificando até que mês foram processados os pagamentos"""
start_date = verifica_start_date(customer_time_numerical, 'database__customer_time_numerical', 57, 58)

"""Get last payment and end-date of each portifolio"""					   
portfolio_dates = spark.sql(f"""
                        SELECT portfolio.portfolioid AS portfolio_id,
                        portfolio.PortfolioDate AS portfolio_date
                        FROM  dbo_portfolio as portfolio
                        WHERE '{start_date}' <= PortfolioDate
                        """)

portfolio_dates = portfolio_dates.withColumn("portfolio_date", month(portfolio_dates.portfolio_date))

all_port_start_months = []

for row in portfolio_dates.select('portfolio_date').distinct().collect():
    all_port_start_months.append(row.portfolio_date)

start_month = read_month(all_port_start_months)

portfolio_count = spark.sql(f"""
                        SELECT contacts.customerid AS customer_id,
                        COUNT(*) AS value,
                        FROM dbo_debts as debts
                        JOIN dbo_portfolio as portfolio
                        ON portfolio.PortfolioID = debts.PortfolioID
                        JOIN dbo_bindings as contacts
                        ON contacts.DebtID = debts.DebtID
                        WHERE 0 < debts.OriginalFirstDefaultBalance
                        AND portfolio.PortfolioDate <= '{start_month}' 
                        GROUP BY contacts.CustomerID
                        """)

portfolio_count = portfolio_count.withColumn("time", lit(start_month)) \
                                .withColumn("variable_id", lit(57))

payment_count = spark.sql(f"""
                        SELECT contacts.CustomerID AS customer_id, 
                        count(debt_trans.Amount) AS value
                        FROM  dbo_transaction AS debt_trans
                        JOIN dbo_debts AS debts
                        ON debts.DebtID = debt_trans.DebtID
                        JOIN dbo_bindings as contacts
                        ON contacts.DebtID = debts.DebtID
                        JOIN dbo_transaction_code AS transaction_code
                        ON debt_trans.TransactionCode = transaction_code.TransactionCode
                        WHERE debt_trans.AccountingDate < '{start_month}' 
                        AND 0 < debt_trans.Amount
                        AND transaction_code.Alias = 'Payment'
                        GROUP BY contacts.CustomerID
                        """)

payment_count = payment_count.withColumn("time", lit(start_month)) \
                                .withColumn("variable_id", lit(58))
                
output = portfolio_count.union(payment_count)

if __name__ == "__main__":
        update_customer_recurrence(output, customer_time_numerical)
        job.commit()
