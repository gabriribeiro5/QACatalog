#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que atualiza os dados de recorrência de customers. #
# Autor: Edinor Cunha Junior - NTT DATA                                       #
# Data: Out/2021                                                              #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
#                                                                             #
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



if __name__ == "__main__":
        #args = getResolvedOptions(sys.argv, ['JOB_NAME',
        #                                        'FTCRM_PORTFOLIOS',
        #                                        'FTCRM_DEBTS',
        #                                        'FTCRM_BINDINGS',
        #                                        'FTCRM_DEBTTRANSACTIONS',
        #                                        'FTCRM_DEBTTRANSACTIONCODES',
        #                                        'CUSTOMER_TIME_NUMERICAL'
        #                                        ])
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        # Criação da conexão Spark
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        #ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
        #ftcrm_debts = args["FTCRM_DEBTS"]
        #ftcrm_bindings = args["FTCRM_BINDINGS"]
        #ftcrm_debttransactioncodes = args["FTCRM_DEBTTRANSATIONCODES"]
        #ftcrm_debttransactions = args['FTCRM_DEBTTRANSACTIONS']
        #customer_time_numerical = args['CUSTOMER_TIME_NUMERICAL']
        ftcrm_portfolios                = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/portfolios/"
        ftcrm_debts                     = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/debts/"
        ftcrm_bindings                  = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/bindings/"
        ftcrm_debttransactionscodes     = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/debttransactioncodes/"
        ftcrm_debttransactions          = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/debttransactions/"     
        customer_time_numerical         = "s3://dlr-dev-bucket-refinedzone/models/customer_time_numerical"

        # Load dos arquivos Parquet para criação do processo ETL
        portfolio = spark.read.format("parquet")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .option("spark.sql.parquet.compression.codec", "snappy")\
                        .option("encoding", "UTF-8")\
                        .load(ftcrm_portfolios)

        debts = spark.read.format("parquet")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .option("spark.sql.parquet.compression.codec", "snappy")\
                        .option("encoding", "UTF-8")\
                        .load(ftcrm_debts)

        bindings = spark.read.format("parquet")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .option("spark.sql.parquet.compression.codec", "snappy")\
                        .option("encoding", "UTF-8")\
                        .load(ftcrm_bindings)

        transaction_code = spark.read.format("parquet")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .option("spark.sql.parquet.compression.codec", "snappy")\
                        .option("encoding", "UTF-8")\
                        .load(ftcrm_debttransactionscodes)

        transaction = spark.read.format("parquet")\
                        .option("header", True)\
                        .option("inferSchema", True)\
                        .option("spark.sql.parquet.compression.codec", "snappy")\
                        .option("encoding", "UTF-8")\
                        .load(ftcrm_debttransactions)

        # Criação das Views Temporárias
        portfolio.createOrReplaceTempView("dbo_portfolio")
        debts.createOrReplaceTempView("dbo_debts")
        bindings.createOrReplaceTempView("dbo_bindings")
        transaction.createOrReplaceTempView("dbo_transaction")
        transaction_code.createOrReplaceTempView("dbo_transaction_code")

        """Verificando até que mês foram processados os pagamentos"""
        try:
                df_customer_time_numerical = spark.read.format("parquet") \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("spark.sql.parquet.compression.codec", "snappy") \
                .option("encoding", "UTF-8") \
                .load(customer_time_numerical)

                df_customer_time_numerical.createOrReplaceTempView("database__customer_time_numerical")

                range_time = spark.sql("""
                        SELECT MIN(time),
                                MAX(time)
                        FROM database__customer_time_numerical
                        WHERE variable_id IN (57, 58)
                        """)

                """Pega o primeiro dia do mês da maior data e converte para string"""
                start_date = range_time.collect()[0]["max"].replace(day=1).strftime("%Y-%m-%d")
        except:
                start_date = "1990-01-01"

        """Get last payment and end-date of each portifolio"""					   
        portfolio_dates = spark.sql(f"""
                                SELECT portfolio.portfolioid AS portfolio_id,
                                portfolio.PortfolioDate AS portfolio_date
                                FROM  dbo_portfolio as portfolio
                                WHERE '{start_date}' <= PortfolioDate
                                """)
        portfolio_dates.show(2)    
        portfolio_dates = portfolio_dates.withColumn("portfolio_date", month(portfolio_dates.portfolio_date))
        portfolio_dates.show(2)
        all_port_start_months = []
        
        for row in portfolio_dates.select('portfolio_date').distinct().collect():
            all_port_start_months.append(row.portfolio_date)

        for start_month in all_port_start_months:
                print("## Processing month: ", start_month)

                portfolio_count = spark.sql(f"""
                                        SELECT contacts.customerid AS customer_id,
                                        COUNT(*) AS value
                                        FROM dbo_debts as debts
                                        JOIN dbo_portfolio as portfolio
                                        ON portfolio.PortfolioID = debts.PortfolioID
                                        JOIN dbo_bindings as contacts
                                        ON contacts.DebtID = debts.DebtID
                                        WHERE 0 < debts.OriginalFirstDefaultBalance
                                        AND portfolio.PortfolioDate <= '{start_month}' 
                                        GROUP BY contacts.CustomerID
                                        """)
                portfolio_count.show(2)
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
                                
                payment_count.show(2)
                
                output = portfolio_count.union(payment_count)

                output.write.format("parquet"
                        ).option("header", True
                        ).option("spark.sql.parquet.compression.codec", "snappy"
                        ).option("encoding", "UTF-8"
                        ).mode("append"
                        ).save(customer_time_numerical)

        job.commit()