#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark para processo ETL dos modelos da Murabei           #
# Autor: Edinor Cunha Junior - NTT DATA                                       #
# Data: Nov/2021                                                              #
# Versão: 1.0                                                                 #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Processo ETL para transformação dos dados da DebContacts que irá alimentar  #
# o modelo preditivo da Murabei.                                              #
# obs: a versão 1.0 é para homologação da lógica aplicada ao processo         #
#----------------------------- Parâmetros ------------------------------------#
#                                                                             #
#>>> DATAMART_DEBTCONTACTS = Referência no S3 onde estão os dados do datamart #
# DebtContacts.                                                               #
#>>> FTCRM_PORTFOLIOS = Referência no S3 onde estão os dados da tabela        #
# FTCRM.dbo.Portfolios                                                        #
#>>> DEBT_INFO = Referência no S3 onde serão armazenados os dados do processo #
# de ETL.                                                                     #                                                  
#                                                                             # 
#=============================================================================#

import sys
import datetime
from functools import reduce
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions 
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, format_string, regexp_replace, lower, when

def union_all(*dfs):
        return reduce(DataFrame.union, dfs)


if __name__ == "__main__":
        # Variaveis de ambient passadas pelo Glue
        #args = getResolvedOptions(sys.argv, ['JOB_NAME',
        #                                     'DATAMART_PAYMENTS',
        #                                     'DEBT_AGENCY_TIME_NUMERICAL',
        #                                     'FTCRM_PORTFOLIOS'
        #                                     ])
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])

        # Criação da conexão Spark
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        # Variaveis do script recebendo os valores do dicionario de argumentos do Glue
        #datamart_payments = args['DATAMART_PAYMENTS']
        #debt_agency      =  args['DEBT_AGENCY_TIME_NUMERICAL']
        #ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
        datamart_payments = "s3://dlr-dev-bucket-refinedzone/pic/datamart/payments/"
        debt_agency_time_numerical = "s3://dlr-dev-bucket-refinedzone/models/debt_agency_time_numerical/"
        ftcrm_portfolios = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/portfolios/"

        today = datetime.date.today()

        # Load dos arquivos parquet para construção do processo ETL
        payments = glueContext.spark_session.read.format("parquet") \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("spark.sql.parquet.compression.codec", "snappy") \
                .option("encoding", "UTF-8") \
                .load(datamart_payments)

        portfolios = glueContext.spark_session.read.format("parquet") \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("spark.sql.parquet.compression.codec", "snappy") \
                .option("encoding", "UTF-8") \
                .load(ftcrm_portfolios)

        # Criação das Views Temporárias
        portfolios.createOrReplaceTempView("Portfolios")
        payments.createOrReplaceTempView("Payments")

        """Buscando as datas no banco de dados"""
        try:
                df_debt_agency_time_numerical = spark.read.format("parquet") \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("spark.sql.parquet.compression.codec", "snappy") \
                .option("encoding", "UTF-8") \
                .load(debt_agency_time_numerical)

                df_debt_agency_time_numerical.createOrReplaceTempView("database__debt_agency__time_numerical")

                last_payment_date = spark.sql("""
                        SELECT MIN(time) AS min_time,
                        MAX(time) AS max_time
                        FROM database__debt_agency__time_numerical
                        WHERE variable_id = 20
                        AND time <= '{today}'
                        """)
        except:
                last_payment_date = None
        
        query_start_date = "2019-01-01"

        if last_payment_date is not None:
                query_start_date = today
        else:
                pass

        """ Buscando os pagamentos aberto por agencia"""
        payments = spark.sql(f"""
        SELECT PortfolioID AS portfolio_id,
           DebtID AS debt_id,
           CustomerID AS customer_id,
           ChannelType AS channel_type,
           AgencyID AS agency_id,
           ArrangementID AS arrangement_id,
           ArrangementAmount AS arrangement_amount,
           TotalInstallments AS total_installments,
           PaymentMethodID AS payment_method_id,
           PaymentMethod AS payment_method,
           InstallmentNumber AS installment_number,
           RefDate AS ref_date,
           AccountingDate AS accounting_date,
           CollectionAmount AS collection_amount,
           CommissionAmount AS commission_amount
        FROM Payments
        WHERE '{query_start_date}' <= AccountingDate 
        """)

        
        # Tratando os dados de pagamento que não tem agência
        payments = payments.fillna(-1, subset=["agency_id"])
        payments = payments.fillna('Não especificado', subset=["payment_method"])

        ########################################################################
        # Verificando o pagamento total para cada um dos meses em cada agência #
        collection_by_agency = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
        "agency_id"]).agg({'collection_amount':'sum'})

        collection_by_agency = collection_by_agency.withColumn("variable", lit('collection_total'))

        ########################################################################
        # Coleta por tipo de canal                                                   
        collection_by_channel_type = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
        "agency_id", "channel_type"]).agg({'collection_amount':'sum'})

        collection_by_channel_type = collection_by_channel_type.withColumn("channel_type", regexp_replace("channel_type", " ", "_")) \
                                                        .withColumn("channel_type", lower(col("channel_type"))) \
                                                        .withColumn("variable", format_string("collection__%s", col('channel_type')))

        ########################################################################
        # Coleta por cash novo ou não
        payments = payments.withColumn("tipo_cash", when(col("installment_number") == 1, \
                lit('cash_novo')).otherwise(lit('colchao')))

        collection_by_tipo_cash = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
            "agency_id", "tipo_cash"]).agg({'collection_amount': 'sum'})
        
        collection_by_tipo_cash = collection_by_tipo_cash.withColumn("tipo_cash", regexp_replace("tipo_cash", " ", "_")) \
                                                        .withColumn("tipo_cash", lower(col("tipo_cash"))) \
                                                        .withColumn("variable", format_string("collection__%s", col('tipo_cash')))

        ########################################################################
        # Coleta por cash novo ou não e canal
        collection_by_channel_type_tipo_cash = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
            "agency_id", "channel_type", "tipo_cash"]).agg({'collection_amount': 'sum'})
        
        collection_by_channel_type_tipo_cash = collection_by_channel_type_tipo_cash\
                .withColumn("tipo_cash", regexp_replace("tipo_cash", " ", "_")) \
                .withColumn("tipo_cash", lower(col("tipo_cash"))) \
                .withColumn("channel_type", regexp_replace("channel_type", " ", "_")) \
                .withColumn("channel_type", lower(col("channel_type"))) \
                .withColumn("variable", format_string("collection__%s__%s", col('tipo_cash'), col('channel_type')))
        
        ########################################################################
        # Coleta metodo de pagamento #
        collection_by_payment_method = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
            "agency_id", "channel_type", "payment_method"]).agg({'collection_amount': 'sum'})
        
        collection_by_payment_method = collection_by_payment_method\
                .withColumn("payment_method", regexp_replace("payment_method", " ", "_")) \
                .withColumn("payment_method", lower(col("payment_method"))) \
                .withColumn("variable", format_string("collection__%s", col('payment_method')))

        
        ########################################################################
        # Coleta metodo de pagamento / canal de pagamento # 
        collection_by_payment_channel_type = payments.groupBy(["ref_date", "portfolio_id", "debt_id", "customer_id",
            "agency_id", "payment_method", "channel_type"]).agg({'collection_amount': 'sum'})
        
        collection_by_payment_channel_type = collection_by_payment_channel_type\
                .withColumn("payment_method", regexp_replace("payment_method", " ", "_")) \
                .withColumn("payment_method", lower(col("payment_method"))) \
                .withColumn("channel_type", regexp_replace("channel_type", " ", "_")) \
                .withColumn("channel_type", lower(col("channel_type"))) \
                .withColumn("variable", format_string("collection__%s__%s", col('payment_method'), col('channel_type')))
        
        ########################################################################
        # Concatenando todas as variaveis
        columns = ["ref_date", "portfolio_id", "debt_id", "customer_id", "agency_id",
        "variable", "collection_amount"]

        all_variables = union_all(collection_by_agency.select(*columns), collection_by_channel_type.select(*columns), 
        collection_by_tipo_cash.select(*columns), collection_by_channel_type_tipo_cash.select(*columns), 
        collection_by_payment_method.select(*columns), collection_by_payment_channel_type.select(*columns))

        output = all_variables.withColumnRenamed("ref_date", "time") \
                        .withColumnRenamd("collection_amount", "value")
        
        output.write.format("parquet"
                ).option("header", True
                ).option("spark.sql.parquet.compression.codec", "snappy"
                ).option("encoding", "UTF-8"
                ).mode("append"
                ).save(debt_agency_time_numerical)
        
        job.commit()
    