#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark para processo ETL dos modelos da Murabei           #
# Autor: Edinor Cunha Junior - NTT DATA                                       #
# Data: Nov/2021                                                              #
# Versão: 2.0                                                                 #
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
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import trim, col, regexp_replace, expr, lower

# Variaveis de ambient passadas pelo Glue
#args = getResolvedOptions(sys.argv, ['JOB_NAME',
#                                     'DATAMART_DEBTCONTACTS',
#                                     'FTCRM_PORTFOLIOS',
#                                     'DEBT_INFO'
#                                     ])
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Criação da conexão Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variaveis do script recebendo os valores do dicionario de argumentos do Glue
#debt_contacts = args['DATAMART_DEBTCONTACTS']
#ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
debt_info = "s3://dlr-dev-bucket-refinedzone/models/debt_info/"
debt_contacts = "s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/"
ftcrm_portfolios = "s3://dlr-dev-bucket-rawzone/pic/ftcrm/portfolios/"


# Load dos arquivos parquet para construção do processo ETL
debtContacts = glueContext.spark_session.read.format("parquet") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .load(debt_contacts)

ftcrmPortfolios = glueContext.spark_session.read.format("parquet") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .load(ftcrm_portfolios)
        
'''Criando as tabelas temporarias'''
debtContacts.createOrReplaceTempView("DebtContacts")
ftcrmPortfolios.createOrReplaceTempView("Portfolios")

try:
    debtInfo = glueContext.spark_session.read.format("parquet") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .load(debt_info)

    min_portfolio_id = debtInfo.agg({"portfolio_id": "min"}).collect()[0][0]
        
    debtInfo.createOrReplaceTempView("database__debt_info")
except:
    min_portfolio_id = -1

    debtInfo = glueContext.spark_session.read.format("parquet") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("spark.sql.parquet.compression.codec", "snappy") \
    .option("encoding", "UTF-8") \
    .load(debt_contacts)
        
    debtInfo.createOrReplaceTempView("database__debt_info")

'''Carregando os porfolios'''
portfolios_id = spark.sql(
    """SELECT DISTINCT PortfolioID AS portfolio_id
       FROM DebtContacts crm WHERE {min_portfolio_id} <= PortfolioID"""
       .format(min_portfolio_id=min_portfolio_id))

def update_debt_info(portfolio_id, debt_info):
    for row in portfolios_id.collect():
        portfolio_id = row['portfolio_id']
        postgres_debt_id = spark.sql(
            """SELECT DISTINCT PortfolioID AS portfolio_postgres,
                DebtID,
                1 AS in_postgres
                FROM database__debt_info
                WHERE PortfolioID = {portfolio_id}""".format(portfolio_id=portfolio_id))

        sqlserver_debt_id = spark.sql(
            """SELECT port.PortfolioDate, crm.*, 1 AS in_sqlserver
            FROM DebtContacts crm
            JOIN Portfolios port
                    ON port.PortfolioID = crm.PortfolioID
                WHERE crm.PortfolioID = {portfolio_id}"""
                .format(portfolio_id=portfolio_id))
        
        sqlserver_debt_id = sqlserver_debt_id.withColumnRenamed('PortfolioID','portfolio_sqlserver') \
                            .withColumnRenamed('DebtID', 'debt_id')
                            
        postgres_debt_id = postgres_debt_id.withColumnRenamed('PortfolioID','portfolio_postgres') \
                            .withColumnRenamed('DebtID', 'debt_id')


        '''Join das Tabelas'''
        merged = sqlserver_debt_id.alias("s").join(postgres_debt_id.alias("p"), \
                 sqlserver_debt_id.debt_id == postgres_debt_id.debt_id, 'inner') \
                 .selectExpr("s.*", "p.in_postgres", "p.portfolio_postgres")

        merged = merged.na.fill(value=0,subset=["in_sqlserver", "in_postgres"])

        to_add = merged.filter( (merged.in_sqlserver == "1") & (merged.in_postgres != "1") )

        to_change_portfolio = merged.filter( (merged.in_sqlserver == "1") &\
                                    (merged.in_postgres == "1") &\
                                    (merged.portfolio_postgres != merged.portfolio_sqlserver) )

        #################################################
        # Removendo debtos que tem mudança de portfolio #

        if to_change_portfolio.count() > 0:
            
            debts_to_remove = ",".join([
                str(x) for x in to_change_portfolio.select("debt_id").distinct()])
            
            debt_info = spark.sql(
                """DELETE FROM database__debt_info AS info
                   WHERE DebtID IN ({debt_id})""".format(debt_id=debts_to_remove))
            
        else:
            pass
        
        ###############################################

        #######################
        # Processando os débitos
        if to_change_portfolio.count() > 0 and to_add.count() > 0:
            to_process = to_change_portfolio.union(to_add)
        else:
            to_process = merged
       
        to_process = to_process.drop("State")
        to_process = to_process.withColumnRenamed("Portfolio", "portfolio") \
                        .withColumnRenamed("portfolio_sqlserver", "portfolio_id") \
                        .withColumnRenamed("DebtID", "debt_id") \
                        .withColumnRenamed("CustomerID", "customer_id") \
                        .withColumnRenamed("PortfolioDate", "portfolio_date") \
                        .withColumnRenamed("FirstDefaultDate", "first_default_date") \
                        .withColumnRenamed("OpenDate", "open_date") \
                        .withColumnRenamed("OriginalFirstDefaultBalance", "reference_balance") \
                        .withColumnRenamed("IdentityType", "customer_type") \
                        .withColumnRenamed("StateAlias", "state",) \
                        .withColumnRenamed("City", "city") \
                        .withColumnRenamed("Products", "product")
                        
       
        to_process.show(2)
        to_process = to_process.withColumn("state", trim(col("state"))) \
                     .withColumn("city", trim(col("city"))) \
                     .withColumn("product", trim(col("product")))
      
        to_process = to_process.withColumn("product", regexp_replace("product", " ", "_")) \
                     .withColumn("product", lower(col("product")))
       
        to_process = to_process.fillna('#missing', subset=["state", "city", "product"])

        outputdf = to_process.withColumn("state", regexp_replace("state", "#missing", "")) \
                     .withColumn("city", regexp_replace("city", "#missing", "")) \
                     .withColumn("product", regexp_replace("product", "#missing", ""))
        
        outputdf.write.format("parquet"
                ).option("header", True
                ).option("spark.sql.parquet.compression.codec", "snappy"
                ).option("encoding", "UTF-8"
                ).mode("append"
                ).save(debt_info)


if __name__ == "__main__":
    update_debt_info(portfolios_id, debt_info)
    job.commit()
