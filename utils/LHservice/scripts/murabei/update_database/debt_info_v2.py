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
# obs: a versão 2.0 é para otimização dos processos                           #
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
from pyspark.sql.functions import trim, col, regexp_replace, lower

'''Variaveis de ambiente passadas pelo Glue'''
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'DATAMART_DEBTCONTACTS',
                                     'FTCRM_PORTFOLIOS',
                                     'DEBT_INFO'
                                     ])

'''Criação da conexão Spark'''
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

'''Variaveis do script recebendo os valores do dicionario de argumentos do Glue'''
datamart_contacts = args['DATAMART_DEBTCONTACTS']
tabela_portfolios = args['FTCRM_PORTFOLIOS']
debt_info = args['DEBT_INFO']

'''função para carregar arquivos Parquet'''
def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)
    return df

'''Load dos arquivos parquet para construção do processo ETL'''
debt_contacts = read_data_from_lake(datamart_contacts)
ftcrm_portfolios = read_data_from_lake(tabela_portfolios)
        
'''Criando as tabelas temporarias'''
debt_contacts.createOrReplaceTempView("DebtContacts")
ftcrm_portfolios.createOrReplaceTempView("Portfolios")

'''Carregando as informações da Debt Info'''
try:
    debtInfo = read_data_from_lake(debt_info)

    min_portfolio_id = debtInfo.agg({"portfolio_id": "min"}).collect()[0][0]
        
    debtInfo.createOrReplaceTempView("database__debt_info")
except:
    min_portfolio_id = -1

    debtInfo = read_data_from_lake(datamart_contacts)
        
    debtInfo.createOrReplaceTempView("database__debt_info")

'''Carregando os porfolios'''
portfolios_id = spark.sql(
    """SELECT DISTINCT PortfolioID AS portfolio_id
       FROM DebtContacts crm WHERE {min_portfolio_id} <= PortfolioID"""
       .format(min_portfolio_id=min_portfolio_id))

'''Função para remover os débitos do portfolio'''
def remove_debitos(to_change_portfolio):
    if to_change_portfolio.count() > 0:
        debts_to_remove = ",".join([
            str(x) for x in to_change_portfolio.select("debt_id").distinct()])
        debt_info = spark.sql(
            """DELETE FROM database__debt_info AS info
                WHERE DebtID IN ({debt_id})""".format(debt_id=debts_to_remove))             
    else:
        pass

'''Função para processamento dos débitos'''
def processando_debitos(to_change_portfolio, to_add, merged):
    if to_change_portfolio.count() > 0 and to_add.count() > 0:
        to_process = to_change_portfolio.union(to_add)

        return to_process
    else:
        to_process = merged
        
        return to_process

'''Função para unir as tabelas'''
def join_tables(sqlserver_debt_id, postgres_debt_id):
    merged = sqlserver_debt_id.alias("s").join(postgres_debt_id.alias("p"), \
                sqlserver_debt_id.debt_id == postgres_debt_id.debt_id, 'inner') \
                .selectExpr("s.*", "p.in_postgres", "p.portfolio_postgres")

    merged = merged.na.fill(value=0,subset=["in_sqlserver", "in_postgres"])

    return merged

'''Função para selecionar os portfolios para adicionar na tabela'''
def portfolios_para_adicionar(merged):
    to_add = merged.filter( (merged.in_sqlserver == "1") & (merged.in_postgres != "1") )

    return to_add

'''Função para selecionar os portfolios para alterar na tabela'''
def portfolios_para_alterar(merged):
    to_change_portfolio = merged.filter( (merged.in_sqlserver == "1") &\
                                (merged.in_postgres == "1") &\
                                (merged.portfolio_postgres != merged.portfolio_sqlserver) )

    return to_change_portfolio

'''Função principal para atualizar a tabela'''
def update_debt_info(portfolios_id, debt_info):
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
        merged = join_tables(sqlserver_debt_id, postgres_debt_id)

        '''Criando portfolios para adicionar'''
        to_add = portfolios_para_adicionar(merged)

        '''Criando lista de portfolios para alterar'''
        to_change_portfolio = portfolios_para_alterar(merged)

        ''' Removendo debitos que tem mudança de portfolio '''
        remove_debitos(to_change_portfolio)
        
        ''' Processando os débitos '''
        to_process = processando_debitos(to_change_portfolio, to_add, merged)
       
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
