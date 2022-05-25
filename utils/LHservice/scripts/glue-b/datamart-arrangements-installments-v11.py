#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que cria datamart arrangements-installments        #
# Autor: Lucas Carvalho Roncoroni - NTT DATA                                  #
# Alteração: Edinor Cunha Junior					      #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script			      #
# Versão: 11.0                                                                #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart         #
# arrangements-installments na refined zone.                                  #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> FTF_RESOURCES = Referência no S3 onde estão os dados da tabela           #
# FTF_RESOURCES.                                                              #
#>>> FTF_RESOURCECAPTIONS = Referência no S3 onde estão os dados da tabela    #
# FTF_RESOURCECAPTIONS.                                                       #
#>>> FTCRM_CANCELLATIONREASONS = Referência no S3 onde estão os dados da      #
# tabela FTCRM_CANCELLATIONREASONS.                                           #
#>>> FTCRM_ARRANGEMENTS = Referência no S3 onde estão os dados da tabela      #
# FTCRM_ARRANGEMENTS.                                                         #
#>>> FTCRM_AGENTS = Referência no S3 onde estão os dados da tabela            #
# FTCRM_AGENTS.                                                               #
#>>> FTCRM_BUSSINESSUNITS = Referência no S3 onde estão os dados da tabela    #
# FTCRM_BUSSINESSUNITS.                                                       #
#>>> FTCRM_DIMDATE = Referência no S3 onde estão os dados da tabela           #  
# FTCRM_DIMDATE.                                                              #
#>>> DEBTCONTACTS = Referência no S3 onde estão os dados do datamart          #
# DEBTCONTACTS.                                                               #
#>>> FTCRM_ARRANGINSTALLMENTS = Referência no S3 onde estão os dados da       #
# tabela FTCRM_ARRANGINSTALLMENTS.                                            #
#>>> ARRANGEMENTSINSTALLMENTS =  Referência no S3 onde os dados do datamart   # 
# de ARRANGEMENTSINSTALLMENTS serão inseridos.                                #
# >>> ARRANGEMENTS = Referência no S3 onde estão os dados                     #
# do datamart arrangements                                                    #
#=============================================================================#

import sys
import json
from math import ceil
from asyncore import read
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import pandas as pd

from pyspark.sql.functions import *
from pyspark.context import SparkContext


# Inicialização do S3
s3 = boto3.resource('s3')


# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FTF_RESOURCES','FTF_RESOURCECAPTIONS', 
                        'FTCRM_CANCELLATIONREASONS', 'FTCRM_ARRANGEMENTS','ARRANGEMENTS',
                        'DEBTCONTACTS','FTCRM_ARRANGINSTALLMENTS','FTCRM_AGENTS',
                        'FTCRM_BUSINESSUNITS','ARRANGEMENTSINSTALLMENTS', 'LOG_PATH', 'PROCESS_TYPE'])

# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Variáveis utilizadas no script
job_name                           = args['JOB_NAME']
job_run_id                         = args['JOB_RUN_ID']
log_path                           = args['LOG_PATH']
process_type                       = args['PROCESS_TYPE']
ftf_resources                      = args['FTF_RESOURCES']
ftf_resource_captions              = args['FTF_RESOURCECAPTIONS']
ftcrm_cancellation_reasons         = args['FTCRM_CANCELLATIONREASONS']
ftcrm_arrangements                 = args['FTCRM_ARRANGEMENTS']
ftcrm_arrangement_installments     = args['FTCRM_ARRANGINSTALLMENTS']
ftcrm_agents                       = args['FTCRM_AGENTS']
ftcrm_bussinessunits               = args['FTCRM_BUSINESSUNITS']
datamart_arrangements_installments = args['ARRANGEMENTSINSTALLMENTS']
datamart_debtcontacts              = args['DEBTCONTACTS']
datamart_arrangements              = args['ARRANGEMENTS']


#######################################################################################################################
#Definição de funções

def get_last_timestamp():
    try:
        df = pd.read_csv('{}{}.csv'.format(log_path, args['JOB_NAME']))
        max_last_modification = df.loc[df['status'] == 'SUCCESS', 'lastmodificationdate'].max().strftime('%Y-%m-%d')
        return max_last_modification

    except:
        max_last_modification = datetime(1970,1,1).strftime('%Y-%m-%d')
        return max_last_modification
	

def save_log(proccess_type, job_run_id, status, n_rows, max_id, max_creation_date, last_modification_date, start_time, end_time, elapsed_time):
    log_data = {
        'proccess_type': proccess_type, 'job_id': job_run_id, 'status': status, 'total_rows': n_rows, 
        'max_id': max_id, 'max_creation_date': max_creation_date, 
        'last_modification_date': last_modification_date, 
        'start_time': start_time, 'end_time': end_time, 'elapsed_time': elapsed_time
        }
    print(json.dumps(log_data, indent=4, sort_keys=True))
    try: 
        df1 = pd.read_csv('{}{}.csv'.format(log_path, job_name))
        df2 = pd.DataFrame([log_data])
        df3 = df1.append(df2)
        df3.to_csv('{}{}.csv'.format(log_path, job_name), index=False)
    except:
        df = pd.DataFrame([log_data])
        df.to_csv('{}{}.csv'.format(log_path, job_name), index=False)

        
def lower_column_names(df):
	'''Padroniza em minúsculo os nomes das colunas.'''	
	return df.toDF(*[c.lower() for c in df.columns])


def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)

    return df

#######################################################################################################################
# Script principal


# Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")

# Carregando dados origem do S3
dboArrangements = read_data_from_lake(ftcrm_arrangements)
dboArrangementInstallments = read_data_from_lake(ftcrm_arrangement_installments)
dboAgents = read_data_from_lake(ftcrm_agents)
dboBusinessUnits = read_data_from_lake(ftcrm_bussinessunits)
dboResources = read_data_from_lake(ftf_resources)
dboResourceCaptions = read_data_from_lake(ftf_resource_captions)
dboCancellationReasons = read_data_from_lake(ftcrm_cancellation_reasons)
dboDebtContacts = read_data_from_lake(datamart_debtcontacts)
arrangementsMatrix = read_data_from_lake(datamart_arrangements)

# Carregando ultimo installmentid no datamart
try:
    lastInstallmentID = glueContext.spark_session.read.format("parquet") \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("spark.sql.parquet.compression.codec", "snappy") \
            .option("encoding", "UTF-8") \
            .load(datamart_arrangements_installments)

    lastInstallmentID = lastInstallmentID.agg({"installmentid": "max"}).collect()[0][0]
except:
    lastInstallmentID = -1

dboArrangements.createOrReplaceTempView("dboArrangements")
dboAgents.createOrReplaceTempView("dboAgents")
dboBusinessUnits.createOrReplaceTempView("dboBusinessUnits")
arrangementsMatrix.createOrReplaceTempView("arrangementsMatrix")
dboResources.createOrReplaceTempView("dboResources")
dboResourceCaptions.createOrReplaceTempView("dboResourceCaptions")
dboArrangementInstallments.createOrReplaceTempView("dboArrangementInstallments")
dboDebtContacts.createOrReplaceTempView("dboDebtContacts")
dboCancellationReasons.createOrReplaceTempView("dboCancellationReasons")

#  Auxiliar CancellationReasons
cancellationReasons = spark.sql("""
                                SELECT CR.CancellationReasonID, RC.Caption 
                                FROM dboCancellationReasons AS CR 
                                INNER JOIN dboResourceCaptions AS RC 
                                ON RC.ResourceID = 47 
                                AND RC.LanguageAlias = 'pt-BR'  
	                            AND CR.Alias = RC.OriginalAlias 
                                """)

cancellationReasons.createOrReplaceTempView("CancellationReasons")

# PaymentMethod
paymentMethod = spark.sql("""
    SELECT R.ResourceID, R.Name, OriginalAlias, RC.Caption 
	FROM dboResources AS R 
	INNER JOIN dboResourceCaptions AS RC ON R.ResourceID = RC.ResourceID 
	WHERE R.ResourceID = 62 AND LanguageAlias = 'pt-BR'
    """)


paymentMethod.createOrReplaceTempView("paymentMethod")


# PaymentSources
paymentSources = spark.sql("""
        SELECT R.ResourceID, R.Name, OriginalAlias, RC.Caption 
        FROM dboResources AS R 
        INNER JOIN dboResourceCaptions AS RC ON R.ResourceID = RC.ResourceID 
        WHERE R.ResourceID = 61 AND LanguageAlias = 'pt-BR'
        """)

paymentSources.createOrReplaceTempView("paymentSources")


# Datamart ArrangementInstallmentsMatrix
ArrangementInstallmentsMatrix = spark.sql(
    """SELECT A.ArrangementID, 
    AI.InstallmentID, 
    D.AccountOwnerID, 
    D.AccountOwner,
    D.PortfolioID, 
    D.Portfolio, 
    D.DebtID, 
    D.IdentityType, 
    D.IdentityNumber,
    D.ProductTypeID, 
    D.ProductTypeAlias, 
    D.Products, 
    D.OriginalFirstDefaultDate,
    D.FirstDefaultDate, 
    D.OriginalFirstDefaultBalance, 
    D.ScoreModelID,
    D.ScoreModelName,
    D.Score, 
    D.State, 
    D.StateAlias, 
    D.City, 
    D.Country, 
    D.Assets,
    D.Liabilities, 
    D.RestrictionStatus, 
    D.CanRestrict, 
    A.TotalInstallments, 
    AI.InstallmentNumber, 
    A.CreationDate, 
    A.CancellationReasonID, 
    CT.Caption AS CancellationReason, 
    A.CancellationDate AS CancellationDate, 
    AI.InstallmentDate,
    A.CreationDate AS Date,
    CAST(AI.InstallmentDate AS date) AS RefDate,
    CONVERT(char(6), AI.InstallmentDate, 112)
    A.ArrangementType,
    A.ArrangementStatus, 
    A.FollowUpID,  
    A.DelinquencyDays, 
    A.OriginalDebtPrincipal, 
    A.OriginalDebtTotalBalance,
    A.ArrangementAmount, 
    AI.InstallmentAmount, 
    AI.PaidAmount,
    AI.PaymentSource AS PaymentSourceID,
    PS.Caption AS PaymentSource,
    AI.PaymentMethod AS PaymentMethodID, 
    PM.Caption AS PaymentMethod,
    AI.InstallmentStatus AS InstallmentStatusID,
    CASE AI.InstallmentStatus 
    WHEN 61000 THEN 'Em Aberto'
    WHEN 61001 THEN 'Pago' 
    WHEN 61002 THEN 'Quebrado' 
    WHEN 61005 THEN 'Parcialmente Pago' 
    WHEN 61009 THEN 'Remessa Pendente' 
    END AS InstallmentStatus, 
    BU.BusinessUnitID AS AgencyID,
    BU.Alias AS Agency, 
    AG.AgentSID, 
    AG.Name AS AgentName, 
    CASE AG.Enabled 
    WHEN 1 THEN 'Active' 
    WHEN 0 THEN 'Inactive' 
    ELSE '' 
    END AS AgentEnabled, 
    CAST(MONTHS_BETWEEN(AG.CreationDate,CURRENT_DATE) AS INT) AS WorkMonths,
    A.LastModificationDate AS LastModificationDate 
	FROM dboDebtContacts AS D 
	INNER JOIN dboArrangements AS A ON D.DebtID = A.DebtID 
	INNER JOIN arrangementsMatrix AS AA ON A.ArrangementID = AA.ArrangementID 
	INNER JOIN dboArrangementInstallments AS AI ON A.ArrangementID = AI.ArrangementID 
	INNER JOIN dboAgents AS AG ON AI.AgentSID = AG.AgentSID 
	INNER JOIN dboBusinessUnits AS BU ON AG.BusinessUnitID = BU.BusinessUnitID 
	LEFT  JOIN CancellationReasons AS CT ON A.CancellationReasonID = CT.CancellationReasonID 
	LEFT  JOIN paymentSources AS PS ON AI.PaymentSource = PS.OriginalAlias 
	LEFT  JOIN paymentMethod AS PM ON AI.PaymentMethod = PM.OriginalAlias 
	WHERE AI.InstallmentID > {} """.format(lastInstallmentID)
)


outputDF = ArrangementInstallmentsMatrix.withColumn("year", date_format(col("RefDate"), "yyyy")) \
           .withColumn("month", date_format(col("RefDate"), "MM")) \
           .withColumn("day", date_format(col("RefDate"), "dd")) \
           .withColumn("refdate", date_format(col("RefDate"), "yyyyMM").cast("int"))



# Converte as colunas para lower case e escreve a tabela no S3
outputDF = lower_column_names(outputDF)

outputDF = outputDF.cache()

outputDF.write.partitionBy("year", "month", "day")  \
        .format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("append") \
        .save(datamart_arrangements_installments)


# Contagem dos registros inseridos
total_registers = outputDF.count()


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = outputDF.agg({"date": "max"}).collect()[0][0]
max_id = outputDF.agg({"installmentid": "max"}).collect()[0][0]
max_creation_date = outputDF.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Fim do script
print(f'Tempo de execução: {elapsed_time}')
print(f'Numero de registros inseridos: {total_registers}')
job.commit()