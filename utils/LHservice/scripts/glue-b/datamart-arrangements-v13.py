#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que cria datamart arrangements                     #
# Autor: Matheus Soares Rodrigues - NTT DATA                                  #
# Data de Criação: Out/2021                                                   #
# Alteração: Vanessa Barros Dos Santos						    			  #
# Data de Modificação: 22/Abr/2022                                            #
# Descrição Modificação: reestruturação geral do script						  #
# Versão: 13.0                                                                #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart         #
# arrangements na refined zone.                                               #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
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
#>>> ARRANGEMENTS = Referência no S3 onde os dados do datamart de             #
# ARRANGEMENTS serão inseridos.                                               #
#                                                                             #
#=============================================================================#

import sys
from datetime import datetime, timedelta
import sys
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql.functions import date_format, col, lit


# Inicialização do S3
s3 = boto3.resource('s3')


# Parâmetros Dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FTF_RESOURCECAPTIONS','FTCRM_CANCELLATIONREASONS', 
                                     'FTCRM_ARRANGEMENTS','FTCRM_AGENTS','FTCRM_BUSINESSUNITS',
                                     'FTCRM_DIMDATE','DEBTCONTACTS','ARRANGEMENTS', 'PROCESS_TYPE', 
                                     'LOG_PATH', 'FTCRM_WORKFLOWSTATUSES'])


# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variáveis utilizadas no script
job_name                   = args['JOB_NAME']
job_run_id                 = args['JOB_RUN_ID']
process_type               = args['PROCESS_TYPE']
log_path                   = args['LOG_PATH']
ftf_resource_captions      = args['FTF_RESOURCECAPTIONS']
ftcrm_cancellation_reasons = args['FTCRM_CANCELLATIONREASONS']
ftcrm_arrangements         = args['FTCRM_ARRANGEMENTS']
ftcrm_agents               = args['FTCRM_AGENTS']
ftcrm_bussinessunits       = args['FTCRM_BUSINESSUNITS']
ftcrm_dimdate              = args['FTCRM_DIMDATE']
ftcrm_workflowstatuses     = args['FTCRM_WORKFLOWSTATUSES']
datamart_debtcontacts      = args['DEBTCONTACTS']
datamart_arrangements      = args['ARRANGEMENTS']


#######################################################################################################################
#Definição de funções


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


# Leitura dos arquivos parquet das tabelas origem
cancellationReasons = read_data_from_lake(ftcrm_cancellation_reasons)
resourceCaptions = read_data_from_lake(ftf_resource_captions)
agents = read_data_from_lake(ftcrm_agents)
arrangements = read_data_from_lake(ftcrm_arrangements)
dimDate = read_data_from_lake(ftcrm_dimdate)
businessUnits = read_data_from_lake(ftcrm_bussinessunits)
debtContacts = read_data_from_lake(datamart_debtcontacts)
workflowstatuses = read_data_from_lake(ftcrm_workflowstatuses)
        
try:
    lastArrangementID = read_data_from_lake(datamart_arrangements)

    lastArrangementID = lastArrangementID.agg({"arrangementid": "max"}).collect()[0][0]
except:
    lastArrangementID = -1


# Criação das views
cancellationReasons.createOrReplaceTempView("CancellationReasons")
resourceCaptions.createOrReplaceTempView("ResourceCaptions")

cancellationReasonsDF = spark.sql(
        """SELECT CR.CancellationReasonID, RC.Caption 
        FROM CancellationReasons AS CR 
        INNER JOIN ResourceCaptions AS RC 
        ON RC.ResourceID = '47' 
        AND RC.LanguageAlias = 'pt-BR' 
        AND CR.Alias = RC.OriginalAlias""")
	
	
cancellationReasonsDF.createOrReplaceTempView("CancellationReasonsDF")

arrangementTypes = spark.sql(
    """SELECT OriginalAlias AS ArrangementTypeID, 
        Caption AS ArrangementType
		FROM ResourceCaptions 
        WHERE ResourceID = 89 AND LanguageAlias = 'pt-BR'""")

arrangementTypes.createOrReplaceTempView("ArrangementTypes")

workflowstatuses.createOrReplaceTempView("WorkFlowStatuses")

workFlowStatuses = spark.sql(
    """SELECT WorkflowStatus AS TypeID, 
       REPLACE(REPLACE(REPLACE(StatusAlias, 'Arrangement', ''), 'installment', ''), 'Status', '') AS Types
	   FROM WorkflowStatuses WHERE WorkflowStatus BETWEEN 60000 AND 61009
    """)

workFlowStatuses.createOrReplaceTempView("WorkFlowStatusesDF")

paymentSources = spark.sql(
    """SELECT OriginalAlias AS PaymentSourceID, Caption AS PaymentSource
	FROM ResourceCaptions WHERE ResourceID = 61 AND LanguageAlias = 'pt-BR'
    """)
 
paymentSources.createOrReplaceTempView("PaymentSources")

paymentMethods = spark.sql(
"""SELECT OriginalAlias AS PaymentMethodID, Caption AS PaymentMethod
   FROM ResourceCaptions WHERE ResourceID = 62 AND LanguageAlias = 'pt-BR'
   """)

paymentMethods.createOrReplaceTempView("PaymentMethods")

# ------------------

agents.createOrReplaceTempView("Agents")
arrangements.createOrReplaceTempView("Arrangements")
businessUnits.createOrReplaceTempView("BusinessUnits")
debtContacts.createOrReplaceTempView("DebtContacts")
dimDate.createOrReplaceTempView("dimDate")


# Query principal
arrangementsMatrix = spark.sql(
    """SELECT A.ArrangementID, 
    A.LegacyArrangementID, 
    DC.AccountOwnerID, 
    DC.AccountOwner, 
    DC.PortfolioID, 
    DC.Portfolio, 
    DC.SubPortfolioID,
    DC.SubPortfolio,   
    DC.DebtID, 
    DC.BindingID, 
    DC.CustomerID, 
    DC.OriginalDebtNumber, 
    DC.DebtNumber, 
    DC.OpenDate, 
    DC.OriginalFirstDefaultDate, 
    DC.FirstDefaultDate, 
    DC.InitialBalance, 
    DC.OpenBalance, 
    DC.OriginalFirstDefaultBalance, 
    DC.FirstDefaultBalance, 
    DC.OriginalProductID, 
    DC.ProductTypeID, 
    DC.ProductCode, 
    DC.ProductName, 
    DC.ProductTypeAlias, 
    DC.Products, 
    DC.Assets, 
    DC.Liabilities, 
    DC.ContactType, 
    DC.IdentityType, 
    DC.IdentityNumber, 
    DC.QualitativeAdressesID, 
    DC.QualitativeAdresses, 
    DC.District, 
    DC.City, 
    DC.State, 
    DC.StateAlias, 
    DC.Country, 
    CASE WHEN AG.Name LIKE '%Whats%' THEN 'WhatsApp' 
    WHEN BU.BusinessUnitTypeID IN (0,2) THEN 'Intern' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (711,749) THEN 'Digital' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (178,817) THEN 'Receptivo' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 820 THEN 'Escob' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 819 THEN 'WhatsApp' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 802 THEN 'URA' 
    WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (712, 756) THEN 'Intern' 
    WHEN BU.BusinessUnitTypeID = 4 THEN 'Escob' 
    WHEN BU.BusinessUnitTypeID = 8 THEN 'Legal' 
    WHEN BU.BusinessUnitTypeID = 11 THEN 'Digital' 
    ELSE 'N.A' END AS ChannelType, 
    BU.BusinessUnitID AS AgencyID, 
    BU.Alias AS Agency, 
    AG.AgentSID, 
    AG.Name AS AgentName, 
    AT.ArrangementTypeID, 
    AT.ArrangementType,
    WS.TypeID AS ArrangementStatusID, 
    WS.Types AS ArrangementStatus,
    A.PromiseDate, 
    A.ArrangementDate, 
    CAST(A.PromiseDate AS Date) AS RefDate, 
    DD.WorkDayinMonth,  
    A.DelinquencyDays, 
    A.CreationDate AS Date, 
    A.LastModificationDate, 
    A.CancellationDate, 
    A.CancellationReasonID, 
    CT.Caption AS CancellationReason, 
    A.TotalInstallments, 
    A.NextInstallment, 
    A.PaidInstallments, 
    A.InstallmentAmount, 
    A.FirstInstallmentDueDate, 
    A.ArrangementPrincipal, 
    A.ArrangementFees, 
    A.ArrangementCosts, 
    A.ArrangementInterests, 
    A.ArrangementCorrections, 
    A.ArrangementDiscount, 
    A.ArrangementAmount, 
    A.ArrangementBalance, 
    A.OriginalDebtPrincipal, 
    A.OriginalDebtFees, 
    A.OriginalDebtInterests, 
    A.OriginalDebtCorrections, 
    A.OriginalDebtCosts, 
    A.OriginalDebtTotalBalance, 
    A.LastPaymentDate, 
    PS.PaymentSourceID, 
    PS.PaymentSource, 
    PM.PaymentMethodID, 
    PM.PaymentMethod, 
    A.PaidPrincipal, 
    A.PaidFees, 
    A.PaidCosts, 
    A.PaidInterests, 
    A.PaidCorrections, 
    A.PaidAmount, 
    A.DownPaymentAmount, 
    A.FollowUpID, 
    A.FollowUpDate, 
    A.FollowUpTrackingID, 
    A.PostponeCounter, 
    A.CampaignDefinitionID
    FROM DebtContacts AS DC 
    INNER JOIN Arrangements AS A ON DC.DebtID = A.DebtID 
    INNER JOIN Agents AS AG ON A.AgentSID = AG.AgentSID 
    INNER JOIN BusinessUnits AS BU ON AG.BusinessUnitID = BU.BusinessUnitID 
    INNER JOIN dimDate AS DD 
    ON YEAR(A.CreationDate) = YEAR(DD.RefDate)
    AND MONTH(A.CreationDate) = MONTH(DD.RefDate) 
    AND DAY(A.CreationDate) = DAY(DD.RefDate)
    LEFT JOIN CancellationReasonsDF AS CT ON A.CancellationReasonID = CT.CancellationReasonID
    LEFT JOIN ArrangementTypes AS AT ON A.ArrangementType = AT.ArrangementTypeID
    LEFT JOIN WorkflowStatusesDF AS WS ON A.ArrangementStatus = WS.TypeID
    LEFT JOIN PaymentSources AS PS ON A.PaymentSource = PS.PaymentSourceID
    LEFT JOIN PaymentMethods AS PM ON A.PaymentMethod = PM.PaymentMethodID
    WHERE A.CreationDate > '2021-10-01 00:00:00' AND
    A.ArrangementID > {} """.format(lastArrangementID))


# Adiciona novas colunas de data
outputDF = arrangementsMatrix.withColumn("year", date_format(col("RefDate"), "yyyy")) \
           .withColumn("month", date_format(col("RefDate"), "MM")) \
           .withColumn("day", date_format(col("RefDate"), "dd")) \
           .withColumn("refdate", date_format(col("RefDate"), "yyyyMM").cast("int"))


# Coloca em lower case os nomes das tabelas de Insert e salva
outputDF = lower_column_names(outputDF)

outputDF.printSchema()

outputDF.write.partitionBy("year", "month", "day")  \
        .format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("append") \
        .save(datamart_arrangements)


# Contagem dos registros inseridos
total_registers = outputDF.count()


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = outputDF.agg({"date": "max"}).collect()[0][0]
max_id = outputDF.agg({"arrangementid": "max"}).collect()[0][0]
max_creation_date = outputDF.agg({"date": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Fim do script
print(f'Tempo de execução: {elapsed_time}')
print(f'Numero de registros inseridos: {total_registers}')
job.commit()