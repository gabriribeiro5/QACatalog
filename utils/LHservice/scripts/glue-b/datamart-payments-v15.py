#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que cria datamart payments                         #
# Autor: Edinor Cunha Junior - NTT DATA                                       #
# Data: Fev/2022                                                              #
# Alteração: Edinor Cunha Junior			       		      #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script			      #
# Versão: 15.0                                                                #
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart         #
# payments na refined zone.                                                   #
#                                                                             #
#----------------------------- Parâmetros ------------------------------------#
#                                                                             #
#>>> FT5L_LEGALASSIGNMENTS = Referência no S3 onde estão os dados da tabela   #
# FT5L_LEGALASSIGNMENTS.                                                      #
#>>> FT5L_LEGALOFFICES = Referência no S3 onde estão os dados da tabela       #
# FT5L_LEGALOFFICES.                                                          #
#>>> FT5L_SUITCOSTS = Referência no S3 onde estão os dados da tabela          #
# FT5L_SUITCOSTS.                                                             #
#>>> FT5L_SUITPARTIES = Referência no S3 onde estão os dados da tabela        #
# FT5L_SUITPARTIES.                                                           #
#>>> FT5L_SUITPARTYDEBTS = Referência no S3 onde estão os dados da tabela     #
# FT5L_SUITPARTYDEBTS.                                                        #
#>>> FT5L_SUITS = Referência no S3 onde estão os dados da tabela FT5L_SUITS.  #
#>>> FTCONTACTS_CONTACTS = Referência no S3 onde estão os dados da tabela     #
# FTCONTACTS_CONTACTS.                                                        #
#>>> FTCONTACTS_IDENTITIES = Referência no S3 onde estão os dados da          #
# tabela FTCONTACTS_IDENTITIES.                                               #
#>>> FTCRM_AGENTS = Referência no S3 onde estão os dados da tabela            #
# FTCRM_AGENTS.                                                               #
#>>> FTCRM_ARRANGINSTALLMENTS = Referência no S3 onde estão os dados da       # 
# tabela FTCRM_ARRANGINSTALLMENTS.                                            #
#>>> FTCRM_ARRANGEMENTS = Referência no S3 onde estão os dados da tabela      #
# FTCRM_ARRANGEMENTS.                                                         #
#>>> FTCRM_BINDINGS = Referência no S3 onde estão os dados da tabela          #
# FTCRM_BINDINGS.                                                             #
#>>> FTCRM_DIMDATE = Referência no S3 onde estão os dados da tabela           #
# FTCRM_DIMDATE.                                                              #
#>>> FTCRM_BUSSINESSUNITS = Referência no S3 onde estão os dados da tabela    #
# FTCRM_BUSSINESSUNITS.                                                       #
#>>> FTCRM_DEBTS = Referência no S3 onde estão os dados da tabela FTCRM_DEBTS #
#>>> FTCRM_DEBTTRANSACTIONS = Referência no S3 onde estão os dados da         #
# tabela FTCRM_DEBTTRANSACTIONS.                                              #
#>>> FTCRM_DEBTTRANSACTIONCODES = Referência no S3 onde estão os dados da     #
# tabela FTCRM_DEBTTRANSACTIONCODES.                                          #
#>>> FTCRM_DEBTTRANSACTIONCOMISSIONS = Referência no S3 onde estão os dados   #
# da tabela FTCRM_DEBTTRANSACTIONCOMISSIONS.                                  #
#>>> FTCRM_INSTALLMENTTRANSACTIONS = Referência no S3 onde estão os dados da  #
# tabela FTCRM_INSTALLMENTTRANSACTIONS.                                       #
#>>> FTCRM_PORTFOLIOS = Referência no S3 onde estão os dados da tabela        #
# FTCRM_PORTFOLIOS.                                                           #
#>>> FTF_RESOURCECAPTIONS = Referência no S3 onde estão os dados da tabela    #
# FTF_RESOURCECAPTIONS.                                                       #
#>>> PAYMENTS = Referência no S3 onde os dados do datamart de PAYMENTS serão  #
# inseridos.                                                                  #
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
from pyspark.sql.functions import date_format, col, when, current_date, lit


# Inicialização do S3
s3 = boto3.resource('s3')


# Variaveis de ambiente passadas pelo job do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FT5L_LEGALASSIGNMENTS', 'FT5L_LEGALOFFICES',
                                     'FT5L_SUITCOSTS','FT5L_SUITPARTIES','FT5L_SUITPARTYDEBTS',
                                     'FT5L_SUITS','FTCONTACTS_CONTACTS','FTCONTACTS_IDENTITIES',
                                     'FTCRM_AGENTS','FTCRM_ARRANGINSTALLMENTS','FTCRM_ARRANGEMENTS',
                                     'FTCRM_BINDINGS','FTCRM_DIMDATE','FTCRM_BUSINESSUNITS',
                                     'FTCRM_DEBTS','FTCRM_DEBTTRANSACTIONS','FTCRM_DEBTTRANSACTIONCODES',   
                                     'FTCRM_DEBTTRANSACTIONCOMISSIONS','FTCRM_INSTALLMENTTRANSACTIONS',
                                     'FTCRM_PORTFOLIOS','FTF_RESOURCECAPTIONS','PAYMENTS', 'LOG_PATH',
                                     'PROCESS_TYPE'
                                    ])

# Criação da conexão Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Variaveis do script
job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
log_path = args['LOG_PATH']
process_type = args['PROCESS_TYPE']
ft5l_legal_legalassignments = args['FT5L_LEGALASSIGNMENTS']
ft5l_legal_legaloffices = args['FT5L_LEGALOFFICES']
ft5l_legal_suitcosts = args['FT5L_SUITCOSTS']
ft5l_legal_suitparties = args['FT5L_SUITPARTIES']
ft5l_legal_suitpartydebts = args['FT5L_SUITPARTYDEBTS']
ft5l_legal_suits = args['FT5L_SUITS']
ftcontacts_contacts = args['FTCONTACTS_CONTACTS']
ftcontacts_identities = args['FTCONTACTS_IDENTITIES']
ftcrm_agents = args['FTCRM_AGENTS']
ftcrm_arrangement_installments = args['FTCRM_ARRANGINSTALLMENTS']
ftcrm_arrangements = args['FTCRM_ARRANGEMENTS']
ftcrm_bindings = args['FTCRM_BINDINGS']
ftcrm_dimdate = args['FTCRM_DIMDATE']
ftcrm_bussinessunits = args['FTCRM_BUSINESSUNITS']
ftcrm_debts = args['FTCRM_DEBTS']
ftcrm_debttransactions = args['FTCRM_DEBTTRANSACTIONS']
ftcrm_debttransactioncodes = args['FTCRM_DEBTTRANSACTIONCODES']
ftcrm_debttransactioncommissions = args['FTCRM_DEBTTRANSACTIONCOMISSIONS']
ftcrm_installmenttransactions = args['FTCRM_INSTALLMENTTRANSACTIONS']
ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
ftf_resource_captions = args['FTF_RESOURCECAPTIONS']
datamart_payments = args['PAYMENTS']


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


# Leitura dos arquivos parquet das tabelas origem
legalAssignments = read_data_from_lake(ft5l_legal_legalassignments)
legalOffices = read_data_from_lake(ft5l_legal_legaloffices)
suitCosts = read_data_from_lake(ft5l_legal_suitcosts)
suitParties = read_data_from_lake(ft5l_legal_suitparties)
suitPartyDebts = read_data_from_lake(ft5l_legal_suitpartydebts)
legalSuits = read_data_from_lake(ft5l_legal_suits)
contacts = read_data_from_lake(ftcontacts_contacts)
identities = read_data_from_lake(ftcontacts_identities)
agents = read_data_from_lake(ftcrm_agents)
arrangements = read_data_from_lake(ftcrm_arrangements)
arrangementsInstallments = read_data_from_lake(ftcrm_arrangement_installments)
bindings = read_data_from_lake(ftcrm_bindings)
dimDate = read_data_from_lake(ftcrm_dimdate)        
businessUnits = read_data_from_lake(ftcrm_bussinessunits)
debts = read_data_from_lake(ftcrm_debts)
debttransactions = read_data_from_lake(ftcrm_debttransactions)
debttransactioncodes = read_data_from_lake(ftcrm_debttransactioncodes)
debttransactioncommissions = read_data_from_lake(ftcrm_debttransactioncommissions)
installmenttransactions = read_data_from_lake(ftcrm_installmenttransactions)
portfolios = read_data_from_lake(ftcrm_portfolios)
resourceCaptions = read_data_from_lake(ftf_resource_captions)

try:
    lastDebtID = read_data_from_lake(datamart_payments)

    lastDebtID = lastDebtID.agg({"debttransactionid": "max"}).collect()[0][0]
except:
    lastDebtID = -1

print("ultimo id: ", lastDebtID)

# Criação da tabela auxiliar 'Legal'
legalAssignments.createOrReplaceTempView("LegalAssignments")
legalSuits.createOrReplaceTempView("LegalSuits")
legalOffices.createOrReplaceTempView("LegalOffices")
suitParties.createOrReplaceTempView("SuitParties")
suitPartyDebts.createOrReplaceTempView("SuitPartyDebts")

legalDF = spark.sql(
    """SELECT S.SuitID, SPD.DebtID, LA.AssignmentID, 
              LA.AssignmentDate AS StartAssignmentDate, 
              CASE WHEN (MIN(LA2.AssignmentDate)) = NULL THEN CURRENT_DATE
              END AS EndAssignmentDate,
              LA.LegalOfficeID, LO.LegalOfficeName
	    FROM LegalSuits AS S
	    INNER JOIN LegalAssignments AS LA ON S.SuitID = LA.SuitID
	    INNER JOIN LegalOffices AS LO ON LA.LegalOfficeID = LO.LegalOfficeID
	    LEFT JOIN LegalAssignments AS LA2 ON LA.SuitID = LA2.SuitID AND LA2.AssignmentID > LA.AssignmentID
	    INNER JOIN SuitParties AS SP ON S.SuitID = SP.SuitID
	    INNER JOIN SuitPartyDebts AS SPD ON SP.SuitPartyID = SPD.SuitPartyID
	    GROUP BY S.SuitID, SPD.DebtID, LA.AssignmentID, 
                LA.AssignmentDate, LA.LegalOfficeID, LO.LegalOfficeName""")

# Criação da tabela principal Payments
legalDF.createOrReplaceTempView("LegalDF")
businessUnits.createOrReplaceTempView("BusinessUnits")
portfolios.createOrReplaceTempView("Portfolios")
debts.createOrReplaceTempView("Debts")
debttransactions.createOrReplaceTempView("DebtTransactions")
debttransactioncodes.createOrReplaceTempView("DebtTransactionCodes")
debttransactioncommissions.createOrReplaceTempView("DebtTransactionCommissions")
bindings.createOrReplaceTempView("Bindings")
contacts.createOrReplaceTempView("Contacts")
identities.createOrReplaceTempView("Identities")
dimDate.createOrReplaceTempView("dimDate")
installmenttransactions.createOrReplaceTempView("InstallmentTransactions")
arrangementsInstallments.createOrReplaceTempView("ArrangementInstallments")
arrangements.createOrReplaceTempView("Arrangements")
agents.createOrReplaceTempView("Agents")
resourceCaptions.createOrReplaceTempView("ResourceCaptions")
suitCosts.createOrReplaceTempView("SuitCosts")

# Query principal
payments = spark.sql(
        """SELECT DISTINCT CASE
                WHEN AG.Name LIKE '%Whats%' THEN 'WhatsApp'
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
                WHEN negative(DT.Amount*DTC.Sign) < 0 THEN 'Repasse Chargeback'
	        WHEN DTC.TransactionCode = 11 THEN 'Teimosinha'
	        WHEN DTC.TransactionCode = 12 THEN 'Putback' 
	        WHEN DTC.TransactionCode = 22 THEN 'Repasse'
                WHEN DTC.TransactionCode = 29 THEN 'Legal' 
	        WHEN L.DebtID IS NOT NULL AND SC.DebtTransactionID IS NULL 
	        AND ( A.ArrangementType IN (40, 140) OR (A.ArrangementID IS NULL 
	        AND DT.TransactionSource = 43) ) THEN 'Legal'
                WHEN SC.DebtTransactionID IS NULL 
	        AND DTC.TransactionCode IN (5,7) AND ( ( A.ArrangementType NOT IN (40, 140) 
	        AND BU.BusinessUnitTypeID != 11 ) OR A.ArrangementType IS NULL ) THEN 'Escob' 
	        WHEN SC.DebtTransactionID IS NULL AND DTC.TransactionCode IN (5,7) 
	        AND A.ArrangementType NOT IN (40, 140) AND IFNULL(BU.BusinessUnitTypeID,0) = 11 THEN 'Digital' 
	        WHEN SC.DebtTransactionID IS NOT NULL THEN 'Legal Refund' 
                ELSE 'N.A' END AS ChannelType,
		CASE
                WHEN AG.Name LIKE '%Whats%' THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID IN (0,2) THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (711,749) THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (178,817) THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 820 THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 819 THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 802 THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (712, 756) THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 4 THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 8 THEN BU.BusinessUnitID
                WHEN BU.BusinessUnitTypeID = 11 THEN BU.BusinessUnitID 
		WHEN negative(DT.Amount*DTC.Sign) < 0 THEN -1 
		WHEN DTC.TransactionCode = 11 THEN -2 
		WHEN DTC.TransactionCode = 12 THEN -3 
		WHEN DTC.TransactionCode = 22 THEN -4
                WHEN DTC.TransactionCode = 29 THEN L.LegalOfficeID 
		WHEN L.DebtID IS NOT NULL AND SC.DebtTransactionID IS NULL 
		AND ( A.ArrangementType IN (40, 140) OR (A.ArrangementID IS NULL 
		AND DT.TransactionSource = 43 ) ) THEN L.LegalOfficeID
                WHEN SC.DebtTransactionID IS NULL 
		AND DTC.TransactionCode IN (5,7) AND ( ( A.ArrangementType NOT IN (40, 140) 
		AND BU.BusinessUnitTypeID != 11 ) OR A.ArrangementType IS NULL ) 
		THEN BU.BusinessUnitID 
                WHEN SC.DebtTransactionID IS NULL 
		AND DTC.TransactionCode IN (5,7) AND A.ArrangementType NOT IN (40, 140) 
		AND IFNULL(BU.BusinessUnitTypeID,0) = 11 THEN BU.BusinessUnitID 
		WHEN SC.DebtTransactionID IS NOT NULL THEN -5 
		ELSE -6 END AS AgencyID,
		CASE
                WHEN AG.Name LIKE '%Whats%' THEN BU.Alias
                WHEN BU.BusinessUnitTypeID IN (0,2) THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (711,749) THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (178,817) THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 820 THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 819 THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID = 802 THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 3 AND AG.BusinessUnitID IN (712, 756) THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 4 THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 8 THEN BU.Alias
                WHEN BU.BusinessUnitTypeID = 11 THEN BU.Alias 
		WHEN negative(DT.Amount*DTC.Sign) < 0 THEN 'Repasse Chargeback' 
		WHEN DTC.TransactionCode = 11 THEN 'Teimosinha' 
		WHEN DTC.TransactionCode = 12 THEN 'Putback' 
		WHEN DTC.TransactionCode = 22 THEN 'Repasse'
                WHEN DTC.TransactionCode = 29 THEN L.LegalOfficeName 
		WHEN L.DebtID IS NOT NULL AND SC.DebtTransactionID IS NULL 
		AND ( A.ArrangementType IN (40, 140) OR (A.ArrangementID IS NULL 
		AND DT.TransactionCode = 23  ) ) THEN L.LegalOfficeName 
                WHEN SC.DebtTransactionID IS NULL 
		AND DTC.TransactionCode IN (5,7) AND ( ( A.ArrangementType NOT IN (40, 140) 
		AND BU.BusinessUnitTypeID != 11 ) OR A.ArrangementType IS NULL ) THEN BU.Alias 
		WHEN SC.DebtTransactionID IS NULL AND DTC.TransactionCode IN (5,7) 
                AND A.ArrangementType NOT IN (40, 140) 
		AND IFNULL(BU.BusinessUnitTypeID,0) = 11 THEN BU.Alias 
                WHEN SC.DebtTransactionID IS NOT NULL THEN 'Legal Refund' 
                ELSE 'N.A' END AS Agency, 
                AO.BusinessUnitID AS AccountOwnerID, 
                AO.Alias AS AccountOwner, 
		CASE 
                WHEN P.PortfolioID IN (79, 37) AND DT.AccountingDate < '2020-02-28' THEN 37 
                WHEN P.PortfolioID IN (79, 37) AND DT.AccountingDate >= '2020-02-28' THEN 79 
                WHEN P.PortfolioID IN (80, 40) AND DT.AccountingDate < '2020-02-28' THEN 40 
                WHEN P.PortfolioID IN (80, 40) AND DT.AccountingDate >= '2020-02-28' THEN 80 
                WHEN P.PortfolioID IN (81, 41) AND DT.AccountingDate < '2020-02-28' THEN 41 
                WHEN P.PortfolioID IN (81, 41) AND DT.AccountingDate >= '2020-02-28' THEN 81
                WHEN P.PortfolioID IN (82, 45) AND DT.AccountingDate < '2020-02-28' THEN 45 
                WHEN P.PortfolioID IN (82, 45) AND DT.AccountingDate >= '2020-02-28' THEN 82 
                ELSE P.PortfolioID END AS PortfolioID,
		CASE 
		WHEN P.PortfolioID IN (79, 37) AND DT.AccountingDate < '2020-02-28' THEN 'Santander_F3' 
	    	WHEN P.PortfolioID IN (79, 37) AND DT.AccountingDate >= '2020-02-28' THEN 'Santander' 
                WHEN P.PortfolioID IN (80, 40) AND DT.AccountingDate < '2020-02-28' THEN 'Marisa2_F3' 
                WHEN P.PortfolioID IN (80, 40) AND DT.AccountingDate >= '2020-02-28' THEN 'Marisa2' 
                WHEN P.PortfolioID IN (81, 41) AND DT.AccountingDate < '2020-02-28' THEN 'Santander2_F3' 
                WHEN P.PortfolioID IN (81, 41) AND DT.AccountingDate >= '2020-02-28' THEN 'Santander2' 
                WHEN P.PortfolioID IN (82, 45) AND DT.AccountingDate < '2020-02-28' THEN 'CitiBank3_F3' 
                WHEN P.PortfolioID IN (82, 45) AND DT.AccountingDate >= '2020-02-28' THEN 'CitiBank3' 
                ELSE P.Alias END AS Portfolio, 
		D.DebtID, 
                B.CustomerID,
                CASE 
		WHEN C.ContactType = 1 THEN 'PF' 
                ELSE 'PJ' END AS IdentityType,
		I.IdentityNumber, 
                DT.DebtTransactionID, 
                DT.ReversedTransactionID, 
                DTR.AccountingDate AS ReversedAccountingDate, 
                DTR.Amount*DTC.Sign*1 AS ReversedAmount,
                CASE 
                WHEN DT.AccountingDate = DTR.AccountingDate THEN 1 
                WHEN DT.AccountingDate != DTR.AccountingDate THEN 0 
                ELSE NULL END AS isReversedIntraMonth,
		DT.AccountingDate, 
                DT.ClearDate, 
                DT.ProcessingDate, 
                CAST(DT.AccountingDate AS Date) AS RefDate,
                DT.AccountingDate AS Date, 
                A.ArrangementType, 
                A.FollowUpID, 
                DD.WorkDayinMonth,
                A.ArrangementID, 
                A.CreationDate AS ArrangementCreationDate, 
                A.TotalInstallments, 
                A.ArrangementAmount, 
                A.OriginalDebtPrincipal, 
                A.OriginalDebtTotalBalance, 
                A.CancellationDate  AS CancellationDate, 
                A.DelinquencyDays,
                AI.InstallmentID, 
                AI.PaymentMethod AS PaymentMethodID, 
                RC.Caption AS PaymentMethod,
                AI.InstallmentNumber, 
                AI.InstallmentAmount, 
                AI.InstallmentDate, 
                CASE AI.InstallmentStatus 
                WHEN 61000 THEN 'Em Aberto' 
                WHEN 61001 THEN 'Pago' 
                WHEN 61002 THEN 'Quebrado' 
                WHEN 61005 THEN 'Parcialmente Pago' 
                WHEN 61009 THEN 'Remessa Pendente' 
                END AS InstallmentStatus,
                CASE 
                WHEN IT.DebtTransactionID IS NOT NULL THEN IT.AppliedAmount 
                ELSE negative(DT.Amount*DTC.Sign) END AS CollectionAmount,
                DTC.Sign, 
                DTCC.CommissionAmount,
                AG.AgentSID, 
                AG.Name AS AgentName, 
                CASE AG.Enabled 
                WHEN 1 THEN 'Active' 
                WHEN 0 THEN 'Inactive' 
                ELSE '' END AS AgentEnabled, 
                DT.AccountingDate AS AgentCreationDate,
                CAST(0 AS smallint) AS Assets,
                SC.DebtTransactionID AS SuitCostDebtTransactionID, 
                DT.TransactionSource,
                DT.LastModificationDate,
                DT.TransactionCode
	        FROM BusinessUnits AS AO
	        INNER JOIN Portfolios AS P ON AO.BusinessUnitID = P.BusinessUnitID
	        INNER JOIN Debts AS D ON P.PortfolioID = D.PortfolioID
	        INNER JOIN DebtTransactions AS DT ON D.DebtID = DT.DebtID
	        INNER JOIN DebtTransactionCodes AS DTC ON DT.TransactionCode = DTC.TransactionCode
	        LEFT  JOIN DebtTransactions AS DTR ON DT.ReversedTransactionID = DTR.DebtTransactionID
	        LEFT  JOIN (Bindings AS B
                INNER JOIN Contacts AS C ON B.CustomerID = C.ContactID
                INNER JOIN Identities AS I ON C.PrimaryIdentityID = I.IdentityID ) ON D.DebtID = B.DebtID
	        INNER JOIN dimDate AS DD
                ON YEAR(DT.AccountingDate) = YEAR(DD.RefDate)
                AND MONTH(DT.AccountingDate) = MONTH(DD.RefDate)
                AND DAY(DT.AccountingDate) = DAY(DD.RefDate)
	        LEFT  JOIN LegalDF AS L ON L.DebtID = DT.DebtID AND DT.AccountingDate BETWEEN L.StartAssignmentDate AND L.EndAssignmentDate
	        LEFT  JOIN (InstallmentTransactions AS IT
                INNER JOIN ArrangementInstallments AS AI ON IT.InstallmentID = AI.InstallmentID
                INNER JOIN Arrangements AS A ON AI.ArrangementID = A.ArrangementID
                INNER JOIN Agents AS AG ON AI.AgentSID = AG.AgentSID
                INNER JOIN BusinessUnits AS BU ON AG.BusinessUnitID = BU.BusinessUnitID
                LEFT  JOIN ResourceCaptions AS RC ON RC.ResourceID = 62 AND RC.LanguageAlias = 'pt-BR' 
                AND AI.PaymentMethod = RC.OriginalAlias) ON DT.DebtTransactionID = IT.DebtTransactionID 
                AND IT.isCashTransaction = 1
	        LEFT  JOIN ( SELECT DISTINCT DebtTransactionID FROM SuitCosts 
                WHERE DebtTransactionID IS NOT NULL ) AS SC ON DT.DebtTransactionID = SC.DebtTransactionID
	        LEFT  JOIN ( SELECT DebtTransactionID, BusinessUnitID, 
                SUM(CommissionAmount) AS CommissionAmount FROM DebtTransactionCommissions 
                GROUP BY DebtTransactionID, BusinessUnitID ) AS DTCC ON DT.DebtTransactionID = DTCC.DebtTransactionID 
                AND BU.BusinessUnitID = DTCC.BusinessUnitID
	        WHERE P.BusinessUnitID IN ( 672,  743 ) AND DTC.isCashTransaction = 1
                AND DT.DebtTransactionID  > {}""".format(lastDebtID))

print("Numero de registros a serem adicionadas: ", payments.count())


suits = legalSuits.alias("S").join(suitParties.alias("SP"), \
        legalSuits["SuitID"] == suitParties["SuitID"], "inner").join(suitPartyDebts.alias("SPD"), \
        "SuitPartyID", "inner").where("S.SuitPosition = 1") \
        .selectExpr("S.SuitID", "S.CreationDate", "S.ArchiveDate", "SPD.DebtID") \
        .withColumn("EndDate", when(col("S.ArchiveDate").isNull(), current_date()) \
        .otherwise("S.ArchiveDate")).distinct() \
        .drop("S.ArchiveDate")
               

payments = payments.alias("P").join(suits.alias("S"), \
         [payments["DebtID"] == suits["DebtID"], 
         payments["AccountingDate"] > suits["CreationDate"], 
         payments["AccountingDate"] < suits["EndDate"]], "left") \
        .selectExpr("P.*", "P.Assets as _Assets") \
        .withColumn("Assets", when(col("_Assets") == 0, 1).otherwise(0)) \
        .drop("_Assets")


# Coloca em lower case as colunas do datamart
payments = lower_column_names(payments)


# Tratando os dados de pagamento que não tem agência
payments = payments.fillna(-1, subset=["agencyid"])
payments = payments.fillna('Não especificado', subset=["paymentmethod"])


# Coleta por cash novo ou não
payments = payments.withColumn("cashtype", when(col("installmentnumber") == 1, \
        lit('cash_novo')).otherwise(lit('colchao')))


outputDF = payments.withColumn("year", date_format(col("refdate"), "yyyy")) \
           .withColumn("month", date_format(col("refdate"), "MM")) \
           .withColumn("day", date_format(col("refdate"), "dd")) \
           .withColumn("refdate", date_format(col("refdate"), "yyyyMM").cast("int"))


outputDF.write.partitionBy("year", "month", "day")  \
        .format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("append") \
        .save(datamart_payments)


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
max_creation_date = outputDF.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Fim do script
print(f'Tempo de execução: {elapsed_time}')
print(f'Numero de registros inseridos: {total_registers}')
job.commit()
