#=============================================================================#
#                                                                             #
# Objetivo: Script PySpark que cria datamart debtcontacts                     #
# Autor: Matheus Soares Rodrigues - NTT DATA                                  #
# Data: Out/2021                                                              #
# Alteração: Edinor Cunha Junior                    			      #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script			      #
# Versão: 11.0                                                                # 
#                                                                             #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart         #
# debtcontacts na refined zone.                                               #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> FT5L_SUITPARTIES = Referência no S3 onde estão os dados da tabela        #
# FT5L_SUITPARTIES.                                                           #
#>>> FT5L_SUITPARTYDEBTS = Referência no S3 onde estão os dados da tabela     #
# FT5L_SUITPARTYDEBTS.                                                        #
#>>> FT5L_SUITS = Referência no S3 onde estão os dados da tabela FT5L_SUITS.  #
#>>> FTF_RESOURCECAPTIONS = Referência no S3 onde estão os dados da tabela    #
# FTF_RESOURCECAPTIONS.                                                       #
#>>> FTCONTACTS_ADDRESSES = Referência no S3 onde estão os dados da tabela    #
# FTCONTACTS_ADDRESSES.                                                       #
#>>> FTCONTACTS_CONTACTS = Referência no S3 onde estão os dados da tabela     #
# FTCONTACTS_CONTACTS.                                                        #
#>>> FTCONTACTS_IDENTITIES =Referência no S3 onde estão os dados da tabela    #
# FTCONTACTS_IDENTITIES.                                                      #
#>>> FTCONTACTS_STATES = Referência no S3 onde estão os dados da tabela       #
# FTCONTACTS_STATES.                                                          #
#>>> FTCRM_BINDINGS = Referência no S3 onde estão os dados da tabela          #
# FTCRM_BINDINGS.                                                             #
#>>> FTCRM_BUSSINESSUNITS = Referência no S3 onde estão os dados da tabela    #
# FTCRM_BUSSINESSUNITS.                                                       #
#>>> FTCRM_CREDITRESTRICTIONS = Referência no S3 onde estão os dados da tabela# 
# FTCRM_CREDITRESTRICTIONS.                                                   #
#>>> FTCRM_DEBTCUSTOMEXTENSIONS = Referência no S3 onde estão os dados da     #
# tabela FTCRM_DEBTCUSTOMEXTENSIONS.                                          #
#>>> FTCRM_DEBTS = Referência no S3 onde estão os dados da tabela FTCRM_DEBTS.#
#>>> FTCRM_PORTFOLIOS = Referência no S3 onde estão os dados da tabela        #
# FTCRM_PORTFOLIOS.                                                           #
#>>> FTCRM_PRODUCTS = Referência no S3 onde estão os dados da tabela          #
# FTCRM_PRODUCTS.                                                             #
#>>> FTCRM_PRODUCTTYPE = Referência no S3 onde estão os dados da tabela       #
# FTCRM_PRODUCTTYPE.                                                          #
#>>> FTCRM_SCOREMODELS = Referência no S3 onde estão os dados da tabela       #
# FTCRM_SCOREMODELS.                                                          #
#>>> FTCRM_SCORES = Referência no S3 onde estão os dados da tabela            # 
# FTCRM_SCORES.                                                               #
#>>> PAYMENTS = Referência no S3 onde os dados do datamart de PAYMENTS.       #
#>>> ARRANGEMENTS = Referência no S3 onde os dados do datamart de             #
# ARRANGEMENTS.                                                               #
#>>> DEBTCONTACTS = Referência no S3 onde os dados do datamart de             #
# DEBTCONTACTS serão inseridos.                                               #
#                                                                             #        
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

# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME','FT5L_SUITPARTIES','FT5L_SUITPARTYDEBTS',
                                     'FT5L_SUITS','FTF_RESOURCECAPTIONS','FTCONTACTS_ADDRESSES',
                                     'FTCONTACTS_CONTACTS','FTCONTACTS_IDENTITIES','FTCONTACTS_STATES',
                                     'FTCONTACTS_PEOPLE','FTCRM_BINDINGS','FTCRM_BUSINESSUNITS','FTCRM_CREDITRESTRICTIONS',
                                     'FTCRM_DEBTCUSTOMEXTENSIONS','FTCRM_DEBTS','FTCRM_PORTFOLIOS','FTCRM_SUBPORTFOLIOS',
                                     'FTCRM_PRODUCTS','FTCRM_PRODUCTTYPE','FTCRM_SCOREMODELS',
                                     'FTCRM_SCORES','PAYMENTS','ARRANGEMENTS',
                                     'DEBTCONTACTS','DUMP_PAYMENTS','DUMP_ARRANGEMENTS','LOG_PATH', 
                                     'PROCESS_TYPE'])
# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variáveis utilizadas no script
job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
log_path = args['LOG_PATH']
process_type = args['PROCESS_TYPE']
ft5l_legal_suitparties = args['FT5L_SUITPARTIES']
ft5l_legal_suitpartydebts = args['FT5L_SUITPARTYDEBTS']
ft5l_legal_suits = args['FT5L_SUITS']
ftf_resource_captions = args['FTF_RESOURCECAPTIONS']
ftcontacts_addresses = args['FTCONTACTS_ADDRESSES']
ftcontacts_contacts = args['FTCONTACTS_CONTACTS']
ftcontacts_identities = args['FTCONTACTS_IDENTITIES']
ftcontacts_states = args['FTCONTACTS_STATES']
ftcontacts_people = args['FTCONTACTS_PEOPLE']
ftcrm_bindings = args['FTCRM_BINDINGS']
ftcrm_bussinessunits = args['FTCRM_BUSINESSUNITS']
ftcrm_creditrestrictions = args['FTCRM_CREDITRESTRICTIONS']
ftcrm_debtcustomextensions = args['FTCRM_DEBTCUSTOMEXTENSIONS']
ftcrm_debts = args['FTCRM_DEBTS']
ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
ftcrm_subportfolios = args['FTCRM_SUBPORTFOLIOS']
ftcrm_products = args['FTCRM_PRODUCTS'] 
ftcrm_producttype = args['FTCRM_PRODUCTTYPE']
ftcrm_scoremodels = args['FTCRM_SCOREMODELS']
ftcrm_scores = args['FTCRM_SCORES']
ftcrm_workflowstatuses = args['FTCRM_WORKFLOWSTATUSES']
datamart_workflowtrackings = args['WORKFLOWTRACKINGS']
datamart_payments = args['PAYMENTS']
datamart_arrangements = args['ARRANGEMENTS']
datamart_debtcontacts = args['DEBTCONTACTS']
dump_datamart_payments = args['DUMP_PAYMENTS']
dump_datamart_arrangements = args['DUMP_ARRANGEMENTS']



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


#Parâmetros principais
MAX_REGISTERS_REPARTITION = 250000


# Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


#Leitura de tabelas base
resourceCaptionsDF = read_data_from_lake(ftf_resource_captions)
suitPartiesDF = read_data_from_lake(ft5l_legal_suitparties)
suitPartyDebtsDF = read_data_from_lake(ft5l_legal_suitpartydebts)
suitsDF = read_data_from_lake(ft5l_legal_suits)
addressesDF = read_data_from_lake(ftcontacts_addresses)
contactsDF = read_data_from_lake(ftcontacts_contacts)
identitiesDF = read_data_from_lake(ftcontacts_identities)
statesDF = read_data_from_lake(ftcontacts_states)
peopleDF = read_data_from_lake(ftcontacts_people)
bindingsDF = read_data_from_lake(ftcrm_bindings)
businessUnitsDF = read_data_from_lake(ftcrm_bussinessunits)
creditRestrictionsDF = read_data_from_lake(ftcrm_creditrestrictions)
debtCustomExtensionsDF = read_data_from_lake(ftcrm_debtcustomextensions)
debtsDF = read_data_from_lake(ftcrm_debts)
portfoliosDF = read_data_from_lake(ftcrm_portfolios)
subportfoliosDF = read_data_from_lake(ftcrm_subportfolios)
productsDF = read_data_from_lake(ftcrm_products)
productTypesDF = read_data_from_lake(ftcrm_producttype)
scoreModelsDF = read_data_from_lake(ftcrm_scoremodels)
scoresDF = read_data_from_lake(ftcrm_scores)
workflowtrackingsDF = read_data_from_lake(datamart_workflowtrackings)
workflowstatusesDF = read_data_from_lake(ftcrm_workflowstatuses)

try:
    lastDebtID = read_data_from_lake(datamart_debtcontacts)
    lastDebtID = lastDebtID.agg({"debtid": "max"}).collect()[0][0]
    
    paymentsDF = read_data_from_lake(datamart_payments)
    arrangementsDF = read_data_from_lake(datamart_arrangements)
except:
    lastDebtID = -1
       
    paymentsDF = read_data_from_lake(dump_datamart_payments)
    arrangementsDF = read_data_from_lake(dump_datamart_arrangements)

print("ultimo id do destino: ", lastDebtID)

# Criação de views temporárias
resourceCaptionsDF.createOrReplaceTempView("ResourceCaptions")
suitPartiesDF.createOrReplaceTempView("SuitParties")
suitPartyDebtsDF.createOrReplaceTempView("SuitPartyDebts")
suitsDF.createOrReplaceTempView("Suits")
addressesDF.createOrReplaceTempView("Addresses")
contactsDF.createOrReplaceTempView("Contacts")
identitiesDF.createOrReplaceTempView("Identities")
statesDF.createOrReplaceTempView("States")
peopleDF.createOrReplaceTempView("People")
bindingsDF.createOrReplaceTempView("Bindings")
businessUnitsDF.createOrReplaceTempView("BusinessUnits")
creditRestrictionsDF.createOrReplaceTempView("CreditRestrictions")
debtCustomExtensionsDF.createOrReplaceTempView("DebtCustomExtensions")
debtsDF.createOrReplaceTempView("Debts")
portfoliosDF.createOrReplaceTempView("Portfolios")
subportfoliosDF.createOrReplaceTempView("SubPortfolios")
productsDF.createOrReplaceTempView("Products")
productTypesDF.createOrReplaceTempView("ProductTypes")
scoreModelsDF.createOrReplaceTempView("ScoreModels")
scoresDF.createOrReplaceTempView("Scores")
paymentsDF.createOrReplaceTempView("Payments")
arrangementsDF.createOrReplaceTempView("Arrangements")
workflowtrackingsDF.createOrReplaceTempView("WorkflowTrackings")
workflowstatusesDF.createOrReplaceTempView("WorkflowStatuses")

# Info Contacts 

identities = spark.sql("""
              SELECT OriginalAlias, Caption
              FROM ResourceCaptions
              WHERE LanguageAlias = 'pt-BR' AND ResourceID = 9
              """)
              
identities.createOrReplaceTempView("IdentitiesTemp")

gender = spark.sql("""
        SELECT OriginalAlias, Caption
        FROM ResourceCaptions
        WHERE LanguageAlias = 'pt-BR' AND ResourceID = 7
        """)

gender.createOrReplaceTempView("Gender")

maritalstatus = spark.sql("""
        SELECT OriginalAlias, Caption
        FROM ResourceCaptions
        WHERE LanguageAlias = 'pt-BR' AND ResourceID = 8
        """)
        
maritalstatus.createOrReplaceTempView("MaritalStatus")


# Transformação dos dados

creditRestrictions = spark.sql("""SELECT DISTINCT
        B.DebtID, MAX(CASE WHEN CR.RestrictionStatus IN (93000, 93002) 
        THEN 0 ELSE 1 END) AS RestrictionStatus
		FROM CreditRestrictions AS CR
		INNER JOIN Bindings AS B ON CR.BindingID = B.BindingID
		GROUP BY B.DebtID
        """)
      
creditRestrictions.createOrReplaceTempView("CreditRestrictionsTemp")

suits = spark.sql("""SELECT 
        SPD.DebtID, 
        MAX(CASE WHEN S.SuitPosition = 1 THEN 1 ELSE 0 END) AS Assets, 
        MAX(CASE WHEN S.SuitPosition = 2 THEN 1 ELSE 0 END) AS Liabilities
    	FROM Suits AS S
    	INNER JOIN SuitParties AS SP ON S.SuitID = SP.SuitID
    	INNER JOIN SuitPartyDebts AS SPD ON SP.SuitPartyID = SPD.SuitPartyID
    	GROUP BY SPD.DebtID
        """)

suits.createOrReplaceTempView("SuitsTemp")

payments = spark.sql("""SELECT
		DebtID, 
        MAX(DebtTransactionID) AS DebtTransactionID
		FROM Payments
		GROUP BY DebtID
        """)       
payments.createOrReplaceTempView("PaymentsTemp")   


arrangements = spark.sql("""
        SELECT DISTINCT DebtID
        FROM Arrangements
        """)

arrangements.createOrReplaceTempView("ArrangementsTemp")
     
      
workflowtrackings = spark.sql ("""
        SELECT DebtID, MAX(WKTrackingID) AS LastWKTrackingID
        FROM WorkflowTrackings
        GROUP BY DebtID
        """)
        
workflowtrackings.createOrReplaceTempView("WorkflowTrackingsTemp")

debts = spark.sql("""SELECT 
        BU.BusinessUnitID AS AccountOwnerID, 
		BU.Alias AS AccountOwner, 
		D.PortfolioID, 
		P.Alias AS Portfolio, 
		D.SubPortfolio AS SubPortfolioID, 
		SP.SubPortfolioName AS SubPortfolio, 
		D.DebtID, D.DebtStatus AS DebtStatusID, 
		REPLACE(WSD.StatusAlias, 'DebtStatus', '') AS DebtStatus, 
		D.Category, B.BindingID, 
		B.BindingStatus AS BindingStatusID, 
		REPLACE(WSB.StatusAlias, 'BindingStatus', '') AS BindingStatus, 
		B.CustomerID, 
		D.OriginalDebtNumber, 
		D.DebtNumber, 
		D.OriginalProductID, 
		D.ProductID, 
		D.OpenDate, 
		D.OriginalFirstDefaultDate, 
		D.FirstDefaultDate, 
		D.InitialBalance, 
		D.OpenBalance, 
		D.OriginalFirstDefaultBalance, 
		D.FirstDefaultBalance, 
		D.CurrentOpenBalance, 
		D.CurrentClosedBalance,
		D.CurrentBalance, 
		D.ChargeOffDate, 
		D.ChargeOffBalance, 
		D.SettlementDate, 
		D.RecallDate, 
		D.SellDate, 
		D.PlacementDate, 
		D.CreationDate AS DebtCreationDate, 
		D.LastModificationDate
		FROM BusinessUnits AS BU
		INNER JOIN Portfolios AS P ON BU.BusinessUnitID = P.BusinessUnitID
		INNER JOIN Debts AS D ON P.PortfolioID = D.PortfolioID
		INNER JOIN Bindings AS B ON D.DebtID = B.DebtID
		LEFT  JOIN SubPortfolios AS SP ON D.SubPortfolio = SP.SubPortfolio
		LEFT  JOIN WorkflowStatuses AS WSD ON D.DebtStatus = WSD.WorkflowStatus
		LEFT  JOIN WorkflowStatuses AS WSB ON B.BindingStatus = WSB.WorkflowStatus
		WHERE P.BusinessUnitID IN ( 672,  743 ) 
        """)
        
debts.createOrReplaceTempView("DebtsTemp")
            
        
contacts = spark.sql("""SELECT DISTINCT 
        D.DebtID, 
        CASE WHEN CC.ContactType = 1 
        THEN 'PF' 
        ELSE 'PJ' 
        END AS ContactType, 
		RCC.Caption AS IdentityType, 
        I.IdentityNumber,
		CC.Name AS CustomerName, 
		CP.BirthDate, 
		CP.DeathDate, 
		RCG.OriginalAlias AS GenderID, 
		RCG.Caption AS Gender, 
		RCM.OriginalAlias AS MaritalStatusID, 
		RCM.Caption AS MaritalStatus, 
		CP.isEmployed, 
		CP.Occupation, 
		CASE WHEN CC.PrimaryAddressID IS NOT NULL 
		THEN 1 WHEN CC.PrimaryAddressID IS NULL 
		AND CC.BestAddressID IS NOT NULL 
		THEN 2 ELSE 0 END AS QualitativeAdressesID, 
		CASE WHEN CC.PrimaryAddressID IS NOT NULL 
		THEN 'Primary' WHEN CC.PrimaryAddressID IS NULL 
		AND CC.BestAddressID IS NOT NULL 
		THEN 'Best' END AS QualitativeAdresses, 
		A.PostalCode, 
		A.District, 
		A.City, 
		S.Name AS State, 
		S.Alias AS StateAlias, 
        'Brasil' AS Country
		FROM DebtsTemp AS D
	    LEFT JOIN (Contacts AS CC 
        LEFT JOIN Identities AS I ON CC.PrimaryIdentityID = I.IdentityID
	    LEFT JOIN IdentitiesTemp AS RCC ON I.IdentityType = RCC.OriginalAlias
	    LEFT JOIN Addresses AS A ON COALESCE(CC.PrimaryAddressID, CC.BestAddressID) = A.AddressID
	    LEFT JOIN States AS S ON A.State = S.Alias) ON D.CustomerID = CC.ContactID
	    LEFT JOIN (People AS CP
        LEFT JOIN Gender AS RCG ON CP.Gender = RCG.OriginalAlias
	    LEFT JOIN MaritalStatus AS RCM ON CP.MaritalStatus = RCM.OriginalAlias ) ON D.CustomerID = CP.ContactID
            """)

contacts.createOrReplaceTempView("ContactsTemp")	
	     
products = spark.sql("""SELECT 
        D.DebtID, 
	    PRO.ProductID, 
	    PRO.ProductTypeID, 
	    PRO.ProductCode, 
	    PRO.ProductName, 
	    PT.Alias AS ProductTypeAlias, 
	    RCP.Caption AS Products
	    FROM DebtsTemp AS D
	    INNER JOIN Products AS PRO ON D.OriginalProductID = PRO.ProductID
	    INNER JOIN ProductTypes AS PT ON PRO.ProductTypeID = PT.ProductTypeID
	    LEFT  JOIN ResourceCaptions AS RCP ON RCP.ResourceID = 23 
        AND RCP.LanguageAlias = 'pt-BR' AND PT.Alias = RCP.OriginalAlias
        """)
products.createOrReplaceTempView("ProductsTemp")

lastpayment = spark.sql("""
	    SELECT D.DebtID,
        P.Date AS LastPaymentDate, 
        SUM(P.CollectionAmount) AS LastPaymentAmount	
	    FROM DebtsTemp AS D
	    INNER JOIN PaymentsTemp AS MP ON D.DebtID = MP.DebtID
	    INNER JOIN Payments AS P ON MP.DebtTransactionID = P.DebtTransactionID
	    GROUP BY D.DebtID, 
        P.Date
        """)
lastpayment.createOrReplaceTempView("LastPaymentTemp")

debtcontacts = spark.sql("""
        SELECT D.AccountOwnerID, 
        D.AccountOwner, 
        D.PortfolioID, 
        D.Portfolio, 
        D.SubPortfolioID, 
        D.SubPortfolio, 
        D.DebtID, 
        D.DebtStatusID, 
        D.DebtStatus, 
        D.Category, 
        D.BindingID, 
        D.BindingStatusID, 
        D.BindingStatus, 
        D.CustomerID, 
        D.OriginalDebtNumber,
        D.DebtNumber,
        D.OpenDate,
        D.OriginalFirstDefaultDate,
        D.FirstDefaultDate, 
        D.InitialBalance, 
        D.OpenBalance, 
        D.OriginalFirstDefaultBalance, 
        D.FirstDefaultBalance, 
        D.CurrentOpenBalance, 
        D.CurrentClosedBalance, 
        D.CurrentBalance, 
        D.ChargeOffDate, 
        D.ChargeOffBalance, 
        D.SettlementDate, 
        D.RecallDate, 
        D.SellDate, 
        D.PlacementDate, 
        D.DebtCreationDate, 
        D.LastModificationDate, 
        D.OriginalProductID, 
        D.ProductID, 
        P.ProductTypeID, 
        P.ProductCode, 
        P.ProductName, 
        P.ProductTypeAlias, 
        P.Products, 
        DCE.CustomCode1,
        DCE.CustomCode2, 
        DCE.CustomCode3, 
        DCE.CustomCode4, 
        DCE.CustomCode5, 
        DCE.CustomNumber1, 
        DCE.CustomNumber2, 
        DCE.CustomNumber3, 
        DCE.CustomNumber4, 
        DCE.CustomNumber5, 
        DCE.CustomString1, 
        DCE.CustomString2, 
        DCE.CustomString3, 
        DCE.CustomString4, 
        DCE.CustomString5, 
        DCE.CustomDate1, 
        DCE.CustomDate2, 
        DCE.CustomDate3, 
        DCE.CustomDate4, 
        DCE.CustomDate5, 
        WT.LastWKTrackingID, 
        COALESCE(S.Assets, 0) AS Assets, 
        COALESCE(S.Liabilities, 0 ) AS Liabilities, 
        CASE WHEN CR.DebtID IS NOT NULL THEN 1
        ELSE 0 END AS AlreadyRestriction, 
        CASE WHEN A.DebtID IS NOT NULL THEN 1 
        ELSE 0 END AS AlreadyArrangement, 
        CASE WHEN LP.DebtID IS NOT NULL THEN 1 
        ELSE 0 END AS AlreadyPaid, 
        LP.LastPaymentDate, 
        LP.LastPaymentAmount, 
        C.ContactType, 
        C.IdentityType, 
        C.IdentityNumber, 
        C.CustomerName,
        C.BirthDate, 
        C.DeathDate, 
        C.GenderID, 
        C.Gender, 
        C.MaritalStatusID, 
        C.MaritalStatus, 
        C.isEmployed, 
        C.Occupation, 
        C.QualitativeAdressesID, 
        C.QualitativeAdresses, 
        C.PostalCode, 
        C.District, 
        C.City, 
        C.State, 
        C.StateAlias, 
        C.Country,
        COALESCE(CR.RestrictionStatus, 0) AS RestrictionStatus
        FROM DebtsTemp AS D
        LEFT JOIN ProductsTemp AS P ON D.DebtID = P.DebtID
        LEFT JOIN WorkflowTrackingsTemp AS WT ON D.DebtID = WT.DebtID
        LEFT JOIN SuitsTemp AS S ON D.DebtID = S.DebtID
        LEFT JOIN CreditRestrictionsTemp AS CR ON D.DebtID = CR.DebtID
        LEFT JOIN ArrangementsTemp AS A ON D.DebtID = A.DebtID
        LEFT JOIN LastPaymentTemp AS LP ON D.DebtID = LP.DebtID
        LEFT JOIN ContactsTemp AS C ON D.DebtID = C.DebtID
        LEFT JOIN DebtCustomExtensions AS DCE ON D.DebtID = DCE.DebtID
        """)
debtcontacts.createOrReplaceTempView("DebtContactsTemp")

debtupdate = spark.sql("""
     SELECT D.DebtID,
     D.DebtStatus AS DebtStatusID,
     REPLACE(WSD.StatusAlias, 'DebtStatus', '') AS DebtStatus,
     D.Category, B.BindingStatus AS BindingStatusID,
     REPLACE(WSB.StatusAlias, 'BindingStatus', '') AS BindingStatus,
     B.CustomerID,
     D.FirstDefaultDate,
     D.FirstDefaultBalance,
     D.CurrentOpenBalance,
     D.CurrentClosedBalance,
     D.CurrentBalance,
     D.ChargeOffDate,
     D.ChargeOffBalance,
     D.SettlementDate,
     D.RecallDate,
     D.SellDate,
     D.LastModificationDate
     FROM BusinessUnits AS BU
     INNER JOIN Portfolios AS P ON BU.BusinessUnitID = P.BusinessUnitID
     INNER JOIN Debts AS D ON P.PortfolioID = D.PortfolioID
     INNER JOIN Bindings AS B ON D.DebtID = B.DebtID
     LEFT JOIN WorkflowStatuses AS WSD ON D.DebtStatus = WSD.WorkflowStatus
     LEFT JOIN WorkflowStatuses AS WSB ON B.BindingStatus = WSB.WorkflowStatus
     WHERE P.BusinessUnitID IN ( 672,  743 )
""")
debtupdate.createOrReplaceTempView("DebtUpdate")

contactupdate = spark.sql("""
     SELECT D.DebtID, 
     CP.BirthDate, 
     CP.DeathDate, 
     RCG.OriginalAlias AS GenderID, 
     RCG.Caption AS Gender, 
     RCM.OriginalAlias AS MaritalStatusID, 
     RCM.Caption AS MaritalStatus, 
     CP.isEmployed, 
     CP.Occupation, 
     CASE WHEN CC.PrimaryAddressID IS NOT NULL 
     THEN 1 WHEN CC.PrimaryAddressID IS NULL AND CC.BestAddressID IS NOT NULL 
     THEN 2 ELSE 0 END AS QualitativeAdressesID, 
     CASE WHEN CC.PrimaryAddressID IS NOT NULL 
     THEN 'Primary' WHEN CC.PrimaryAddressID IS NULL 
     AND CC.BestAddressID IS NOT NULL 
     THEN 'Best' END AS QualitativeAdresses, 
     A.PostalCode, 
     A.District, 
     A.City,
     S.Name AS State, 
     S.Alias AS StateAlias, 
     'Brasil' AS Country
     FROM DebtUpdate AS D
     LEFT JOIN (Contacts AS CC
     LEFT JOIN Addresses AS A ON COALESCE(CC.PrimaryAddressID, CC.BestAddressID) = A.AddressID
     LEFT JOIN States AS S ON A.State = S.Alias) ON D.CustomerID = CC.ContactID
     LEFT  JOIN (People AS CP
     LEFT JOIN Gender AS RCG ON CP.Gender = RCG.OriginalAlias
     LEFT JOIN MaritalStatus AS RCM ON CP.MaritalStatus = RCM.OriginalAlias ) ON D.CustomerID = CP.ContactID
     """)
     
contactupdate.createOrReplaceTempView("ContactUpdate")


lastpaymentupdate = spark.sql("""
		SELECT D.DebtID, 
		P.Date AS LastPaymentDate, 
		SUM(P.CollectionAmount) AS LastPaymentAmount	
		FROM DebtUpdate AS D
		INNER JOIN PaymentsTemp AS MP ON D.DebtID = MP.DebtID
		INNER JOIN Payments AS P ON MP.DebtTransactionID = P.DebtTransactionID
		GROUP BY D.DebtID, P.Date
                """)
lastpaymentupdate.createOrReplaceTempView("LastPaymentUpdate")

debtcontacts = debtcontacts.alias("DC").join(debtupdate.alias("D"), 
        debtcontacts.DebtID == debtupdate.DebtID, "left"
        ).selectExpr("DC.*",
        "D.DebtStatusID AS _DebtStatusID",
	    "D.DebtStatus AS _DebtStatus", 
	    "D.Category AS _Category", 
	    "D.BindingStatusID AS _BindingStatusID", 
	    "D.BindingStatus AS _BindingStatus", 
	    "D.CustomerID AS _CustomerID", 
	    "D.FirstDefaultDate AS _FirstDefaultDate", 
	    "D.FirstDefaultBalance AS _FirstDefaultBalance", 
	    "D.CurrentOpenBalance AS _CurrentOpenBalance", 
	    "D.CurrentClosedBalance AS _CurrentClosedBalance", 
	    "D.CurrentBalance AS _CurrentBalance", 
	    "D.ChargeOffDate AS _ChargeOffDate", 
	    "D.ChargeOffBalance AS _ChargeOffBalance", 
	    "D.SettlementDate AS _SettlementDate", 
	    "D.RecallDate AS _RecallDate", 
	    "D.SellDate AS _SellDate", 
	    "D.LastModificationDate AS _LastModificationDate")\
        .withColumn("DebtStatusID", col("_DebtStatusID"))\
        .withColumn("DebtStatus", col("_DebtStatus"))\
        .withColumn("Category", col("_Category"))\
        .withColumn("BindingStatusID", col("_BindingStatusID"))\
        .withColumn("CustomerID", col("_CustomerID"))\
        .withColumn("FirstDefaultDate", col("_FirstDefaultDate"))\
        .withColumn("FirstDefaultBalance", col("_FirstDefaultBalance"))\
        .withColumn("CurrentOpenBalance", col("_CurrentOpenBalance"))\
        .withColumn("CurrentClosedBalance", col("_CurrentClosedBalance"))\
        .withColumn("ChargeOffDate", col("_ChargeOffDate"))\
        .withColumn("ChargeOffBalance", col("_ChargeOffBalance"))\
        .withColumn("SettlementDate", col("_SettlementDate"))\
        .withColumn("RecallDate", col("_RecallDate"))\
        .withColumn("SellDate", col("_SellDate"))\
        .withColumn("LastModificationDate", col("_LastModificationDate"))\
        .drop("_DebtStatusID",
        "_DebtStatus",
        "_Category",
        "_BindingStatusID",
        "_CustomerID",
        "_FirstDefaultDate",
        "_FirstDefaultBalance",
        "_CurrentOpenBalance",
        "_CurrentClosedBalance",
        "_ChargeOffDate",
        "_ChargeOffBalance",
        "_SettlementDate",
        "_RecallDate",
        "_SellDate",
        "_LastModificationDate")

debtcontacts = debtcontacts.alias("DC").join(contactupdate.alias("C"),
        debtcontacts.DebtID == contactupdate.DebtID, "left")\
    .selectExpr("DC.*",
    "C.BirthDate AS _BirthDate", 
	"C.DeathDate AS _DeathDate", 
	"C.GenderID AS _GenderID", 
	"C.Gender AS _Gender", 
	"C.MaritalStatusID AS _MaritalStatusID", 
	"C.MaritalStatus AS _MaritalStatus", 
	"C.isEmployed AS _isEmployed", 
	"C.Occupation AS _Occupation", 
	"C.QualitativeAdressesID AS _QualitativeAdressesID", 
	"C.QualitativeAdresses AS _QualitativeAdresses", 
	"C.PostalCode AS _PostalCode", 
	"C.District AS _District",
	"C.City AS _City", 
	"C.State AS _State", 
	"C.StateAlias AS _StateAlias", 
	"C.Country AS _Country")\
    .withColumn("BirthDate", col("_BirthDate"))\
    .withColumn("DeathDate", col("_DeathDate"))\
    .withColumn("GenderID", col("_GenderID"))\
    .withColumn("Gender", col("_Gender"))\
    .withColumn("MaritalStatusID", col("_MaritalStatusID"))\
    .withColumn("MaritalStatus", col("_MaritalStatus"))\
    .withColumn("isEmployed", col("_isEmployed"))\
    .withColumn("Occupation", col("_Occupation"))\
    .withColumn("QualitativeAdressesID", col("_QualitativeAdressesID"))\
    .withColumn("QualitativeAdresses", col("_QualitativeAdresses"))\
    .withColumn("PostalCode", col("_PostalCode"))\
    .withColumn("District", col("_District"))\
    .withColumn("City", col("_City"))\
    .withColumn("State", col("_State"))\
    .withColumn("StateAlias", col("_StateAlias"))\
    .withColumn("Country", col("_Country"))\
    .drop("_BirthDate",
    "_DeathDate",
    "_GenderID",
    "_Gender",
    "_MaritalStatusID",
    "_MaritalStatus",
    "_isEmployed",
    "_Occupation",
    "_QualitativeAdressesID",
    "_QualitativeAdresses",
    "_PostalCode",
    "_City",
    "_State",
    "_StateAlias",
    "_Country")

debtcontacts = debtcontacts.alias("DC").join(lastpaymentupdate.alias("P"),
        debtcontacts.DebtID == lastpaymentupdate.DebtID, "left")\
        .selectExpr("DC.*", 
        "P.LastPaymentDate AS _LastPaymentDate", 
        "P.LastPaymentAmount AS _LastPaymentAmount")\
        .withColumn("AlreadyPaid", lit(1))\
        .withColumn("LastPaymentDate", col("_LastPaymentDate"))\
        .withColumn("LastPaymentAmount", col("_LastPaymentAmount"))\
        .drop(    
        "_LastPaymentDate",
        "_LastPaymentAmount",
        )

debtcontacts = debtcontacts.alias('DC').join(creditRestrictions.alias('C'),
            debtcontacts.DebtID == creditRestrictions.DebtID, 'left')\
            .selectExpr('DC.*',
            'C.RestrictionStatus as _RestrictionStatus')\
            .withColumn("RestrictionStatus", col("_RestrictionStatus"))\
            .withColumn("AlreadyRestriction", lit(1))\
            .drop("_RestrictionStatus")

debtcontacts = debtcontacts.alias("DC").join(suits.alias("S"),
        debtcontacts.DebtID == suits.DebtID, "left")\
        .selectExpr("DC.*", 
        "S.Assets as _Assets", 
        "S.Liabilities as _Liabilities")\
        .withColumn("Assets", col("_Assets"))\
        .withColumn("Liabilities", col("_Liabilities"))\
        .drop("_Assets", "_Liabilities")


debtcontacts = debtcontacts.alias("DC").join(arrangements.alias("A"),
        debtcontacts.DebtID == arrangements.DebtID, "inner")\
        .selectExpr("DC.*")\
        .withColumn("AlreadyArrangement", lit(1))
       

debtcontacts = debtcontacts.alias("DC").join(workflowtrackings.alias("W"),
        debtcontacts.DebtID == workflowtrackings.DebtID, "left")\
        .selectExpr("DC.*", "W.LastWKTrackingID AS _LastWKTrackingID")\
        .withColumn("LastWKTrackingID", col("_LastWKTrackingID"))\
        .drop("_LastWKTrackingID")


# Reparticiona a tabela        
num_repartitions = ceil(debtcontacts.count() / MAX_REGISTERS_REPARTITION)
outputDF = debtcontacts.repartition(num_repartitions)


# Transforma as colunas em lower case e escreve a tabela no S3
outputDF = lower_column_names(outputDF)


outputDF.write.format("parquet") \
        .option("header", True) \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("encoding", "UTF-8") \
        .mode("append") \
        .save(datamart_debtcontacts)
print("visualizando schema DM")
outputDF.printSchema()


# Contagem dos registros inseridos
total_registers = outputDF.count()


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = outputDF.agg({"lastmodificationdate": "max"}).collect()[0][0]
max_id = outputDF.agg({"debtid": "max"}).collect()[0][0]
max_creation_date = outputDF.agg({"debtcreationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Fim do script
print(f'Tempo de execução: {elapsed_time}')
print(f'Numero de registros inseridos: {total_registers}')
job.commit()

