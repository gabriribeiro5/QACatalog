#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import numpy as np
import datetime
import math
import io
import boto3
import json
import dask.dataframe as dd
from pyspark.sql import functions
import awswrangler as wr
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from concurrent.futures import ThreadPoolExecutor
from pandas.tseries.offsets import MonthEnd, MonthBegin


# #### Formatação de datas pra pegar os parquets do mês atual e anterior da tabela payments

today = datetime.date.today()
dateLastMonth = today.replace(day=1) - datetime.timedelta(days=1)
month = today.strftime("%m")
monthYear = today.strftime("%Y%m")
yearMonth = today.strftime("%Y")
lastMonth = dateLastMonth.strftime("%m")
yearLastMonth = dateLastMonth.strftime("%Y")
lastMonthYear = dateLastMonth.strftime("%Y%m")

#teste
month = "10"
lastMonth = "09"
yearMonth = "2021"


dtMonthPayments = dd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/payments/year='+yearMonth+'/month='+month+'/').compute()

dtLastMonthPayments = dd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/payments/year='+yearMonth+'/month='+lastMonth+'/').compute()

payments = pd.concat([dtMonthPayments, dtLastMonthPayments], ignore_index=True)

del dtLastMonthPayments, dtMonthPayments

AssignmentsOptimizationRestrictive = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datascience/assignmentsoptimizationrestrictive/AssignmentsOptimizationRestrictive.parquet').compute()

AssignmentsOptimizationRestrictiveSegments = pd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datascience/assignmentsoptimizationrestrictivesegments/AssignmentsOptimizationRestrictiveSegments.parquet')

AssignmentsOptimizationRestrictivePortfolios = pd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datascience/assignmentsoptimizationrestrictiveportfolios/AssignmentsOptimizationRestrictivePortfolios.parquet')

businessUnits = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/ftcrm/businessunits/*.parquet').compute()

businessUnits = businessUnits.rename(columns={"businessunitid":"AgencyID"})


# ### Definição das datas necessárias pra análise

today = datetime.date.today()
reference_date = today - MonthBegin() # mÊs atual
optimization_month = reference_date + MonthBegin() # próximo mês

mes_atual = optimization_month - MonthBegin()
mes_anterior = mes_atual - MonthBegin()
cuts_age = [0, 5, 10, math.inf]

mes_atual = pd.to_datetime(mes_atual, format="%Y-%m-%d")
mes_anterior = pd.to_datetime(mes_anterior, format="%Y-%m-%d")

#teste
mes_anterior = pd.to_datetime('2021-09-01', format="%Y-%m-%d")
mes_atual = pd.to_datetime('2021-10-01', format="%Y-%m-%d")


# ### Carrega o nome das agências

 


agency = businessUnits[["AgencyID", "name", "alias"]]
agency = agency.rename(columns={'AgencyID': 'agency_id'})


# ###  Listando portfolio ativos e com  AccountOwnerID = 672

portfolio_ids__enabled = AssignmentsOptimizationRestrictivePortfolios[AssignmentsOptimizationRestrictivePortfolios.isEnabled == 1][["PortfolioID"]]
portfolio_ids__enabled.sort_values(by=['PortfolioID'], inplace=True)

portifolios = portfolio_ids__enabled["PortfolioID"].tolist()

# ### Seleciona dividas que não estão com acordos ativos 
# #### As dividas que possuem acordos ativos são marcadas com bindingid not NULL, bindingid é usado para direcionar dívidas para as diferentes agencias

debtcontacts_bindingid_null = dd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/part*.parquet', columns=["DebtID","DebtStatus","BindingID", "AccountOwnerID"]).compute()

debt_bindingid_null = debtcontacts_bindingid_null[(debtcontacts_bindingid_null.BindingID.isnull()) & (debtcontacts_bindingid_null.AccountOwnerID == 672)]

debt_bindingid_null = debt_bindingid_null.rename(columns={'DebtID': 'debt_id', 'DebtStatus': 'status'})

# ### Seleciona dividas ajuizadas 
# ##### Dívidas ajuizadas não devem ser cobradas


debtcontacts = dd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/part*.parquet', columns=["Assets","Liabilities", "CustomerID", "AccountOwnerID", "DebtStatus"]).compute()

customer_ajuizado = debtcontacts[(debtcontacts.AccountOwnerID == 672) & (debtcontacts.DebtStatus == 7000) & ((debtcontacts.Assets == 1) | (debtcontacts.Liabilities == 1))]

del debtcontacts

customer_ajuizado = customer_ajuizado.rename(columns={'Assets': 'assets', 'Liabilities': 'liabilities',"CustomerID": "customer_id"})

customer_ajuizado = customer_ajuizado.groupby("customer_id", as_index = False)[["assets", "liabilities" ]].max()

# ### Verifica as agências que estão habilitadas receber dívidas

segment_restriction = pd.merge(AssignmentsOptimizationRestrictiveSegments, AssignmentsOptimizationRestrictive, on="SegmentID")

#teste
data = '2021-11-01 00:00:00'

segment_restriction = segment_restriction[segment_restriction.DistributionDate == data][["SegmentID","Segment","AgencyID","ReferenceBalance","AvailableBalance","isEnabled_y","EnforcedFlag"]]

segment_restriction = segment_restriction.rename(columns={'SegmentID':'segment_id', 'Segment':'segment', 'AgencyID':'agency_id', 'ReferenceBalance':'reference_balance','AvailableBalance':'available_balance','isEnabled_y':'is_enabled','EnforcedFlag':'enforced_flag'})

segment_restriction.sort_values(by=['segment_id'], inplace=True)

# ### Seleciona os segmentos disponíveis

segment_portfolio = AssignmentsOptimizationRestrictivePortfolios[AssignmentsOptimizationRestrictivePortfolios.isEnabled == 1][["SegmentID", "PortfolioID"]]

segment_portfolio = segment_portfolio.rename(columns={'SegmentID': 'segment_id', 'PortfolioID':'portfolio_id'})

# ## new_assignments_template 
# ### Seleciona as distribuições mais recentes

# ### Selecionar as distribuições ativas que estarão com AssignmentStatus != 95005

 


#assignments_new = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datamart/assignment-debts-analytical/*.parquet', columns = ["AssignmentDate", "DebtID", "AgencyID", "AssignmentID", "DebtStatus", "DebtStatusID"]).compute()
assignments_new = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datamart/assignment-debts-analytical/outubro/*.parquet', columns = ["AssignmentDate", "DebtID", "AgencyID", "AssignmentID", "DebtStatus", "DebtStatusID"]).compute() 

#teste
assignments_new2 = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datamart/assignment-debts-analytical/setembro/*.parquet', columns = ["AssignmentDate", "DebtID", "AgencyID", "AssignmentID", "DebtStatus", "DebtStatusID"]).compute() 
assignments_new = pd.concat([assignments_new, assignments_new2])

assignments_new = assignments_new.drop_duplicates()

assignments_new["AssignmentDate"] = pd.to_datetime(assignments_new["AssignmentDate"])


 


#teste
reference_date = pd.to_datetime('2021-10-01', format="%Y-%m-%d")
reference_date


 


assignmentDate = assignments_new[(assignments_new.AssignmentDate <= reference_date) & (assignments_new.DebtStatus == 'Active')]


 


debtcontacts_new = pd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/', columns = ["PortfolioID", "CustomerID", "BindingID", "DebtID", "SettlementDate", "RecallDate", "SellDate", "DebtStatus"])


 


debtcontacts_new_portifolios = debtcontacts_new[debtcontacts_new.PortfolioID.isin(portifolios)]


 


max_assignment = pd.merge(assignmentDate, debtcontacts_new_portifolios, on="DebtID")


del debtcontacts_new_portifolios, assignmentDate


 


max_assignment = max_assignment.groupby("DebtID")[["AssignmentID"]].min()


# #### Verifica se o debtos estão abertos
# 

 


ref_date = reference_date


 


debtcontacts_new["SellDate"] = pd.to_datetime(debtcontacts_new["SellDate"])


 


debts = debtcontacts_new[(debtcontacts_new.SettlementDate.isnull() | debtcontacts_new.RecallDate.isnull() | debtcontacts_new.SellDate.isnull()) | ((debtcontacts_new.SettlementDate > ref_date) & (~debtcontacts_new.SettlementDate.isnull())) | ((~debtcontacts_new.RecallDate.isnull()) & (debtcontacts_new.RecallDate > ref_date)) | ((debtcontacts_new.SellDate > ref_date) & (~debtcontacts_new.SellDate.isnull()))]

debts['DebtStatus'] = debts['DebtStatus'].fillna(0)


debts['DebtStatus'] = debts['DebtStatus'].astype('int')


# #### Pega apenas as dívidas disponíveis para distribuição DebtStatus = 7000

 


debts = debts[debts.DebtStatus == 7000]


# #### Remove os escritórios que são digitais, esses podem estar a dívida concomitantemente em mais de um escritório

 


units = businessUnits[businessUnits.businessunittypeid.isin([4,8])]


# #### Remove algumas agências

 


ass = assignments_new[~assignments_new.AgencyID.isin(["716","175","756","820"])]


 


new_assignments_template = pd.merge(pd.merge(pd.merge(ass,max_assignment,on='AssignmentID'),debts,on='DebtID'), units, on="AgencyID")


 


new_assignments_template[new_assignments_template.DebtID == 35379265 ]


 


new_assignments_template[new_assignments_template.duplicated()]


 


del ass, max_assignment, debts, debtcontacts_new, assignments_new, businessUnits


 


new_assignments_template= new_assignments_template[["AssignmentDate", "PortfolioID", "DebtID", "CustomerID", "AgencyID", "BindingID"]]


 


new_assignments_template.columns = ['assignment_time', 'portfolio_id','debt_id','customer_id','agency_id','binding_id']


 


new_assignments_template.sort_values(by=['portfolio_id', 'debt_id'], inplace=True)

# ### process_portfolio

# ### Verifica para quais agências as dívidas foram distribuidas no mês atual 

 


#teste
assingments_process_portfolio = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datamart/assignment-debts-analytical/outubro/*.parquet', columns = ["RefDate","DebtID", "PortfolioID", "CustomerID", "AgencyID", "AssignmentDate"]).compute() 


 


#teste
assingments_process_portfolio2 = dd.read_parquet('s3://dlr-dev-bucket-rawzone/pic/datamart/assignment-debts-analytical/setembro/*.parquet', columns = ["RefDate","DebtID", "PortfolioID", "CustomerID", "AgencyID", "AssignmentDate"]).compute() 




assingments_process_portfolio = pd.concat([assingments_process_portfolio, assingments_process_portfolio2])


 


assingments_process_portfolio = assingments_process_portfolio.drop_duplicates()


 


assingments_process_portfolio["AssignmentDate"] = pd.to_datetime(assingments_process_portfolio["AssignmentDate"])


 


assingments_process_portfolio["RefDate"] = pd.to_datetime(assingments_process_portfolio["RefDate"], format="%Y%m")


 


# temp_start = assingments_process_portfolio["AssignmentDate"].max() - MonthBegin(1)


 


# ref_date_month = pd.to_datetime(temp_start, format="%Y-%m-%d") + pd.offsets.Day(10)


 


# ref_date_month


 


# assingments_process_portfolio.dtypes


 


# assingments_process_portfolio["months_in_agency"] = np.floor((ref_date_month - assingments_process_portfolio["AssignmentDate"])/np.timedelta64(1, 'M'))

 


#teste
#avaliar a formatação do mês atual


assingments_1 = assingments_process_portfolio[(assingments_process_portfolio.PortfolioID.isin(portifolios)) & (~assingments_process_portfolio.AgencyID.isin([716, 175,756,820])) & (assingments_process_portfolio.RefDate == mes_atual)]


 


assingments_1.columns = ["time", "debt_id", "portfolio_id", "customer_id", "agency_id", "assignment_date"]


val__assingments_1 = len(assingments_1)


 


# Inserindo segmento em assingments_1
assingments_1 = pd.merge(assingments_1, segment_portfolio, validate="many_to_one")


 


assert val__assingments_1 == len(assingments_1),             "assingments_1 mudou de tamanho ao inserir o segmento"


 


assingments_1.drop(["portfolio_id", "assignment_date"], axis=1, inplace=True)


# ### Verifica para quais agências as dívidas foram distribuidas no mês anterior 

 


#teste
#avaliar a formatação do mês anterior


 


assingments_2 = assingments_process_portfolio[(assingments_process_portfolio.PortfolioID.isin(portifolios)) & (~assingments_process_portfolio.AgencyID.isin([716, 175,756,820])) & (assingments_process_portfolio.RefDate == mes_anterior)] 


 


assingments_2.columns = ["time", "debt_id", "portfolio_id", "customer_id", "agency_id", "assignment_date"]


 


val__assingments_2 = len(assingments_2)


 


# Inserindo segmento em assingments_2
assingments_2 = pd.merge(assingments_2, segment_portfolio, validate="many_to_one")


 


assert val__assingments_2 == len(assingments_2),             "assingments_2 mudou de tamanho ao inserir o segmento"



assingments_2.drop(["portfolio_id", "assignment_date"], axis=1, inplace=True)


 


assingments = pd.concat([assingments_1, assingments_2])
assingments["time"] = pd.to_datetime(assingments["time"], format="%Y-%m-%d")


 


#teste
assingments = assingments.drop_duplicates(["time", "debt_id", "customer_id", "segment_id"])


 


count_assingments = assingments.groupby([
    "time", "debt_id"])["debt_id"].count()
assert (count_assingments == 1).all(), (
    "portfolio_id: Existe algum debtos com mais de um "
    "assingment")


 


assingments[assingments.debt_id == 12234496]


 


del assingments_1, assingments_2, assingments_process_portfolio


# #### Ajustando as dividas da FNX, a agencia 660 e 481 devem ser agrupadas para a distribuição

 


assingments["agency_id"] = assingments["agency_id"].replace({660: 481})


# ## Verifica as dívidas de forma distribuidas, quais estavam aptas para cobrança (não estavam em acordo ativo, ou judicializado)

# #### a tabela database__debt__time_categorical é a debtcontacts para cada mês

 


#debtcontacts_columns = pd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/', columns=["PortfolioID","DebtID", "CustomerID", "DebtStatus", "DebtCreationDate"])


 


#teste
debtcontacts_columns = pd.read_csv('s3://dlr-dev-bucket-refinedzone/pic/datamart/teste/database_debt_time_categorical_202201221101.csv')


 


#teste
debtcontacts_columns2 = pd.read_csv('s3://dlr-dev-bucket-refinedzone/pic/datamart/teste/database_debt_time_categorical_202201221204.csv')

debtcontacts_columns = pd.concat([debtcontacts_columns, debtcontacts_columns2])

#teste - analisar como esses dados vão vir
#debtcontacts_columns['DebtCreationDate'] = pd.to_datetime(debtcontacts_columns['DebtCreationDate']).dt.date

#teste - analisar como esses dados vão vir
#debtcontacts_columns['DebtCreationDate'] = debtcontacts_columns['DebtCreationDate'].apply(lambda dt: dt.replace(day=1))

debtcontacts_columns["time"] = pd.to_datetime(debtcontacts_columns["time"])

#debt_status_1 = debtcontacts_columns[(debtcontacts_columns.DebtStatus == 7000)  & (debtcontacts_columns.PortfolioID.isin(portifolios)) & (debtcontacts_columns.DebtCreationDate == mes_atual)]

debt_status_1 = debtcontacts_columns[(debtcontacts_columns.debt_status == 7000)  & (debtcontacts_columns.portfolio_id.isin(portifolios)) & (debtcontacts_columns.time == mes_atual)]

debt_status_1['debt_status'] = debt_status_1['debt_status'].astype('int')

#teste - analisar ordem das colunas
debt_status_1.columns = ["time", "portfolio_id", "debt_id", "customer_id", "debt_status"]

val__debt_status_1 = len(debt_status_1)

debt_status_1 = pd.merge(debt_status_1, segment_portfolio, validate="many_to_one")

assert val__debt_status_1 == len(debt_status_1),"debt_status_1 mudou de tamanho ao inserir o segmento"

debt_status_1.drop(["portfolio_id"], axis=1, inplace=True)


# #### Pegando as dívidas distribuidas do mês anterior

#debt_status_2 = debtcontacts_columns[(debtcontacts_columns.DebtStatus == 7000)  & (debtcontacts_columns.PortfolioID.isin(portifolios)) & (debtcontacts_columns.DebtCreationDate == mes_anterior)]

debt_status_2 = debtcontacts_columns[(debtcontacts_columns.debt_status == 7000)  & (debtcontacts_columns.portfolio_id.isin(portifolios)) & (debtcontacts_columns.time == mes_anterior)]

debt_status_2['debt_status'] = debt_status_2['debt_status'].astype('int')

debt_status_2.columns = [ "time", "portfolio_id", "debt_id","customer_id", "debt_status"]


 


val__debt_status_2 = len(debt_status_2)


 


# Inserindo segmento em debt_status_2
debt_status_2 = pd.merge(debt_status_2, segment_portfolio, validate="many_to_one")


assert val__debt_status_2 == len(debt_status_2),"debt_status_2 mudou de tamanho ao inserir o segmento"

debt_status_2.drop(["portfolio_id"], axis=1, inplace=True)

debt_status = pd.concat([debt_status_1, debt_status_2])
debt_status["time"] = pd.to_datetime(debt_status["time"], format="%Y%m")

debt_status["customer_id"] = debt_status["customer_id"].astype(int)

del debt_status_1, debt_status_2, debtcontacts_columns

#######################################
# Associando o status de distribuição #
assingments_status = assingments.merge(debt_status, how="outer", validate="one_to_one") 

del debt_status, assingments

assingments_status['debt_status'] = assingments_status['debt_status'].fillna(0)

assingments_status['debt_status'] = assingments_status['debt_status'].astype('int')

assingments_status['debt_status'] = assingments_status['debt_status'].astype('str')


# ####  Ajustando o status quando não tem status
assingments_status["debt_status"] = assingments_status["debt_status"].replace(['0'], "sem_status")


# #### Identifica as dívidas que estão ajuizadas
index_ajuizado = assingments_status["customer_id"].isin(customer_ajuizado["customer_id"].to_list())
assingments_status.loc[index_ajuizado, "debt_status"] = "ajuizado"

# index_na_month = assingments_status["months_in_agency"].isna()
# assingments_status = assingments_status[~index_na_month].copy()

# #### Informações da dívida 
debtcontacts = pd.read_parquet('s3://dlr-dev-bucket-refinedzone/pic/datamart/debtcontacts/', columns=["PortfolioID", "Portfolio", "DebtID", "Products", "FirstDefaultDate", "OriginalFirstDefaultBalance"])

debt_info = debtcontacts[debtcontacts.PortfolioID.isin(portifolios)]

del debtcontacts

debt_info = debt_info.rename(columns={"PortfolioID":"portfolio_id", "Portfolio":"portfolio", "DebtID": "debt_id", "Products": "products", "OriginalFirstDefaultBalance":"reference_balance", "FirstDefaultDate":"first_default_date"})


# #### Fazendo o cálculo de idade da dívida para meses completos 

debt_info["first_default_date"] = debt_info["first_default_date"].dt.floor("d") - pd.offsets.MonthBegin(1)

len_assingments = len(assingments_status)

assingments_status = debt_info.merge(assingments_status, validate="one_to_many")

assert len_assingments == len(assingments_status), ("Algumas dívidas distribuidas não tem debt info.")


# ### Query para cash novo deve pegar os dados do mês de distribuição e do mês anterior

#teste
mes_anterior = pd.to_datetime('2021-09-01', format="%Y-%m-%d")
mes_atual = pd.to_datetime('2021-10-01', format="%Y-%m-%d")

payments["RefDate"] = pd.to_datetime(payments["RefDate"], format='%Y%m')

cash_novo = payments[(payments.PortfolioID.isin(portifolios)) & (payments.CashType == 'cash_novo') & (payments.RefDate.isin([mes_atual, mes_anterior]))][["RefDate", "PortfolioID", "DebtID", "CustomerID", "AgencyID", "CollectionAmount" ]]

cash_novo = cash_novo.rename(columns={"RefDate":"time", "PortfolioID":"portfolio_id", "DebtID":"debt_id", "CustomerID":"customer_id", "AgencyID":"agency_id", "CollectionAmount":"cash_novo"})
# #### Ajustando as dividas da FNX, a agencia 660 e 481 devem ser agrupadas para a distribuição

cash_novo["agency_id"] = cash_novo["agency_id"].replace({660: 481})
cash_novo["cash_novo"] = pd.to_numeric(cash_novo["cash_novo"])

cash_novo = cash_novo.groupby(["time", "portfolio_id", "debt_id", "agency_id", "customer_id"], as_index=False, observed=True)["cash_novo"].sum()

assingments_status = assingments_status.merge(cash_novo,  how="left", validate="one_to_one")

assingments_status["cash_novo"] = assingments_status["cash_novo"].fillna(value=0)


# ### Calculando os cortes 

delta_days = assingments_status["time"] - assingments_status["first_default_date"]
debt_age_years = (delta_days / np.timedelta64(1, "M")).round() / 12

debt_age_years_cut = pd.cut(debt_age_years, bins=cuts_age, right=True, labels=cuts_age[:-1])

# assert not debt_age_years_cut.isna().any(), (
#             "Algum corte do debt age está sendo feito errado.")


assingments_status["debt_age_years_cut"] = debt_age_years_cut

assingments_status[assingments_status["debt_age_years_cut"].isna()]

# assert not assingments_status.isna().any().any(),\
#             "Sobrou algum NA na base de dados."

assingments_status = assingments_status[~assingments_status["debt_age_years_cut"].isna()]

##############################################
# Ponderando por 60/40 #poderacao__cash_novo #
# Para o calculo da eficiencia considera todo o valor de cash novo
poderacao__cash_novo = assingments_status.groupby(["time", "agency_id", "segment_id", "products","debt_age_years_cut"], as_index=False, observed=True)["cash_novo"].sum()

cash_mes_atual = poderacao__cash_novo[poderacao__cash_novo["time"] == pd.to_datetime(mes_atual, format="%Y-%m-%d")].rename(columns={"cash_novo": "cash_novo__mes_atual"})

del cash_mes_atual["time"]

cash_mes_anterior = poderacao__cash_novo[poderacao__cash_novo["time"] == pd.to_datetime(mes_anterior, format="%Y-%m-%d")].rename(columns={"cash_novo": "cash_novo__mes_anterior"})

del cash_mes_anterior["time"]
merged_cash = cash_mes_anterior.merge(cash_mes_atual, how="outer")

assingment_status_corrigido = assingments_status.convert_dtypes()
assingments_status['debt_status'] = assingment_status_corrigido["debt_status"].astype('str')


# ### Para o calculo da eficiencia considerar apenas o valor de face livre (dívidas disponíveis) que está disponível em 7000 e que não está ajuizado
# 

index_dividas_livres = assingments_status["debt_status"] == '7000'
poderacao__reference_balance = assingments_status[index_dividas_livres].groupby(["time", "agency_id", "segment_id", "products", "debt_age_years_cut"],as_index=False, observed=True)["reference_balance"].sum()
index_actual = poderacao__reference_balance["time"] == pd.to_datetime(mes_atual, format="%Y-%m-%d")
face_mes_atual = poderacao__reference_balance[index_actual].rename(columns={"reference_balance": "face__total_cobranca__mes_atual"})

del face_mes_atual["time"]

index_previous = poderacao__reference_balance["time"] == pd.to_datetime(mes_anterior, format="%Y-%m-%d")
face_mes_anterior = poderacao__reference_balance[index_previous].rename(columns={"reference_balance": "face__total_cobranca__mes_anterior"})


 


del face_mes_anterior["time"]


 


merged_face = face_mes_anterior.merge(face_mes_atual, how="outer")


 


# Unindo as duas bases
# poderacao = poderacao__cash_novo.merge(poderacao__reference_balance, validate="one_to_one",how="outer")
# poderacao["cash_novo"].fillna(value=0, inplace=True)


 


# validando ponderacao
# index_na_reference_balance = poderacao["reference_balance"].isna()
# if index_na_reference_balance.any():
#     poderacao["reference_balance"].fillna(value=0, inplace=True)
#     poderacao = poderacao[poderacao["reference_balance"] != 0].copy()


# poderacao.reset_index(inplace=True)


efficiencia = merged_cash.merge(merged_face, how="outer")#.merge(poderacao)

#new_assignments_template são as dívidas que estão disponíveis no momento da otimização
assingments_7000 = new_assignments_template

del new_assignments_template

assingments_7000["agency_id"] = assingments_7000["agency_id"].replace({660: 481})

# Validando se assingments_7000 vem informações
if(len(assingments_7000) < 1):
    print("O portflio {} não tem assingments_7000")
    base_otimizacao = pd.DataFrame([])
    debts_livres: pd.DataFrame([])

###################################################################
# Colocando uma margem de 10 dias para o caso de dividas que são
# distribuidas até o dia 10 ainda são consideradas com parte do mes
# as que forem posteriores a 10 dias passam a ser consideradas como
# distribuidas no próximo mês
assingments_7000["reference_month"] = (assingments_7000["assignment_time"] - pd.offsets.Day(11)) + pd.offsets.MonthBegin(1)

optimization_month = pd.to_datetime('2021-11-01') 

assingments_7000["months_in_agency_opt_month"] = ((optimization_month - assingments_7000["reference_month"])/np.timedelta64(1, "M")).round()

# O prazo para a distribuição das dívidas é de 3 meses no escritório
# após 4 meses as dívidas são consideradas disponíveis para a
# redistribuição
assingments_7000["debt_status"] = "decurso de prazo"

assingments_7000.loc[ assingments_7000["months_in_agency_opt_month"] <= 3, "debt_status"] = "menor 4M"

# Marcando as dívidas que estão ajuizadas para que não estejam
# disponíveis para a redistribuição
index_ajuizado = assingments_7000["customer_id"].isin(customer_ajuizado["customer_id"])
assingments_7000.loc[index_ajuizado, "debt_status"] = "ajuizado"

# Verificando quais dividas não possuem bindingid_null
# elas não podem ser distribuidos
index_bindingid_null = assingments_7000["debt_id"].isin(debt_bindingid_null["debt_id"].to_list())
assingments_7000.loc[index_bindingid_null, "debt_status"] = "bindingid_null"


 


# Criando segment id na base de assigments
assingments_7000 = assingments_7000.merge(segment_portfolio, on="portfolio_id", validate="many_to_one")


 


val__segment = assingments_7000[["portfolio_id", "segment_id"]].drop_duplicates().groupby("portfolio_id")["segment_id"].count()
assert (val__segment.values == 1).all(),     "Há portfolios vinculados a mais de um segmento"


 


val__restriction = len(AssignmentsOptimizationRestrictive[AssignmentsOptimizationRestrictive.DistributionDate == reference_date])


 


# Verificando quais escritórios são permitidos de cobrar as dívidas
# caso o escritório não esteja is_enabled todos as dívidas são
# colocadas para distribuição
if val__restriction == 0:
    # Se não foram carregados segmentos, considerar todos como
    # disponíveis para receber dívidas
    assingments_7000["is_enabled"] = True
    assingments_7000["enforced_flag"] = False
else:
    # Caso contrário consiera os segmentos que não foram colocados
    # como não disponíveis
    assingments_7000 = assingments_7000.merge(
        segment_restriction[[
            'segment_id', 'agency_id', 'is_enabled', 'enforced_flag']],
        how="left")
    assingments_7000["is_enabled"].fillna(value=False, inplace=True)
    assingments_7000["enforced_flag"].fillna(value=False, inplace=True)


 


index_not_enabled = ( ~assingments_7000["is_enabled"] | assingments_7000["enforced_flag"])


 


# Caso um escritório seja descadastrados e sua dívida não esteja
# ajuizada essa divida deve ser distribuida independente do decurso
# de prazo
index_not_ajuizado = ~assingments_7000["debt_status"].isin(["ajuizado", "bindingid_null"])
assingments_7000.loc[index_not_enabled & index_not_ajuizado,"debt_status"] = "descadastrado"


 


val_n = len(assingments_7000)


 


val_n = len(assingments_7000)


 


assingments_7000 = pd.merge(assingments_7000, debt_info, how="left", validate="one_to_one")


 


assert len(assingments_7000) == val_n,    "len(assingments_7000) == val_n ."


 


delta_days = optimization_month - assingments_7000["first_default_date"]
debt_age_years = (delta_days / np.timedelta64(1, "M")).round() / 12


 


debt_age_years_cut = pd.cut(debt_age_years, bins=cuts_age, right=True, labels=cuts_age[:-1])


 


assert not debt_age_years_cut.isna().any(), (
    "Algum corte do debt age está sendo feito errado [%s].")


 


assingments_7000["debt_age_years_cut"] = debt_age_years_cut


 


index_debts_cobranca = ~assingments_7000["debt_status"].isin(["ajuizado", "bindingid_null"])


 


assingments_em_cobranca = assingments_7000.loc[index_debts_cobranca, [
        "segment_id", "debt_id", "binding_id", "customer_id",
        "agency_id", "debt_age_years_cut", "products",
        "debt_status", "assignment_time", "reference_month",
        "months_in_agency_opt_month", "reference_balance"]]


 


assert not assingments_em_cobranca["agency_id"].isin([716, 175, 756, 820]).any(), (
    "assingments_em_cobranca: Existem dividas das "
    "agencias 716, 175, 756, 820 (Vellum...)")


# ### Criando a base de otimização 

 


grouped_assingments_7000 = assingments_7000.groupby([
    "segment_id", "products", "debt_age_years_cut",
    "debt_status", "agency_id", "is_enabled", "enforced_flag"],
    observed=True)["reference_balance"].sum()


 


grouped_assingments_7000 = grouped_assingments_7000.unstack("debt_status")
grouped_assingments_7000.fillna(value=0, inplace=True)


 


# Soma as dívidas que não estão ajuizadas como total de cobrança
grouped_assingments_7000["total_cobranca"] = grouped_assingments_7000.drop(["ajuizado", "bindingid_null"], axis=1, errors="ignore").sum(axis=1)


 


livre_columns = [x for x in ["decurso de prazo", "descadastrado"] if x in grouped_assingments_7000.columns]
grouped_assingments_7000["face_livre"] = grouped_assingments_7000[livre_columns].sum(axis=1)
grouped_assingments_7000.reset_index(inplace=True)


 


# Unindo os debtos livres aos calculos de performance
base_otimizacao = efficiencia.merge(grouped_assingments_7000, how="right")


 


assert not base_otimizacao["agency_id"].isin([
    716, 175, 756, 820]).any(), (
        "base_otimizacao: Existem dividas das agencias 716, 175, "
        "756, 820 (Vellum...)")


 


all_agg_results = base_otimizacao
del base_otimizacao 


 


pd_free_debts = assingments_em_cobranca
del assingments_em_cobranca


 


all_agg_results["time"] = optimization_month
all_agg_results["agency_id"] = pd.to_numeric(all_agg_results["agency_id"])


 


# Agregando resultados por segmento
if "descadastrado" not in all_agg_results.columns:
    all_agg_results["descadastrado"] = 0


 


all_agg_results= all_agg_results.convert_dtypes()


 


all_agg_results["face__total_cobranca__mes_anterior"] = pd.to_numeric(all_agg_results["face__total_cobranca__mes_anterior"])


 


all_agg_results["face__total_cobranca__mes_atual"] = pd.to_numeric(all_agg_results["face__total_cobranca__mes_atual"])


 


all_agg_results["ajuizado"] = pd.to_numeric(all_agg_results["ajuizado"])


 


all_agg_results["decurso de prazo"] = pd.to_numeric(all_agg_results["decurso de prazo"])


aggregation_data = all_agg_results.groupby([
    "time", "is_enabled", "enforced_flag", "agency_id", "segment_id",
    "products", "debt_age_years_cut"], as_index=False, observed=True)[[
        "cash_novo__mes_anterior",
        "cash_novo__mes_atual",
        "face__total_cobranca__mes_anterior",
        "face__total_cobranca__mes_atual",
        "ajuizado",
        "decurso de prazo",
        "descadastrado",
        "menor 4M",
        "total_cobranca",
        "face_livre"]].sum()



all_agg_results["descadastrado"] = all_agg_results["descadastrado"].astype(int)


 


all_agg_results["menor 4M"] = all_agg_results["menor 4M"].astype(float)


 


aggregation_data = all_agg_results.groupby([
    "time", "is_enabled", "enforced_flag", "agency_id", "segment_id",
    "products", "debt_age_years_cut"], as_index=False, observed=True)[[
        "cash_novo__mes_anterior",
        "cash_novo__mes_atual",
        "face__total_cobranca__mes_anterior",
        "face__total_cobranca__mes_atual",
        "ajuizado",
        "menor 4M",
        "decurso de prazo","descadastrado",
        "total_cobranca",
        "face_livre"]].sum()


 


aggregation_data["bloqueado"] = aggregation_data["total_cobranca"] - aggregation_data["face_livre"]


 


# Fazendo o cálculo da eficiência ponderada
aggregation_data["cash_novo__ponderado"] = aggregation_data["cash_novo__mes_anterior"] * 0.4 + aggregation_data["cash_novo__mes_atual"] * 0.6


 


aggregation_data["face__total_cobranca__ponderado"] = aggregation_data["face__total_cobranca__mes_anterior"] * 0.4 + aggregation_data["face__total_cobranca__mes_atual"] * 0.6


 


aggregation_data["eficiencia__60_40"] = aggregation_data["cash_novo__ponderado"] / aggregation_data["face__total_cobranca__ponderado"]


 


# Adicinando o nome da agência a base de dados para facilitar as
# validações
agency_to_merge = agency.rename(columns={"name": "agency"})[["agency_id", "agency"]]
n_val = len(aggregation_data)
aggregation_data = agency_to_merge.merge(aggregation_data[[
    "time", "agency_id", "is_enabled", "enforced_flag", "segment_id",
    "products", "debt_age_years_cut", "cash_novo__mes_anterior",
    "cash_novo__mes_atual", "cash_novo__ponderado",
    "face__total_cobranca__mes_anterior",
    "face__total_cobranca__mes_atual",
    "face__total_cobranca__ponderado",
    "eficiencia__60_40",
    "ajuizado",
    "menor 4M",
    "decurso de prazo",
    "descadastrado",
    "bloqueado",
    "face_livre",
    "total_cobranca"]], validate="one_to_many")

assert len(aggregation_data) == n_val,     "Algumas agências não estão no de-para [agency_to_merge]"

results_val = aggregation_data.groupby(["time", "agency_id", "segment_id", "products","debt_age_years_cut"], observed=True)["cash_novo__ponderado"].count()

assert (1 == results_val).all(),    "Existem valores duplicados"


# #### Formatando os tipos das colunas

pd_free_debts["binding_id"] = pd_free_debts["binding_id"].astype("int64")
pd_free_debts["customer_id"] = pd_free_debts["customer_id"].astype("int64")
pd_free_debts["reference_balance"] = pd_free_debts["reference_balance"].astype("float64")
aggregation_data["descadastrado"] = aggregation_data["descadastrado"].astype("float64")  


pd_free_debts = pd_free_debts.rename(columns={'products': 'product'})
aggregation_data = aggregation_data.rename(columns={'products': 'product'})


# #### Salvar no s3 como parquet

now = datetime.datetime.now().isoformat()

month_optimization = "11"

# #### Salvando a base de debtos livres

s3_file_free_debts = 'models/data_prep/'+yearMonth+'-'+month_optimization+'/free_debts__'+now+'.parquet'
wr.s3.to_parquet(pd_free_debts,path='s3://dlr-dev-bucket-refinedzone/models/data_prep/'+yearMonth+'-'+month_optimization+'/free_debts__'+now+'.parquet')    


# #### Salvando as bases de otimização

s3_file_optimization_db = 'models/data_prep/'+yearMonth+'-'+month_optimization+'/otimization_db__'+now+'.parquet'
wr.s3.to_parquet(aggregation_data,path='s3://dlr-dev-bucket-refinedzone/models/data_prep/'+yearMonth+'-'+month_optimization+'/otimization_db__'+now+'.parquet')


# #### Salvando o metadata

# Salvando a metadata do run
s3_file_metadata = 'models/data_prep/'+yearMonth+'-'+month_optimization+'/metadata__'+now+'.json'
last_run_dict = {
    "run_time": now,
    "files": {
        "otimization_db": s3_file_optimization_db,
        "free_debts_db": s3_file_free_debts,
        "metadata": s3_file_metadata,
    },
    "optimization_month": optimization_month.isoformat(),
    "mes_atual": mes_atual.isoformat(),
    "mes_anterior": mes_anterior.isoformat(),
    "cuts_age": mes_anterior.isoformat(),
    "portfolio_ids__enabled": portfolio_ids__enabled[
        "PortfolioID"].tolist(),
}

run_metadata = json.dumps(last_run_dict, indent=2).encode("utf-8")

s3 = boto3.resource('s3')
s3object = s3.Object('dlr-dev-bucket-refinedzone', 'models/data_prep/'+yearMonth+'-'+month_optimization+'/metadata__'+now+'.json')

s3object.put(
    Body=(bytes(run_metadata))
)

s3object_last = s3.Object('dlr-dev-bucket-refinedzone', 'models/data_prep/'+yearMonth+'-'+month_optimization+'/metadata__last_run.json')

s3object_last.put(
    Body=(bytes(run_metadata))
)

