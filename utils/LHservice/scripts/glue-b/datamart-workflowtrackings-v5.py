#=============================================================================#
# 																			  #
# Objetivo: Script PySpark que cria datamart workflowtrackings 				  #
# Autor: Renato Candido Kurosaki - NTT DATA 								  #
# Alteração: Edinor Cunha Júnior											  #
# Data de Modificação: 25/Mar/2022                                            #
# Descrição Modificação: reestruturação geral do script						  #
# Versão: 5.0                                                          	      #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Lê dados da raw zone, aplica as regras de negócio e cria o datamart 		  #
# workflowtrackings na refined zone.  										  #
#                                                                             #
#------------------------------- Parâmetros ----------------------------------#
#                                                                             #
#>>> FTCRM_WORKFLOWTASKRESULTS = Referência no S3 onde estão os dados da 	  #
# tabela FTCRM_WORKFLOWTASKRESULTS.											  #
#>>> FTCRM_BUSSINESSUNITS = Referência no S3 onde estão os dados da tabela 	  #
# FTCRM_BUSSINESSUNITS.														  #
#>>> FTCRM_AGENTS = Referência no S3 onde estão os dados da tabela 			  #
# FTCRM_AGENTS.																  #
#>>> DEBTCONTACTS =  Referência no S3 onde os dados do datamart de 			  #
# DEBTCONTACTS serão inseridos.												  #
#>>> FTCRM_WORKFLOWTRACKINGS =  Referência no S3 onde estão os dados da 	  #
# tabela FTCRM_WORKFLOWTRACKINGS.											  #
#>>> WORKFLOWTRACKINGS = Referência no S3 onde os dados do datamart de 		  #
# WORKFLOWTRACKINGS serão inseridos.										  #
#																			  #
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


# Parâmetros dinâmicos
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FTCRM_WORKFLOWTASKRESULTS', 'FTCRM_BUSINESSUNITS',
									 'FTCRM_AGENTS', 'DEBTCONTACTS','FTCRM_WORKFLOWTRACKINGS',
									 'LOG_PATH', 'PROCESS_TYPE', 'WORKFLOWTRACKINGS'])

# Inicialização do Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Variáveis
job_name				  = args['JOB_NAME']
job_run_id				  = args['JOB_RUN_ID'] 					
log_path				  = args['LOG_PATH']
process_type			  = args['PROCESS_TYPE']
ftcrm_workflowtaskresults = args['FTCRM_WORKFLOWTASKRESULTS']
ftcrm_businessunits 	  = args['FTCRM_BUSINESSUNITS']
ftcrm_agents 			  = args['FTCRM_AGENTS']
datamart_debtcontacts 	  = args['DEBTCONTACTS']
ftcrm_workflowtrackings   = args['FTCRM_WORKFLOWTRACKINGS']
s3_output 			  	  = args['WORKFLOWTRACKINGS']

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


#Data Hora do Início do script
start_time = datetime.now()
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Inicio do script: {start_time_str}")


df_workflowtaskresults = read_data_from_lake(ftcrm_workflowtaskresults)
df_businessunits 		= read_data_from_lake(ftcrm_businessunits)
df_agents 				= read_data_from_lake(ftcrm_agents)
df_debtcontacts 		= read_data_from_lake(datamart_debtcontacts)
df_workflowtrackings 	= read_data_from_lake(ftcrm_workflowtrackings)

wktaskresulid_list = [2, 3, 4, 5, 6, 7,
					  10, 11, 12, 13, 16, 17,
					  21, 
					  46, 49, 
					  66, 67, 68,
					  72,
					  85,
					  108, 109,
					  111,
					  137, 139,
					  140,
					  153,
					  210, 215, 216,
					  229, 230, 231, 232, 236,
					  252, 253,
					  267, 268,
					  270, 271, 272, 273, 274, 275, 276, 277, 278, 279,
					  280, 281, 282, 283, 284, 285, 286, 287, 288,
					  302]

wkentityid_list = [2]

wktrackings = df_workflowtrackings.select("wktrackingid",\
				date_format("creationdate", "yyyyMM").cast("int").alias("refdate"),\
				col("creationdate").alias("date"),\
				"wktaskresultid",\
				"creationusersid",\
				"itemid")\
				.filter(df_workflowtrackings.wkentityid.isin(wkentityid_list) & df_workflowtrackings.wktaskresultid.isin(wktaskresulid_list))

try:
	datamart_wkt = read_data_from_lake(s3_output)
	last_id = datamart_wkt.agg({"wktrackingid": "max"}).collect()[0][0]
except:
	last_id = -1


# Criação das views temporarias
df_workflowtaskresults.createOrReplaceTempView("workflowtaskresults")
df_businessunits.createOrReplaceTempView("businessunits")
df_agents.createOrReplaceTempView("agents")
df_debtcontacts.createOrReplaceTempView("debtcontacts")
wktrackings.createOrReplaceTempView("workflowtrackings")


# Query principal
workflowtrackingsMatrix = spark.sql(
				f"""
				SELECT WT.wktrackingid, 
				WT.refdate, 
				WT.date, 
				WT.wktaskresultid, 
				WTR.taskresultdescription, 
				BU.businessunitid AS AgencyID, 
				BU.alias AS Agency, 
				WT.creationusersid, 
				AG.name AS AgentName, 
				CASE AG.enabled 
					WHEN 1 THEN 'Active' 
					WHEN 0 THEN 'Inactive' 
					ELSE '' END AS AgentEnabled, 
				D.accountownerID, 
				D.accountowner, 
				D.portfolioid, 
				D.portfolio, 
				D.debtid, 
				D.identitytype, 
				D.identitynumber, 
				D.producttypeid, 
				D.producttypealias, 
				D.products, 
				D.originalfirstdefaultdate, 
				D.firstdefaultdate, 
				D.originalfirstdefaultbalance, 
				D.currentbalance, 
				D.scoremodelid, 
				D.scoremodelname, 
				D.score, 
				D.state, 
				D.statealias, 
				D.city, 
				D.country, 
				D.assets, 
				D.liabilities, 
				D.restrictionstatus, 
				D.canrestrict
				FROM debtcontacts AS D
				INNER JOIN workflowtrackings AS WT ON D.bindingid = WT.itemid
				INNER JOIN workflowtaskresults AS WTR ON WT.wktaskresultid = WTR.wktaskresultid
				INNER JOIN agents AS AG ON WT.creationusersid = AG.agentsid
				INNER JOIN businessunits AS BU ON AG.businessunitid = BU.businessunitid
				WHERE WT.wktrackingid > {last_id}
				""")


# Criação de novas colunas
outputDF = workflowtrackingsMatrix.withColumn("year", date_format(col("date"), "yyyy")) \
           						.withColumn("month", date_format(col("date"), "MM")) \
           						.withColumn("day", date_format(col("date"), "dd"))\
           						.repartition("year", "month", "day")


# Coloca em lower case as tabelas do datamart e realiza o insert
outputDF = lower_column_names(outputDF)


outputDF.write.partitionBy("year", "month", "day")\
			  .format("parquet")\
			  .option("header", True)\
			  .option("spark.sql.parquet.compression.codec", "snappy")\
			  .option("encoding", "UTF-8")\
			  .mode("append")\
			  .save(s3_output)


# Contagem dos registros inseridos
total_registers = outputDF.count()


#Data Hora do final do script
end_time = datetime.now()
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Fim do script: {end_time_str}")


# Salva informações de LOG do processo
status = 'SUCCESS'
max_modification_date = outputDF.agg({"date": "max"}).collect()[0][0]
max_id = outputDF.agg({"wktrackingid": "max"}).collect()[0][0]
max_creation_date = outputDF.agg({"creationdate": "max"}).collect()[0][0]
elapsed_time = end_time - start_time
save_log(process_type, job_run_id, status, total_registers, max_id, max_creation_date, max_modification_date, start_time, end_time, elapsed_time)


# Fim do script
print(f'Tempo de execução: {elapsed_time}')
print(f'Numero de registros inseridos: {total_registers}')
job.commit()