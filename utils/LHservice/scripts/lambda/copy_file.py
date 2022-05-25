#=============================================================================#
#                                                                             #
# Objetivo: Função lambda responsável por copiar os arquivos do transfer para #
# dentro do DataLake na area bronze para iniciar o Data Quality do arquivo 	  #
# Autor: Edinor Cunha Júnior - NTT DATA      								  #
# Data: Mar/2022                                                              #
# Versão: 1.0                                                                 #
# 																			  #
#------------------------------- Descrição -----------------------------------#
#                                                                             #
# Cópia os arquivos do S3 Tranfer para o S3 da área Bronze do Data Lake       #
#                                                                             #
#=============================================================================#

import os
import json
import boto3

# Atribuição das variaveis de ambiente
bucket_path = os.environ['bucket_path']

# Inicialização do Boto3
s3_client = boto3.client('s3')

# Inicio do Script principal
def lambda_handler(event, context):
    s3_bucket_source = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']
    s3_bucket_destination = bucket_path
    copy_object={'Bucket': s3_bucket_source, 'Key': s3_object_key}
    s3_client.copy_object(CopySource=copy_object, Bucket=s3_bucket_destination, Key=s3_object_key)

    return {
        'body': json.dumps('File has been Successfully Copied')
    }

