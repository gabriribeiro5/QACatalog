#======================================================================#
# Objetivo: Input dos dados referentes do arquivo de parceiros         #
# Autor: Vanessa Barros dos Santos- NTT DATA                           #
# Data: Abr/2022                                                       #
#======================================================================#


import sys
import json
import boto3

# Parametros principais
s3 = boto3.resource('s3')

# Definição das funções

def load_json(bucket_name, bucket_key):
    content_object = s3.Object(bucket_name, bucket_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_file = json.loads(file_content)
    return json_file

def load_csv(path, col, spark):
    df = spark.read.option("delimiter", ";") \
                    .csv(path)
    df = df.toDF(*col)
    return df  