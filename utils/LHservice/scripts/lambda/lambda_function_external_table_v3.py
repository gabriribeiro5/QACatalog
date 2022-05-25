import json
import os
import psycopg2


host = os.environ['redshift_host']
port = os.environ['redshift_port']
user = os.environ['redshift_user']
dbname = os.environ['redshift_db']
iam_role = os.environ['iam_role']
password = os.environ['redshift_password']
database = os.environ['database_catalog']

def lambda_handler(event, context):
    
    conn = psycopg2.connect(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password)
    cur = conn.cursor()
    
    query = "CREATE EXTERNAL SCHEMA IF NOT EXISTS datamart FROM DATA CATALOG DATABASE '{}' IAM_ROLE '{}' CREATE EXTERNAL DATABASE IF NOT EXISTS;".format(database, iam_role)
    
    cur.execute(query)
    
    cur.close()
    
    conn.commit()
    
    conn.close()
    
    return {
        "body": json.dumps("Tabela Externa criada com sucesso!")
    }