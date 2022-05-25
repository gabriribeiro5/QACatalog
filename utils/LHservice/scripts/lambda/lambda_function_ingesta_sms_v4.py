import json
import os
import psycopg2


host = os.environ['redshift_host']
port = os.environ['redshift_port']
user = os.environ['redshift_user']
dbname = os.environ['redshift_db']
iam_role = os.environ['iam_role']
password = os.environ['redshift_password']

schema = os.environ['schema']
table = os.environ['table']

def lambda_handler(event, context):
    jsonStateMachine = event

    file_name = jsonStateMachine['originalFile']['s3FileName']
    key = jsonStateMachine['originalFile']['s3ObjectKey']
    s3 = jsonStateMachine['originalFile']['s3BucketName']
    
    path = "s3://%s/%s"%(s3, key)

    if("lideranca" in key):
        partner_id = 691
    elif("millennium" in key):
        partner_id = 685
    else:
        partner_id = None

    conn = psycopg2.connect(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password)
    cur = conn.cursor()
    
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table}(
            idmensagem VARCHAR(30) NOT NULL,
            tipodocumento INT,
            numerodocumento BIGINT,
            tipocodigoreferencia INT,
            codigoreferencia VARCHAR(30),
            ddd INT NOT NULL,
            telefone INT NOT NULL,
            direcaomensagem CHAR(1) NOT NULL,
            tipomensagem INT NOT NULL,
            datahoramensagem TIMESTAMPTZ NOT NULL,
            statusmensagem INT NOT NULL,
            codigoerromensagem VARCHAR(30),
            textomensagem VARCHAR(250) NOT NULL,
            dadosvariaveisusadostexto SUPER,
            midiaanexadamensagem VARCHAR(250),
            operadoradetectada VARCHAR(20),
            idocorrenciacrm INT,
            agencyid INT DEFAULT NULL
        );
            
        COPY {schema}.{table}(idmensagem, 
                                tipodocumento, 
                                numerodocumento,
                                tipocodigoreferencia, 
                                codigoreferencia, 
                                ddd, 
                                telefone, 
                                direcaomensagem, 
                                tipomensagem,
                                datahoramensagem, 
                                statusmensagem, 
                                codigoerromensagem, 
                                textomensagem)
        FROM '{path}'
        IAM_ROLE '{iam_role}'
        TIMEFORMAT 'auto'
        DELIMITER ';';
        
        UPDATE {schema}.{table} 
        SET agencyid = {partner_id} 
        WHERE agencyid IS NULL;
    """
    cur.execute(query)
    
    cur.close()
    
    conn.commit()
    
    conn.close()

    jsonStateMachine['taskResult'] = {
        "body": "COMPLETED",
        "filename": file_name
    }
    
    return {
        "body": "COMPLETED"
    }