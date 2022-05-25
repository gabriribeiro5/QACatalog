import json
import os
import psycopg2


host = os.environ['redshift_host']
port = os.environ['redshift_port']
user = os.environ['redshift_user']
dbname = os.environ['redshift_db']
password = os.environ['redshift_password']
pass_sandbox = os.environ['pass_sandbox'] 

groups_list = ["pricing", "financial", "data_science", "murabei", 
               "portfolio_managers", "operational_managers",
               "commercial", "information_technology"]

def create_group(group, cur):
    query = "CREATE GROUP {}".format(group)
    cur.execute(query)

def create_user(user, pass_sandbox, cur):
    query = "CREATE USER {} WITH password {}".format(user, pass_sandbox)
    cur.execute(query)

def alter_group(user, group, cur):
    query = "ALTER GROUP {} ADD USER {}".format(group, user)
    cur.execute(query)

def grant_usage(group, schemas, cur):
    query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schemas, group)
    cur.execute(query)

def grant_select(schemas, group, cur):
    query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}".format(schemas, group)
    cur.execute(query)


def lambda_handler(event, context):
    
    conn = psycopg2.connect(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password)
    cur = conn.cursor()

    for group in groups_list:
        if group == "pricing":
            schemas = ['pic_crm', 'crm']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "financial":
            schema = "financial"
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            grant_usage(group=group, schemas=schema, cur=cur)
            grant_select(schemas=schema, group=group, cur=cur)
        elif group == "data_science":
            schemas = ['pic_contacts', 'crm', 'pic_crm', 'financial', 'pic_foundation', 'pic_legal']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "murabei":
            schemas = ['pic_contacts', 'crm', 'pic_crm','financial', 'pic_foundation', 'pic_legal']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "portfolio_managers":
            schemas = ['pic_contacts', 'crm', 'pic_crm', 'financial', 'pic_foundation']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "operation_managers":
            schemas = ['pic_contacts', 'crm', 'pic_crm','financial', 'pic_foundation']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "commercial":
            schemas = ['pic_contacts', 'pic_legal']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)
        elif group == "information_technology":
            schemas = ['pic_contacts', 'crm', 'pic_crm','financial', 'pic_foundation', 'pic_legal']
            create_group(group=group, cur=cur)
            create_user(user=group, pass_sandbox=pass_sandbox, cur=cur)
            alter_group(user=group, group=group, cur=cur)
            for schema in schemas:
                grant_usage(group=group, schemas=schema, cur=cur)
                grant_select(schemas=schema, group=group, cur=cur)

    cur.close()
    
    conn.commit()
    
    conn.close()
    
    return {
        "body": json.dumps("Sandbox Criado com sucesso!")
    }