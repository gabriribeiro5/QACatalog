import json
import os
import psycopg2

host = os.environ['redshift_host']
port = os.environ['redshift_port']
user = os.environ['redshift_user']
dbname = os.environ['redshift_db']
iam_role = os.environ['iam_role']
password = os.environ['redshift_password']

tables = [
    {"name": "workflowtrackings",
     "sortkey": "debtid",
     "schema": "crm_internal",
     "origin": "crm.workflowtrackings"},

    {"name": "arrangementinstallments",
     "sortkey": "arrangementid",
     "schema": "crm_internal",
     "origin": "crm.arrangementinstallments"},

    {"name": "arrangements",
     "sortkey": "debtid",
     "schema": "crm_internal",
     "origin": "crm.arrangements"},

    {"name": "payments",
     "sortkey": "debtid",
     "schema": "financial_internal",
     "origin": "financial.payments"},

    {"name": "debtcontacts",
     "sortkey": "customerid",
     "schema": "crm_internal",
     "origin": "crm.debtcontacts"},

    {"name": "mensagem",
     "sortkey": "telefone",
     "schema": "partnership_internal",
     "origin": "partnership.mensagem"},

    {"name": "discador",
     "sortkey": "telefone",
     "schema": "partnership_internal",
     "origin": "partnership.discador"},

    {"name": "contactemails",
     "sortkey": "contactid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.contactemails"},

    {"name": "contacts",
     "sortkey": "contactid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.contacts"},

    {"name": "addresses",
     "sortkey": "addressid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.addresses"},

    {"name": "contactaddresses",
     "sortkey": "contactid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.contactaddresses"},

    {"name": "phones",
     "sortkey": "phoneid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.phones"},

    {"name": "contactphones",
     "sortkey": "contactid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.contactphones"},

    {"name": "identities",
     "sortkey": "contactid",
     "schema": "pic_contacts_internal",
     "origin": "pic_contacts.identities"}
]

def lambda_handler(event, context):
    jsonStateMachine = event


    conn = psycopg2.connect(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password)
    cur = conn.cursor()

    for t in tables:
        temp_table = t["name"] + "_temp"
        print("creating schema")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {t['schema']};")
        print("creating temp table")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {t['schema']}.{temp_table} (LIKE {t['origin']});")
        print("changing sortkey....")
        cur.execute(f"ALTER TABLE {t['schema']}.{temp_table} ALTER SORTKEY ({t['sortkey']});")
        print("importing data...")
        cur.execute(f"INSERT INTO {t['schema']}.{temp_table} (SELECT * FROM {t['origin']});")
        print("dropping old table...")
        cur.execute(f"DROP TABLE IF EXISTS {t['schema']}.{t['name']};")
        print("renaming temporary table...")
        cur.execute(f"ALTER TABLE {t['schema']}.{temp_table} RENAME TO {t['name']};")

    conn.commit()
    cur.close()
    conn.close()

    return {
        "body": "COMPLETED"
    }
