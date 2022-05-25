import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import months_between, col, lit, add_months

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'DATAMART_DEBTCONTACTS',
                                     'FTCRM_PORTFOLIOS',
                                     'DAG_DEBT_STATUS'
                                     ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datamart_debtcontacts = args['DATAMART_DEBTCONTACTS']
ftcrm_portfolios = args['FTCRM_PORTFOLIOS']
dag_debt_status = args['DAG_DEBT_STATUS']

today = datetime.date.today()

# Baixando uma foto dos status do Datamart.CRM.DebtContacts
debtContacts = glueContext.spark_session.read.format("parquet"
        ).option("header", True
        ).option("inferSchema", True
        ).option("spark.sql.parquet.compression.codec", "snappy"
        ).option("encoding", "UTF-8"
        ).load(datamart_debtcontacts)

status = debtContacts.where("DebtStatus IN (7000, 7020, 7030)"
        ).selectExpr("portfolioid AS portfolio_id", 
                        "debtid AS debt_id", 
                        "customerid AS customer_id", 
                        "debtstatus AS value"
        ).withColumn("value", col("value").cast("int").cast("string"))

time = today
if today.day != 1:
    time = time.replace(day=1)

status = status.withColumn("time", lit(time).cast("timestamp")
                ).withColumn("variable_id", lit(60))

# Trazendo o portfolio date para o calculo do portfolio age
portfolios = glueContext.spark_session.read.format("parquet"
        ).option("header", True
        ).option("inferSchema", True
        ).option("spark.sql.parquet.compression.codec", "snappy"
        ).option("encoding", "UTF-8"
        ).load(ftcrm_portfolios)

portfolios = portfolios.selectExpr(
        "portfolioid AS portfolio_id",
        "CAST(portfoliodate AS DATE) AS portfolio_date"
        ).withColumn("portfolio_date", add_months(col("portfolio_date"), -1))

status = status.join(portfolios, "portfolio_id")

status = status.withColumn("portfolio_age", round(months_between(col("time"), 
                    col("portfolio_date"))).cast("int")).drop("portfolio_date")

status.write.format("parquet"
    ).option("header", True
    ).option("spark.sql.parquet.compression.codec", "snappy"
    ).option("encoding", "UTF-8"
    ).mode("append"
    ).save(dag_debt_status)