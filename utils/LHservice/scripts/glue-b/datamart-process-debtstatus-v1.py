import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from functools import reduce
from math import ceil

MAX_REGISTERS_REPARTITION = 250000

def rename_columns(df):
    for column in df.columns:
        df = df.withColumnRenamed(column,column)
    return df

def union_list_dataframes(df_list):
    df = reduce(DataFrame.union, df_list)
    return df

def read_data_from_lake(s3_path):
    df = spark.read.format("parquet") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .load(s3_path)

    return df

def write_data_bucket(df, path_s3, write_mode):
    df.write.format("parquet") \
                    .option("header", True) \
                    .option("spark.sql.parquet.compression.codec", "snappy") \
                    .option("encoding", "UTF-8") \
                    .mode(write_mode) \
                    .save(path_s3)

def build_debtstatus_workflowstatus(ftcrm_workflowstatuses):
    df_workflowstatus = read_data_from_lake(ftcrm_workflowstatuses)
    df_workflowstatus.createOrReplaceTempView("WorkflowStatuses")

    df = spark.sql("""
        SELECT workflowstatus AS statusid,
        REPLACE(statusalias, 'Status', '') AS status,
        CASE WHEN workflowstatus IN (60000, 60003, 60005, 60006, 60100, 60103) THEN 7020
			 WHEN workflowstatus IN (60001, 60101) THEN 7030
			 WHEN workflowstatus IN (60002, 60102) THEN 7010 ELSE 0 END AS debtstatusid,
        CASE WHEN workflowstatus IN (60000, 60003, 60005, 60006, 60100, 60103) THEN 'Promisse'
			 WHEN workflowstatus IN (60001, 60101) THEN 'Paying'
			 WHEN workflowstatus IN (60002, 60102) THEN 'Settled' ELSE 'n/a' END AS debtstatus
        FROM WorkflowStatuses
    """)

    spark.catalog.dropTempView("WorkflowStatuses")

    return df

def build_debtstatus_processing(datamart_debtcontacts, datamart_arrangements, df_debtstatus_workflowstatus):
    df_debtcontacts = read_data_from_lake(datamart_debtcontacts)
    df_arrangements = read_data_from_lake(datamart_arrangements)

    df_debtcontacts.createOrReplaceTempView("DebtContacts")
    df_arrangements.createOrReplaceTempView("Arrangements")
    df_debtstatus_workflowstatus.createOrReplaceTempView("DebtStatus_WorkflowStatus")

    # Start PIC
    df_aux_start = spark.sql("""
        SELECT debtid,
            CAST(NULL AS int) AS orderrecs,
            7000 AS debtstatusid,
            'Start Collection' AS debtstatus,
            CAST(NULL AS int) AS arrangementid,
            CAST(NULL AS int) AS arrangementstatusid,
            CAST(NULL AS string) AS arrangementstatus,
            debtcreationdate AS startdate,
            CAST(NULL AS date) AS enddate
        FROM DebtContacts
    """)

    # All Arrangements
    df_aux_arrangements = spark.sql("""
        SELECT M.debtid,
            CAST(NULL AS int) AS orderrecs,
            A.debtstatusid,
            A.debtstatus,
            M.arrangementid,
            A.statusid,
            A.status,
            CASE WHEN M.date < M.promisedate AND M.date < M.arrangementdate THEN M.date
                 WHEN M.promisedate < M.arrangementdate THEN M.promisedate
            ELSE arrangementdate END AS startdate,
            CASE WHEN M.cancellationdate IS NOT NULL THEN M.cancellationdate
                 WHEN A.statusid IN (7020, 7030) THEN current_timestamp()
                 WHEN M.arrangementstatus IN (60003, 60103, 60005, 60006) THEN
                    CASE WHEN M.date < M.promisedate AND M.date < M.arrangementdate THEN M.date
                         WHEN M.promisedate < M.arrangementdate THEN M.promisedate
                    ELSE arrangementdate END
            END AS enddate
            FROM Arrangements AS M
        	INNER JOIN DebtStatus_WorkflowStatus AS A ON M.arrangementstatus = A.statusid
    """)

    # End PIC
    df_aux_end = spark.sql("""
        SELECT debtid,
            CAST(NULL AS int) AS orderrecs,
            M.debtstatus AS descriptionid,
            'End Collection' AS description,
            CAST(NULL AS int) AS arrangementid,
            A.statusid,
            A.status,
            IFNULL(IFNULL(recalldate, selldate), settlementdate) AS startdate,
            CAST(current_timestamp() AS DATE) AS enddate
            FROM DebtContacts AS M
        	INNER JOIN DebtStatus_WorkflowStatus AS A ON M.debtstatus = A.statusid
        	WHERE M.debtstatus IN (7010, 7040, 7050)
    """)

    # Union all dataframes
    df = union_list_dataframes([df_aux_start, df_aux_arrangements, df_aux_end])
    df = df.withColumn('id', F.monotonically_increasing_id())

    spark.catalog.dropTempView("DebtContacts")
    spark.catalog.dropTempView("Arrangements")
    spark.catalog.dropTempView("DebtStatus_WorkflowStatus")

    return df

def ordering_events_debtstatus_processing(df_debtstatus_processing):

    columns = df_debtstatus_processing.columns
    columns.remove("orderrecs")

    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")

    df_cte = spark.sql(f"""
    SELECT {", ".join(columns)},
           ROW_NUMBER() OVER(PARTITION BY debtid ORDER BY
                CASE WHEN debtStatusid = 7000 AND debtstatus = 'Start Collection' THEN 1
                     WHEN debtStatusid IN (7010, 7040, 70050) THEN 3
                ELSE 2 END,
            CASE WHEN debtstatus = 'End Collection' THEN 1
            ELSE 0 END,
            startdate,
            IFNULL(arrangementid, 999999999)) AS orderrecs
    FROM DebtStatus_Processing
""")

    spark.catalog.dropTempView("DebtStatus_Processing")

    return df_cte

def gap_quebra_promessa(df_debtstatus_processing):

    df_debtstatus_processing = df_debtstatus_processing.drop("id")

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)

    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")

    df_debtstatus_pile = spark.sql(f"""
            SELECT DISTINCT M1.debtid,
                M1.enddate,
                M2.startdate
            FROM DebtStatus_Processing M1
    		INNER JOIN DebtStatus_Processing M2
                ON M1.debtid = M2.debtid AND M1.orderrecs = (M2.orderrecs - 1)
    		WHERE DATEDIFF(M1.enddate, M2.startdate) > 1
        """)

    df_debtstatus_pile.createOrReplaceTempView("DebtStatus_Pile")

    df_to_insert = spark.sql("""
            SELECT DISTINCT M.debtid,
                0 AS orderrecs,
                7000 AS debtstatusid,
                'Active' AS debtstatus,
                NULL AS arrangementid,
                NULL AS arrangementstatusid,
                NULL AS arrangementstatus,
                DATE_ADD(H.enddate, 1) AS startdate,
                DATE_ADD(H.startdate, -1) AS enddate
    		FROM DebtStatus_Processing M
    		INNER JOIN DebtStatus_Pile H ON M.debtid = H.debtid
        """)

    df_debtstatus_processing = df_debtstatus_processing.unionByName(df_to_insert)

    spark.catalog.dropTempView("DebtStatus_Pile")

    spark.catalog.dropTempView("DebtStatus_Processing")

    return df_debtstatus_processing

def atualiza_null_data_inicio(df_debtstatus_processing):

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    grouped_by = {'debtid'}
    columns = ['M.' + column for column in df_debtstatus_processing.columns]

    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")

    df_debtstatus_startdatenull = spark.sql("""
        SELECT debtid,
               orderrecs
        FROM DebtStatus_Processing
    	WHERE startdate IS NULL
    """)

    df_debtstatus_startdatenull.createOrReplaceTempView("DebtStatus_StartDateNULL")

    df_start_date_null = spark.sql("""
        SELECT M.debtid,
               SDN.orderrecs,
               DATE_ADD(IFNULL(enddate, startdate), 1) AS refdate
		FROM DebtStatus_Processing AS M
		INNER JOIN DebtStatus_StartDateNULL AS SDN
            ON M.debtid = SDN.debtid AND M.orderrecs = SDN.orderrecs - 1
    """)

    df = df_debtstatus_processing.alias("M").join(df_start_date_null.alias("SDN"),
                                            ['debtid'], how='left')

    df = df.withColumn("M.startdate", F.when(F.col("SDN.refdate").isNotNull(), F.col("SDN.refdate")))
    df = df.select(columns)

    spark.catalog.dropTempView("DebtStatus_Processing")
    spark.catalog.dropTempView("DebtStatus_StartDateNULL")

    return df



def atualiza_data_fim(df_debtstatus_processing):

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    #columns = ['M1.' + column for column in df_debtstatus_processing.columns]
    columns = df_debtstatus_processing.columns

    df_aux = df_debtstatus_processing.alias('M1')\
                            .join(df_debtstatus_processing.alias('M2'),
                                  [F.col('M1.debtid') == F.col("M2.debtid"),
                                   F.col("M1.orderrecs") == 1,
                                   F.col("M2.orderrecs") == 2],
                                  how='inner')

    df_aux = df_aux.withColumn('M1.enddate',
                    F.when(F.col('M1.startdate') == F.col('M2.startdate'), F.col('M1.startdate'))\
                    .when(F.col('M1.startdate') < F.col('M2.startdate'), F.date_add(F.col('M2.startdate'), -1))\
                    .otherwise(F.col('M1.startdate')))

    df_aux = df_aux.withColumn('M1.enddate',
                              F.when(F.col('M1.enddate').isNull() &\
                                    (F.col('M1.orderrecs') == 1), F.current_date()))\
                   .select(F.col("M1.debtid").alias("id"),
                           F.col("M1.orderrecs").alias("order"),
                           F.col("M1.enddate").alias("correct_date"))

    df = df_debtstatus_processing\
        .join(df_aux,
              how="left",
              on=[df_debtstatus_processing.debtid == df_aux.id,
                  df_debtstatus_processing.orderrecs == df_aux.order])\
        .withColumn("enddate", F.coalesce(F.col("correct_date"),
                                          F.col("enddate")))

    return df.select(columns)

def aux_tables(df_debtstatus_processing):

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")

    df_maximumrecs = spark.sql("""
        SELECT debtid,
               MAX(orderrecs) AS maxorderrecs
		FROM DebtStatus_Processing
		GROUP BY debtid
    """)

    df_maximumrecs.createOrReplaceTempView("MaximumRecs")

    df_debtstatus_maximumrecs = spark.sql("""
        SELECT M.debtid AS debtid, MR.maxorderrecs, M.enddate
        FROM DebtStatus_Processing AS M
    	INNER JOIN MaximumRecs AS MR ON M.debtid = MR.debtid AND M.orderrecs = MR.maxorderrecs
    """)

    df_debtstatus_maximumrecs = rename_columns(df_debtstatus_maximumrecs)

    df_debtstatus_maximumrecs.createOrReplaceTempView("DebtStatus_MaximumRecs")

    df = spark.sql("""
        SELECT M.debtid,
               M.orderrecs
    	FROM DebtStatus_Processing AS M
    	INNER JOIN DebtStatus_MaximumRecs AS MR ON M.debtid = MR.debtid
    	WHERE M.enddate IS NULL AND MR.maxorderrecs != 1 AND M.orderrecs != MR.maxorderrecs
    """)

    spark.catalog.dropTempView("DebtStatus_Processing")

    spark.catalog.dropTempView("DebtStatus_MaximumRecs")

    return df, df_debtstatus_maximumrecs

def atualiza_data_fim_p2(df_debtstatus_processing, df_debtstatus_recsnull):

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    columns = ['M.' + column for column in df_debtstatus_processing.columns]
    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")
    df_debtstatus_recsnull.createOrReplaceTempView("DebtStatus_RecsNull")

    df_aux = spark.sql("""
        SELECT M.debtid,
               M.startdate,
               M.orderrecs
		FROM DebtStatus_Processing AS M
		WHERE EXISTS ( SELECT * FROM DebtStatus_RecsNull AS RN
                       WHERE M.debtid = RN.debtid
                       AND M.orderrecs = (RN.orderrecs + 1) )
    """)

    df_aux.createOrReplaceTempView("M1")

    df = spark.sql("""
        SELECT *
        FROM DebtStatus_Processing AS M
    	LEFT JOIN M1 ON M.debtid = M1.debtid
        LEFT JOIN DebtStatus_RecsNull AS RN
        ON M.debtid = RN.debtid
            AND M.orderrecs = RN.orderrecs
    """)

    df = df.withColumn("M.enddate", F.when(
                                    (F.date_add(F.col("M1.startdate"), -1) == F.col("M.startdate")),
                                    F.date_add(F.col("M1.startdate"), -1))\
                                    .otherwise(F.col("M.startdate")) )

    df = df.select(columns)
    
    spark.catalog.dropTempView("M1")

    return df

def atualiza_data_fim_p3(df_debtstatus_processing, df_debtstatus_maximumrecs):

    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    columns = ['M.' + column for column in df_debtstatus_processing.columns]

    max_recs = df_debtstatus_maximumrecs.withColumn("nullflag", F.lit(1))

    df_debtstatus_processing = df_debtstatus_processing\
        .alias("M")\
        .join(max_recs.alias("MR"),
              on=[F.col("M.debtid") == F.col("MR.debtid"),
                  F.col("M.orderrecs") == F.col("MR.maxorderrecs")],
              how="left")\
        .withColumn("M.enddate",
                    F.when(F.col("M.enddate").isNull() & (F.col("MR.nullflag") == 1),
                           F.current_date()).otherwise(F.col("M.enddate")))\
        .select(columns)

    return df_debtstatus_processing


def gap_quebra_promessa_p2(df_debtstatus_processing):
    
    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    
    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")
    debtstatus_pile = spark.sql(f"""
                    SELECT DISTINCT M1.debtid AS debtid,
                    M1.enddate AS enddate,
                    M2.startdate AS startdate
                    FROM DebtStatus_Processing AS M1
                    INNER JOIN DebtStatus_Processing AS M2 ON M1.debtid = M2.debtid AND M1.orderrecs = (M2.orderrecs - 1)
                    WHERE DATEDIFF(M1.enddate, M2.startdate) > 1
                    """)
    debtstatus_pile = rename_columns(debtstatus_pile)

    debtstatus_pile.createOrReplaceTempView("DebtStatus_Pile")
 
    df_to_insert = spark.sql("""
                    SELECT DISTINCT M.debtid AS debtid,
                    0 AS orderrecs,
                    7000 AS debtstatusid,
                    'Active' AS debtstatus,
                    NULL AS arrangementid,
                    NULL AS arrangementstatusid,
                    NULL AS arrangementstatus,
                    DATE_ADD(H.enddate, 1) AS startdate,
                    DATE_ADD(H.startdate, -1) AS enddate
                    FROM DebtStatus_Processing AS M
                    INNER JOIN DebtStatus_Pile AS H ON M.debtid = H.debtid
                    """)
                    
    df_to_insert = rename_columns(df_to_insert)
    
    df_debtstatus_processing = df_debtstatus_processing.unionByName(df_to_insert)
    
    spark.catalog.dropTempView("DebtStatus_Pile")
    
    spark.catalog.dropTempView("DebtStatus_Processing")
    
    return df_debtstatus_processing


def insere_tracking_remanescente(df_debtstatus_processing):
    
    df_debtstatus_processing = rename_columns(df_debtstatus_processing)
    
    df_debtstatus_processing.createOrReplaceTempView("DebtStatus_Processing")
 
    df_maximumrecs = spark.sql("""
                SELECT debtid,
                MAX(orderrecs) AS maxorderrecs
                FROM DebtStatus_Processing
                GROUP BY debtid
                """)
    
    df_maximumrecs.createOrReplaceTempView("MaximumRecs")
    
    df_maximumrecs_pt2 = spark.sql("""
                SELECT M.debtid AS debtid,
                MR.maxorderrecs AS maxorderrecs,
                M.enddate AS enddate
                FROM DebtStatus_Processing AS M
                INNER JOIN MaximumRecs AS MR ON M.debtid = MR.debtid AND M.orderrecs = MR.maxorderrecs
                """)
 
    df_maximumrecs_pt2 = rename_columns(df_maximumrecs_pt2)
 
    df_maximumrecs_pt2.createOrReplaceTempView("DebtStatus_MaximumRecs_pt2")
    
    df = spark.sql("""
                SELECT M.debtid AS debtid,
                M.orderrecs + 1 AS orderrecs,
                7000 AS debtstatusid,
                'Active' AS debtstatus,
                NULL AS arrangementid,
                NULL AS arrangementstatusid,
                NULL AS arrangementstatus,
                DATE_ADD(M.enddate, 1) AS startdate,
                CAST(current_timestamp() AS DATE) AS enddate
                FROM DebtStatus_Processing AS M
                INNER JOIN DebtStatus_MaximumRecs_pt2 AS MR ON M.debtid = MR.debtid AND M.orderrecs = MR.maxorderrecs
                WHERE M.enddate != CAST(current_timestamp() AS DATE)
                """)
                
    df = rename_columns(df)
    
    df.printSchema()
 
    df_final = df_debtstatus_processing.unionByName(df)
    
    df_final.printSchema()
    
    return df_final

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME',
									 'FTCRM_WORKFLOWSTATUSES',
                                     'DEBTCONTACTS',
                                     'ARRANGEMENTS',
                                     'DEBTSTATUS'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    #TODO configurar a variavel workflowstatuses corretamente no JOB
    #ftcrm_workflowstatuses      = args['FTCRM_WORKFLOWSTATUSES']
    ftcrm_workflowstatuses      = "s3://dlr-prod-bucket-rawzone/pic/ftcrm/workflowstatuses/"
    datamart_debtcontacts       = args['DEBTCONTACTS']
    datamart_arrangements       = args['ARRANGEMENTS']
    datamart_aux_debtstatus     = args['DEBTSTATUS']


    # Categorizando os Status dos Acordos
    df_debtstatus_workflowstatus = build_debtstatus_workflowstatus(ftcrm_workflowstatuses)

    # Criando / Input dos dados (Start, Arrangement, End)
    df_debtstatus_processing = build_debtstatus_processing(datamart_debtcontacts,
                                                           datamart_arrangements,
                                                           df_debtstatus_workflowstatus)

    # Ordenando os eventos
    df_debtstatus_processing = ordering_events_debtstatus_processing(df_debtstatus_processing)
    print("Ok ordering_events_debtstatus_processing")
    
    # Inserindo gaps de quebra de promessa
    df_debtstatus_processing = gap_quebra_promessa(df_debtstatus_processing)
    print("Ok gap_quebra_promessa")

    # Ordenando os eventos pt2 - ordenando_eventos_p2
    df_debtstatus_processing = ordering_events_debtstatus_processing(df_debtstatus_processing)
    print("Ok ordering_events_debtstatus_processing")

    # Atualizando data Inicio para aqueles que são NULL
    df_debtstatus_processing = atualiza_null_data_inicio(df_debtstatus_processing)
    print("Ok atualiza_null_data_inicio")
    

    # Atualizando data fim para todos os Start PIC
    # &
    # Atualizando data fim para divida que tenha apenas Start PIC
    df_debtstatus_processing = atualiza_data_fim(df_debtstatus_processing)
    print("Ok atualiza_data_fim")
    

    # Aux tables
    df_debtstatus_recsnull, df_debtstatus_maximumrecs = aux_tables(df_debtstatus_processing)
    print("Ok aux_tables")

    # TODO: checar numeros de rows apartir daqui
    # Atualizando data fim para acordos que quebram ou liquidaram e estão null
    df_debtstatus_processing = atualiza_data_fim_p2(df_debtstatus_processing, df_debtstatus_recsnull)
    print("Ok atualiza_data_fim_p2")

    # Atualizando data fim para acordos que estão em aberto
    df_debtstatus_processing = atualiza_data_fim_p3(df_debtstatus_processing, df_debtstatus_maximumrecs)
    print("Ok atualiza_data_fim_p3")

    # Ordenando os eventos pt3
    df_debtstatus_processing = ordering_events_debtstatus_processing(df_debtstatus_processing)
    print("Ok ordering_events_debtstatus_processing")

    # Inserindo gaps de quebra de promessa pt2
    df_debtstatus_processing = gap_quebra_promessa_p2(df_debtstatus_processing)
    print("Ok gap_quebra_promessa_p2")

    # Ordenando os eventos pt2
    df_debtstatus_processing = ordering_events_debtstatus_processing(df_debtstatus_processing)
    print("Ok ordering_events_debtstatus_processing")

    # Inserindo o trackings remanescentes após a quebra de acordo
    df_datamart_debtstatus = insere_tracking_remanescente(df_debtstatus_processing)
    print("Ok insere_tracking_remanescente")

    # reparticionando o df final
    num_repartitions = ceil(df_datamart_debtstatus.count() / MAX_REGISTERS_REPARTITION)
    df_datamart_debtstatus = df_datamart_debtstatus.repartition(num_repartitions)
    print("Ok repartition e df final")

    write_data_bucket(df_datamart_debtstatus, datamart_aux_debtstatus, "overwrite")

    job.commit()
