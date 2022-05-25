###############################################################################
# Objetivo: Script PySpark que atualiza o datamart assignment-debts-analytical
# Autor: Rony Aguiar Reis - NTT DATA
# Data: Dec/2021
# Versao: 1.0

# Descricao:
# Le os dados da raw zone e atualiza o datamart assignment-debts-analytical
# na refined zone.

# Parametros:

# >>> DATAMART_AUX_DIMDATE = Referencia no S3 de onde os dados
# Datamart.Aux.dimDate estao inseridos.

# >>> FTCRM_DBO_DIGITALDEBTASSIGNMENTS = Refer�ncia no S3 onde est�o os dados
# FTCRM.dbo.DigitalDebtAssignments

# >>> FTCRM_DBO_DEBTTRANSACTIONCODES = Refer�ncia no S3 onde est�o os dados
# FTCRM.dbo.DebtTransactionCodes

# >>> FTCRM_DBO_DEBTTRANSACTIONS = Refer�ncia no S3 onde est�o os dados
# FTCRM.dbo.DebtTransactions 

# >>> FTCRM_DBO_PORTFOLIOS = Refer�ncia no S3 onde est�o os dados da tabela
# FTCRM.dbo.Portfolios

# >>> FTCRM_DBO_DEBTS = Refer�ncia no S3 onde est�o os dados da tabela
# FTCRM.dbo.Debts

# >>> FTCRM_DBO_ASSIGNMENTS = Refer�ncia no S3 onde est�o os dados da tabela
# FTCRM.dbo.Assignments

# >>> FTCRM_DBO_BUSINESSUNITS = Refer�ncia no S3 onde est�o os dados da tabela
# FTCRM.dbo.BusinessUnits

# >>> DATAMART_CRM_DEBTCONTACTS = Refer�ncia no S3 onde est�o os dados do
# Datamart.CRM.DebtContacts

# >>> FT5L_LEGAL_SUITS = Refer�ncia no S3 onde est�o os dados FT5L.Legal.Suits

# >>> FT5L_LEGAL_SUITPARTIES = Refer�ncia no S3 onde est�o os dados
# FT5L.Legal.SuitParties

# >>> FT5L_LEGAL_SUITPARTYDEBTS = Refer�ncia no S3 onde est�o os dados
# FT5L.Legal.SuitPartyDebts

# >>> DATAMART_CRM_WORKFLOWTRACKINGS = Refer�ncia no S3 onde est�o os dados
# Datamart.CRM.WorkflowTrackings

# >>> DATAMART_CRM_AGENCYGOALS = Refer�ncia no S3 onde est�o os dados
# Datamart.CRM.AgencyGoals 

#>>> ASSIGNMENT_PATH = Refer�ncia no S3 onde os dados do datamart ser�o inseridos
###############################################################################

import sys
import pyspark.sql.functions as F
# import awsglue.transforms as T
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Window
from math import ceil

# Maximum number of partitions (avoid small files)
MAX_REGISTERS_REPARTITION = 250000

# Helper functions by Renato Candido Kurosaki
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


# Start the script
if __name__ == "__main__":

    # List of arguments that are paths to s3 parquets
    parquet_resources = ["FTCRM_DIMDATE",
                         "DEBTSTATUS", 
                         "FTCRM_DIGITALDEBTASSIGNMENTS", 
                         "FTCRM_DEBTTRANSACTIONCODES",
                         "FTCRM_DEBTTRANSACTIONS",
                         "FTCRM_PORTFOLIOS",
                         "FTCRM_DEBTS",
                         "FTCRM_ASSIGNMENTS",
                         "FTCRM_BUSINESSUNITS",
                         "DEBTCONTACTS",
                         "FT5L_SUITS",
                         "FT5L_SUITPARTIES",
                         "FT5L_SUITPARTYDEBTS",
                         "WORKFLOWTRACKINGS",
                         "AGENCYGOALS"]

    # Parse arguments
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ASSIGNMENTS",
                                         *parquet_resources])

    # Define the spark/glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    Job(glueContext).init(args["JOB_NAME"], args)

    datamart_unique_debts = args["ASSIGNMENTS"]

    # Instantiate dataframes from s3
    for view_name in parquet_resources:
        print(f"creating view for {view_name}")
        s3_path = args[view_name]
        # views are being created with uppercase names
        read_data_from_lake(s3_path).createOrReplaceTempView(view_name)

    ###########################################################################
    # dimDate creation
    ###########################################################################

    dim_date = spark.sql("""
 SELECT DISTINCT CAST(StartMonthDate AS char(6))
 FROM ftcrm_dimdate WHERE RefDate = CAST(CURRENT_DATE() AS date)
 ORDER BY 1""")
    date = dim_date.select("StartMonthDate").alias("YearMonth").collect()[0][0]
    print(f"date: {date}")

    ###########################################################################
    # Auxiliary date stage
    ###########################################################################

    # Declare PortfolioDate
    portfolio_date = spark.sql("""
SELECT MIN(PortfolioDate)
FROM ftcrm_portfolios""").collect()[0][0]

    print(f"portfolio_date: {portfolio_date}")
    
    # Generate aux_date (AuxDate)
    aux_date = spark.sql(f"""
SELECT
    CAST(CAST(DD.RefDate AS char(6)) AS int) AS RefDate,
    CAST(DD.StartMonthDate AS date) AS StartMonthDate,
    CAST(CONCAT(CAST(CAST(DD.EndMonthDate AS date) AS varchar(10)), ' 23:59:00.000') AS date) AS EndMonthDate,
    CAST(DD.StartMonthDate AS date) AS DistributionDate
FROM ftcrm_dimdate AS DD
WHERE CAST(DD.RefDate AS char(6)) BETWEEN '{portfolio_date}' AND CAST(CURRENT_DATE() AS char(6))
    AND CAST(DD.RefDate AS char(6)) = '{date}'""")

    # IDENTITY part
    aux_date = aux_date.withColumn("ID", F.monotonically_increasing_id())
    
    ###########################################################################
    # Assignments stage
    ###########################################################################
    def fillna_with_today(df, column_name):
        """Fills NA values in a column with the current_date().
        df: the dataframe where the column is;
        column_name: the name of the column to be filled with current_date().
        
        Return: the dataframe with null values replaced by the current date."""
        return df.withColumn(column_name, F.when(F.col(column_name).isNull(), 
                            F.current_date()).otherwise(F.col(column_name)))

    # Create the AuxDate view
    aux_date.createOrReplaceTempView("AuxDate")

    
    # Escob assignment
    support_assignments = spark.sql("""
SELECT
    A.AssignmentID,
    A.DebtID,
    DC.CustomerID,
    DC.IdentityType,
    DC.IdentityNumber,
    DC.AccountOwnerID,
    DC.AccountOwner,
    DC.PortfolioID,
    DC.Portfolio,
    CAST(A.AssignmentDate AS date) AS AssignmentDate,
    A.RecallDate AS EndDate,
    A.BusinessUnitID AS AgencyID,
    BUA.Alias AS Agency,
    CAST(A.AssignmentDate AS char(6)) AS RefAssignmentDate,
    A.RecallDate AS RefEndDate,
    DC.OriginalFirstDefaultDate,
    DC.SettlementDate,
    DC.RecallDate,
    DC.SellDate
FROM debtcontacts AS DC
INNER JOIN ftcrm_assignments AS A
    ON DC.DebtID = A.DebtID
INNER JOIN ftcrm_businessunits AS BUA
    ON A.BusinessUnitID = BUA.BusinessUnitID
""")

    support_assignments = fillna_with_today(support_assignments, "EndDate")

    support_assignments = fillna_with_today(support_assignments, "RefEndDate")

    # Create the Datamart_Support_Assignments view
    support_assignments.createOrReplaceTempView("Datamart_Support_Assignments")

    # Digital Serasa assignment
    assignment_serasa = spark.sql("""
SELECT
    A.DigitalDebtAssignmentID,
    A.DebtID,
    CAST(A.AssignmentDate AS date) AS AssignmentDate,
    A.RecallDate AS EndDate,
    1 AS Serasa
FROM debtcontacts AS DC
INNER JOIN ftcrm_digitaldebtassignments AS A ON DC.DebtID = A.DebtID
WHERE A.BusinessUnitID = 670""")

    assignment_serasa = fillna_with_today(assignment_serasa, "AssignmentDate")

    assignment_serasa\
        .createOrReplaceTempView("datamart_support_assignmentserasa")

    # Digital emDia assignment
    assignment_emdia = spark.sql("""
SELECT
    A.DigitalDebtAssignmentID,
    A.DebtID,
    CAST(A.AssignmentDate AS date) AS AssignmentDate,
    1 AS emDia,
    A.RecallDate AS EndDate
FROM debtcontacts AS DC
INNER JOIN ftcrm_digitaldebtassignments AS A ON DC.DebtID = A.DebtID
WHERE A.BusinessUnitID = 714""")

    assignment_emdia = fillna_with_today(assignment_emdia, "EndDate")

    assignment_emdia\
        .createOrReplaceTempView("datamart_support_assignmentemdia")


    ###########################################################################
    # Suits stage
    ###########################################################################

    # Assets assignment
    assets = spark.sql("""
SELECT DISTINCT
    S.SuitID,
    S.CreationDate,
    CAST(S.CreationDate AS char(6)) AS RefCreationDate,
    S.ArchiveDate AS ArchiveDate,
    S.ArchiveDate AS RefArchiveDate,
    SPD.DebtID
FROM ft5l_suits AS S
INNER JOIN ft5l_suitparties AS SP ON S.SuitID = SP.SuitID
INNER JOIN ft5l_suitpartydebts AS SPD ON SP.SuitPartyID = SPD.SuitPartyID
WHERE S.SuitPosition = 1""")

    assets = fillna_with_today(assets, "ArchiveDate")
    assets = fillna_with_today(assets, "RefArchiveDate")

    # Cast some columns to integer
    assets = assets.withColumn("RefArchiveDate", 
                               F.col("RefArchiveDate").cast("int"))

    assets = assets.withColumn("RefCreationDate",
                               F.col("RefCreationDate").cast("int"))

    assets.createOrReplaceTempView("Assets")

    # Libilities assignment
    libilities = spark.sql("""
SELECT DISTINCT
    S.SuitID,
    S.CreationDate,
    CAST(S.CreationDate AS char(6)) AS RefCreationDate, --todo: integer cast
    S.ArchiveDate AS ArchiveDate,
    S.ArchiveDate AS RefArchiveDate, --todo: fill nulls with current_date, integer cast
    SPD.DebtID
FROM ft5l_suits AS S
INNER JOIN ft5l_suitparties AS SP ON S.SuitID = SP.SuitID
INNER JOIN ft5l_suitpartydebts AS SPD ON SP.SuitPartyID = SPD.SuitPartyID
WHERE S.SuitPosition = 2""")

    libilities = fillna_with_today(libilities, "ArchiveDate")
    libilities = fillna_with_today(libilities, "RefArchiveDate")

    libilities = libilities.withColumn("RefCreationDate",
                                         F.col("RefCreationDate").cast("int"))
    libilities = libilities.withColumn("RefArchiveDate",
                                         F.col("RefArchiveDate").cast("int"))

    libilities.createOrReplaceTempView("Libilities")

    ###########################################################################
    # WorkflowTrackings stage
    ###########################################################################
    workflowtrackings = spark.sql(f"""
SELECT DISTINCT RefDate, DebtID, AgencyID
FROM workflowtrackings
WHERE RefDate = '{portfolio_date}'""")

    workflowtrackings\
        .createOrReplaceTempView("datamart_support_workflowtrackings")


    ###########################################################################
    # Balance Range / Default Years Range stage
    ###########################################################################

    balance_range = spark.createDataFrame([
        (-999999.00, 500.00, 'R$0 - R$500'),
        (500.01, 1000.00, 'R$500 - R$1k'),
	(1000.01, 1500.00, 'R$1k - R$1,5k'),
	(1500.01, 2000.00, 'R$1,5k - R$2k'),
	(2000.01, 2500.00, 'R$2k - R$2,5k'),
	(2500.01, 3000.00, 'R$2,5k - R$3k'),
	(3000.01, 5000.00, 'R$3k - R$5k'),
	(5000.01, 10000.00, 'R$5k - R$10k'),
	(10000.01, 30000.00, 'R$10k - R$30k'),
	(30000.01, 999999999999.00, 'Above R$30k')],
        ["StartBalance", "EndBalance", "BalanceRange"])

    balance_range = balance_range\
        .withColumn("ID", F.monotonically_increasing_id())

    balance_range.createOrReplaceTempView("balance_range")

    default_years = spark.createDataFrame([
        (-999999, 365, 'Up to 1y'),
        (366, 730, '1y - 2y'),
	(731, 1095, '2y - 3y'),
	(1096, 1460, '3y - 4y'),
	(1461, 1825, '4y - 5y'),
	(1826, 2190, '5y - 6y'),
	(2191, 2555, '6y - 7y'),
	(2556, 2920, '7y - 8y'),
	(2921, 999999, 'Above 8y')], 
        ["StartDay", "EndDay", "DefaultYearsRange"])

    default_years = default_years\
        .withColumn("ID", F.monotonically_increasing_id())

    default_years.createOrReplaceTempView("defaultyears_assignmentdebts")

    assignments_timewindow = spark.sql("""
SELECT 
    A.*,
    AD.RefDate AS Aux_RefDate, 
    AD.StartMonthDate AS Aux_StartMonthDate,
    AD.EndMonthDate AS Aux_EndMonthDate, 
    AD.DistributionDate
FROM Datamart_Support_Assignments AS A
INNER JOIN AuxDate AS AD 
    ON AD.DistributionDate BETWEEN A.AssignmentDate AND A.EndDate
""")

    assignments_timewindow.createOrReplaceTempView("Assignments_TimeWindow")

    non_unique_debts = spark.sql("""
SELECT 
    A.Aux_StartMonthDate AS StartMonthDate,
    A.Aux_RefDate AS RefDate,
    A.DistributionDate,
    A.AccountOwnerID,
    A.AccountOwner,
    A.PortfolioID,
    A.Portfolio,
    A.DebtID,
    CAST(NULL AS int) AS DebtStatusID,
    CAST(NULL AS varchar(100)) AS DebtStatus,
    CAST(NULL AS int) AS ArrangementID,
    A.SettlementDate,
    A.RecallDate,
    A.SellDate,
    A.CustomerID,
    A.IdentityType,
    A.IdentityNumber,
    A.AssignmentDate,
    A.EndDate,
    A.AssignmentID,
    A.AgencyID,
    A.Agency,
    A.OriginalFirstDefaultDate,
DATEDIFF(A.OriginalFirstDefaultDate, A.DistributionDate) AS DelinquencyDays,
CAST(NULL AS int) AS DelinquencyDaysDescriptionID,
CAST(NULL AS varchar(50)) AS DelinquencyDaysDescription,
CASE WHEN AJ.SuitID IS NOT NULL THEN 1 ELSE 0 END AS Assets,
CASE WHEN LJ.SuitID IS NOT NULL THEN 1 ELSE 0 END AS Libilities,
CASE WHEN A.Aux_RefDate = AG.RefDate THEN 'Sim' ELSE 'Não' END AS GoalAgency,
CASE WHEN A.Aux_RefDate = AG.RefDate THEN AG.Goal ELSE 0 END AS Goal,
CASE WHEN A.Aux_RefDate = AG.RefDate THEN AG.GoalUntilToday ELSE 0
    END AS GoalUntilToday,
CASE WHEN A.Aux_RefDate = W.RefDate THEN 1 ELSE 0 END AS Worked,
CURRENT_DATE() AS ReprocessingDate
FROM Assignments_TimeWindow AS A
LEFT  JOIN Assets AS AJ ON A.DebtID = AJ.DebtID 
    AND A.DistributionDate BETWEEN AJ.CreationDate AND AJ.ArchiveDate
LEFT  JOIN Libilities AS LJ ON A.DebtID = LJ.DebtID 
    AND A.DistributionDate BETWEEN LJ.CreationDate AND LJ.ArchiveDate
LEFT  JOIN agencygoals AS AG ON A.PortfolioID = AG.PortfolioID 
    AND A.AgencyID = AG.AgencyID AND A.Aux_RefDate = AG.RefDate
LEFT  JOIN datamart_support_workflowtrackings AS W ON A.DebtID = W.DebtID
    AND A.AgencyID = W.AgencyID AND A.Aux_RefDate = W.RefDate
""")

    non_unique_debts = non_unique_debts\
        .withColumnRenamed("StartMonthDate", "Date")
    

    non_unique_debts = non_unique_debts\
        .withColumn("ID", F.monotonically_increasing_id())

    non_unique_debts.createOrReplaceTempView("non_unique_debts")

    # Rank non-unique debts
    CTE = spark.sql("""
SELECT ID, 
DENSE_RANK() OVER(PARTITION BY DebtID ORDER BY CASE 
        WHEN AssignmentDate = DistributionDate THEN 2 ELSE 1 END ) 
    AS AssignmentRank
FROM non_unique_debts
""")

    CTE.createOrReplaceTempView("CTE")

    # Add the rank column
    non_unique_debts = non_unique_debts.join(CTE, on="ID")
    non_unique_debts.createOrReplaceTempView("non_unique_debts")

    # Get unique debts
    unique_debts = spark.sql("""
SELECT
    A.Date,
    RefDate,
    DistributionDate,
    AccountOwnerID, 
    AccountOwner, 
    PortfolioID, 
    Portfolio, 
    DebtID, 
    DebtStatusID,
    DebtStatus, 
    ArrangementID,
    SettlementDate, 
    RecallDate, 
    SellDate,
    CustomerID,
    IdentityType,
    IdentityNumber,
    AssignmentDate,
    AssignmentID,
    AgencyID,
    Agency,
    OriginalFirstDefaultDate, 
    DelinquencyDays,
    DelinquencyDaysDescriptionID,
    DelinquencyDaysDescription,
    Assets,
    Libilities,
    GoalAgency,
    Goal,
    GoalUntilToday,
    Worked,
    ReprocessingDate
FROM non_unique_debts AS A
WHERE AssignmentRank = 1
""")
    unique_debts = unique_debts.withColumn("RefDate", 
                                           F.date_format(F.col("Date"), "yyyyMM").cast("int"))

    unique_debts.createOrReplaceTempView("unique_debts")

    # Update Serasa and emDia status
    unique_debts = spark.sql("""
SELECT
    U.Date,
    U.RefDate,
    U.DistributionDate,
    U.AccountOwnerID, 
    U.AccountOwner, 
    U.PortfolioID, 
    U.Portfolio, 
    U.DebtID AS DebtID, 
    U.DebtStatusID,
    U.DebtStatus, 
    U.ArrangementID,
    U.SettlementDate, 
    U.RecallDate, 
    U.SellDate,
    U.CustomerID,
    U.IdentityType,
    U.IdentityNumber,
    U.AssignmentDate,
    U.AssignmentID,
    U.AgencyID,
    U.Agency,
    U.OriginalFirstDefaultDate, 
    U.DelinquencyDays,
    U.DelinquencyDaysDescriptionID,
    U.DelinquencyDaysDescription,
    U.Assets,
    U.Libilities,
    U.GoalAgency,
    U.Goal,
    U.GoalUntilToday,
    U.Worked,
    A.Serasa,
    U.ReprocessingDate
FROM unique_debts AS U
LEFT JOIN datamart_support_assignmentserasa AS A 
    ON U.DebtID = A.DebtID 
        AND U.DistributionDate BETWEEN A.AssignmentDate AND A.EndDate
""")

    unique_debts.createOrReplaceTempView("unique_debts")

    unique_debts = spark.sql("""
SELECT
    U.Date,
    U.RefDate,
    U.DistributionDate,
    U.AccountOwnerID, 
    U.AccountOwner, 
    U.PortfolioID, 
    U.Portfolio, 
    U.DebtID AS DebtID, 
    U.DebtStatusID,
    U.DebtStatus, 
    U.ArrangementID,
    U.SettlementDate, 
    U.RecallDate, 
    U.SellDate,
    U.CustomerID,
    U.IdentityType,
    U.IdentityNumber,
    U.AssignmentDate,
    U.AssignmentID,
    U.AgencyID,
    U.Agency,
    U.OriginalFirstDefaultDate, 
    U.DelinquencyDays,
    U.DelinquencyDaysDescriptionID,
    U.DelinquencyDaysDescription,
    U.Assets,
    U.Libilities,
    U.GoalAgency,
    U.Goal,
    U.GoalUntilToday,
    U.Worked,
    U.Serasa,
    A.emDia,
    U.ReprocessingDate
FROM unique_debts AS U
LEFT JOIN datamart_support_assignmentemdia AS A 
    ON U.DebtID = A.DebtID 
        AND U.DistributionDate BETWEEN A.AssignmentDate AND A.EndDate
""")

    unique_debts = unique_debts.withColumn("emDia", F.when(F.col("emDia").isNull(), F.lit(0)).otherwise(F.col("emDia"))).withColumn("Serasa", F.when(F.col("Serasa").isNull(), F.lit(0)).otherwise(F.col("Serasa")))

    unique_debts.createOrReplaceTempView("unique_debts")

    # DebtStatus_Unique
    # CTE creation
    cte = spark.sql("""
SELECT A.DebtID, DS.OrderRecs
FROM unique_debts AS A
INNER JOIN DebtStatus AS DS 
    ON A.DebtID = DS.DebtID 
        AND A.DistributionDate BETWEEN DS.StartDate 
        AND DS.EndDate
""")

    cte.createOrReplaceTempView("CTE")

    debtstatus_unique = spark.sql("""
SELECT DebtID, MAX(OrderRecs) AS OrderRecs
FROM CTE
GROUP BY DebtID
""")

    debtstatus_unique.createOrReplaceTempView("debtstatus_unique")

    unique_debts = spark.sql("""
SELECT
    UD.Date,
    UD.RefDate,
    UD.DistributionDate,
    UD.AccountOwnerID, 
    UD.AccountOwner, 
    UD.PortfolioID, 
    UD.Portfolio, 
    UD.DebtID AS DebtID,
    DS.DebtStatusID,
    DS.DebtStatus,
    DS.ArrangementID,
    UD.SettlementDate, 
    UD.RecallDate, 
    UD.SellDate,
    UD.CustomerID,
    UD.IdentityType,
    UD.IdentityNumber,
    UD.AssignmentDate,
    UD.AssignmentID,
    UD.AgencyID,
    UD.Agency,
    UD.OriginalFirstDefaultDate, 
    UD.DelinquencyDays,
    UD.Assets,
    UD.Libilities,
    UD.GoalAgency,
    UD.Goal,
    UD.GoalUntilToday,
    UD.Worked,
    UD.Serasa,
    UD.emDia,
    UD.ReprocessingDate
FROM unique_debts AS UD
LEFT JOIN debtstatus_unique AS U ON UD.DebtID = U.DebtID
LEFT JOIN DebtStatus AS DS 
    ON UD.DebtID = DS.DebtID AND U.OrderRecs = DS.OrderRecs
""")

    unique_debts.createOrReplaceTempView("unique_debts")

    ###########################################################################
    # Update AssignmentDebts_UniqueDebts stage
    ###########################################################################

    unique_debts = spark.sql("""
SELECT
    M.Date,
    M.RefDate,
    M.DistributionDate,
    M.AccountOwnerID, 
    M.AccountOwner, 
    M.PortfolioID, 
    M.Portfolio, 
    M.DebtID AS DebtID, 
    M.DebtStatusID,
    M.DebtStatus, 
    M.ArrangementID,
    M.SettlementDate, 
    M.RecallDate, 
    M.SellDate,
    M.CustomerID,
    M.IdentityType,
    M.IdentityNumber,
    M.AssignmentDate,
    M.AssignmentID,
    M.AgencyID,
    M.Agency,
    M.OriginalFirstDefaultDate, 
    M.DelinquencyDays,
    DF.ID AS DelinquencyDaysDescriptionID,
    DF.DefaultYearsRange AS DelinquencyDaysDescription,
    M.Assets,
    M.Libilities,
    M.GoalAgency,
    M.Goal,
    M.GoalUntilToday,
    M.Worked,
    M.Serasa,
    M.emDia,
    M.ReprocessingDate
FROM unique_debts AS M
LEFT JOIN defaultyears_assignmentdebts AS DF
    ON M.DelinquencyDays BETWEEN DF.StartDay AND DF.EndDay
""")

    unique_debts.createOrReplaceTempView("unique_debts")

    range_dates = spark.sql(f"""
SELECT CAST(StartMonthDate AS date) AS StartMonthDate
FROM ftcrm_dimdate
WHERE CAST(StartMonthDate AS char(6)) = '{date}'
ORDER BY StartMonthDate
""").withColumn("ID", F.monotonically_increasing_id())

    month_first_date = range_dates.select("StartMonthDate").collect()[0][0]

    print(f"month_first_date: {month_first_date}")

    debt_transactions_life_to_month = spark.sql(f"""
SELECT
    DT.DebtID,
    SUM(-TC.Sign * DT.Amount) AS TransactionAmount
FROM ftcrm_debttransactions AS DT
INNER JOIN ftcrm_debttransactioncodes AS TC 
    ON TC.TransactionCode = DT.TransactionCode
WHERE TC.Sign <> 0 AND TC.TransactionCode <> 1 
    AND DT.AccountingDate < '{month_first_date}'
GROUP BY DT.DebtID
""")

    debt_transactions_life_to_month\
        .createOrReplaceTempView("debt_transactions_life_to_month")

    month_beginning_debts = spark.sql(f"""
SELECT D.DebtID, 
    CASE WHEN P.PortfolioID = 18 THEN D.InitialBalance
        ELSE D.OriginalFirstDefaultBalance END - T.TransactionAmount
        AS CurrentBalance
FROM ftcrm_debts AS D
INNER JOIN ftcrm_portfolios AS P ON P.PortfolioID = D.PortfolioID
LEFT JOIN debt_transactions_life_to_month AS T ON T.DebtID = D.DebtID
WHERE (D.SettlementDate >= '{month_first_date}' OR D.SettlementDate IS NULL) 
    AND (D.RecallDate >= '{month_first_date}' OR D.RecallDate IS NULL)
""")

    month_beginning_debts.createOrReplaceTempView("month_beginning_debts")
    month_beginning_debts.show()

    unique_debts = spark.sql("""
SELECT
    A.Date,
    A.RefDate,
    A.DistributionDate,
    A.AccountOwnerID, 
    A.AccountOwner, 
    A.PortfolioID, 
    A.Portfolio, 
    A.DebtID AS DebtID, 
    A.DebtStatusID,
    A.DebtStatus, 
    A.ArrangementID,
    A.SettlementDate, 
    A.RecallDate, 
    A.SellDate,
    A.CustomerID,
    A.IdentityType,
    A.IdentityNumber,
    A.AssignmentDate,
    A.AssignmentID,
    A.AgencyID,
    A.Agency,
    A.OriginalFirstDefaultDate, 
    A.DelinquencyDays,
    A.DelinquencyDaysDescriptionID,
    A.DelinquencyDaysDescription,
    A.Assets,
    A.Libilities,
    A.GoalAgency,
    A.Goal,
    A.GoalUntilToday,
    A.Worked,
    A.Serasa,
    A.emDia,
    MB.CurrentBalance AS CurrentBalanceStartMonth,
    BR.ID AS CurrentBalanceStartMonthDescriptionID,
    BR.BalanceRange AS CurrentBalanceStartMonthDescription,
    A.ReprocessingDate
FROM unique_debts AS A
LEFT JOIN month_beginning_debts AS MB
    ON A.DebtID = MB.DebtID
LEFT JOIN balance_range AS BR
    ON MB.CurrentBalance BETWEEN BR.StartBalance AND BR.EndBalance
-- Causa dataframe vazio em dev
-- WHERE A.RefDate = '{month_first_date}'
""")

    ###########################################################################
    # Final stage (load data to the lake)
    ###########################################################################

    # This stage loads data to the s3 bucket
    num_repartitions = ceil(unique_debts.count() / MAX_REGISTERS_REPARTITION)

    unique_debts = unique_debts.repartition(num_repartitions)

    write_data_bucket(unique_debts, datamart_unique_debts, "append")
