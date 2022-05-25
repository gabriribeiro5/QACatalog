import pyspark.sql.functions as F

def validate(ref_column, df):

    df_ok = df.filter((F.col(ref_column) == "1") | (F.col(ref_column).isNull()))
    df_nok = df.filter(~F.col(ref_column) == "1")
    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', None),
        (2, 'OK', "1"),
        (3, 'NOK', ""),
        (4, 'NOK', "0"),
        (5, 'NOK', "233"),
        (6, 'NOK', "-21"),
    ]

    columns = ['id', 'status', 'person_status']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('person_status', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()