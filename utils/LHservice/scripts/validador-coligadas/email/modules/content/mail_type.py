import pyspark.sql.functions as F

def validate(ref_column, df):
    range = ['1', '2', '3', '4']
    df_ok = df.filter(F.col(ref_column).isin(range))
    df_nok = df.filter((F.col(ref_column).isin(range) == False) | (F.col(ref_column).isNull()))
    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '2'),
        (2, 'OK', '1'),
        (3, 'OK', '3'),
        (4, 'OK', '4'),
        (5, 'NOK', '0'),
        (6, 'NOK', '233'),
        (7, 'NOK', None),
    ]

    columns = ['id', 'status', 'mail_type']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('mail_type', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()