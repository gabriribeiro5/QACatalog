import pyspark.sql.functions as F

def validate(direction_column, df):

    df_ok = df.filter(F.col(direction_column).isin(["1", "2", "3"]))
    df_nok = df.filter((F.col(direction_column).isin(["1", "2", "3"]) == False) | (F.col(direction_column).isNull()))
    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', "2"),
        (2, 'OK', "1"),
        (3, 'OK', "3"),
        (4, 'NOK', "0"),
        (5, 'NOK', "23"),
        (6, 'NOK', None),
    ]

    columns = ['id', 'status', 'sourcedirection']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('sourcedirection', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()