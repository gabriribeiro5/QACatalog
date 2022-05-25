from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validation_column, df):

    df.cache()

    df_ok = df.filter((F.upper(df[validation_column]) == 'A') | (F.upper(df[validation_column]) == 'R'))
    df_nok = df.filter(((F.upper(df[validation_column]) != 'A') & (F.upper(df[validation_column]) != 'R')) | (df[validation_column].isNull()))

    df.unpersist()

    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', 'A'),
        (2, 'OK', 'a'),
        (3, 'OK', 'R'),
        (4, 'OK', 'r'),
        (5, 'NOK', None),
        (6, 'NOK', ''),
        (7, 'NOK', '1'),
        (8, 'NOK', 'AR'),
        (9, 'NOK', 'RA'),
        (10, 'NOK', 'Z'),
        (11, 'NOK', 'z'),
    ]

    columns = ['id', 'status', 'call_direction']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('call_direction', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()

