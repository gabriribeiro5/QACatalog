from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_call_status(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_call_status(call_status):
    try:    
        if int(call_status) in range(1, 11):
            return True
    except:
        return False

    return False


if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '1'),
        (2, 'OK', '10'),
        (3, 'NOK', None),
        (4, 'NOK', ''),
        (5, 'NOK', '11'),
        (6, 'NOK', 'X'),
    ]

    columns = ['id', 'status', 'call_status']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('call_status', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
