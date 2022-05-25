from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_conversation_status(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_conversation_status(conversation_status):
    try:    
        if int(conversation_status) in range(101, 105):
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
        (1, 'OK', '101'),
        (2, 'OK', '104'),
        (3, 'NOK', None),
        (4, 'NOK', ''),
        (5, 'NOK', '109'),
        (6, 'NOK', 'X'),
    ]

    columns = ['id', 'status', 'conversation_status']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('conversation_status', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
