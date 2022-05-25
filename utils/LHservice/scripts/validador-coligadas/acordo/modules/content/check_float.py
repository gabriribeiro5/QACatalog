from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_float_number(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_float_number(float_number):
    try:
        float_number = int(float_number)
        return False
    except:
        float_number = float(float_number)    
        if isinstance(float_number, float):
            return True

    return False

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from pyspark.sql import functions as F, Window as W, types as T

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '3.9'),
        (2, 'OK', '2.5'),
        (3, 'NOK', '1'),
        (4, 'NOK', '2'),
    ]

    columns = ['id', 'status', 'float_number']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('float_number', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
