import re

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType



def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_postal_code(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_postal_code(postal_code):
    try:
        if postal_code == None or postal_code == '' or re.match(r"[0-9]{8}", str(postal_code)):
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
        (1, 'OK', '79050650'),
        (2, 'OK', '79040540'),
        (3, 'OK', None),
        (4, 'OK', ''),
        (5, 'NOK', '021231'),
        (6, 'NOK', 'home'),
    ]

    columns = ['id', 'status', 'postal_code']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('postal_code', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
