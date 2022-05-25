import re

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType



def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_ura_route(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_ura_route(route_ura_number):
    try:
        if route_ura_number == None or route_ura_number == '' or re.match(r"[1-9]\.[1-9]\.*", str(route_ura_number)):
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
        (1, 'OK', '1.3.4.6'),
        (2, 'OK', '5.7.9.4'),
        (3, 'OK', None),
        (4, 'OK', ''),
        (5, 'NOK', '021231'),
        (6, 'NOK', 'Xd.se.f'),
    ]

    columns = ['id', 'status', 'ura_route']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('ura_route', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
