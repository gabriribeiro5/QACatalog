import re

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType



def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_ura_path(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_ura_path(route_ura_path):
    try:
        if route_ura_path == None or route_ura_path == '' or re.match(r'^([\. a-zA-z]+)+$', str(route_ura_path)):
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
        (1, 'OK', 'home.segundavia.boleto.sms'),
        (2, 'OK', 'home.segundavia.quitacao.email'),
        (3, 'OK', None),
        (4, 'OK', ''),
        (5, 'NOK', '021231'),
        (6, 'NOK', '1.3.1.2'),
    ]

    columns = ['id', 'status', 'ura_path']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('ura_path', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
