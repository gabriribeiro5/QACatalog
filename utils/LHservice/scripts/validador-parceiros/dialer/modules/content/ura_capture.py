import re

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType



def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_ura_capture(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_ura_capture(ura_cature):
    try:
        if ura_cature == None or ura_cature == '' or re.match(r"[1-9]*\.\d*", str(ura_cature)):
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
        (1, 'OK', '1911282.3232326.4454555.355656'),
        (2, 'OK', '13231566.4654654321.123546.123225665'),
        (3, 'OK', None),
        (4, 'OK', ''),
        (5, 'NOK', '021231'),
        (6, 'NOK', 'home'),
    ]

    columns = ['id', 'status', 'ura_capture']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('ura_capture', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
