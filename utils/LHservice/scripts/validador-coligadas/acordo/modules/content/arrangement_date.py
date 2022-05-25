from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_date_format(item), BooleanType())
    df = df.withColumn("_validation_column", ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_date_format(variavel_data):
  try:
    datetime.strptime(variavel_data, '%Y-%m-%d')
    return True
  except:
    return False
  return False


#### Testes ####
if __name__ == "__main__":
    
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '2022-05-15'),
        (2, 'OK', '2022-05-13'),
        (3, 'NOK', None),
        (4, 'NOK', '15-05-2022'),
        (5, 'NOK', '22/05/15'),
        (6, 'NOK', '2022-05-32'),
    ]

    columns = ['id', 'status', 'data']
    df = spark.createDataFrame(data=data, schema = columns)
    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('data', df)
    print('Tabela de Aceitos:')
    df_ok.show()

    print('Tabela de Rejeitados:')
    df_nok.show()

    