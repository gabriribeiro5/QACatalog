from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_digito(item), BooleanType())
    df = df.withColumn("_validation_column", ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')

    return df_ok, df_nok


def validate_digito(vd):
    
    try:
        if type(vd) == str:
            if len(vd) <= 30:
                return True
            else:
                return False
        else:
            return False         
            
    except:
         return False


if __name__ == "__main__":

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', "123456789qwertytestedeidcom30"),
        (2, 'OK', "123456789qwertytestedeidcom301"),
        (3, 'NOK', "123456789qwertytestedeidcomatetrintacaracteresmuitoalemdoesperado"),
        (4, 'NOK', 123456),
    ]

    columns = ['id', 'status', 'portfolio_produtct']
    df = spark.createDataFrame(data=data, schema = columns)
    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('portfolio_produtct', df)
    print('Tabela de Aceitos:')
    df_ok.show()

    print('Tabela de Rejeitados:')
    df_nok.show()
