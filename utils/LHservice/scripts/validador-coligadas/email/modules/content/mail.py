from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_notes(item), BooleanType())
    df = df.withColumn("_validation_column", ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')

    return df_ok, df_nok


def validate_notes(vd):
    
    try:
        if(vd == None):
            return False
        if type(vd) == str:
            if len(vd) <= 100:
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
        (3, 'OK', None),
        (4, 'NOK', "123456789qwertyuiopasdfghjklzxcvbnmtestedeidcomatetrintacaracteresmuitoalemdoesperado"),
        (5, 'NOK', 123456),
    ]

    columns = ['id', 'status', 'notes']
    df = spark.createDataFrame(data=data, schema = columns)
    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('notes', df)
    print('Tabela de Aceitos:')
    df_ok.show()

    print('Tabela de Rejeitados:')
    df_nok.show()
