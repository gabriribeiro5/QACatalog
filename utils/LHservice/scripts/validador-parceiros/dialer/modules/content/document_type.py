from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_document_type(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_document_type(document_type):
    try:    
        if document_type == None  or document_type == '' or int(document_type) == 2 or int(document_type) == 3:
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
        (1, 'OK', '2'),
        (2, 'OK', '3'),
        (3, 'OK', None),
        (4, 'OK', ''),
        (5, 'NOK', '1'),
        (6, 'NOK', 'X'),
    ]

    columns = ['id', 'status', 'document_type']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('document_type', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()

