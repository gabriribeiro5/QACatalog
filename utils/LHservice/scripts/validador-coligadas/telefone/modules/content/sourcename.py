import pyspark.sql.functions as F

def validate(ref_column, df):
    
    df_ok = df.filter(F.lower(F.col(ref_column)) == F.col(ref_column))
    df_nok = df.filter(((F.lower(F.col(ref_column)) == F.col(ref_column)) == False) | (F.col(ref_column).isNull()))
    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', 'emdia'),
        (2, 'OK', 'lideranca'),
        (3, 'NOK', None),
        (4, 'NOK', None),
        (5, 'NOK', 'Lideranca'),
        (6, 'NOK', 'RETURN'),
    ]

    columns = ['id', 'status', 'sourcename']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('sourcename', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()