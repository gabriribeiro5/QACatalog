import pyspark.sql.functions as F

def validate_column_types(df, schema):
    error_columns = []
    for column in schema['columns']:
        try:
            df = df.withColumn(column['name'], F.col(column['name']).cast(column['type']))
        except:
            error_columns.append(column['name'])
    return error_columns


if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    schema = {'columns': [
                {'name': 'long_column','type': 'long'},
                {'name': 'string_column','type': 'string'},
                {'name': 'date_column','type': 'date'},
                {'name': 'float_column','type': 'float'},
                {'name': 'int_column','type': 'int'},
                {'name': 'int_column_2','type': 'int'}
            ]}

    data_ok = [('04232671000139', 'KFJGOEOSLDAL', '2022-05-09', '139444.22', '1', '123')]
    columns_ok = ['long_column', 'string_column', 'date_column', 'float_column', 'int_column', 'int_column_2']

    data_nok = [('04232671000139', 'KFJGOEOSLDAL', '139444.22', '1')]
    columns_nok = ['long_column', 'string_column', 'float_column', 'int_column']

    df_ok = spark.createDataFrame(data_ok, columns_ok)
    df_nok = spark.createDataFrame(data_nok, columns_nok)

    print('Demonstração de colunas - sucesso:')
    print(columns_ok)
    output_ok = validate_column_types(df_ok, schema)
    print('colunas com falha:')
    print(output_ok)

    print('Demonstração de colunas - falha:')
    print(columns_nok)
    output_nok = validate_column_types(df_nok, schema)
    print('colunas com falha:')
    print(output_nok)