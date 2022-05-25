import pyspark.sql.functions as F

def validate_required_fields(df, schema, id_column):
    df_ok = df
    for column in schema['columns']:
        if(column['required']):
            df_ok = df_ok.filter(F.col(column['name']).isNotNull())
    
    df_nok = df.join(df_ok, id_column, 'leftanti')
    
    return df_ok, df_nok

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', 'required_field', 'not_required_field'),
        (2, 'OK', 'required_field', None),
        (3, 'NOK', None, 'not_required_field'),
        (4, 'NOK', None, None)
    ]

    columns = ['id', 'status', 'required', 'not_required']
    df = spark.createDataFrame(data=data, schema = columns)

    schema = {'columns': [
                {'name': 'id', 'required': True},
                {'name': 'status', 'required': True},
                {'name': 'required', 'required': True},
                {'name': 'not_required', 'required': False}
            ]}

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate_required_fields(df, schema, 'id')

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()