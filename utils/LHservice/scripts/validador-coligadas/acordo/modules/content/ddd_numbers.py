from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

ddd_list = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34',\
             '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', \
             '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', \
             '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99']

def validate(validation_column, df):
    ufd_function = F.udf(lambda item: validate_ddd(item), BooleanType())
    df = df.withColumn('_validation_column',ufd_function(df[validation_column]))
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_ddd(ddd): 
    try:
        ddd_str = str(ddd)
        if ddd_str in ddd_list:             
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
        (1, 'OK', '11'),
        (2, 'OK', '67'),
        (3, 'NOK', None),
        (4, 'NOK', ''),
        (5, 'NOK', '02'),
        (6, 'NOK', 'X'),
    ]

    columns = ['id', 'status', 'ddd_numbers']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('ddd_numbers', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
