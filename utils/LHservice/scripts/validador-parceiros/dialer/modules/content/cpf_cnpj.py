from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(id, document_type_column, validate_column, df):
    ufd_function_cpf = F.udf(lambda item: validate_cpf(item), BooleanType())
    ufd_function_cnpj = F.udf(lambda item: validate_cnpj(item), BooleanType())

    columns = df.columns
    df_cpf = df.where(df[document_type_column] == "2").withColumn('_validation_column_cpf', ufd_function_cpf(df[validate_column]))
    df_cnpj = df.where(df[document_type_column] == "3").withColumn('_validation_column_cnpj', ufd_function_cnpj(df[validate_column]))
    
    df = df_cpf.join(df_cnpj, columns, "full").orderBy(id)
    df = df.withColumn('_validation_column_na', F.when(F.col(document_type_column).isNull(), True).otherwise(False))
    df.cache()

    drop_columns = ['_validation_column_cpf', '_validation_column_cnpj', '_validation_column_na']
    df_ok = df.filter((df._validation_column_cpf) | (df._validation_column_cnpj) | (df._validation_column_na)).drop(*drop_columns)
    df_nok = df.filter(((~df._validation_column_cpf) | (~df._validation_column_cnpj)) & (~df._validation_column_na)).drop(*drop_columns)

    df.unpersist()

    return df_ok, df_nok


def validate_cpf(numbers):

    if not numbers:
        return False

    # Obtém os números do CPF e ignora outros caracteres
    value = [int(number) for number in numbers if number.isdigit()]
    
    cpf = value
    # Verifica se o CPF tem 11 digitos
    if len(cpf) != 11:
        return False

    # Valida igualdade de números
    if cpf == cpf[::-1]:
        return False

    # Valida os campos verificadores do CPF
    for element in range(9, 11):
        result = sum((cpf[number] * ((element+1) - number) for number in range(0, element)))
        digit = ((result * 10) % 11) % 10
        if digit != cpf[element]:
            return False

    return True



def validate_cnpj(numbers):

    if not numbers:
        return False

    # Obtém os números do CPF e ignora outros caracteres
    value = [int(number) for number in numbers if number.isdigit()]
    
    cnpj = value
    # Verifica se o CNPJ tem 14 digitos
    if len(cnpj) != 14:
        return False

    # Valida igualdade de números
    if cnpj == cnpj[::-1]:
        return False 

    # Pegamos os primeiros 9 digitos do cnpj e geramos os digitos validadores
    _cnpj = cnpj[:12]
    prod = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    while len(_cnpj) < 14:

        result = sum([x*y for (x, y) in zip(_cnpj, prod)]) % 11

        if result > 1:
            dv = 11 - result
        else:
            dv = 0

        _cnpj.append(dv)
        prod.insert(0, 6)

    if _cnpj != cnpj:
        return False

    return True

if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '2', '902.664.910-06'),
        (2, 'OK', '2', '90266491006'),
        (3, 'OK', '3', '24.659.944/0001-33'),
        (4, 'OK', '3', '24659944000133'),
        (5, 'OK', None, None),
        (6, 'OK', '',''),
        (7, 'NOK', '2', '902.664.910-07'),
        (8, 'NOK', '2', '90266491007'),
        (9, 'NOK', '3', '902.664.910-06'),
        (10, 'NOK', '3', '90266491006'),
        (11, 'NOK', '3', '24.659.944/0001-34'),
        (12, 'NOK', '3', '24659944000134'),
        (13, 'NOK', '2', '24.659.944/0001-33'),
        (14, 'NOK', '2', '24659944000133'),
    ]

    columns = ['id', 'status', 'document_type', 'document_number']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('id', 'document_type', 'document_number', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()