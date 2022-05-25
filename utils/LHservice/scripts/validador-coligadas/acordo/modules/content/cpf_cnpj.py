from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(validate_column, df):
    ufd_function_cpf_cnpj = F.udf(lambda item: validate_cpf_cnpj(item), BooleanType())
    df = df.withColumn('_validation_column', ufd_function_cpf_cnpj(df[validate_column]))    
    df.cache()

    df_ok = df.filter(df._validation_column).drop('_validation_column')
    df_nok = df.filter(~df._validation_column).drop('_validation_column')
    df.unpersist()

    return df_ok, df_nok


def validate_cpf_cnpj(numbers):

    if not numbers:
        return False

    # Obtém os números do CPF e ignora outros caracteres
    value = [int(number) for number in numbers if number.isdigit()]
    
    
    # Verifica se o CPF tem 11 digitos
    if len(value) == 11:
        
        cpf = value
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
    
    elif len(value) == 14:

        cnpj = value
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
        (1, 'OK', '902.664.910-06'),
        (2, 'OK', '90266491006'),
        (3, 'OK', '24.659.944/0001-33'),
        (4, 'OK', '24659944000133'),
        (5, 'NOK', None),
        (6, 'NOK', '902.664.910-07'),
        (7, 'NOK', '90266491007'),
        (8, 'NOK', '902.664.910-06'),
        (9, 'NOK', '90266491006'),
        (10, 'NOK', '24.659.944/0001-34'),
        (11, 'NOK', '24659944000134'),
        (12, 'NOK', '24.659.944/0001-33'),
        (13, 'NOK', '24659944000133'),
    ]

    columns = ['id', 'status', 'document_number']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('document_number', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()