from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(status_column, agent_column, conversation_column, voice_start_column, df):
    df_fill = df.na.fill({agent_column: "", conversation_column: "", voice_start_column: ""})
    df_fill.cache()

    df_ok = df_fill.filter((df_fill[status_column] != '10') | ((df_fill[status_column] == '10') & (df_fill[agent_column] != '') & (df_fill[conversation_column] != '') & (df_fill[voice_start_column] != '')))
    df_nok = df_fill.filter((df_fill[status_column] == '10') & ((df_fill[agent_column] == '') | (df_fill[conversation_column] == '') | (df_fill[voice_start_column] == '')))
    df_fill.unpersist()

    return df_ok, df_nok


if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '10', '1000', '0001', '2022-04-05 12:24:25.603000000'),
        (2, 'OK', '9', '1000', '0001', '2022-04-05 12:24:25.603000000'),
        (3, 'OK', '9', '', '0001', '2022-04-05 12:24:25.603000000'),
        (4, 'OK', '9', '1000', '', '2022-04-05 12:24:25.603000000'),
        (5, 'OK', '9', '1000', '0001', ''),
        (6, 'OK', '9', None, '0001', '2022-04-05 12:24:25.603000000'),
        (7, 'OK', '9', '1000', None, '2022-04-05 12:24:25.603000000'),
        (8, 'OK', '9', '1000', '0001', None),
        (9, 'OK', '9', '', '', ''),
        (10, 'OK', '9', None, None, None),
        (11, 'NOK', '10', '', '0001', '2022-04-05 12:24:25.603000000'),
        (12, 'NOK', '10', '1000', '', '2022-04-05 12:24:25.603000000'),
        (13, 'NOK', '10', '1000', '0001', ''),
        (14, 'NOK', '10', None, '0001', '2022-04-05 12:24:25.603000000'),
        (15, 'NOK', '10', '1000', None, '2022-04-05 12:24:25.603000000'),
        (16, 'NOK', '10', '1000', '0001', None),
        (17, 'NOK', '10', '', '', ''),
        (18, 'NOK', '10', None, None, None),
    ]

    columns = ['id', 'status', 'ConversationStatusID', 'AgentID', 'CallLegID', 'VoiceStartTime']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('ConversationStatusID', 'AgentID', 'CallLegID', 'VoiceStartTime', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
