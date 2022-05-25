from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

def validate(start_time_column, end_time_column, voice_start_column, df):
    print("1")
    dt_df = df.withColumn(start_time_column, F.col(start_time_column).cast("timestamp")) \
                        .withColumn(end_time_column, F.col(end_time_column).cast("timestamp")) \
                        .withColumn(voice_start_column, F.col(voice_start_column).cast("timestamp"))
    dt_df.cache()
    print("2")
    df_ok = dt_df.filter((dt_df[voice_start_column] > dt_df[start_time_column]) & \
                        (dt_df[voice_start_column] < dt_df[end_time_column]))
    print("3")
    df_nok = dt_df.filter(~((dt_df[voice_start_column] > dt_df[start_time_column]) & \
                        (dt_df[voice_start_column] < dt_df[end_time_column])))
    print("4")
    dt_df.unpersist()
    print("5")
    return df_ok, df_nok


if __name__ == '__main__':

    from pyspark.context import SparkContext
    from awsglue.context import GlueContext

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    data = [
        (1, 'OK', '2022-04-05 12:24:25.603000000', '2022-04-05 13:50:25.603000000', '2022-04-05 12:30:25.603000000'),
        (2, 'OK', '2022-04-05 14:25:25.603000000', '2022-04-05 15:00:25.603000000', '2022-04-05 14:27:25.603000000'),
        (3, 'OK', '2022-04-05 07:00:00.603000000', '2022-04-05 07:05:25.603000000', '2022-04-05 07:00:25.603000000'),
        (4, 'NOK', '2022-04-05 12:30:25.603000000', '2022-04-05 12:00:25.603000000', '2022-04-05 12:35:25.603000000'),
        (5, 'NOK', '2022-04-05 12:30:25.603000000', '2022-04-05 12:40:25.603000000', '2022-04-05 12:00:25.603000000'),
        (6, 'NOK', '2022-04-05 12:50:25.603000000', '2022-04-05 12:40:25.603000000', '2022-04-05 12:30:25.603000000'),
        (7, 'NOK', '2022-04-05 12:50:25.603000000', '2022-04-05 12:40:25.603000000', '2022-04-05 12:55:25.603000000')
    ]

    columns = ['id', 'status', 'TaskStartTime', 'TaskEndTime', 'VoiceStartTime']
    df = spark.createDataFrame(data=data, schema = columns)

    print('Tabela de entrada:')
    df.show()

    df_ok, df_nok = validate('TaskStartTime', 'TaskEndTime', 'VoiceStartTime', df)

    print('Tabela Aceitos:')
    df_ok.show()

    print('Tabela Rejeitados:')
    df_nok.show()
