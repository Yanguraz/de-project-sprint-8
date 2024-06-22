import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Настройки безопасности Kafka
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
}

# Настройки PostgreSQL для локального докера
docker_postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://localhost:5432/postgres',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.create_subscribers_feedback',
}

# Настройки PostgreSQL для облачного сервиса
postgresql_settings = {
    'user': 'student',
    'password': 'de-student',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
}

# Необходимые библиотеки
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0"

# Топики Kafka
TOPIC_IN = 'student.topic.cohort23.islamiang'
TOPIC_OUT = 'student.topic.cohort23.islamiang'

# Текущая метка времени UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp() * 1000))

# Функция для записи каждого батча в PostgreSQL и Kafka
def foreach_batch_function(df, epoch_id):
      df.persist()

      # Добавление пустого столбца 'feedback'
      feedback_df = df.withColumn('feedback', F.lit(None).cast(StringType()))

      # Запись в PostgreSQL
      feedback_df.write.format('jdbc').mode('append').options(**docker_postgresql_settings).save()

      # Сериализация для Kafka
      df_to_stream = feedback_df.select(F.to_json(F.struct(F.col('*'))).alias('value')).select('value')

      # Запись в Kafka
      df_to_stream.write.format('kafka').options(**kafka_security_options).option('topic', TOPIC_OUT).save()

      df.unpersist()

# Инициализация сессии Spark
def spark_init(spark_session_name) -> SparkSession:
    try:
        return SparkSession.builder.appName(spark_session_name) \
            .config("spark.jars.packages", spark_jars_packages) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
    except Exception as e:
        print("Ошибка создания сессии Spark:", e)

# Чтение и фильтрация кампаний ресторанов из Kafka
def restaurant_read_stream(spark):
    try:
        df = spark.readStream.format('kafka').options(**kafka_security_options).option('subscribe', TOPIC_IN).load()

        df_json = df.withColumn('key_str', F.col('key').cast(StringType())) \
                    .withColumn('value_json', F.col('value').cast(StringType())) \
                    .drop('key', 'value')

        incoming_message_schema = StructType([
            StructField('restaurant_id', StringType(), nullable=True),
            StructField('adv_campaign_id', StringType(), nullable=True),
            StructField('adv_campaign_content', StringType(), nullable=True),
            StructField('adv_campaign_owner', StringType(), nullable=True),
            StructField('adv_campaign_owner_contact', StringType(), nullable=True),
            StructField('adv_campaign_datetime_start', LongType(), nullable=True),
            StructField('adv_campaign_datetime_end', LongType(), nullable=True),
            StructField('datetime_created', LongType(), nullable=True),
        ])

        df_string = df_json.withColumn('value', F.from_json(F.col('value_json'), incoming_message_schema)).drop('value_json')

        df_filtered = df_string.select(
            F.col('value.restaurant_id').alias('restaurant_id'),
            F.col('value.adv_campaign_id').alias('adv_campaign_id'),
            F.col('value.adv_campaign_content').alias('adv_campaign_content'),
            F.col('value.adv_campaign_owner').alias('adv_campaign_owner'),
            F.col('value.adv_campaign_owner_contact').alias('adv_campaign_owner_contact'),
            F.col('value.adv_campaign_datetime_start').alias('adv_campaign_datetime_start'),
            F.col('value.adv_campaign_datetime_end').alias('adv_campaign_datetime_end'),
            F.col('value.datetime_created').alias('datetime_created')
        ).filter((F.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (F.col('adv_campaign_datetime_end') > current_timestamp_utc))

        return df_filtered
    except KafkaError as e:
        print("Ошибка подключения к Kafka:", e)

# Чтение подписчиков из PostgreSQL
def subscribers_restaurants(spark):
    try:
        df = spark.read.format('jdbc').options(**postgresql_settings).load()
        df = df.dropDuplicates(['client_id', 'restaurant_id'])
        return df
    except Exception as e:
        print("Ошибка чтения из PostgreSQL:", e)

# Соединение кампаний ресторанов с подписчиками
def join_dataframes(restaurant_df, subscribers_df):
    df = restaurant_df.join(subscribers_df, 'restaurant_id').withColumn('trigger_datetime_created', F.lit(current_timestamp_utc)).select(
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        'datetime_created',
        'client_id',
        'trigger_datetime_created'
    )
    return df

if __name__ == '__main__':
    spark = spark_init("RestaurantSubscribeStreamingService")
    spark.conf.set('spark.sql.streaming.checkpointLocation', '/tmp/spark_checkpoint')

    restaurant_df = restaurant_read_stream(spark)
    subscribers_df = subscribers_restaurants(spark)
    result_df = join_dataframes(restaurant_df, subscribers_df)

    query = result_df.writeStream.foreachBatch(foreach_batch_function).start()
    try:
        query.awaitTermination()
    finally:
        query.stop()
