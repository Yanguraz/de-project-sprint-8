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
