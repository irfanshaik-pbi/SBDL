from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_conf


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_spark_session(env)) \
            .config('spark.sql.autoBroadcastJoinThreshold', -1) \
            .config('spark.sql.adapative.enabled', 'false') \
            .config('spark.driver.extraJavaOptions',
                    '-DLog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf= get_spark_session(env)) \
            .enableHiveSupport() \
            .getOrCreate()
