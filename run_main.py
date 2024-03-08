import json

from pyspark.sql import SparkSession
from pyhocon import ConfigFactory
from pyspark.sql.types import StructType

import dao
from happybase import Connection

from kafka_consumer import get_df_from_kafka, get_df_from_schema, print_df_on_console, read_schema


def get_spark_session(spark_conf):
    application_name = spark_conf['application_name']
    master = spark_conf['master']
    log_level = spark_conf['log_level']
    spark = (
        SparkSession.builder.appName(application_name)
        .master(master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)

    return spark, spark.sparkContext


if __name__ == '__main__':
    conf = ConfigFactory.parse_file('application.conf')

    spark_conf = conf['spark']
    spark, sc = get_spark_session(spark_conf=spark_conf)

    kafka_conf = conf['kafka']
    kafka_df = get_df_from_kafka(spark=spark, kafka_conf=kafka_conf)

    schema = read_schema('schema.json')
    parsed_df = get_df_from_schema(kafka_df, schema)

    query = print_df_on_console(parsed_df)

    query.start()

    # hbase_connection = dao.HBaseDao()
    #
    # rows = hbase_connection.get_data(key=b'6544649161377464', table='lookup')
    # print(rows[b'bt:score'])

    # sc.broadcast(hbase_connection)
