import json

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType


def get_df_from_kafka(spark, kafka_conf):
    bootstrap_server = kafka_conf['bootstrap_server']
    topic = kafka_conf['topic']

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    return df

def read_schema(file_path):
    with open(file_path, "r") as f:
        schema_json = json.load(f)
    json_schema = StructType.fromJson(schema_json)

    return json_schema
def print_df_on_console(df):
    return df.writeStream.outputMode("append").format("console")


def get_df_from_schema(df, json_schema):
    return df.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("data")).select("data.*")