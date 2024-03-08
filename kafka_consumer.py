

global_variable = "123"
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