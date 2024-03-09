from pyspark.sql import SparkSession

def get_df_from_kafka(spark, kafka_conf=None):
    bootstrap_server = 'broker:29092'
    topic = "sampletopic"

    print("connecting to bootstrapserver " + bootstrap_server)
    print("topic subscribing " + topic)

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return df

def print_df_on_console(df):
    return df.writeStream.outputMode("append").format("console").start()

application_name = "myApplication"
master = "local[*]"
log_level = "ERROR"
spark = (
    SparkSession.builder.appName(application_name)
    .master(master)
    .getOrCreate()
)
spark.sparkContext.setLogLevel(log_level)

kafka_df = get_df_from_kafka(spark=spark)

query = print_df_on_console(kafka_df)

query.awaitTermination()