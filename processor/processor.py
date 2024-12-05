import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, sum, count, to_json, to_timestamp, to_date, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType
import socket
import time
from kafka import KafkaConsumer


# Kafka configuration
kafka_broker = "kafka:9092"
raw_topic = "raw-data"
processed_topic = "processed-data"

def wait_for_kafka(kafka_bootstrap_servers, timeout=60):
    """
    Waits for Kafka to be available by repeatedly attempting to connect
    to the Kafka bootstrap server within a specified timeout.
    """
    start_time = time.time()
    while True:
        try:
            host, port = kafka_bootstrap_servers.split(":")
            sock = socket.create_connection((host, int(port)), timeout=5)
            sock.close()
            print("Kafka is available")
            return
        except Exception as e:
            if time.time() - start_time > timeout:
                raise Exception("Kafka did not become ready in time") from e
            print("Waiting for Kafka to be ready...")
            time.sleep(5)


# Wait for Kafka to be ready
wait_for_kafka("kafka:9092")


def wait_for_kafka_topic(bootstrap_servers, topic, timeout=300):
    """
    Wait for the specified Kafka topic to be available.
    """
    start_time = time.time()
    while True:
        try:
            # consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
            consumer = KafkaConsumer(
                "raw-data",
                bootstrap_servers='kafka:9092',
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='PLAIN',
                sasl_plain_username='consumer',
                sasl_plain_password='consumer-secret',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='my-group'
            )
            topics = consumer.topics()
            if topic in topics:
                print(f"Kafka topic '{topic}' is available.")
                consumer.close()
                return
            else:
                print(f"Waiting for Kafka topic '{topic}' to be created...")
        except Exception as e:
            print(f"Error while checking Kafka topic: {e}")
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Kafka topic '{topic}' did not become available within {timeout} seconds.")
        time.sleep(5)


# Wait for Kafka topic
wait_for_kafka_topic(kafka_broker, raw_topic)

# Kafka configuration
kafka_broker = "kafka:9092"
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_username = "consumer"  # Or consumer, depending on role
kafka_sasl_password = "consumer-secret"  # Match the JAAS config

print('PreSession')
# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaBatchProcessing") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .config("spark.kafka.bootstrap.servers", kafka_broker) \
    .config("spark.kafka.security.protocol", kafka_security_protocol) \
    .config("spark.kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .config("spark.kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_sasl_username}\" password=\"{kafka_sasl_password}\";") \
    .getOrCreate()
print('AfterSession')

# Define schema for the incoming Kafka data
tweet_schema = StructType([
    StructField("date", StringType(), True),
    StructField("tweets", ArrayType(StructType([
        StructField("review_rating", StringType(), True),  # Initially received as string
        StructField("review_likes", StringType(), True),   # Initially received as string
        StructField("review_id", StringType(), True),
        StructField("pseudo_author_id", StringType(), True),
        StructField("author_name", StringType(), True),
        StructField("review_text", StringType(), True),
        StructField("author_app_version", StringType(), True),
        StructField("review_timestamp", StringType(), True)
    ])), True)
])


# Read raw data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", raw_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";'
    ) \
    .load()



# Parse Kafka messages
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), tweet_schema)) \
    .select("data.*")
    # .select("data.date", "data.tweets")


# Explode tweets into individual rows
exploded_tweets = parsed_stream.withColumn("tweet", explode(col("tweets"))) \
    .select(
        col("date"),
        col("tweet.review_rating").cast(FloatType()).alias("review_rating"),
        col("tweet.review_likes").cast(IntegerType()).alias("review_likes")
    )


# Perform batch aggregation
aggregated_data = exploded_tweets.groupBy("date").agg(
    avg("review_rating").alias("average_rating"),
    sum("review_likes").alias("total_likes"),
    count("review_rating").alias("total_review_count")
).withColumn("number_of_tweets", col("total_review_count"))

# Serialize the output for Kafka
output_stream = aggregated_data.selectExpr(
    "CAST(date AS STRING) AS key",
    "to_json(struct(*)) AS value"
)

query = output_stream.writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("topic", processed_topic) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";'
    ) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/kafka-output") \
    .trigger(processingTime="10 seconds") \
    .start()



# Keep the query running indefinitely
query.awaitTermination()

