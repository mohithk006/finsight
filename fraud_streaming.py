from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "txn-raw"
OUTPUT_TOPIC = "txn-flagged"

CHECKPOINT_PATH = "hdfs://localhost:9000/finsight/checkpoints/fraud"
METRICS_PATH = "hdfs://localhost:9000/finsight/processed/streaming_metrics"


PAYLOAD_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])


MESSAGE_SCHEMA = StructType([
    StructField("schema", StructType([]), True),
    StructField("payload", PAYLOAD_SCHEMA, True),
])


spark = (
    SparkSession.builder
    .appName("FinSight-Fraud-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


transactions = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), MESSAGE_SCHEMA).alias("message"))
    .select("message.payload.*")
)


def process_batch(batch_df, batch_id):
    total_count = batch_df.count()

    if total_count == 0:
        return

    flagged_df = batch_df.filter(
        col("type").isin("TRANSFER", "CASH_OUT")
        & (col("amount") > 200000)
        & (col("newbalanceDest") == 0)
    )

    flagged_count = flagged_df.count()

    fraud_rate = (flagged_count / total_count) * 100

    if flagged_count > 0:
        output_df = flagged_df.select(
            col("nameOrig").alias("key"),
            to_json(struct(*flagged_df.columns)).alias("value")
        )

        (
            output_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )

    metrics_df = spark.createDataFrame(
        [
            (
                int(batch_id),
                total_count,
                flagged_count,
                float(fraud_rate),
            )
        ],
        [
            "batch_id",
            "total_count",
            "flagged_count",
            "fraud_rate",
        ],
    )

    (
        metrics_df.write
        .mode("append")
        .parquet(METRICS_PATH)
    )

    print(
        f"MICRO-BATCH: {batch_id} | "
        f"Total transactions: {total_count} | "
        f"Flagged transactions: {flagged_count} | "
        f"Fraud rate: {fraud_rate:.4f}%"
    )


query = (
    transactions.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)


print("========================================")
print("FinSight Fraud Streaming")
print("Input      :", INPUT_TOPIC)
print("Output     :", OUTPUT_TOPIC)
print("Checkpoint :", CHECKPOINT_PATH)
print("Metrics    :", METRICS_PATH)
print("========================================")
print("Streaming query started.")
print("Waiting for Kafka transactions...")


try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStreaming stopped by user.")
finally:
    spark.stop()

