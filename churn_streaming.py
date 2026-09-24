from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    from_json,
    from_unixtime,
    lit,
    sum,
    to_json,
    when,
    window,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "txn-raw"
OUTPUT_TOPIC = "txn-churn"

CHECKPOINT_PATH = (
    "hdfs://localhost:9000/finsight/checkpoints/churn"
)

ALERT_PATH = (
    "hdfs://localhost:9000/finsight/processed/churn_alerts"
)

MAX_OFFSETS_PER_TRIGGER = 500


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
    .appName("FinSight-Churn-Streaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option(
        "maxOffsetsPerTrigger",
        MAX_OFFSETS_PER_TRIGGER,
    )
    .load()
)


transactions = (
    raw_stream
    .selectExpr(
        "CAST(value AS STRING) AS json"
    )
    .select(
        from_json(
            col("json"),
            MESSAGE_SCHEMA,
        ).alias("message")
    )
    .select("message.payload.*")
    .withColumn(
        "event_time",
        from_unixtime(
            col("step") * 3600
        ).cast("timestamp"),
    )
)


customer_windows = (
    transactions
    .withWatermark(
        "event_time",
        "1 hour",
    )
    .groupBy(
        "nameOrig",
        window(
            col("event_time"),
            "24 hours",
            "12 hours",
        ),
    )
    .agg(
        count("*").alias("window_txn_count"),

        avg("amount").alias("window_avg_amount"),

        sum(
            when(
                col("type") == "CASH_OUT",
                1,
            ).otherwise(0)
        ).alias("cash_out_count"),

        sum(
            when(
                col("type") == "PAYMENT",
                1,
            ).otherwise(0)
        ).alias("payment_count"),

        sum(
            when(
                col("type") == "DEBIT",
                1,
            ).otherwise(0)
        ).alias("debit_count"),

        sum(
            when(
                col("newbalanceOrig") <= 500,
                1,
            ).otherwise(0)
        ).alias("low_balance_count"),
    )
)


def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    alerts = (
        batch_df

        # Signal 1:
        # Low transaction frequency.
        .withColumn(
            "signal_1",
            col("window_txn_count") < 2,
        )

        # Signal 2:
        # Average amount is unusually low.
        .withColumn(
            "signal_2",
            col("window_avg_amount") < 20.0,
        )

        # Signal 3:
        # CASH_OUT only.
        .withColumn(
            "signal_3",
            (
                (col("cash_out_count") == col("window_txn_count"))
                & (col("payment_count") == 0)
                & (col("debit_count") == 0)
            ),
        )

        # Signal 4:
        # Two or more low/zero balances in the window.
        .withColumn(
            "signal_4",
            col("low_balance_count") >= 2,
        )

        .withColumn(
            "signal_count",
            (
                col("signal_1").cast("int")
                + col("signal_2").cast("int")
                + col("signal_3").cast("int")
                + col("signal_4").cast("int")
            ),
        )

        .filter(
            col("signal_count") >= 2
        )

        .withColumn(
            "signals",
            concat_ws(
                ", ",

                when(
                    col("signal_1"),
                    lit("LOW_FREQUENCY"),
                ),

                when(
                    col("signal_2"),
                    lit("LOW_AVG_AMOUNT"),
                ),

                when(
                    col("signal_3"),
                    lit("CASH_OUT_ONLY"),
                ),

                when(
                    col("signal_4"),
                    lit("LOW_OR_ZERO_BALANCE"),
                ),
            ),
        )
        .select(
            col("nameOrig").alias("customerId"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("signals"),
            col("signal_count"),
        )
    )

    alert_count = alerts.count()

    print(
        f"MICRO-BATCH: {batch_id} | "
        f"Churn alerts: {alert_count}",
        flush=True,
    )

    if alert_count == 0:
        return

    (
        alerts
        .select(
            to_json(
                struct(*alerts.columns)
            ).alias("value")
        )
        .write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "topic",
            OUTPUT_TOPIC,
        )
        .save()
    )

    (
        alerts.write
        .mode("append")
        .parquet(ALERT_PATH)
    )


query = (
    customer_windows.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


print("========================================", flush=True)
print("FinSight Churn Streaming", flush=True)
print("Input      :", INPUT_TOPIC, flush=True)
print("Output     :", OUTPUT_TOPIC, flush=True)
print("Checkpoint :", CHECKPOINT_PATH, flush=True)
print("Alerts     :", ALERT_PATH, flush=True)
print("========================================", flush=True)
print("Streaming query started.", flush=True)


try:
    query.awaitTermination()

except KeyboardInterrupt:
    print(
        "\nChurn streaming stopped by user.",
        flush=True,
    )

finally:
    spark.stop()

