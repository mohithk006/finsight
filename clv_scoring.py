from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    max,
    sum,
    when,
)


RAW_DATA_PATH = (
    "hdfs://localhost:9000/finsight/raw/transactions/txn-raw"
)

CLV_OUTPUT_PATH = (
    "hdfs://localhost:9000/finsight/processed/clv_scores"
)

HIVE_TABLE = "finsight.customer_clv"


VOLUME_WEIGHT = 0.30
FREQUENCY_WEIGHT = 0.25
DIVERSITY_WEIGHT = 0.25
RECENCY_WEIGHT = 0.20


spark = (
    SparkSession.builder
    .appName("FinSight-CLV-Scoring")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Read full transaction history
# ---------------------------------------------------------

print(
    "Reading full HDFS transaction history...",
    flush=True,
)

transactions = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(RAW_DATA_PATH)
    .select(
        "step",
        "type",
        "amount",
        "nameOrig",
    )
)


# ---------------------------------------------------------
# Latest transaction step
# ---------------------------------------------------------

max_step = (
    transactions
    .agg(
        max("step").alias("max_step")
    )
    .first()["max_step"]
)

if max_step is None:
    raise RuntimeError(
        "No transaction data found in HDFS."
    )


print(
    f"Maximum transaction step: {max_step}",
    flush=True,
)


# ---------------------------------------------------------
# Customer-level historical metrics
# ---------------------------------------------------------

customer_metrics = (
    transactions
    .groupBy("nameOrig")
    .agg(
        # Total cumulative transaction amount
        sum("amount").alias(
            "total_transaction_amount"
        ),

        # Total number of transactions
        count("*").alias(
            "transaction_count"
        ),

        # Number of different transaction types
        countDistinct("type").alias(
            "distinct_transaction_types"
        ),

        # Most recent transaction
        max("step").alias(
            "last_active_step"
        ),
    )
)


# ---------------------------------------------------------
# Maximum values used for normalization
# ---------------------------------------------------------

max_amount = (
    customer_metrics
    .agg(
        max(
            "total_transaction_amount"
        ).alias("max_amount")
    )
    .first()["max_amount"]
)

max_frequency = (
    customer_metrics
    .agg(
        max(
            "transaction_count"
        ).alias("max_frequency")
    )
    .first()["max_frequency"]
)

max_amount = max_amount or 0.0
max_frequency = max_frequency or 0


# ---------------------------------------------------------
# CLV component scores
# ---------------------------------------------------------

clv_scores = (
    customer_metrics

    # ---------------------------------------------
    # Transaction Volume Score - 30%
    # ---------------------------------------------

    .withColumn(
        "transaction_volume_score",
        when(
            lit(max_amount) > 0,
            col("total_transaction_amount")
            / lit(max_amount),
        ).otherwise(
            lit(0.0)
        ),
    )

    # ---------------------------------------------
    # Transaction Frequency Score - 25%
    # ---------------------------------------------

    .withColumn(
        "transaction_frequency_score",
        when(
            lit(max_frequency) > 0,
            col("transaction_count")
            / lit(max_frequency),
        ).otherwise(
            lit(0.0)
        ),
    )

    # ---------------------------------------------
    # Product Diversity Score - 25%
    # ---------------------------------------------

    .withColumn(
        "product_diversity_score",
        (
            col("distinct_transaction_types")
            / lit(5.0)
        ),
    )

    # ---------------------------------------------
    # Recency Score - 20%
    #
    # Inverse relationship:
    # 0 steps since activity  -> 1.0
    # 1 step                  -> 0.5
    # 2 steps                 -> 0.333...
    #
    # More than 48 inactive
    # steps                    -> 0
    # ---------------------------------------------

    .withColumn(
        "steps_since_last_transaction",
        lit(max_step)
        - col("last_active_step"),
    )

    .withColumn(
        "recency_score",
        when(
            col("steps_since_last_transaction") > 48,
            lit(0.0),
        ).otherwise(
            lit(1.0)
            / (
                col("steps_since_last_transaction")
                + lit(1.0)
            )
        ),
    )

    # ---------------------------------------------
    # Final CLV score
    # ---------------------------------------------

    .withColumn(
        "clv_score",
        (
            col("transaction_volume_score")
            * lit(VOLUME_WEIGHT)

            + col("transaction_frequency_score")
            * lit(FREQUENCY_WEIGHT)

            + col("product_diversity_score")
            * lit(DIVERSITY_WEIGHT)

            + col("recency_score")
            * lit(RECENCY_WEIGHT)
        ),
    )

    # ---------------------------------------------
    # CLV classification
    # ---------------------------------------------

    .withColumn(
        "clv_tier",
        when(
            col("clv_score") > 0.70,
            lit("High Value"),
        )
        .when(
            col("clv_score") >= 0.40,
            lit("Growth Potential"),
        )
        .otherwise(
            lit("At Risk"),
        ),
    )

    .select(
        col("nameOrig").alias("customerId"),
        col("transaction_volume_score"),
        col("transaction_frequency_score"),
        col("product_diversity_score"),
        col("recency_score"),
        col("clv_score"),
        col("clv_tier"),
    )
)


# ---------------------------------------------------------
# Write CLV output
# ---------------------------------------------------------

print(
    "Writing CLV scores to HDFS...",
    flush=True,
)

(
    clv_scores
    .write
    .mode("overwrite")
    .parquet(CLV_OUTPUT_PATH)
)


# ---------------------------------------------------------
# Register external Hive table
# ---------------------------------------------------------

print(
    "Registering Hive external table...",
    flush=True,
)

spark.sql(
    "CREATE DATABASE IF NOT EXISTS finsight"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {HIVE_TABLE}
    (
        customerId STRING,
        transaction_volume_score DOUBLE,
        transaction_frequency_score DOUBLE,
        product_diversity_score DOUBLE,
        recency_score DOUBLE,
        clv_score DOUBLE,
        clv_tier STRING
    )
    USING PARQUET
    LOCATION '{CLV_OUTPUT_PATH}'
    """
)


# ---------------------------------------------------------
# Verification
# ---------------------------------------------------------

print("========================================", flush=True)
print("FinSight 7.4 CLV Scoring COMPLETE", flush=True)
print("========================================", flush=True)

print(
    "CLV output:",
    CLV_OUTPUT_PATH,
    flush=True,
)

print(
    "Hive table:",
    HIVE_TABLE,
    flush=True,
)

print(
    "\nSample CLV scores:",
    flush=True,
)

(
    clv_scores
    .orderBy(
        col("clv_score").desc()
    )
    .show(10, False)
)

print(
    "\nHive table verification:",
    flush=True,
)

spark.sql(
    f"SELECT * FROM {HIVE_TABLE} LIMIT 10"
).show(
    10,
    False,
)


spark.stop()
