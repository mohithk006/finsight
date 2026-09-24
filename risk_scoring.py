from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    greatest,
    lit,
    max,
    sum,
    when,
)


RAW_DATA_PATH = (
    "hdfs://localhost:9000/finsight/raw/transactions/txn-raw"
)

RISK_OUTPUT_PATH = (
    "hdfs://localhost:9000/finsight/processed/risk_scores"
)

SUMMARY_OUTPUT_PATH = (
    "hdfs://localhost:9000/finsight/processed/daily_summary"
)


# ---------------------------------------------------------
# 7.3 factor weights
# Specification does not provide numeric weights.
# Equal 25% weights are therefore used.
# ---------------------------------------------------------

FREQUENCY_WEIGHT = 0.25
TRANSFER_AMOUNT_WEIGHT = 0.25
CASH_OUT_WEIGHT = 0.25
DESTINATION_WEIGHT = 0.25


spark = (
    SparkSession.builder
    .appName("FinSight-Risk-Scoring")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Read complete HDFS transaction history
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
        "nameDest",
        "isFraud",
    )
)


# ---------------------------------------------------------
# Determine latest step
# ---------------------------------------------------------

max_step = (
    transactions
    .agg(max("step").alias("max_step"))
    .first()["max_step"]
)

if max_step is None:
    raise RuntimeError("No transactions found in HDFS.")


window_start = max_step - 167

print(
    f"Maximum transaction step: {max_step}",
    flush=True,
)

print(
    f"7-day window: steps {window_start} to {max_step}",
    flush=True,
)


# ---------------------------------------------------------
# Select rolling 7-day history
# ---------------------------------------------------------

rolling_7_day = transactions.filter(
    col("step").between(
        window_start,
        max_step,
    )
)


# ---------------------------------------------------------
# Customer-level risk factors
# ---------------------------------------------------------

risk_factors = (
    rolling_7_day
    .groupBy("nameOrig")
    .agg(
        # Transaction frequency
        count("*").alias(
            "transaction_frequency"
        ),

        # Average TRANSFER amount
        avg(
            when(
                col("type") == "TRANSFER",
                col("amount"),
            )
        ).alias(
            "average_transfer_amount"
        ),

        # CASH_OUT proportion
        (
            sum(
                when(
                    col("type") == "CASH_OUT",
                    1,
                ).otherwise(0)
            )
            / count("*")
        ).alias(
            "cash_out_proportion"
        ),

        # Unique destination accounts
        countDistinct(
            "nameDest"
        ).alias(
            "unique_destination_count"
        ),
    )
)


# ---------------------------------------------------------
# Find maximum factor values for normalization
# ---------------------------------------------------------

max_frequency = (
    risk_factors
    .agg(
        max("transaction_frequency").alias(
            "max_frequency"
        )
    )
    .first()["max_frequency"]
)

max_transfer_amount = (
    risk_factors
    .agg(
        max("average_transfer_amount").alias(
            "max_transfer_amount"
        )
    )
    .first()["max_transfer_amount"]
)

max_cash_out = (
    risk_factors
    .agg(
        max("cash_out_proportion").alias(
            "max_cash_out"
        )
    )
    .first()["max_cash_out"]
)

max_destinations = (
    risk_factors
    .agg(
        max("unique_destination_count").alias(
            "max_destinations"
        )
    )
    .first()["max_destinations"]
)


max_frequency = max_frequency or 0
max_transfer_amount = max_transfer_amount or 0.0
max_cash_out = max_cash_out or 0.0
max_destinations = max_destinations or 0


# ---------------------------------------------------------
# Normalize all four factors to 0-1
# ---------------------------------------------------------

normalized = (
    risk_factors

    .withColumn(
        "frequency_score",
        when(
            lit(max_frequency) > 0,
            col("transaction_frequency")
            / lit(max_frequency),
        ).otherwise(
            lit(0.0)
        ),
    )

    .withColumn(
        "transfer_amount_score",
        when(
            lit(max_transfer_amount) > 0,
            coalesce(
                col("average_transfer_amount"),
                lit(0.0),
            )
            / lit(max_transfer_amount),
        ).otherwise(
            lit(0.0)
        ),
    )

    .withColumn(
        "cash_out_score",
        when(
            lit(max_cash_out) > 0,
            coalesce(
                col("cash_out_proportion"),
                lit(0.0),
            )
            / lit(max_cash_out),
        ).otherwise(
            lit(0.0)
        ),
    )

    .withColumn(
        "destination_score",
        when(
            lit(max_destinations) > 0,
            coalesce(
                col("unique_destination_count"),
                lit(0),
            )
            / lit(max_destinations),
        ).otherwise(
            lit(0.0)
        ),
    )
)


# ---------------------------------------------------------
# Composite normalized risk score
# ---------------------------------------------------------

risk_scores = (
    normalized

    .withColumn(
        "risk_score_raw",
        (
            col("frequency_score")
            * lit(FREQUENCY_WEIGHT)

            + col("transfer_amount_score")
            * lit(TRANSFER_AMOUNT_WEIGHT)

            + col("cash_out_score")
            * lit(CASH_OUT_WEIGHT)

            + col("destination_score")
            * lit(DESTINATION_WEIGHT)
        ),
    )

    # Guarantee 0 <= risk_score <= 1
    .withColumn(
        "risk_score",
        greatest(
            lit(0.0),
            col("risk_score_raw"),
        ),
    )

    # -----------------------------------------------------
    # Risk tiers
    # -----------------------------------------------------

    .withColumn(
        "risk_tier",
        when(
            col("risk_score") < 0.25,
            lit("Low"),
        )
        .when(
            col("risk_score") <= 0.60,
            lit("Medium"),
        )
        .otherwise(
            lit("High"),
        ),
    )

    .select(
        col("nameOrig").alias("customerId"),
        col("risk_score"),
        col("risk_tier"),
    )
)


# ---------------------------------------------------------
# Write risk scores
# ---------------------------------------------------------

print(
    "Writing customer risk scores...",
    flush=True,
)

(
    risk_scores
    .write
    .mode("overwrite")
    .parquet(RISK_OUTPUT_PATH)
)


# ---------------------------------------------------------
# Daily transaction summary
# ---------------------------------------------------------

print(
    "Building daily transaction summary...",
    flush=True,
)

daily_summary = (
    transactions
    .groupBy(
        "type",
        "step",
    )
    .agg(
        count("*").alias(
            "transaction_volume"
        ),

        sum("amount").alias(
            "total_amount"
        ),

        sum(
            when(
                col("isFraud") == 1,
                1,
            ).otherwise(0)
        ).alias(
            "fraud_count"
        ),
    )
)


# ---------------------------------------------------------
# Write daily summary
# ---------------------------------------------------------

print(
    "Writing daily summary...",
    flush=True,
)

(
    daily_summary
    .write
    .mode("overwrite")
    .parquet(SUMMARY_OUTPUT_PATH)
)


# ---------------------------------------------------------
# Final verification
# ---------------------------------------------------------

print("========================================", flush=True)
print("FinSight 7.3 Risk Scoring COMPLETE", flush=True)
print("========================================", flush=True)

print(
    f"7-day window: {window_start} -> {max_step}",
    flush=True,
)

print(
    "Risk output:",
    RISK_OUTPUT_PATH,
    flush=True,
)

print(
    "Daily summary:",
    SUMMARY_OUTPUT_PATH,
    flush=True,
)

print(
    "\nSample risk scores:",
    flush=True,
)

(
    risk_scores
    .orderBy(
        col("risk_score").desc()
    )
    .show(10, False)
)

print(
    "\nSample daily summary:",
    flush=True,
)

(
    daily_summary
    .orderBy(
        col("step"),
        col("type"),
    )
    .show(10, False)
)


spark.stop()

