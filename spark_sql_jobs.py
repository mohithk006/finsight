from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    lit,
    max as spark_max
)
import argparse


HIVE_TABLE = "finsight.transactions"

COMPLIANCE_OUTPUT = "hdfs://localhost:9000/finsight/processed/compliance_summary"
CUSTOMER_FRAUD_OUTPUT = "hdfs://localhost:9000/finsight/processed/customer_fraud_summary"
DORMANCY_OUTPUT = "hdfs://localhost:9000/finsight/processed/dormancy_report"
DORMANCY_CSV_OUTPUT = "hdfs://localhost:9000/finsight/exports/dormancy_report.csv"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("FinSight-Spark-SQL-Jobs")
        .master("local[*]")
        .config(
            "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive",
            "true"
        )
        .config(
            "spark.sql.hive.convertMetastoreParquet",
            "false"
        )
        .enableHiveSupport()
        .getOrCreate()
    )


def run_compliance(spark):
    print("Running 7.5 compliance aggregation...")

    df = spark.table(HIVE_TABLE)

    max_step = df.agg(spark_max("step").alias("max_step")).first()["max_step"]
    window_start = max_step - 167

    weekly = (
        df.filter((col("step") >= window_start) & (col("step") <= max_step))
        .groupBy("type")
        .agg(
            count("*").alias("transaction_volume"),
            spark_sum("amount").alias("total_amount"),
            spark_sum(
                when(col("isFraud") == 1, 1).otherwise(0)
            ).alias("fraud_count")
        )
        .withColumn(
            "fraud_rate",
            when(
                col("transaction_volume") > 0,
                col("fraud_count") / col("transaction_volume") * 100
            ).otherwise(lit(0.0))
        )
        .withColumn("window_start_step", lit(window_start))
        .withColumn("window_end_step", lit(max_step))
        .orderBy("type")
    )

    weekly.write.mode("overwrite").parquet(COMPLIANCE_OUTPUT)

    print(f"7.5 compliance window: {window_start} -> {max_step}")
    print(f"Compliance output: {COMPLIANCE_OUTPUT}")
    weekly.show(truncate=False)


def run_customer_fraud(spark):
    print("Running 7.5 customer fraud summary...")

    df = spark.table(HIVE_TABLE)

    summary = (
        df.groupBy("nameOrig")
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_sum(
                when(col("isFraud") == 1, 1).otherwise(0)
            ).alias("fraud_count")
        )
        .withColumn(
            "fraud_rate",
            when(
                col("transaction_count") > 0,
                col("fraud_count") / col("transaction_count") * 100
            ).otherwise(lit(0.0))
        )
        .withColumnRenamed("nameOrig", "customerId")
    )

    summary.write.mode("overwrite").parquet(CUSTOMER_FRAUD_OUTPUT)

    print(f"Customer fraud output: {CUSTOMER_FRAUD_OUTPUT}")
    summary.show(10, truncate=False)


def run_dormancy(spark):
    print("Running 7.6 account dormancy report...")

    df = spark.table(HIVE_TABLE)

    max_step = df.agg(spark_max("step").alias("max_step")).first()["max_step"]

    customers = (
        df.filter(col("nameOrig").startswith("C"))
        .groupBy("nameOrig")
        .agg(
            spark_max("step").alias("last_active_step"),
            count("*").alias("transaction_count")
        )
        .withColumn(
            "inactivity_steps",
            lit(max_step) - col("last_active_step")
        )
        .filter(col("transaction_count") >= 5)
        .filter(col("inactivity_steps") > 72)
        .withColumnRenamed("nameOrig", "customerId")
        .withColumn(
            "severity",
            when(
                col("inactivity_steps") <= 120,
                "Dormant"
            ).otherwise("Severely Dormant")
        )
        .withColumn("max_dataset_step", lit(max_step))
        .orderBy(col("inactivity_steps").desc())
    )

    customers.write.mode("overwrite").parquet(DORMANCY_OUTPUT)

    (
        customers.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(DORMANCY_CSV_OUTPUT)
    )

    print(f"Dormancy output: {DORMANCY_OUTPUT}")
    print(f"Dormancy CSV: {DORMANCY_CSV_OUTPUT}")
    customers.show(20, truncate=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        required=True,
        choices=["compliance", "customer_fraud", "dormancy"]
    )

    args = parser.parse_args()

    spark = create_spark_session()

    try:
        if args.mode == "compliance":
            run_compliance(spark)
        elif args.mode == "customer_fraud":
            run_customer_fraud(spark)
        elif args.mode == "dormancy":
            run_dormancy(spark)

        print(f"FinSight Spark SQL mode '{args.mode}' COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
