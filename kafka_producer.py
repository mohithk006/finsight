import csv
import json
import time

from kafka import KafkaProducer


CSV_FILE = "/home/clouduser/finsight/data/raw/Transactions.csv"
KAFKA_TOPIC = "txn-raw"
BOOTSTRAP_SERVERS = "localhost:9092"
MESSAGES_PER_SECOND = 1000


SCHEMA = {
    "type": "struct",
    "name": "Transaction",
    "fields": [
        {"type": "int32", "field": "step"},
        {"type": "string", "field": "type"},
        {"type": "double", "field": "amount"},
        {"type": "string", "field": "nameOrig"},
        {"type": "double", "field": "oldbalanceOrg"},
        {"type": "double", "field": "newbalanceOrig"},
        {"type": "string", "field": "nameDest"},
        {"type": "double", "field": "oldbalanceDest"},
        {"type": "double", "field": "newbalanceDest"},
        {"type": "int32", "field": "isFraud"},
        {"type": "int32", "field": "isFlaggedFraud"},
    ],
}


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=32768,
        compression_type="gzip",
    )


def create_payload(row):
    return {
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
    }


def main():
    producer = create_producer()

    count = 0
    start_time = time.time()

    try:
        with open(CSV_FILE, "r", newline="") as file:
            reader = csv.DictReader(file)

            for row in reader:
                message = {
                    "schema": SCHEMA,
                    "payload": create_payload(row),
                }

                producer.send(KAFKA_TOPIC, value=message)
                count += 1

                if count % 1000 == 0:
                    producer.flush()

                    elapsed = time.time() - start_time
                    target_time = count / MESSAGES_PER_SECOND

                    if elapsed < target_time:
                        time.sleep(target_time - elapsed)

                    elapsed = time.time() - start_time

                    print(
                        f"Sent: {count:,} | "
                        f"Rate: {count / elapsed:,.0f} msg/sec | "
                        f"Elapsed: {elapsed:.1f} sec"
                    )

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - start_time

    print(f"Total messages sent: {count:,}")
    print(f"Total time: {elapsed:.2f} seconds")

    if elapsed > 0:
        print(f"Average rate: {count / elapsed:,.0f} messages/sec")


if __name__ == "__main__":
    main()
