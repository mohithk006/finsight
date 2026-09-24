# FinSight — Big Data Banking Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.1-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)](https://spark.apache.org/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.x-yellow.svg)](https://hadoop.apache.org/)
[![Hive](https://img.shields.io/badge/Apache%20Hive-blue.svg)](https://hive.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-green.svg)](https://www.mongodb.com/)
[![Neo4j](https://img.shields.io/badge/Neo4j-blue.svg)](https://neo4j.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-yellow.svg)](https://powerbi.microsoft.com/)

*FinSight is an end-to-end big data banking analytics platform, combining real-time streaming, distributed processing, data warehousing, graph analytics, workflow automation, and interactive business intelligence.*

---

## 📊 Dashboard

### Fraud Alert Board
![Fraud Alert Board](docs/images/fraud-alert-board.png)

### Customer 360
![Customer 360](docs/images/customer-360.png)

### Risk & Compliance
![Risk & Compliance](docs/images/risk-compliance.png)

---

## 📋 Table of Contents

- [Introduction](#-introduction)
- [Key Features](#-key-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Processing Pipeline](#-processing-pipeline)
- [Results](#-results)

---

## Introduction

FinSight addresses three major banking analytics requirements:

- **Real-Time Fraud Detection** — identifies suspicious high-value transactions using streaming analytics.
- **Customer 360 & Churn Analysis** — combines customer-level attributes and behavioral indicators for customer risk analysis.
- **Risk & Compliance Analytics** — generates risk, compliance, and dormancy reports from transaction history.

The platform processes **6.3M+ transactions** and **10K customer profiles** across streaming and batch workflows.

---

## Key Features

- **Real-Time Fraud Detection** using Kafka and Spark Structured Streaming.
- **Real-Time Customer Churn Detection** using streaming customer behavior analysis.
- **Batch Risk Scoring** using historical transaction data.
- **Customer Lifetime Value (CLV) Scoring** with transaction volume, frequency, product diversity, and recency.
- **Compliance Aggregation** using Spark SQL and Hive.
- **Account Dormancy Analysis** based on transaction inactivity.
- **Graph-Based Analysis** using Neo4j for account and transaction relationships.
- **Interactive Power BI Dashboards** for fraud monitoring, customer analytics, and compliance reporting.

---

## Architecture

```mermaid
flowchart TB
    A["NovaCrest Transactions.csv"] --> B["Apache Kafka"]

    B --> C["Spark Structured Streaming"]
    B --> D["Kafka Connect HDFS Sink"]

    C --> E["Fraud Detection"]
    C --> F["Churn Detection"]

    D --> G["HDFS / Parquet"]

    G --> H["Spark Core / Spark SQL"]

    H --> I["Risk Scoring"]
    H --> J["CLV Scoring"]
    H --> K["Compliance"]
    H --> L["Dormancy"]

    H --> M["Hive"]
    M --> N["Alteryx"]

    O["MongoDB<br/>Customer Profiles"] --> N
    P["Neo4j<br/>Graph Analytics"] --> N

    N --> Q["Power BI"]

    Q --> R["Fraud Alert Board"]
    Q --> S["Customer 360"]
    Q --> T["Risk & Compliance"]
