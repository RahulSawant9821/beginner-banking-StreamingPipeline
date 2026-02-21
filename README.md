**Real-Time Banking Transaction Streaming Pipeline**


**Overview**

This project simulates a real-time banking transaction system using:

Apache Kafka for event streaming

PySpark Structured Streaming for real-time processing

PostgreSQL for storage

Docker Compose for infrastructure orchestration

The pipeline generates synthetic UK banking transactions and streams them into Kafka, processes them with Spark, and persists them into PostgreSQL.



**Architecture**

Producer → Kafka → Spark Structured Streaming → PostgreSQL

Transaction events are generated every second.

Events are published to Kafka topic transactions.

Spark consumes and parses JSON messages.

Micro-batches are written to PostgreSQL table raw_transactions.


**Tech Stack**

Python

Apache Kafka

PySpark (Structured Streaming)

PostgreSQL

Docker & Docker Compose

Faker (Synthetic Data Generation)



📂 Project Structure
├── producer.py              # Kafka transaction generator
├── spark_streaming.py       # Spark consumer & PostgreSQL writer
├── docker-compose.yml       # Kafka + Zookeeper + Postgres setup
└── README.md




**Setup Instructions**

1️. Start Infrastructure
docker-compose up -d


This starts:

Zookeeper

Kafka (localhost:9092)

PostgreSQL (localhost:5432)

2. Create PostgreSQL Table

Connect to PostgreSQL:

psql -U finflow -d finflow


Create table:

CREATE TABLE raw_transactions (
    transaction_id TEXT,
    timestamp TEXT,
    user_id INT,
    merchant TEXT,
    category TEXT,
    amount DOUBLE PRECISION,
    currency TEXT,
    location TEXT,
    status TEXT
);

3. Start Kafka Producer
python producer.py


This generates synthetic UK banking transactions every second.

4️. Start Spark Streaming Job
spark-submit spark_streaming.py


Spark consumes Kafka events and writes them into PostgreSQL.

Sample Transaction Event
{
  "transaction_id": "uuid",
  "timestamp": "2026-02-21T12:34:56",
  "user_id": 2345,
  "merchant": "Amazon",
  "category": "Shopping",
  "amount": 145.32,
  "currency": "GBP",
  "location": "London",
  "status": "completed"
}

**Key Concepts Demonstrated**

Event-driven architecture

Structured Streaming micro-batch processing

Kafka-Spark integration

JDBC-based data persistence

Containerized distributed systems

Fault-tolerant stream processing with checkpointing
