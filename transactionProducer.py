import json
import random
import time
from uuid import uuid4
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Tool used to generates fake data for you
fake = Faker('en_GB')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers = ['172.23.32.1:9092'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

merchants = ["Amazon", "Tesco", "Starbucks", "Uber", "Deliveroo", "Netflix"]
categories = ["Shopping", "Food", "Transport", "Entertainment", "Bills"]
locations = ["London", "Manchester", "Birmingham", "Leeds", "Bristol"]

# random data generator
def generate_transaction():
    return {
        "transaction_id": str(uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": random.randint(1000, 5000),
        "merchant": random.choice(merchants),
        "category": random.choice(categories),
        "amount": round(random.uniform(1.0, 2000.0), 2),
        "currency": "GBP",
        "location": random.choice(locations),
        "status": random.choice(["completed", "pending", "failed"])
    }

topic_name = "transactions"

print("Sending transactions to Kafka topic:", topic_name)
while True:
    txn = generate_transaction()
    producer.send(topic_name, txn)
    print("Sent:", txn)
    time.sleep(1)