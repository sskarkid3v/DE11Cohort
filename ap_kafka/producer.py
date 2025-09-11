from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producing to demo-topic... Ctrl+C to stop.")
while True:
    event = {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(5, 500), 2)
    }
    producer.send("demo-topic", event)
    print("Sent:", event)
    time.sleep(1)
