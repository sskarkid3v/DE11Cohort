from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "demo-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="demo-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consuming from demo-topic... Ctrl+C to stop.")
for msg in consumer:
    print("Received:", msg.value)
