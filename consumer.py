from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'health_data_predicted',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    row = message.value
    if isinstance(row, str):
        row = json.loads(row)

    print(f"{message.offset}")
    print(json.dumps(row, indent=4))