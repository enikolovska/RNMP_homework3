from kafka import KafkaProducer
import json
import pandas as pd
import time

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3)

    print(f"Kafka Producer connected to {'localhost:9092'}")
    return producer

def send_data_to_kafka(producer, data_file, topic):
    df = pd.read_csv(data_file)
    target_col = 'Diabetes_binary'

    if target_col not in df.columns:
        feature_cols = df.columns.tolist()
    else:
        feature_cols = [col for col in df.columns if col != target_col]
    sent_count = 0

    for idx, row in df.iterrows():
        message = {}
        for col in feature_cols:
            message[col] = float(row[col]) if pd.notna(row[col]) else 0.0

        print(f"Sending row {idx}")
        producer.send(topic, value=message)
        sent_count += 1
        time.sleep(1)


def main():
    print("Kafka Producer for Health Data")
    producer = create_producer()
    send_data_to_kafka(producer,'data/online.csv', 'health_data')
    producer.flush()
    producer.close()
    print("\nProducer closed")

if __name__ == "__main__":
    main()