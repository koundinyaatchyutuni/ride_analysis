from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime

# Read and sort the CSV data based on pickup timestamp
data = pd.read_csv('/home/koundinya/Desktop/ride_analysis/data/uber_data.csv')

# Ensure datetime column is parsed correctly
# data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
data = data.sort_values('tpep_pickup_datetime')

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    linger_ms=10,  # optional: batches small messages to improve throughput
    retries=5      # retry up to 5 times on failure
)

print(f"[{datetime.now()}] Starting Kafka producer...")

# Send each row as a JSON message every 1 second
try:
    for index, row in data.iterrows():
        message = row.to_dict()
        producer.send('ride-topic', value=message)
        print(f"[{datetime.now()}] Sent record {index + 1}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[INFO] Producer stopped manually.")
except Exception as e:
    print(f"[ERROR] {e}")
finally:
    # Flush and close the producer
    producer.flush()
    producer.close()
    print(f"[{datetime.now()}] Kafka producer closed.")
