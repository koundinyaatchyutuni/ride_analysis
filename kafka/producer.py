from kafka import KafkaProducer
import csv
import json
import time
producer=KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda a:json.dumps(a).encode('utf-8')
)

with open('/home/koundinya/Desktop/ride_analysis/data/uber_request_data.csv','r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('rides-topic', value=row)
        print(f"Produced: {row}")
        time.sleep(0.5)