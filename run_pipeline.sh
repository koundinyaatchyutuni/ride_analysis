#!/bin/bash
# CONFIGURATION
KAFKA_DIR="/home/koundinya/kafka"          # Update this with your Kafka installation path
SPARK_DIR="/home/koundinya/spark"          # Update this with your Spark installation path
TOPIC_NAME="ride-topic"
PRODUCER_SCRIPT="producer.py"
SPARK_SCRIPT="consumer.py"

# Step 1: Start Zookeeper
echo "🟡 Starting Zookeeper..."
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
sleep 5

# Step 2: Start Kafka Broker
echo "🟡 Starting Kafka broker..."
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
sleep 5

# Step 3: Create Kafka Topic (if it doesn't exist)
echo "🟡 Creating topic: $TOPIC_NAME (if not exists)..."
$KAFKA_DIR/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $TOPIC_NAME

# Step 4: Run Kafka Producer
echo "🟢 Starting Kafka producer script..."
nohup python3 $PRODUCER_SCRIPT > producer.log 2>&1 &

# Step 5: Run Spark Structured Streaming Job
echo "🟢 Submitting Spark streaming job..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 $SPARK_SCRIPT

echo "✅ All services started. Kafka producer and Spark job are running."
