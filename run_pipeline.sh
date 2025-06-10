#!/bin/bash

ZOOKEEPER_CMD="bin/zookeeper-server-start.sh config/zookeeper.properties"
KAFKA_CMD="bin/kafka-server-start.sh config/server.properties"
CONSUMER_CMD="sbt \"runMain KafkaToSQLiteConsumer\""
PRODUCER_CMD="sbt \"runMain MrSplitMyFileIntoCsv\""

# ZOOKEEPER
echo "Starting ZooKeeper"
gnome-terminal -- bash -c "$ZOOKEEPER_CMD; exec bash"
sleep 20

# KAFKA
echo "Starting Kafka"
gnome-terminal -- bash -c "$KAFKA_CMD; exec bash"
sleep 10

# CONSUMER
echo "Starting Consumer"
gnome-terminal -- bash -c "$CONSUMER_CMD; exec bash"
sleep 5

# PRODUCER
echo "Starting Producer"
gnome-terminal -- bash -c "$PRODUCER_CMD; exec bash"
sleep 5

# VISU
echo "Starting Visualisation"
gnome-terminal -- bash -c "cd streamlit-app-py && streamlit run app.py; exec bash"