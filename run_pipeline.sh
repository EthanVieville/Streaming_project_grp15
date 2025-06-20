#!/bin/bash

echo "Starting ZooKeeper"
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 20

echo "Starting Kafka"
bin/kafka-server-start.sh config/server.properties &
sleep 10

echo "Starting Consumer"
sbt runMain KafkaToSQLiteConsumer &
sleep 5

echo "Starting Producer"
sbt runMain MrSplitMyFileIntoCsv &
sleep 5

echo "Starting visualisation"
cd streamlit-app-py && streamlit run app.py &