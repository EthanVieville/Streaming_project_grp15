#!/bin/bash

echo "Starting ZooKeeper"
kafka_2.13-3.7.0/bin/zookeeper-server-start.sh kafka_2.13-3.7.0/config/zookeeper.properties &
sleep 10


echo "Starting Kafka"
kafka_2.13-3.7.0/bin/kafka-server-start.sh kafka_2.13-3.7.0/config/server.properties &
sleep 10

echo "Starting Consumer"
sbt runMain KafkaToSQLiteConsumer &
sleep 5

echo "Starting Producer"
#sbt runMain MrSplitMyFileIntoCsv &
sleep 5

echo "Starting visualisation"
#cd streamlit-app-py && streamlit run app.py &