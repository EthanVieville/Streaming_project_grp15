@echo off

start "ZooKeeper" cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
timeout /t 20

start "Kafka" cmd /k "bin\windows\kafka-server-start.bat config\server.properties"
timeout /t 10

start "Consumer" cmd /k "sbt runMain KafkaToSQLiteConsumer"
timeout /t 5

start "Producer" cmd /k "sbt runMain MrSplitMyFileIntoCsv"
timeout /t 5

start "Streamlit" cmd /k "cd streamlit-app-py && streamlit run app.py"
