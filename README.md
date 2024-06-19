# MyFirstKafkaProject

როგორ დავსტარტოთ პროექტი?
•  Start Zookeeper and Kafka:  (command prompt-დან)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
•  Activate Virtual Environment: ტერმინალიდან პროექტში 
venv\Scripts\activate
•  Run Your Python Script: python kafkasioni.py

შემოწმება ტოპიკში მონაცემების: (git bash-დან)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic valid_data --from-beginning
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic monitoring --from-beginning

ტოპიკის შექმნა
bin/kafka-topics.sh --create --topic valid_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

